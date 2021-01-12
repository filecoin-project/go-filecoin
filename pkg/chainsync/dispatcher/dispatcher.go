package dispatcher

import (
	"container/list"
	"context"
	types2 "github.com/filecoin-project/venus/pkg/types"
	"runtime/debug"
	"sync"
	"time"

	"github.com/filecoin-project/venus/pkg/chainsync/types"
	"github.com/streadway/handy/atomic"

	logging "github.com/ipfs/go-log/v2"
)

var log = logging.Logger("chainsync.dispatcher")

// DefaultInQueueSize is the bucketSize of the channel used for receiving targets from producers.
const DefaultInQueueSize = 5

// DefaultWorkQueueSize is the bucketSize of the work queue
const DefaultWorkQueueSize = 15

// dispatchSyncer is the interface of the logic syncing incoming chains
type dispatchSyncer interface {
	Head() *types2.TipSet
	HandleNewTipSet(context.Context, *types.Target) error
}

// NewDispatcher creates a new syncing dispatcher with default queue sizes.
func NewDispatcher(catchupSyncer dispatchSyncer) *Dispatcher {
	return NewDispatcherWithSizes(catchupSyncer, DefaultWorkQueueSize, DefaultInQueueSize)
}

// NewDispatcherWithSizes creates a new syncing dispatcher.
func NewDispatcherWithSizes(syncer dispatchSyncer, workQueueSize, inQueueSize int) *Dispatcher {
	return &Dispatcher{
		workTracker:     types.NewTargetTracker(workQueueSize),
		syncer:          syncer,
		incoming:        make(chan *types.Target, inQueueSize),
		control:         make(chan interface{}, 1),
		registeredCb:    func(t *types.Target, err error) {},
		cancelControler: list.New(),
		maxCount:        3,
	}
}

// cbMessage registers a user callback to be fired following every successful
// sync.
type cbMessage struct {
	cb func(*types.Target, error)
}

// Dispatcher receives, sorts and dispatches targets to the catchupSyncer to control
// chain syncing.
//
// New targets arrive over the incoming channel. The dispatcher then puts them
// into the workTracker which sorts them by their claimed chain height. The
// dispatcher pops the highest priority target from the queue and then attempts
// to sync the target using its internal catchupSyncer.
//
// The dispatcher has a simple control channel. It reads this for external
// controls. Currently there is only one kind of control message.  It registers
// a callback that the dispatcher will call after every non-erroring sync.
type Dispatcher struct {
	// workTracker is a priority queue of target chain heads that should be
	// synced
	workTracker *types.TargetTracker
	// incoming is the queue of incoming sync targets to the dispatcher.
	incoming chan *types.Target
	// syncer is used for dispatching sync targets for chain heads to sync
	// local chain state to these targets.
	syncer dispatchSyncer

	// registeredCb is a callback registered over the control channel.  It
	// is called after every successful sync.
	registeredCb func(*types.Target, error)
	// control is a queue of control messages not yet processed.
	control chan interface{}

	cancelControler *list.List
	lk              sync.Mutex
	conCurrent      atomic.Int
	maxCount        int64
}

// SendOwnBlock handles chain info from a node's own mining system
func (d *Dispatcher) SyncTracker() *types.TargetTracker {
	return d.workTracker
}

// SendHello handles chain information from bootstrap peers.
func (d *Dispatcher) SendHello(ci *types2.ChainInfo) error {
	return d.addTracker(ci)
}

// SendOwnBlock handles chain info from a node's own mining system
func (d *Dispatcher) SendOwnBlock(ci *types2.ChainInfo) error {
	return d.addTracker(ci)
}

// SendGossipBlock handles chain info from new blocks sent on pubsub
func (d *Dispatcher) SendGossipBlock(ci *types2.ChainInfo) error {
	return d.addTracker(ci)
}

func (d *Dispatcher) addTracker(ci *types2.ChainInfo) error {
	d.incoming <- &types.Target{
		ChainInfo: *ci,
		Base:      d.syncer.Head(),
		Start:     time.Now(),
	}
	return nil
}

// Start launches the business logic for the syncing subsystem.
func (d *Dispatcher) Start(syncingCtx context.Context) {
	go d.processIncoming(syncingCtx)

	go d.syncWorker(syncingCtx)
}

func (d *Dispatcher) processIncoming(ctx context.Context) {
	defer func() {
		log.Info("exiting sync dispatcher")
		if r := recover(); r != nil {
			log.Errorf("panic: %v", r)
			debug.PrintStack()
		}
	}()

	for {
		// Handle shutdown
		select {
		case <-ctx.Done():
			log.Info("context done")
			return
		case ctrl := <-d.control:
			log.Infof("processing control: %v", ctrl)
			d.processCtrl(ctrl)
		case target := <-d.incoming:
			// Sort new targets by putting on work queue.
			if d.workTracker.Add(target) {
				log.Infof("received height %d Blocks: %d  %s current work len %d  incoming len: %d",
					target.Head.Height(), target.Head.Len(), target.Head.Key(), d.workTracker.Len(), len(d.incoming))
			}
		}
	}
}

func (d *Dispatcher) SetConcurrent(number int64) {
	d.lk.Lock()
	defer d.lk.Unlock()
	d.maxCount = number
	diff := d.conCurrent.Get() - d.maxCount
	if diff > 0 {
		ele := d.cancelControler.Back()
		for ele != nil && diff > 0 {
			ele.Value.(context.CancelFunc)()
			preEle := ele.Prev()
			d.cancelControler.Remove(ele)
			ele = preEle
			diff--
		}
	}
}

func (d *Dispatcher) syncWorker(ctx context.Context) {
	ticker := time.NewTicker(time.Millisecond * 500)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			for { //avoid to sleep 25 millisecond
				syncTarget, popped := d.workTracker.Select()
				if popped {
					// Do work
					d.lk.Lock()
					if d.conCurrent.Get() < d.maxCount {
						syncTarget.State = types.StateInSyncing
						ctx, cancel := context.WithCancel(ctx)
						d.cancelControler.PushBack(cancel)
						d.conCurrent.Add(1)

						go func() {
							err := d.syncer.HandleNewTipSet(ctx, syncTarget)
							d.workTracker.Remove(syncTarget)
							if err != nil {
								log.Infof("failed sync of %v at %d  %s", syncTarget.Head.Key(), syncTarget.Head.Height(), err)
							}
							d.registeredCb(syncTarget, err)
							d.conCurrent.Add(-1)
						}()
					}
					d.lk.Unlock()
				} else {
					break
				}
			}

		case <-ctx.Done():
			log.Info("context done")
			return
		}
	}
}

// RegisterCallback registers a callback on the dispatcher that
// will fire after every successful target sync.
func (d *Dispatcher) RegisterCallback(cb func(*types.Target, error)) {
	d.control <- cbMessage{cb: cb}
}

func (d *Dispatcher) processCtrl(ctrlMsg interface{}) {
	// processCtrl takes a control message, determines its type, and performs the
	// specified action.
	//
	// Using interfaces is overkill for now but is the way to make this
	// extensible.  (Delete this comment if we add more than one control)
	switch typedMsg := ctrlMsg.(type) {
	case cbMessage:
		d.registeredCb = typedMsg.cb
	default:
		// We don't know this type, log and ignore
		log.Info("dispatcher control can not handle type %T", typedMsg)
	}
}
