package messagepool

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	stdbig "math/big"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/venus/pkg/config"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	tbig "github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/hashicorp/go-multierror"
	lru "github.com/hashicorp/golang-lru"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/namespace"
	"github.com/ipfs/go-datastore/query"
	logging "github.com/ipfs/go-log/v2"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	lps "github.com/whyrusleeping/pubsub"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/venus/pkg/chain"
	"github.com/filecoin-project/venus/pkg/constants"
	vcrypto "github.com/filecoin-project/venus/pkg/crypto"
	"github.com/filecoin-project/venus/pkg/crypto/sigs"
	"github.com/filecoin-project/venus/pkg/messagepool/journal"
	"github.com/filecoin-project/venus/pkg/net/msgsub"
	"github.com/filecoin-project/venus/pkg/repo"
	"github.com/filecoin-project/venus/pkg/types"
	"github.com/filecoin-project/venus/pkg/vm"
	"github.com/filecoin-project/venus/pkg/vm/gas"

	"github.com/raulk/clock"
)

type MpoolChange int

const (
	MpoolAdd MpoolChange = iota
	MpoolRemove
)

type MpoolUpdate struct {
	Type    MpoolChange
	Message *types.SignedMessage
}

var log = logging.Logger("messagepool")

var futureDebug = false

var rbfNumBig = big.NewInt(int64((ReplaceByFeeRatioDefault - 1) * RbfDenom))
var rbfDenomBig = big.NewInt(RbfDenom)

const RbfDenom = 256

var RepublishInterval = time.Duration(10*constants.MainNetBlockDelaySecs+constants.PropagationDelaySecs) * time.Second

var minimumBaseFee = big.NewInt(int64(constants.MinimumBaseFee))
var baseFeeLowerBoundFactor = big.NewInt(10)
var baseFeeLowerBoundFactorConservative = big.NewInt(100)

var MaxActorPendingMessages = 1000
var MaxUntrustedActorPendingMessages = 10

var MaxNonceGap = uint64(4)

var (
	ErrMessageTooBig = errors.New("message too big")

	ErrMessageValueTooHigh = errors.New("cannot send more filecoin than will ever exist")

	ErrNonceTooLow = errors.New("message nonce too low")

	ErrGasFeeCapTooLow = errors.New("gas fee cap too low")

	ErrNotEnoughFunds = errors.New("not enough funds to execute transaction")

	ErrInvalidToAddr = errors.New("message had invalid to address")

	ErrSoftValidationFailure  = errors.New("validation failure")
	ErrRBFTooLowPremium       = errors.New("replace by fee has too low GasPremium")
	ErrTooManyPendingMessages = errors.New("too many pending messages for actor")
	ErrNonceGap               = errors.New("unfulfilled nonce gap")
)

const (
	localMsgsDs = "/mpool/local"

	localUpdates = "update"
)

// Journal event types.
const (
	evtTypeMpoolAdd = iota
	evtTypeMpoolRemove
	evtTypeMpoolRepub
)

// MessagePoolEvt is the journal entry for message pool events.
type MessagePoolEvt struct { // nolint
	Action   string
	Messages []MessagePoolEvtMessage
	Error    error `json:",omitempty"`
}

type MessagePoolEvtMessage struct { // nolint
	types.UnsignedMessage

	CID cid.Cid
}

func init() {
	// if the republish interval is too short compared to the pubsub timecache, adjust it
	minInterval := pubsub.TimeCacheDuration + time.Duration(constants.PropagationDelaySecs)
	if RepublishInterval < minInterval {
		RepublishInterval = minInterval
	}
}

type gasPredictor interface {
	CallWithGas(context.Context, *types.UnsignedMessage, []types.ChainMsg, *types.TipSet) (*vm.Ret, error)
}

type actorProvider interface {
	// GetActorAt returns the actor state defined by the chain up to some tipset
	GetActorAt(context.Context, *types.TipSet, address.Address) (*types.Actor, error)
}

type MessagePool struct {
	lk sync.Mutex

	ds repo.Datastore

	addSema chan struct{}

	closer chan struct{}

	repubTk      *clock.Ticker
	repubTrigger chan struct{}

	republished map[cid.Cid]struct{}

	localAddrs map[address.Address]struct{}

	pending map[address.Address]*msgSet

	curTsLk sync.Mutex // DO NOT LOCK INSIDE lk
	curTs   *types.TipSet

	cfgLk sync.Mutex
	cfg   *MpoolConfig

	api Provider

	minGasPrice tbig.Int

	currentSize int

	// pruneTrigger is a channel used to trigger a mempool pruning
	pruneTrigger chan struct{}

	// pruneCooldown is a channel used to allow a cooldown time between prunes
	pruneCooldown chan struct{}

	blsSigCache *lru.TwoQueueCache

	changes *lps.PubSub

	localMsgs datastore.Datastore

	netName string

	sigValCache *lru.TwoQueueCache

	evtTypes [3]journal.EventType
	journal  journal.Journal

	gasPriceSchedule *gas.PricesSchedule

	gp        gasPredictor
	ap        actorProvider
	GetMaxFee DefaultMaxFeeFunc

	gcfgLk sync.Mutex
	gcfg   *MpoolGasConfig
}

func newDefaultMaxFeeFunc() DefaultMaxFeeFunc {
	return func() (out abi.TokenAmount, err error) {
		out = abi.TokenAmount{Int: types.DefaultDefaultMaxFee.Int}
		return
	}
}

type msgSet struct {
	msgs          map[uint64]*types.SignedMessage
	nextNonce     uint64
	requiredFunds *stdbig.Int
}

func newMsgSet(nonce uint64) *msgSet {
	return &msgSet{
		msgs:          make(map[uint64]*types.SignedMessage),
		nextNonce:     nonce,
		requiredFunds: stdbig.NewInt(0),
	}
}

func ComputeMinRBF(curPrem abi.TokenAmount) abi.TokenAmount {
	minPrice := tbig.Add(curPrem, tbig.Div(tbig.Mul(curPrem, rbfNumBig), rbfDenomBig))
	return tbig.Add(minPrice, tbig.NewInt(1))
}

func CapGasFee(mff DefaultMaxFeeFunc, msg *types.Message, sendSepc *types.MessageSendSpec) {
	var maxFee abi.TokenAmount
	if sendSepc != nil {
		maxFee = sendSepc.MaxFee
	}
	if maxFee.Int == nil || maxFee.Equals(big.Zero()) {
		mf, err := mff()
		if err != nil {
			log.Errorf("failed to get default max gas fee: %+v", err)
			mf = big.Zero()
		}
		maxFee = mf
	}

	if maxFee.Equals(big.Zero()) {
		mf, err := mff()
		if err != nil {
			log.Errorf("failed to get default max gas fee: %+v", err)
			mf = big.Zero()
		}

		maxFee = mf
	}

	gl := types.NewInt(uint64(msg.GasLimit))
	totalFee := types.BigMul(msg.GasFeeCap, gl)

	if totalFee.LessThanEqual(maxFee) {
		return
	}

	msg.GasFeeCap = big.Div(maxFee, gl)
	msg.GasPremium = big.Min(msg.GasFeeCap, msg.GasPremium) // cap premium at FeeCap
}

func (ms *msgSet) add(m *types.SignedMessage, mp *MessagePool, strict, untrusted bool) (bool, error) {
	nextNonce := ms.nextNonce
	nonceGap := false

	maxNonceGap := MaxNonceGap
	maxActorPendingMessages := MaxActorPendingMessages
	if untrusted {
		maxNonceGap = 0
		maxActorPendingMessages = MaxUntrustedActorPendingMessages
	}

	switch {
	case m.Message.Nonce == nextNonce:
		nextNonce++
		// advance if we are filling a gap
		for _, fillGap := ms.msgs[nextNonce]; fillGap; _, fillGap = ms.msgs[nextNonce] {
			nextNonce++
		}

	case strict && m.Message.Nonce > nextNonce+maxNonceGap:
		return false, xerrors.Errorf("message nonce has too big a gap from expected nonce (Nonce: %d, nextNonce: %d): %v", m.Message.Nonce, nextNonce, ErrNonceGap)

	case m.Message.Nonce > nextNonce:
		nonceGap = true
	}

	exms, has := ms.msgs[m.Message.Nonce]
	if has {
		// refuse RBF if we have a gap
		if strict && nonceGap {
			return false, xerrors.Errorf("rejecting replace by fee because of nonce gap (Nonce: %d, nextNonce: %d): %v", m.Message.Nonce, nextNonce, ErrNonceGap)
		}

		mc := m.Cid()
		exmsc := exms.Cid()
		if mc != exmsc {
			// check if RBF passes
			minPrice := ComputeMinRBF(exms.Message.GasPremium)
			if tbig.Cmp(m.Message.GasPremium, minPrice) >= 0 {
				log.Debugw("add with RBF", "oldpremium", exms.Message.GasPremium,
					"newpremium", m.Message.GasPremium, "addr", m.Message.From, "nonce", m.Message.Nonce)
			} else {
				log.Debugf("add with duplicate nonce. message from %s with nonce %d already in mpool,"+
					" increase GasPremium to %s from %s to trigger replace by fee: %s",
					m.Message.From, m.Message.Nonce, minPrice, m.Message.GasPremium,
					ErrRBFTooLowPremium)
				return false, xerrors.Errorf("message from %s with nonce %d already in mpool,"+
					" increase GasPremium to %s from %s to trigger replace by fee: %v",
					m.Message.From, m.Message.Nonce, minPrice, m.Message.GasPremium,
					ErrRBFTooLowPremium)
			}
		} else {
			return false, xerrors.Errorf("message from %s with nonce %d already in mpool: %v",
				m.Message.From, m.Message.Nonce, ErrSoftValidationFailure)
		}

		ms.requiredFunds.Sub(ms.requiredFunds, exms.Message.RequiredFunds().Int)
		//ms.requiredFunds.Sub(ms.requiredFunds, exms.Message.Value.Int)
	}

	if !has && strict && len(ms.msgs) >= maxActorPendingMessages {
		log.Errorf("too many pending messages from actor %s", m.Message.From)
		return false, ErrTooManyPendingMessages
	}

	if strict && nonceGap {
		log.Debugf("adding nonce-gapped message from %s (nonce: %d, nextNonce: %d)",
			m.Message.From, m.Message.Nonce, nextNonce)
	}

	ms.nextNonce = nextNonce
	ms.msgs[m.Message.Nonce] = m
	ms.requiredFunds.Add(ms.requiredFunds, m.Message.RequiredFunds().Int)
	//ms.requiredFunds.Add(ms.requiredFunds, m.Message.Value.Int)

	return !has, nil
}

func (ms *msgSet) rm(nonce uint64, applied bool) {
	m, has := ms.msgs[nonce]
	if !has {
		if applied && nonce >= ms.nextNonce {
			// we removed a message we did not know about because it was applied
			// we need to adjust the nonce and check if we filled a gap
			ms.nextNonce = nonce + 1
			for _, fillGap := ms.msgs[ms.nextNonce]; fillGap; _, fillGap = ms.msgs[ms.nextNonce] {
				ms.nextNonce++
			}
		}
		return
	}

	ms.requiredFunds.Sub(ms.requiredFunds, m.Message.RequiredFunds().Int)
	//ms.requiredFunds.Sub(ms.requiredFunds, m.Message.Value.Int)
	delete(ms.msgs, nonce)

	// adjust next nonce
	if applied {
		// we removed a (known) message because it was applied in a tipset
		// we can't possibly have filled a gap in this case
		if nonce >= ms.nextNonce {
			ms.nextNonce = nonce + 1
		}
		return
	}

	// we removed a message because it was pruned
	// we have to adjust the nonce if it creates a gap or rewinds state
	if nonce < ms.nextNonce {
		ms.nextNonce = nonce
	}
}

func (ms *msgSet) getRequiredFunds(nonce uint64) tbig.Int {
	requiredFunds := new(stdbig.Int).Set(ms.requiredFunds)

	m, has := ms.msgs[nonce]
	if has {
		requiredFunds.Sub(requiredFunds, m.Message.RequiredFunds().Int)
		//requiredFunds.Sub(requiredFunds, m.Message.Value.Int)
	}

	return tbig.Int{Int: requiredFunds}
}

func New(api Provider,
	ds repo.Datastore,
	forkParams *config.ForkUpgradeConfig,
	netName string,
	gp gasPredictor,
	ap actorProvider,
	j journal.Journal,
) (*MessagePool, error) {
	cache, _ := lru.New2Q(constants.BlsSignatureCacheSize)
	verifcache, _ := lru.New2Q(constants.VerifSigCacheSize)

	cfg, err := loadConfig(ds)
	if err != nil {
		return nil, xerrors.Errorf("error loading mpool config: %v", err)
	}

	gcfg, err := loadGasConfig(ds)
	if err != nil {
		return nil, xerrors.Errorf("error loading mpool gas config: %w", err)
	}

	if j == nil {
		j = journal.NilJournal()
	}

	mp := &MessagePool{
		ds:            ds,
		addSema:       make(chan struct{}, 1),
		closer:        make(chan struct{}),
		repubTk:       constants.Clock.Ticker(RepublishInterval),
		repubTrigger:  make(chan struct{}, 1),
		localAddrs:    make(map[address.Address]struct{}),
		pending:       make(map[address.Address]*msgSet),
		minGasPrice:   tbig.NewInt(0),
		pruneTrigger:  make(chan struct{}, 1),
		pruneCooldown: make(chan struct{}, 1),
		blsSigCache:   cache,
		sigValCache:   verifcache,
		changes:       lps.New(50),
		localMsgs:     namespace.Wrap(ds, datastore.NewKey(localMsgsDs)),
		api:           api,
		netName:       netName,
		gp:            gp,
		ap:            ap,
		cfg:           cfg,
		evtTypes: [...]journal.EventType{
			evtTypeMpoolAdd:    j.RegisterEventType("mpool", "add"),
			evtTypeMpoolRemove: j.RegisterEventType("mpool", "remove"),
			evtTypeMpoolRepub:  j.RegisterEventType("mpool", "repub"),
		},
		gcfg:    gcfg,
		journal: j,

		gasPriceSchedule: gas.NewPricesSchedule(forkParams),
		GetMaxFee:        newDefaultMaxFeeFunc(),
	}

	// enable initial prunes
	mp.pruneCooldown <- struct{}{}

	// load the current tipset and subscribe to head changes _before_ loading local messages
	mp.curTs = api.SubscribeHeadChanges(func(rev, app []*types.TipSet) error {
		err := mp.HeadChange(rev, app)
		if err != nil {
			log.Errorf("mpool head notif handler error: %+v", err)
		}
		return err
	})

	mp.curTsLk.Lock()
	mp.lk.Lock()

	go func() {
		err := mp.loadLocal()

		mp.lk.Unlock()
		mp.curTsLk.Unlock()

		if err != nil {
			log.Errorf("loading local messages: %+v", err)
		}

		log.Info("mpool ready")

		mp.runLoop()
	}()

	return mp, nil
}

func (mp *MessagePool) DeleteByAdress(address address.Address) error {
	mp.lk.Lock()
	defer mp.lk.Unlock()

	if mp.pending != nil {
		mp.pending[address] = nil
	}
	return nil
}

func (mp *MessagePool) PublishMsgForWallet(addr address.Address) error {
	now := time.Now()
	defer func() {
		diff := time.Since(now).Seconds()
		if diff > 1 {
			log.Infof("publish msg wallet spent time:%f", diff)
		}
	}()
	mp.curTsLk.Lock()
	defer mp.curTsLk.Unlock()

	mp.lk.Lock()
	defer mp.lk.Unlock()

	out := make([]*types.SignedMessage, 0)
	for a := range mp.pending {
		if a.String() == addr.String() {
			out = append(out, mp.pendingFor(a)...)
			break
		}
	}

	log.Infof("mpool has [%v] msg for [%s], will republish ...", len(out), addr.String())

	// ??????
	for _, msg := range out {
		msgb, err := msg.Serialize()
		if err != nil {
			log.Errorf("could not serialize: %s", err)
			continue
		}

		err = mp.api.PubSubPublish(msgsub.Topic(mp.netName), msgb)
		if err != nil {
			log.Errorf("could not publish: %s", err)
			continue
		}
	}

	return nil
}

func (mp *MessagePool) Close() error {
	close(mp.closer)
	return mp.journal.Close()
}

func (mp *MessagePool) Prune() {
	// this magic incantation of triggering prune thrice is here to make the Prune method
	// synchronous:
	// so, its a single slot buffered channel. The first send fills the channel,
	// the second send goes through when the pruning starts,
	// and the third send goes through (and noops) after the pruning finishes
	// and goes through the loop again
	mp.pruneTrigger <- struct{}{}
	mp.pruneTrigger <- struct{}{}
	mp.pruneTrigger <- struct{}{}
}

func (mp *MessagePool) runLoop() {
	for {
		select {
		case <-mp.repubTk.C:
			if err := mp.republishPendingMessages(); err != nil {
				log.Errorf("error while republishing messages: %s", err)
			}
		case <-mp.repubTrigger:
			if err := mp.republishPendingMessages(); err != nil {
				log.Errorf("error while republishing messages: %s", err)
			}

		case <-mp.pruneTrigger:
			if err := mp.pruneExcessMessages(); err != nil {
				log.Errorf("failed to prune excess messages from mempool: %s", err)
			}

		case <-mp.closer:
			mp.repubTk.Stop()
			return
		}
	}
}

func (mp *MessagePool) addLocal(m *types.SignedMessage) error {
	mp.localAddrs[m.Message.From] = struct{}{}

	buf := new(bytes.Buffer)
	err := m.MarshalCBOR(buf)
	if err != nil {
		return xerrors.Errorf("error serializing message: %v", err)
	}
	msgb := buf.Bytes()

	c := m.Cid()
	if err := mp.localMsgs.Put(datastore.NewKey(string(c.Bytes())), msgb); err != nil {
		return xerrors.Errorf("persisting local message: %v", err)
	}

	return nil
}

// verifyMsgBeforeAdd verifies that the message meets the minimum criteria for block inclusio
// and whether the message has enough funds to be included in the next 20 blocks.
// If the message is not valid for block inclusion, it returns an error.
// For local messages, if the message can be included in the next 20 blocks, it returns true to
// signal that it should be immediately published. If the message cannot be included in the next 20
// blocks, it returns false so that the message doesn't immediately get published (and ignored by our
// peers); instead it will be published through the republish loop, once the base fee has fallen
// sufficiently.
// For non local messages, if the message cannot be included in the next 20 blocks it returns
// a (soft) validation error.
func (mp *MessagePool) verifyMsgBeforeAdd(m *types.SignedMessage, curTs *types.TipSet, local bool) (bool, error) {
	epoch := curTs.Height()
	minGas := mp.gasPriceSchedule.PricelistByEpoch(epoch).OnChainMessage(m.ChainLength())

	if err := m.VMMessage().ValidForBlockInclusion(minGas.Total(), constants.NewestNetworkVersion); err != nil {
		return false, xerrors.Errorf("message will not be included in a block: %v", err)
	}

	// this checks if the GasFeeCap is suffisciently high for inclusion in the next 20 blocks
	// if the GasFeeCap is too low, we soft reject the message (Ignore in pubsub) and rely
	// on republish to push it through later, if the baseFee has fallen.
	// this is a defensive check that stops minimum baseFee spam attacks from overloading validation
	// queues.
	// Note that for local messages, we always add them so that they can be accepted and republished
	// automatically.
	publish := local

	var baseFee big.Int
	if len(curTs.Blocks()) > 0 {
		baseFee = curTs.Blocks()[0].ParentBaseFee
	} else {
		var err error
		baseFee, err = mp.api.ChainComputeBaseFee(context.TODO(), curTs)
		if err != nil {
			return false, xerrors.Errorf("computing basefee: %v", err)
		}
	}

	baseFeeLowerBound := getBaseFeeLowerBound(baseFee, baseFeeLowerBoundFactorConservative)
	if m.Message.GasFeeCap.LessThan(baseFeeLowerBound) {
		if local {
			log.Warnf("local message will not be immediately published because GasFeeCap doesn't meet the lower bound for inclusion in the next 20 blocks (GasFeeCap: %s, baseFeeLowerBound: %s)",
				m.Message.GasFeeCap, baseFeeLowerBound)
			publish = false
		} else {
			return false, xerrors.Errorf("GasFeeCap doesn't meet base fee lower bound for inclusion in the next 20 blocks (GasFeeCap: %s, baseFeeLowerBound: %s): %v",
				m.Message.GasFeeCap, baseFeeLowerBound, ErrSoftValidationFailure)
		}
	}

	return publish, nil
}

func (mp *MessagePool) Push(m *types.SignedMessage) (cid.Cid, error) {
	err := mp.checkMessage(m)
	if err != nil {
		return cid.Undef, err
	}

	// serialize push access to reduce lock contention
	mp.addSema <- struct{}{}
	defer func() {
		<-mp.addSema
	}()

	mp.curTsLk.Lock()
	publish, err := mp.addTs(m, mp.curTs, true, false)
	if err != nil {
		mp.curTsLk.Unlock()
		return cid.Undef, err
	}
	mp.curTsLk.Unlock()

	if publish {
		buf := new(bytes.Buffer)
		err := m.MarshalCBOR(buf)
		if err != nil {
			return cid.Undef, xerrors.Errorf("error serializing message: %v", err)
		}

		err = mp.api.PubSubPublish(msgsub.Topic(mp.netName), buf.Bytes())
		if err != nil {
			return cid.Undef, xerrors.Errorf("error publishing message: %v", err)
		}
	}

	return m.Cid(), nil
}

func (mp *MessagePool) checkMessage(m *types.SignedMessage) error {
	// big messages are bad, anti DOS
	if m.ChainLength() > 32*1024 {
		return xerrors.Errorf("mpool message too large (%dB): %v", m.ChainLength(), ErrMessageTooBig)
	}

	// Perform syntactic validation, minGas=0 as we check the actual mingas before we add it
	if err := m.Message.ValidForBlockInclusion(0, constants.NewestNetworkVersion); err != nil {
		return xerrors.Errorf("message not valid for block inclusion: %v", err)
	}

	if m.Message.To == address.Undef {
		return ErrInvalidToAddr
	}

	if !m.Message.Value.LessThan(vcrypto.TotalFilecoinInt) {
		return ErrMessageValueTooHigh
	}

	if m.Message.GasFeeCap.LessThan(minimumBaseFee) {
		return ErrGasFeeCapTooLow
	}

	if err := mp.VerifyMsgSig(m); err != nil {
		log.Warnf("signature verification failed: %s", err)
		return err
	}

	return nil
}

func (mp *MessagePool) Add(m *types.SignedMessage) error {
	err := mp.checkMessage(m)
	if err != nil {
		return err
	}

	// serialize push access to reduce lock contention
	mp.addSema <- struct{}{}
	defer func() {
		<-mp.addSema
	}()

	mp.curTsLk.Lock()
	defer mp.curTsLk.Unlock()

	_, err = mp.addTs(m, mp.curTs, false, false)
	return err
}

func sigCacheKey(m *types.SignedMessage) (string, error) {
	c := m.Cid()
	switch m.Signature.Type {
	case crypto.SigTypeBLS:
		if len(m.Signature.Data) < 90 {
			return "", fmt.Errorf("bls signature too short")
		}

		return string(c.Bytes()) + string(m.Signature.Data[64:]), nil
	case crypto.SigTypeSecp256k1:
		return string(c.Bytes()), nil
	default:
		return "", xerrors.Errorf("unrecognized signature type: %d", m.Signature.Type)
	}
}

func (mp *MessagePool) VerifyMsgSig(m *types.SignedMessage) error {
	sck, err := sigCacheKey(m)
	if err != nil {
		return err
	}

	_, ok := mp.sigValCache.Get(sck)
	if ok {
		// already validated, great
		return nil
	}

	c := m.Message.Cid()
	if err := sigs.Verify(&m.Signature, m.Message.From, c.Bytes()); err != nil {
		return err
	}

	mp.sigValCache.Add(sck, struct{}{})

	return nil
}

func (mp *MessagePool) checkBalance(m *types.SignedMessage, curTs *types.TipSet) error {
	balance, err := mp.getStateBalance(m.Message.From, curTs)
	if err != nil {
		return xerrors.Errorf("failed to check sender balance: %s: %v", err, ErrSoftValidationFailure)
	}

	requiredFunds := m.Message.RequiredFunds()
	if tbig.Cmp(balance, requiredFunds) < 0 {
		return xerrors.Errorf("not enough funds (required: %s, balance: %s): %v", types.FIL(requiredFunds), types.FIL(balance), ErrNotEnoughFunds)
	}

	// add Value for soft failure check
	//requiredFunds = types.BigAdd(requiredFunds, m.Message.Value)

	mset, ok := mp.pending[m.Message.From]
	if ok {
		requiredFunds = tbig.Add(requiredFunds, mset.getRequiredFunds(m.Message.Nonce))
	}

	if tbig.Cmp(balance, requiredFunds) < 0 {
		// Note: we fail here for ErrSoftValidationFailure to signal a soft failure because we might
		// be out of sync.
		return xerrors.Errorf("not enough funds including pending messages (required: %s, balance: %s): %v", types.FIL(requiredFunds), types.FIL(balance), ErrSoftValidationFailure)
	}

	return nil
}

func (mp *MessagePool) addTs(m *types.SignedMessage, curTs *types.TipSet, local, untrusted bool) (bool, error) {
	snonce, err := mp.getStateNonce(m.Message.From, curTs)
	if err != nil {
		return false, xerrors.Errorf("failed to look up actor state nonce: %s: %v", err, ErrSoftValidationFailure)
	}

	if snonce > m.Message.Nonce {
		return false, xerrors.Errorf("minimum expected nonce is %d: %v", snonce, ErrNonceTooLow)
	}

	mp.lk.Lock()
	defer mp.lk.Unlock()

	publish, err := mp.verifyMsgBeforeAdd(m, curTs, local)
	if err != nil {
		return false, err
	}

	if err := mp.checkBalance(m, curTs); err != nil {
		return false, err
	}

	err = mp.addLocked(m, !local, untrusted)
	if err != nil {
		return false, err
	}

	if local {
		err = mp.addLocal(m)
		if err != nil {
			return false, xerrors.Errorf("error persisting local message: %v", err)
		}
	}

	return publish, nil
}

func (mp *MessagePool) addLoaded(m *types.SignedMessage) error {
	err := mp.checkMessage(m)
	if err != nil {
		return err
	}

	curTs := mp.curTs

	if curTs == nil {
		return xerrors.Errorf("current tipset not loaded")
	}

	snonce, err := mp.getStateNonce(m.Message.From, curTs)
	if err != nil {
		return xerrors.Errorf("failed to look up actor state nonce: %s: %v", err, ErrSoftValidationFailure)
	}

	if snonce > m.Message.Nonce {
		return xerrors.Errorf("minimum expected nonce is %d: %w", snonce, ErrNonceTooLow)
	}

	_, err = mp.verifyMsgBeforeAdd(m, curTs, true)
	if err != nil {
		return err
	}

	if err := mp.checkBalance(m, curTs); err != nil {
		return err
	}

	return mp.addLocked(m, false, false)
}

func (mp *MessagePool) addSkipChecks(m *types.SignedMessage) error {
	mp.lk.Lock()
	defer mp.lk.Unlock()

	return mp.addLocked(m, false, false)
}

func (mp *MessagePool) addLocked(m *types.SignedMessage, strict, untrusted bool) error {
	log.Debugf("mpooladd: %s %d", m.Message.From, m.Message.Nonce)
	if m.Signature.Type == crypto.SigTypeBLS {
		mp.blsSigCache.Add(m.Cid(), m.Signature)
	}

	if _, err := mp.api.PutMessage(m); err != nil {
		log.Warnf("mpooladd cs.PutMessage failed: %s", err)
		return err
	}

	if _, err := mp.api.PutMessage(&m.Message); err != nil {
		log.Warnf("mpooladd cs.PutMessage failed: %s", err)
		return err
	}

	mset, ok := mp.pending[m.Message.From]
	if !ok {
		nonce, err := mp.getStateNonce(m.Message.From, mp.curTs)
		if err != nil {
			return xerrors.Errorf("failed to get initial actor nonce: %v", err)
		}

		mset = newMsgSet(nonce)
		mp.pending[m.Message.From] = mset
	}

	incr, err := mset.add(m, mp, strict, untrusted)
	if err != nil {
		log.Debug(err)
		return err
	}

	if incr {
		mp.currentSize++
		if mp.currentSize > mp.cfg.SizeLimitHigh {
			// send signal to prune messages if it hasnt already been sent
			select {
			case mp.pruneTrigger <- struct{}{}:
			default:
			}
		}
	}

	mp.changes.Pub(MpoolUpdate{
		Type:    MpoolAdd,
		Message: m,
	}, localUpdates)

	mp.journal.RecordEvent(mp.evtTypes[evtTypeMpoolAdd], func() interface{} {
		mc := m.Cid()
		return MessagePoolEvt{
			Action:   "add",
			Messages: []MessagePoolEvtMessage{{UnsignedMessage: m.Message, CID: mc}},
		}
	})

	return nil
}

func (mp *MessagePool) GetNonce(addr address.Address) (uint64, error) {
	mp.curTsLk.Lock()
	defer mp.curTsLk.Unlock()

	mp.lk.Lock()
	defer mp.lk.Unlock()

	return mp.getNonceLocked(addr, mp.curTs)
}

func (mp *MessagePool) getNonceLocked(addr address.Address, curTs *types.TipSet) (uint64, error) {
	stateNonce, err := mp.getStateNonce(addr, curTs) // sanity check
	if err != nil {
		return 0, err
	}

	mset, ok := mp.pending[addr]
	if ok {
		if stateNonce > mset.nextNonce {
			log.Errorf("state nonce was larger than mset.nextNonce (%d > %d)", stateNonce, mset.nextNonce)

			return stateNonce, nil
		}

		return mset.nextNonce, nil
	}

	return stateNonce, nil
}

func (mp *MessagePool) getStateNonce(addr address.Address, curTs *types.TipSet) (uint64, error) {
	act, err := mp.api.GetActorAfter(addr, curTs)
	if err != nil {
		return 0, err
	}

	return act.Nonce, nil
}

func (mp *MessagePool) getStateBalance(addr address.Address, ts *types.TipSet) (tbig.Int, error) {
	act, err := mp.api.GetActorAfter(addr, ts)
	if err != nil {
		return tbig.Zero(), err
	}

	return act.Balance, nil
}

// this method is provided for the gateway to push messages.
// differences from Push:
//  - strict checks are enabled
//  - extra strict add checks are used when adding the messages to the msgSet
//    that means: no nonce gaps, at most 10 pending messages for the actor
func (mp *MessagePool) PushUntrusted(m *types.SignedMessage) (cid.Cid, error) {
	err := mp.checkMessage(m)
	if err != nil {
		return cid.Undef, err
	}

	// serialize push access to reduce lock contention
	mp.addSema <- struct{}{}
	defer func() {
		<-mp.addSema
	}()

	mp.curTsLk.Lock()
	publish, err := mp.addTs(m, mp.curTs, false, true)
	if err != nil {
		mp.curTsLk.Unlock()
		return cid.Undef, err
	}
	mp.curTsLk.Unlock()

	if publish {
		buf := new(bytes.Buffer)
		err := m.MarshalCBOR(buf)
		if err != nil {
			return cid.Undef, xerrors.Errorf("error serializing message: %v", err)
		}

		err = mp.api.PubSubPublish(msgsub.Topic(mp.netName), buf.Bytes())
		if err != nil {
			return cid.Undef, xerrors.Errorf("error publishing message: %v", err)
		}
	}

	return m.Cid(), nil
}

func (mp *MessagePool) Remove(from address.Address, nonce uint64, applied bool) {
	mp.lk.Lock()
	defer mp.lk.Unlock()

	mp.remove(from, nonce, applied)
}

func (mp *MessagePool) remove(from address.Address, nonce uint64, applied bool) {
	mset, ok := mp.pending[from]
	if !ok {
		return
	}

	if m, ok := mset.msgs[nonce]; ok {
		mp.changes.Pub(MpoolUpdate{
			Type:    MpoolRemove,
			Message: m,
		}, localUpdates)

		mp.journal.RecordEvent(mp.evtTypes[evtTypeMpoolRemove], func() interface{} {
			return MessagePoolEvt{
				Action:   "remove",
				Messages: []MessagePoolEvtMessage{{UnsignedMessage: m.Message, CID: m.Cid()}}}
		})

		mp.currentSize--
	}

	// NB: This deletes any message with the given nonce. This makes sense
	// as two messages with the same sender cannot have the same nonce
	mset.rm(nonce, applied)

	if len(mset.msgs) == 0 {
		delete(mp.pending, from)
	}
}

func (mp *MessagePool) Pending() ([]*types.SignedMessage, *types.TipSet) {
	mp.curTsLk.Lock()
	defer mp.curTsLk.Unlock()

	mp.lk.Lock()
	defer mp.lk.Unlock()

	return mp.allPending()
}

func (mp *MessagePool) allPending() ([]*types.SignedMessage, *types.TipSet) {
	out := make([]*types.SignedMessage, 0)
	for a := range mp.pending {
		out = append(out, mp.pendingFor(a)...)
	}

	return out, mp.curTs
}

func (mp *MessagePool) PendingFor(a address.Address) ([]*types.SignedMessage, *types.TipSet) {
	mp.curTsLk.Lock()
	defer mp.curTsLk.Unlock()

	mp.lk.Lock()
	defer mp.lk.Unlock()
	return mp.pendingFor(a), mp.curTs
}

func (mp *MessagePool) pendingFor(a address.Address) []*types.SignedMessage {
	mset := mp.pending[a]
	if mset == nil || len(mset.msgs) == 0 {
		return nil
	}

	set := make([]*types.SignedMessage, 0, len(mset.msgs))

	for _, m := range mset.msgs {
		set = append(set, m)
	}

	sort.Slice(set, func(i, j int) bool {
		return set[i].Message.Nonce < set[j].Message.Nonce
	})

	return set
}

func (mp *MessagePool) HeadChange(revert []*types.TipSet, apply []*types.TipSet) error {
	mp.curTsLk.Lock()
	defer mp.curTsLk.Unlock()

	repubTrigger := false
	rmsgs := make(map[address.Address]map[uint64]*types.SignedMessage)
	add := func(m *types.SignedMessage) {
		s, ok := rmsgs[m.Message.From]
		if !ok {
			s = make(map[uint64]*types.SignedMessage)
			rmsgs[m.Message.From] = s
		}
		s[m.Message.Nonce] = m
	}
	rm := func(from address.Address, nonce uint64) {
		s, ok := rmsgs[from]
		if !ok {
			mp.Remove(from, nonce, true)
			return
		}

		if _, ok := s[nonce]; ok {
			delete(s, nonce)
			return
		}

		mp.Remove(from, nonce, true)
	}

	maybeRepub := func(cid cid.Cid) {
		if !repubTrigger {
			mp.lk.Lock()
			_, republished := mp.republished[cid]
			mp.lk.Unlock()
			if republished {
				repubTrigger = true
			}
		}
	}

	var merr error

	for _, ts := range revert {
		tsKey := ts.Parents()
		pts, err := mp.api.LoadTipSet(tsKey)
		if err != nil {
			log.Errorf("error loading reverted tipset parent: %s", err)
			merr = multierror.Append(merr, err)
			continue
		}

		mp.curTs = pts

		msgs, err := mp.MessagesForBlocks(ts.Blocks())
		if err != nil {
			log.Errorf("error retrieving messages for reverted block: %s", err)
			merr = multierror.Append(merr, err)
			continue
		}

		for _, msg := range msgs {
			add(msg)
		}
	}

	for _, ts := range apply {
		mp.curTs = ts

		for _, b := range ts.Blocks() {
			bmsgs, smsgs, err := mp.api.MessagesForBlock(b)
			if err != nil {
				xerr := xerrors.Errorf("failed to get messages for apply block %s(height %d) (msgroot = %s): %v", b.Cid(), b.Height, b.Messages, err)
				log.Errorf("error retrieving messages for block: %s", xerr)
				merr = multierror.Append(merr, xerr)
				continue
			}

			for _, msg := range smsgs {
				rm(msg.Message.From, msg.Message.Nonce)
				maybeRepub(msg.Cid())
			}

			for _, msg := range bmsgs {
				rm(msg.From, msg.Nonce)
				maybeRepub(msg.Cid())
			}
		}
	}

	if repubTrigger {
		select {
		case mp.repubTrigger <- struct{}{}:
		default:
		}
	}

	for _, s := range rmsgs {
		for _, msg := range s {
			if err := mp.addSkipChecks(msg); err != nil {
				log.Errorf("Failed to readd message from reorg to mpool: %s", err)
			}
		}
	}

	if len(revert) > 0 && futureDebug {
		mp.lk.Lock()
		msgs, ts := mp.allPending()
		mp.lk.Unlock()

		buckets := map[address.Address]*statBucket{}

		for _, v := range msgs {
			bkt, ok := buckets[v.Message.From]
			if !ok {
				bkt = &statBucket{
					msgs: map[uint64]*types.SignedMessage{},
				}
				buckets[v.Message.From] = bkt
			}

			bkt.msgs[v.Message.Nonce] = v
		}

		for a, bkt := range buckets {
			// TODO that might not be correct with GatActorAfter but it is only debug code
			act, err := mp.api.GetActorAfter(a, ts)
			if err != nil {
				log.Debugf("%s, err: %s\n", a, err)
				continue
			}

			var cmsg *types.SignedMessage
			var ok bool

			cur := act.Nonce
			for {
				cmsg, ok = bkt.msgs[cur]
				if !ok {
					break
				}
				cur++
			}

			ff := uint64(math.MaxUint64)
			for k := range bkt.msgs {
				if k > cur && k < ff {
					ff = k
				}
			}

			if ff != math.MaxUint64 {
				m := bkt.msgs[ff]
				mc := m.Cid()

				// cmsg can be nil if no messages from the current nonce are in the mpool
				ccid := "nil"
				if cmsg != nil {
					ccid = cmsg.Cid().String()
				}

				log.Debugw("Nonce gap",
					"actor", a,
					"future_cid", mc,
					"future_nonce", ff,
					"current_cid", ccid,
					"current_nonce", cur,
					"revert_tipset", revert[0].Key(),
					"new_head", ts.Key(),
				)
			}
		}
	}

	return merr
}

func (mp *MessagePool) runHeadChange(from *types.TipSet, to *types.TipSet, rmsgs map[address.Address]map[uint64]*types.SignedMessage) error {
	add := func(m *types.SignedMessage) {
		s, ok := rmsgs[m.Message.From]
		if !ok {
			s = make(map[uint64]*types.SignedMessage)
			rmsgs[m.Message.From] = s
		}
		s[m.Message.Nonce] = m
	}
	rm := func(from address.Address, nonce uint64) {
		s, ok := rmsgs[from]
		if !ok {
			return
		}

		if _, ok := s[nonce]; ok {
			delete(s, nonce)
			return
		}

	}

	revert, apply, err := chain.ReorgOps(mp.api.LoadTipSet, from, to)
	if err != nil {
		return xerrors.Errorf("failed to compute reorg ops for mpool pending messages: %v", err)
	}

	var merr error

	for _, ts := range revert {
		msgs, err := mp.MessagesForBlocks(ts.Blocks())
		if err != nil {
			log.Errorf("error retrieving messages for reverted block: %s", err)
			merr = multierror.Append(merr, err)
			continue
		}

		for _, msg := range msgs {
			add(msg)
		}
	}

	for _, ts := range apply {
		for _, b := range ts.Blocks() {
			bmsgs, smsgs, err := mp.api.MessagesForBlock(b)
			if err != nil {
				xerr := xerrors.Errorf("failed to get messages for apply block %s(height %d) (msgroot = %s): %v", b.Cid(), b.Height, b.Messages, err)
				log.Errorf("error retrieving messages for block: %s", xerr)
				merr = multierror.Append(merr, xerr)
				continue
			}

			for _, msg := range smsgs {
				rm(msg.Message.From, msg.Message.Nonce)
			}

			for _, msg := range bmsgs {
				rm(msg.From, msg.Nonce)
			}
		}
	}

	return merr
}

type statBucket struct {
	msgs map[uint64]*types.SignedMessage
}

func (mp *MessagePool) MessagesForBlocks(blks []*types.BlockHeader) ([]*types.SignedMessage, error) {
	out := make([]*types.SignedMessage, 0)

	for _, b := range blks {
		bmsgs, smsgs, err := mp.api.MessagesForBlock(b)
		if err != nil {
			return nil, xerrors.Errorf("failed to get messages for apply block %s(height %d) (msgroot = %s): %v", b.Cid(), b.Height, b.Messages, err)
		}
		out = append(out, smsgs...)

		for _, msg := range bmsgs {
			smsg := mp.RecoverSig(msg)
			if smsg != nil {
				out = append(out, smsg)
			} else {
				log.Debugf("could not recover signature for bls message %s", msg.Cid())
			}
		}
	}

	return out, nil
}

func (mp *MessagePool) RecoverSig(msg *types.UnsignedMessage) *types.SignedMessage {
	val, ok := mp.blsSigCache.Get(msg.Cid())
	if !ok {
		return nil
	}
	sig, ok := val.(crypto.Signature)
	if !ok {
		log.Errorf("value in signature cache was not a signature (got %T)", val)
		return nil
	}

	return &types.SignedMessage{
		Message:   *msg,
		Signature: sig,
	}
}

func (mp *MessagePool) Updates(ctx context.Context) (<-chan MpoolUpdate, error) {
	out := make(chan MpoolUpdate, 20)
	sub := mp.changes.Sub(localUpdates)

	go func() {
		defer mp.changes.Unsub(sub, localUpdates)
		defer close(out)

		for {
			select {
			case u := <-sub:
				select {
				case out <- u.(MpoolUpdate):
				case <-ctx.Done():
					return
				case <-mp.closer:
					return
				}
			case <-ctx.Done():
				return
			case <-mp.closer:
				return
			}
		}
	}()

	return out, nil
}

func (mp *MessagePool) loadLocal() error {
	res, err := mp.localMsgs.Query(query.Query{})
	if err != nil {
		return xerrors.Errorf("query local messages: %v", err)
	}

	for r := range res.Next() {
		if r.Error != nil {
			return xerrors.Errorf("r.Error: %v", r.Error)
		}

		var sm types.SignedMessage
		if err := sm.UnmarshalCBOR(bytes.NewReader(r.Value)); err != nil {
			return xerrors.Errorf("unmarshaling local message: %v", err)
		}

		if err := mp.addLoaded(&sm); err != nil {
			if xerrors.Is(err, ErrNonceTooLow) {
				continue // todo: drop the message from local cache (if above certain confidence threshold)
			}

			log.Errorf("adding local message: %+v", err)
		}

		mp.localAddrs[sm.Message.From] = struct{}{}
	}

	return nil
}

func (mp *MessagePool) Clear(local bool) {
	mp.lk.Lock()
	defer mp.lk.Unlock()

	// remove everything if local is true, including removing local messages from
	// the datastore
	if local {
		for a := range mp.localAddrs {
			mset, ok := mp.pending[a]
			if !ok {
				continue
			}

			for _, m := range mset.msgs {
				err := mp.localMsgs.Delete(datastore.NewKey(string(m.Cid().Bytes())))
				if err != nil {
					log.Warnf("error deleting local message: %s", err)
				}
			}
		}

		mp.pending = make(map[address.Address]*msgSet)
		mp.republished = nil

		return
	}

	// remove everything except the local messages
	for a := range mp.pending {
		_, isLocal := mp.localAddrs[a]
		if isLocal {
			continue
		}
		delete(mp.pending, a)
	}
}

func getBaseFeeLowerBound(baseFee, factor tbig.Int) tbig.Int {
	baseFeeLowerBound := tbig.Div(baseFee, factor)
	if tbig.Cmp(baseFeeLowerBound, minimumBaseFee) < 0 {
		baseFeeLowerBound = minimumBaseFee
	}

	return baseFeeLowerBound
}
