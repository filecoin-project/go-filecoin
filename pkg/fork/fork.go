package fork

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/network"
	"github.com/filecoin-project/go-state-types/rt"
	ipfsblock "github.com/ipfs/go-block-format"
	"github.com/ipfs/go-cid"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
	cbor "github.com/ipfs/go-ipld-cbor"
	logging "github.com/ipfs/go-log/v2"
	mh "github.com/multiformats/go-multihash"
	xerrors "github.com/pkg/errors"
	cbg "github.com/whyrusleeping/cbor-gen"
	"go.opencensus.io/trace"

	"github.com/filecoin-project/specs-actors/actors/migration/nv3"
	"github.com/filecoin-project/specs-actors/v2/actors/migration/nv4"
	"github.com/filecoin-project/specs-actors/v2/actors/migration/nv7"
	"github.com/filecoin-project/specs-actors/v3/actors/migration/nv10"

	builtin0 "github.com/filecoin-project/specs-actors/actors/builtin"
	miner0 "github.com/filecoin-project/specs-actors/actors/builtin/miner"
	multisig0 "github.com/filecoin-project/specs-actors/actors/builtin/multisig"
	power0 "github.com/filecoin-project/specs-actors/actors/builtin/power"
	adt0 "github.com/filecoin-project/specs-actors/actors/util/adt"

	"github.com/filecoin-project/venus/pkg/chain"
	"github.com/filecoin-project/venus/pkg/config"
	"github.com/filecoin-project/venus/pkg/constants"
	"github.com/filecoin-project/venus/pkg/specactors/adt"
	"github.com/filecoin-project/venus/pkg/specactors/builtin"
	init_ "github.com/filecoin-project/venus/pkg/specactors/builtin/init"
	"github.com/filecoin-project/venus/pkg/specactors/builtin/multisig"
	vmstate "github.com/filecoin-project/venus/pkg/state/tree"
	"github.com/filecoin-project/venus/pkg/types"
	"github.com/filecoin-project/venus/pkg/util/blockstoreutil"
)

var log = logging.Logger("fork")

var ErrExpensiveFork = errors.New("refusing explicit call due to state fork at epoch")

// MigrationCache can be used to cache information used by a migration. This is primarily useful to
// "pre-compute" some migration state ahead of time, and make it accessible in the migration itself.
type MigrationCache interface {
	Write(key string, value cid.Cid) error
	Read(key string) (bool, cid.Cid, error)
	Load(key string, loadFunc func() (cid.Cid, error)) (cid.Cid, error)
}

// MigrationFunc is a migration function run at every upgrade.
//
// - The cache is a per-upgrade cache, pre-populated by pre-migrations.
// - The oldState is the state produced by the upgrade epoch.
// - The returned newState is the new state that will be used by the next epoch.
// - The height is the upgrade epoch height (already executed).
// - The tipset is the tipset for the last non-null block before the upgrade. Do
//   not assume that ts.Height() is the upgrade height.
type MigrationFunc func(
	ctx context.Context,
	cache MigrationCache,
	oldState cid.Cid,
	height abi.ChainEpoch,
	ts *types.TipSet,
) (newState cid.Cid, err error)

// PreMigrationFunc is a function run _before_ a network upgrade to pre-compute part of the network
// upgrade and speed it up.
type PreMigrationFunc func(
	ctx context.Context,
	cache MigrationCache,
	oldState cid.Cid,
	height abi.ChainEpoch,
	ts *types.TipSet,
) error

// PreMigration describes a pre-migration step to prepare for a network state upgrade. Pre-migrations
// are optimizations, are not guaranteed to run, and may be canceled and/or run multiple times.
type PreMigration struct {
	// PreMigration is the pre-migration function to run at the specified time. This function is
	// run asynchronously and must abort promptly when canceled.
	PreMigration PreMigrationFunc

	// StartWithin specifies that this pre-migration should be started at most StartWithin
	// epochs before the upgrade.
	StartWithin abi.ChainEpoch

	// DontStartWithin specifies that this pre-migration should not be started DontStartWithin
	// epochs before the final upgrade epoch.
	//
	// This should be set such that the pre-migration is likely to complete before StopWithin.
	DontStartWithin abi.ChainEpoch

	// StopWithin specifies that this pre-migration should be stopped StopWithin epochs of the
	// final upgrade epoch.
	StopWithin abi.ChainEpoch
}

type Upgrade struct {
	Height    abi.ChainEpoch
	Network   network.Version
	Expensive bool
	Migration MigrationFunc

	// PreMigrations specifies a set of pre-migration functions to run at the indicated epochs.
	// These functions should fill the given cache with information that can speed up the
	// eventual full migration at the upgrade epoch.
	PreMigrations []PreMigration
}

type UpgradeSchedule []Upgrade

type migrationLogger struct{}

func (ml migrationLogger) Log(level rt.LogLevel, msg string, args ...interface{}) {
	switch level {
	case rt.DEBUG:
		log.Debugf(msg, args...)
	case rt.INFO:
		log.Infof(msg, args...)
	case rt.WARN:
		log.Warnf(msg, args...)
	case rt.ERROR:
		log.Errorf(msg, args...)
	}
}

func defaultUpgradeSchedule(cf *ChainFork, upgradeHeight *config.ForkUpgradeConfig) UpgradeSchedule {
	var us UpgradeSchedule

	updates := []Upgrade{{
		Height:    upgradeHeight.UpgradeBreezeHeight,
		Network:   network.Version1,
		Migration: cf.UpgradeFaucetBurnRecovery,
	}, {
		Height:    upgradeHeight.UpgradeSmokeHeight,
		Network:   network.Version2,
		Migration: nil,
	}, {
		Height:    upgradeHeight.UpgradeIgnitionHeight,
		Network:   network.Version3,
		Migration: cf.UpgradeIgnition,
	}, {
		Height:    upgradeHeight.UpgradeRefuelHeight,
		Network:   network.Version3,
		Migration: cf.UpgradeRefuel,
	}, {
		Height:    upgradeHeight.UpgradeActorsV2Height,
		Network:   network.Version4,
		Expensive: true,
		Migration: cf.UpgradeActorsV2,
	}, {
		Height:    upgradeHeight.UpgradeTapeHeight,
		Network:   network.Version5,
		Migration: nil,
	}, {
		Height:    upgradeHeight.UpgradeLiftoffHeight,
		Network:   network.Version5,
		Migration: cf.UpgradeLiftoff,
	}, {
		Height:    upgradeHeight.UpgradeKumquatHeight,
		Network:   network.Version6,
		Migration: nil,
	}, {
		Height:    upgradeHeight.UpgradeCalicoHeight,
		Network:   network.Version7,
		Migration: cf.UpgradeCalico,
	}, {
		Height:    upgradeHeight.UpgradePersianHeight,
		Network:   network.Version8,
		Migration: nil,
	}, {
		Height:    upgradeHeight.UpgradeOrangeHeight,
		Network:   network.Version9,
		Migration: nil,
	}, {
		Height:    upgradeHeight.UpgradeActorsV3Height,
		Network:   network.Version10,
		Migration: cf.UpgradeActorsV3,
		PreMigrations: []PreMigration{{
			PreMigration:    cf.PreUpgradeActorsV3,
			StartWithin:     120,
			DontStartWithin: 60,
			StopWithin:      35,
		}, {
			PreMigration:    cf.PreUpgradeActorsV3,
			StartWithin:     30,
			DontStartWithin: 15,
			StopWithin:      5,
		}},
		Expensive: true,
	}}

	for _, u := range updates {
		if u.Height < 0 {
			// upgrade disabled
			continue
		}
		us = append(us, u)
	}
	return us
}

func (us UpgradeSchedule) Validate() error {
	// Make sure each upgrade is valid.
	for _, u := range us {
		if u.Network <= 0 {
			return xerrors.Errorf("cannot upgrade to version <= 0: %d", u.Network)
		}

		for _, m := range u.PreMigrations {
			if m.StartWithin <= 0 {
				return xerrors.Errorf("pre-migration must specify a positive start-within epoch")
			}

			if m.DontStartWithin < 0 || m.StopWithin < 0 {
				return xerrors.Errorf("pre-migration must specify non-negative epochs")
			}

			if m.StartWithin <= m.StopWithin {
				return xerrors.Errorf("pre-migration start-within must come before stop-within")
			}

			// If we have a dont-start-within.
			if m.DontStartWithin != 0 {
				if m.DontStartWithin < m.StopWithin {
					return xerrors.Errorf("pre-migration dont-start-within must come before stop-within")
				}
				if m.StartWithin <= m.DontStartWithin {
					return xerrors.Errorf("pre-migration start-within must come after dont-start-within")
				}
			}
		}
		if !sort.SliceIsSorted(u.PreMigrations, func(i, j int) bool {
			return u.PreMigrations[i].StartWithin > u.PreMigrations[j].StartWithin //nolint:scopelint,gosec
		}) {
			return xerrors.Errorf("pre-migrations must be sorted by start epoch")
		}
	}

	// Make sure the upgrade order makes sense.
	for i := 1; i < len(us); i++ {
		prev := &us[i-1]
		curr := &us[i]
		if !(prev.Network <= curr.Network) {
			return xerrors.Errorf("cannot downgrade from version %d to version %d", prev.Network, curr.Network)
		}
		// Make sure the heights make sense.
		if prev.Height < 0 {
			// Previous upgrade was disabled.
			continue
		}
		if !(prev.Height < curr.Height) {
			return xerrors.Errorf("upgrade heights must be strictly increasing: upgrade %d was at height %d, followed by upgrade %d at height %d", i-1, prev.Height, i, curr.Height)
		}
	}
	return nil
}

type chainReader interface {
	GetHead() *types.TipSet
	GetTipSet(types.TipSetKey) (*types.TipSet, error)
	GetTipSetByHeight(context.Context, *types.TipSet, abi.ChainEpoch, bool) (*types.TipSet, error)
	GetTipSetState(context.Context, *types.TipSet) (vmstate.Tree, error)
	GetGenesisBlock(context.Context) (*types.BlockHeader, error)
	SubHeadChanges(context.Context) chan []*chain.HeadChange
}

type IFork interface {
	HandleStateForks(ctx context.Context, root cid.Cid, height abi.ChainEpoch, ts *types.TipSet) (cid.Cid, error)
	GetNtwkVersion(ctx context.Context, height abi.ChainEpoch) network.Version
	HasExpensiveFork(ctx context.Context, height abi.ChainEpoch) bool
	GetForkUpgrade() *config.ForkUpgradeConfig
	Start(ctx context.Context) error
}

var _ = IFork((*ChainFork)(nil))

type versionSpec struct {
	networkVersion network.Version
	atOrBelow      abi.ChainEpoch
}

type migration struct {
	upgrade       MigrationFunc
	preMigrations []PreMigration
	cache         *nv10.MemMigrationCache
}

type ChainFork struct {
	cr        chainReader
	bs        blockstore.Blockstore
	ipldstore cbor.IpldStore

	// Determines the network version at any given epoch.
	networkVersions []versionSpec
	latestVersion   network.Version

	// Maps chain epochs to upgrade functions.
	stateMigrations map[abi.ChainEpoch]*migration
	// A set of potentially expensive/time consuming upgrades. Explicit
	// calls for, e.g., gas estimation fail against this epoch with
	// ErrExpensiveFork.
	expensiveUpgrades map[abi.ChainEpoch]struct{}

	// upgrade param
	networkType int
	forkUpgrade *config.ForkUpgradeConfig
}

func NewChainFork(ctx context.Context, cr chainReader, ipldstore cbor.IpldStore, bs blockstore.Blockstore, networkParams *config.NetworkParamsConfig) (*ChainFork, error) {

	fork := &ChainFork{
		cr:          cr,
		bs:          bs,
		ipldstore:   ipldstore,
		networkType: networkParams.NetworkType,
		forkUpgrade: networkParams.ForkUpgradeParam,
	}

	// If we have upgrades, make sure they're in-order and make sense.
	us := defaultUpgradeSchedule(fork, networkParams.ForkUpgradeParam)
	if err := us.Validate(); err != nil {
		return nil, err
	}

	stateMigrations := make(map[abi.ChainEpoch]*migration, len(us))
	expensiveUpgrades := make(map[abi.ChainEpoch]struct{}, len(us))
	var networkVersions []versionSpec
	lastVersion := network.Version0
	if len(us) > 0 {
		// If we have any upgrades, process them and create a version schedule.
		for _, upgrade := range us {
			if upgrade.Migration != nil || upgrade.PreMigrations != nil {
				migration := &migration{
					upgrade:       upgrade.Migration,
					preMigrations: upgrade.PreMigrations,
					cache:         nv10.NewMemMigrationCache(),
				}
				stateMigrations[upgrade.Height] = migration
			}
			if upgrade.Expensive {
				expensiveUpgrades[upgrade.Height] = struct{}{}
			}
			networkVersions = append(networkVersions, versionSpec{
				networkVersion: lastVersion,
				atOrBelow:      upgrade.Height,
			})
			lastVersion = upgrade.Network
		}
	} else {
		// Otherwise, go directly to the latest version.
		lastVersion = constants.NewestNetworkVersion
	}

	fork.networkVersions = networkVersions
	fork.latestVersion = lastVersion
	fork.stateMigrations = stateMigrations
	fork.expensiveUpgrades = expensiveUpgrades

	return fork, nil
}

func (c *ChainFork) Start(ctx context.Context) error {
	log.Info("preMigrationWorker start ...")
	go c.preMigrationWorker(ctx)

	return nil
}

func (c *ChainFork) StateTree(ctx context.Context, st cid.Cid) (*vmstate.State, error) {
	return vmstate.LoadState(ctx, c.ipldstore, st)
}

func (c *ChainFork) HandleStateForks(ctx context.Context, root cid.Cid, height abi.ChainEpoch, ts *types.TipSet) (cid.Cid, error) {
	retCid := root
	var err error
	u := c.stateMigrations[height]
	if u != nil && u.upgrade != nil {
		startTime := time.Now()
		log.Warnw("STARTING migration", "height", height, "from", root)
		// Yes, we clone the cache, even for the final upgrade epoch. Why? Reverts. We may
		// have to migrate multiple times.
		tmpCache := u.cache.Clone()
		retCid, err = u.upgrade(ctx, tmpCache, root, height, ts)
		if err != nil {
			log.Errorw("FAILED migration", "height", height, "from", root, "error", err)
			return cid.Undef, err
		}
		// Yes, we update the cache, even for the final upgrade epoch. Why? Reverts. This
		// can save us a _lot_ of time because very few actors will have changed if we
		// do a small revert then need to re-run the migration.
		u.cache.Update(tmpCache)
		log.Warnw("COMPLETED migration",
			"height", height,
			"from", root,
			"to", retCid,
			"duration", time.Since(startTime),
		)
	}

	return retCid, nil
}

func (c *ChainFork) HasExpensiveFork(ctx context.Context, height abi.ChainEpoch) bool {
	_, ok := c.expensiveUpgrades[height]
	return ok
}

func (c *ChainFork) GetNtwkVersion(ctx context.Context, height abi.ChainEpoch) network.Version {
	// The epochs here are the _last_ epoch for every version, or -1 if the
	// version is disabled.
	for _, spec := range c.networkVersions {
		if height <= spec.atOrBelow {
			return spec.networkVersion
		}
	}
	return c.latestVersion
}

func runPreMigration(ctx context.Context, fn PreMigrationFunc, cache *nv10.MemMigrationCache, ts *types.TipSet) {
	height := ts.Height()
	parent := ts.Blocks()[0].ParentStateRoot

	startTime := time.Now()

	log.Warn("STARTING pre-migration")
	// Clone the cache so we don't actually _update_ it
	// till we're done. Otherwise, if we fail, the next
	// migration to use the cache may assume that
	// certain blocks exist, even if they don't.
	tmpCache := cache.Clone()
	err := fn(ctx, tmpCache, parent, height, ts)
	if err != nil {
		log.Errorw("FAILED pre-migration", "error", err)
		return
	}
	// Finally, if everything worked, update the cache.
	cache.Update(tmpCache)
	log.Warnw("COMPLETED pre-migration", "duration", time.Since(startTime))
}

func (c *ChainFork) preMigrationWorker(ctx context.Context) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	type op struct {
		after    abi.ChainEpoch
		notAfter abi.ChainEpoch
		run      func(ts *types.TipSet)
	}

	var wg sync.WaitGroup
	defer wg.Wait()

	// Turn each pre-migration into an operation in a schedule.
	var schedule []op
	for upgradeEpoch, migration := range c.stateMigrations {
		cache := migration.cache
		for _, prem := range migration.preMigrations {
			preCtx, preCancel := context.WithCancel(ctx)
			migrationFunc := prem.PreMigration

			afterEpoch := upgradeEpoch - prem.StartWithin
			notAfterEpoch := upgradeEpoch - prem.DontStartWithin
			stopEpoch := upgradeEpoch - prem.StopWithin
			// We can't start after we stop.
			if notAfterEpoch > stopEpoch {
				notAfterEpoch = stopEpoch - 1
			}

			// Add an op to start a pre-migration.
			schedule = append(schedule, op{
				after:    afterEpoch,
				notAfter: notAfterEpoch,

				// TODO: are these values correct?
				run: func(ts *types.TipSet) {
					wg.Add(1)
					go func() {
						defer wg.Done()
						runPreMigration(preCtx, migrationFunc, cache, ts)
					}()
				},
			})

			// Add an op to cancel the pre-migration if it's still running.
			schedule = append(schedule, op{
				after:    stopEpoch,
				notAfter: -1,
				run:      func(ts *types.TipSet) { preCancel() },
			})
		}
	}

	// Then sort by epoch.
	sort.Slice(schedule, func(i, j int) bool {
		return schedule[i].after < schedule[j].after
	})

	// Finally, when the head changes, see if there's anything we need to do.
	//
	// We're intentionally ignoring reorgs as they don't matter for our purposes.
	for change := range c.cr.SubHeadChanges(ctx) {
		for _, head := range change {
			for len(schedule) > 0 {
				op := &schedule[0]
				if head.Val.Height() < op.after {
					break
				}

				// If we haven't passed the pre-migration height...
				if op.notAfter < 0 || head.Val.Height() < op.notAfter {
					op.run(head.Val)
				}
				schedule = schedule[1:]
			}
		}
	}
}

func doTransfer(tree vmstate.Tree, from, to address.Address, amt abi.TokenAmount) error {
	fromAct, found, err := tree.GetActor(context.TODO(), from)
	if err != nil {
		return xerrors.Errorf("failed to get 'from' actor for transfer: %v", err)
	}
	if !found {
		return xerrors.Errorf("did not find 'from' actor for transfer: %v", from.String())
	}

	fromAct.Balance = big.Sub(fromAct.Balance, amt)
	if fromAct.Balance.Sign() < 0 {
		return xerrors.Errorf("(sanity) deducted more funds from target account than it had (%s, %s)", from, types.FIL(amt))
	}

	if err := tree.SetActor(context.TODO(), from, fromAct); err != nil {
		return xerrors.Errorf("failed to persist from actor: %v", err)
	}

	toAct, found, err := tree.GetActor(context.TODO(), to)
	if err != nil {
		return xerrors.Errorf("failed to get 'to' actor for transfer: %v", err)
	}
	if !found {
		return xerrors.Errorf("did not find 'to' actor for transfer: %v", from.String())
	}

	toAct.Balance = big.Add(toAct.Balance, amt)

	if err := tree.SetActor(context.TODO(), to, toAct); err != nil {
		return xerrors.Errorf("failed to persist to actor: %v", err)
	}

	return nil
}

func (c *ChainFork) ParentState(ts *types.TipSet) cid.Cid {
	if ts == nil {
		tts := c.cr.GetHead()
		return tts.Blocks()[0].ParentStateRoot
	}
	return ts.Blocks()[0].ParentStateRoot
}

func (c *ChainFork) UpgradeFaucetBurnRecovery(ctx context.Context, cache MigrationCache, root cid.Cid, epoch abi.ChainEpoch, ts *types.TipSet) (cid.Cid, error) {
	// Some initial parameters
	FundsForMiners := types.NewAttoFILFromFIL(1_000_000)
	LookbackEpoch := abi.ChainEpoch(32000)
	AccountCap := types.NewAttoFILFromFIL(0)
	BaseMinerBalance := types.NewAttoFILFromFIL(20)
	DesiredReimbursementBalance := types.NewAttoFILFromFIL(5_000_000)

	isSystemAccount := func(addr address.Address) (bool, error) {
		id, err := address.IDFromAddress(addr)
		if err != nil {
			return false, xerrors.Errorf("id address: %v", err)
		}

		if id < 1000 {
			return true, nil
		}
		return false, nil
	}

	minerFundsAlloc := func(pow, tpow abi.StoragePower) abi.TokenAmount {
		return big.Div(big.Mul(pow, FundsForMiners), tpow)
	}

	// Grab lookback state for account checks
	lbts, err := c.cr.GetTipSetByHeight(ctx, ts, LookbackEpoch, false)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to get tipset at lookback height: %v", err)
	}

	pts, err := c.cr.GetTipSet(lbts.Parents())
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to get tipset : %v", err)
	}

	lbtree, err := c.cr.GetTipSetState(ctx, pts)
	if err != nil {
		return cid.Undef, xerrors.Errorf("loading state tree failed: %v", err)
	}

	tree, err := c.StateTree(ctx, root)
	if err != nil {
		return cid.Undef, xerrors.Errorf("getting state tree: %v", err)
	}

	type transfer struct {
		From address.Address
		To   address.Address
		Amt  abi.TokenAmount
	}

	// todo not needed
	var transfers []transfer
	//subcalls := make([]types.ExecutionTrace, 0)
	//transferCb := func(trace types.ExecutionTrace) {
	//	subcalls = append(subcalls, trace)
	//}

	// Take all excess funds away, put them into the reserve account
	err = tree.ForEach(func(addr address.Address, act *types.Actor) error {
		switch act.Code {
		case builtin0.AccountActorCodeID, builtin0.MultisigActorCodeID, builtin0.PaymentChannelActorCodeID:
			sysAcc, err := isSystemAccount(addr)
			if err != nil {
				return xerrors.Errorf("checking system account: %v", err)
			}

			if !sysAcc {
				transfers = append(transfers, transfer{
					From: addr,
					To:   builtin.ReserveAddress,
					Amt:  act.Balance,
				})
			}
		case builtin0.StorageMinerActorCodeID:
			var st miner0.State
			if err := c.ipldstore.Get(ctx, act.Head, &st); err != nil {
				return xerrors.Errorf("failed to load miner state: %v", err)
			}

			var available abi.TokenAmount
			{
				defer func() {
					if err := recover(); err != nil {
						log.Warnf("Get available balance failed (%s, %s, %s): %s", addr, act.Head, act.Balance, err)
					}
					available = abi.NewTokenAmount(0)
				}()
				// this panics if the miner doesnt have enough funds to cover their locked pledge
				available = st.GetAvailableBalance(act.Balance)
			}

			if !available.IsZero() {
				transfers = append(transfers, transfer{
					From: addr,
					To:   builtin.ReserveAddress,
					Amt:  available,
				})
			}
		}
		return nil
	})
	if err != nil {
		return cid.Undef, xerrors.Errorf("foreach over state tree failed: %v", err)
	}

	// Execute transfers from previous step
	//fmt.Printf("num:%v, transfers:%v\n", len(transfers), transfers)
	for _, t := range transfers {
		if err := doTransfer(tree, t.From, t.To, t.Amt); err != nil {
			return cid.Undef, xerrors.Errorf("transfer %s %s->%s failed: %v", t.Amt, t.From, t.To, err)
		}
	}

	// pull up power table to give miners back some funds proportional to their power
	var ps power0.State
	powAct, find, err := tree.GetActor(ctx, builtin0.StoragePowerActorAddr)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to load power actor: %v", err)
	}

	if !find {
		return cid.Undef, xerrors.New("did not find power actor")
	}

	if err := c.ipldstore.Get(ctx, powAct.Head, &ps); err != nil {
		return cid.Undef, xerrors.Errorf("failed to get power actor state: %v", err)
	}

	totalPower := ps.TotalBytesCommitted

	var transfersBack []transfer
	// Now, we return some funds to places where they are needed
	err = tree.ForEach(func(addr address.Address, act *types.Actor) error {
		lbact, _, err := lbtree.GetActor(ctx, addr)
		if err != nil {
			return xerrors.Errorf("failed to get actor in lookback state")
		}

		prevBalance := abi.NewTokenAmount(0)
		if lbact != nil {
			prevBalance = lbact.Balance
		}

		switch act.Code {
		case builtin0.AccountActorCodeID, builtin0.MultisigActorCodeID, builtin0.PaymentChannelActorCodeID:
			nbalance := big.Min(prevBalance, AccountCap)
			if nbalance.Sign() != 0 {
				transfersBack = append(transfersBack, transfer{
					From: builtin.ReserveAddress,
					To:   addr,
					Amt:  nbalance,
				})
			}
		case builtin0.StorageMinerActorCodeID:
			var st miner0.State
			if err := c.ipldstore.Get(ctx, act.Head, &st); err != nil {
				return xerrors.Errorf("failed to load miner state: %v", err)
			}

			var minfo miner0.MinerInfo
			if err := c.ipldstore.Get(ctx, st.Info, &minfo); err != nil {
				return xerrors.Errorf("failed to get miner info: %v", err)
			}

			sectorsArr, err := adt0.AsArray(adt.WrapStore(ctx, c.ipldstore), st.Sectors)
			if err != nil {
				return xerrors.Errorf("failed to load sectors array: %v", err)
			}

			slen := sectorsArr.Length()

			power := big.Mul(big.NewInt(int64(slen)), big.NewInt(int64(minfo.SectorSize)))

			mfunds := minerFundsAlloc(power, totalPower)
			transfersBack = append(transfersBack, transfer{
				From: builtin.ReserveAddress,
				To:   minfo.Worker,
				Amt:  mfunds,
			})

			// Now make sure to give each miner who had power at the lookback some FIL
			lbact, found, err := lbtree.GetActor(ctx, addr)
			if err == nil {
				if found {
					var lbst miner0.State
					if err := c.ipldstore.Get(ctx, lbact.Head, &lbst); err != nil {
						return xerrors.Errorf("failed to load miner state: %v", err)
					}

					lbsectors, err := adt0.AsArray(adt.WrapStore(ctx, c.ipldstore), lbst.Sectors)
					if err != nil {
						return xerrors.Errorf("failed to load lb sectors array: %v", err)
					}

					if lbsectors.Length() > 0 {
						transfersBack = append(transfersBack, transfer{
							From: builtin.ReserveAddress,
							To:   minfo.Worker,
							Amt:  BaseMinerBalance,
						})
					}
				} else {
					log.Warnf("did not find actor: %s", addr.String())
				}
			} else {
				log.Warnf("failed to get miner in lookback state: %s", err)
			}
		}
		return nil
	})
	if err != nil {
		return cid.Undef, xerrors.Errorf("foreach over state tree failed: %v", err)
	}

	for _, t := range transfersBack {
		if err := doTransfer(tree, t.From, t.To, t.Amt); err != nil {
			return cid.Undef, xerrors.Errorf("transfer %s %s->%s failed: %v", t.Amt, t.From, t.To, err)
		}
	}

	// transfer all burnt funds back to the reserve account
	burntAct, find, err := tree.GetActor(ctx, builtin0.BurntFundsActorAddr)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to load burnt funds actor: %v", err)
	}
	if !find {
		return cid.Undef, xerrors.New("did not find burnt funds actor")
	}
	if err := doTransfer(tree, builtin0.BurntFundsActorAddr, builtin.ReserveAddress, burntAct.Balance); err != nil {
		return cid.Undef, xerrors.Errorf("failed to unburn funds: %v", err)
	}

	// Top up the reimbursement service
	reimbAddr, err := address.NewFromString("t0111")
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to parse reimbursement service address")
	}

	reimb, find, err := tree.GetActor(ctx, reimbAddr)
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to load reimbursement account actor: %v", err)
	}
	if !find {
		return cid.Undef, xerrors.New("did not find reimbursement actor")
	}

	difference := big.Sub(DesiredReimbursementBalance, reimb.Balance)
	if err := doTransfer(tree, builtin.ReserveAddress, reimbAddr, difference); err != nil {
		return cid.Undef, xerrors.Errorf("failed to top up reimbursement account: %v", err)
	}

	// Now, a final sanity check to make sure the balances all check out
	total := abi.NewTokenAmount(0)
	err = tree.ForEach(func(addr address.Address, act *types.Actor) error {
		total = big.Add(total, act.Balance)
		return nil
	})
	if err != nil {
		return cid.Undef, xerrors.Errorf("checking final state balance failed: %v", err)
	}

	exp := types.NewAttoFILFromFIL(constants.FilBase)
	if !exp.Equals(total) {
		return cid.Undef, xerrors.Errorf("resultant state tree account balance was not correct: %s", total)
	}

	return tree.Flush(ctx)
}

func setNetworkName(ctx context.Context, store adt.Store, tree *vmstate.State, name string) error {
	ia, find, err := tree.GetActor(ctx, builtin0.InitActorAddr)
	if err != nil {
		return xerrors.Errorf("getting init actor: %v", err)
	}
	if !find {
		return xerrors.New("did not find init actor")
	}

	initState, err := init_.Load(store, ia)
	if err != nil {
		return xerrors.Errorf("reading init state: %v", err)
	}

	if err := initState.SetNetworkName(name); err != nil {
		return xerrors.Errorf("setting network name: %v", err)
	}

	c, err := store.Put(ctx, initState)
	if err != nil {
		return xerrors.Errorf("writing new init state: %v", err)
	}
	ia.Head = c

	if err := tree.SetActor(ctx, builtin0.InitActorAddr, ia); err != nil {
		return xerrors.Errorf("setting init actor: %v", err)
	}

	return nil
}

// TODO: After the Liftoff epoch, refactor this to use resetMultisigVesting
func resetGenesisMsigs0(ctx context.Context, sm *ChainFork, store adt0.Store, tree *vmstate.State, startEpoch abi.ChainEpoch) error {
	gb, err := sm.cr.GetGenesisBlock(ctx)
	if err != nil {
		return xerrors.Errorf("getting genesis block: %v", err)
	}

	gts, err := types.NewTipSet(gb)
	if err != nil {
		return xerrors.Errorf("getting genesis tipset: %v", err)
	}

	genesisTree, err := sm.StateTree(ctx, gts.Blocks()[0].ParentStateRoot)
	if err != nil {
		return xerrors.Errorf("loading state tree: %v", err)
	}

	err = genesisTree.ForEach(func(addr address.Address, genesisActor *types.Actor) error {
		if genesisActor.Code == builtin0.MultisigActorCodeID {
			currActor, find, err := tree.GetActor(ctx, addr)
			if err != nil {
				return xerrors.Errorf("loading actor: %v", err)
			}
			if !find {
				return xerrors.Errorf("did not find actor: %s", addr.String())
			}

			var currState multisig0.State
			if err := store.Get(ctx, currActor.Head, &currState); err != nil {
				return xerrors.Errorf("reading multisig state: %v", err)
			}

			currState.StartEpoch = startEpoch

			head, err := store.Put(ctx, &currState)
			if err != nil {
				return xerrors.Errorf("writing new multisig state: %v", err)
			}
			currActor.Head = head

			if err := tree.SetActor(ctx, addr, currActor); err != nil {
				return xerrors.Errorf("setting multisig actor: %v", err)
			}
		}
		return nil
	})

	if err != nil {
		return xerrors.Errorf("iterating over genesis actors: %v", err)
	}

	return nil
}

func makeKeyAddr(splitAddr address.Address, count uint64) (address.Address, error) {
	var b bytes.Buffer
	if err := splitAddr.MarshalCBOR(&b); err != nil {
		return address.Undef, xerrors.Errorf("marshalling split address: %v", err)
	}

	if err := binary.Write(&b, binary.BigEndian, count); err != nil {
		return address.Undef, xerrors.Errorf("writing count into a buffer: %v", err)
	}

	if err := binary.Write(&b, binary.BigEndian, []byte("Ignition upgrade")); err != nil {
		return address.Undef, xerrors.Errorf("writing fork name into a buffer: %v", err)
	}

	addr, err := address.NewActorAddress(b.Bytes())
	if err != nil {
		return address.Undef, xerrors.Errorf("create actor address: %v", err)
	}

	return addr, nil
}

func splitGenesisMultisig0(ctx context.Context, addr address.Address, store adt0.Store, tree *vmstate.State, portions uint64, epoch abi.ChainEpoch) error {
	if portions < 1 {
		return xerrors.Errorf("cannot split into 0 portions")
	}

	mact, find, err := tree.GetActor(ctx, addr)
	if err != nil {
		return xerrors.Errorf("getting msig actor: %v", err)
	}
	if !find {
		return xerrors.Errorf("did not find actor: %s", addr.String())
	}

	mst, err := multisig.Load(store, mact)
	if err != nil {
		return xerrors.Errorf("getting msig state: %v", err)
	}

	signers, err := mst.Signers()
	if err != nil {
		return xerrors.Errorf("getting msig signers: %v", err)
	}

	thresh, err := mst.Threshold()
	if err != nil {
		return xerrors.Errorf("getting msig threshold: %v", err)
	}

	ibal, err := mst.InitialBalance()
	if err != nil {
		return xerrors.Errorf("getting msig initial balance: %v", err)
	}

	se, err := mst.StartEpoch()
	if err != nil {
		return xerrors.Errorf("getting msig start epoch: %v", err)
	}

	ud, err := mst.UnlockDuration()
	if err != nil {
		return xerrors.Errorf("getting msig unlock duration: %v", err)
	}

	pending, err := adt0.MakeEmptyMap(store).Root()
	if err != nil {
		return xerrors.Errorf("failed to create empty map: %v", err)
	}

	newIbal := big.Div(ibal, big.NewInt(int64(portions)))
	newState := &multisig0.State{
		Signers:               signers,
		NumApprovalsThreshold: thresh,
		NextTxnID:             0,
		InitialBalance:        newIbal,
		StartEpoch:            se,
		UnlockDuration:        ud,
		PendingTxns:           pending,
	}

	scid, err := store.Put(ctx, newState)
	if err != nil {
		return xerrors.Errorf("storing new state: %v", err)
	}

	newActor := types.Actor{
		Code:    builtin0.MultisigActorCodeID,
		Head:    scid,
		Nonce:   0,
		Balance: big.Zero(),
	}

	i := uint64(0)
	for i < portions {
		keyAddr, err := makeKeyAddr(addr, i)
		if err != nil {
			return xerrors.Errorf("creating key address: %v", err)
		}

		idAddr, err := tree.RegisterNewAddress(keyAddr)
		if err != nil {
			return xerrors.Errorf("registering new address: %v", err)
		}

		err = tree.SetActor(ctx, idAddr, &newActor)
		if err != nil {
			return xerrors.Errorf("setting new msig actor state: %v", err)
		}

		if err := doTransfer(tree, addr, idAddr, newIbal); err != nil {
			return xerrors.Errorf("transferring split msig balance: %v", err)
		}

		i++
	}

	return nil
}

func resetMultisigVesting0(ctx context.Context, store adt0.Store, tree *vmstate.State, addr address.Address, startEpoch abi.ChainEpoch, duration abi.ChainEpoch, balance abi.TokenAmount) error {
	act, find, err := tree.GetActor(ctx, addr)
	if err != nil {
		return xerrors.Errorf("getting actor: %v", err)
	}
	if !find {
		return xerrors.Errorf("did not find actor: %s", addr.String())
	}

	if !builtin.IsMultisigActor(act.Code) {
		return xerrors.Errorf("actor wasn't msig: %v", err)
	}

	var msigState multisig0.State
	if err := store.Get(ctx, act.Head, &msigState); err != nil {
		return xerrors.Errorf("reading multisig state: %v", err)
	}

	msigState.StartEpoch = startEpoch
	msigState.UnlockDuration = duration
	msigState.InitialBalance = balance

	head, err := store.Put(ctx, &msigState)
	if err != nil {
		return xerrors.Errorf("writing new multisig state: %v", err)
	}
	act.Head = head

	if err := tree.SetActor(ctx, addr, act); err != nil {
		return xerrors.Errorf("setting multisig actor: %v", err)
	}

	return nil
}

func (c *ChainFork) UpgradeIgnition(ctx context.Context, cache MigrationCache, root cid.Cid, epoch abi.ChainEpoch, ts *types.TipSet) (cid.Cid, error) {
	store := adt.WrapStore(ctx, c.ipldstore)

	if c.forkUpgrade.UpgradeLiftoffHeight <= epoch {
		return cid.Undef, xerrors.Errorf("liftoff height must be beyond ignition height")
	}

	nst, err := nv3.MigrateStateTree(ctx, store, root, epoch)
	if err != nil {
		return cid.Undef, xerrors.Errorf("migrating actors state: %v", err)
	}

	tree, err := c.StateTree(ctx, nst)
	if err != nil {
		return cid.Undef, xerrors.Errorf("getting state tree: %v", err)
	}

	err = setNetworkName(ctx, store, tree, "ignition")
	if err != nil {
		return cid.Undef, xerrors.Errorf("setting network name: %v", err)
	}

	split1, err := address.NewFromString("t0115")
	if err != nil {
		return cid.Undef, xerrors.Errorf("first split address: %v", err)
	}

	split2, err := address.NewFromString("t0116")
	if err != nil {
		return cid.Undef, xerrors.Errorf("second split address: %v", err)
	}

	err = resetGenesisMsigs0(ctx, c, store, tree, c.forkUpgrade.UpgradeLiftoffHeight)
	if err != nil {
		return cid.Undef, xerrors.Errorf("resetting genesis msig start epochs: %v", err)
	}

	err = splitGenesisMultisig0(ctx, split1, store, tree, 50, epoch)
	if err != nil {
		return cid.Undef, xerrors.Errorf("splitting first msig: %v", err)
	}

	err = splitGenesisMultisig0(ctx, split2, store, tree, 50, epoch)
	if err != nil {
		return cid.Undef, xerrors.Errorf("splitting second msig: %v", err)
	}

	err = nv3.CheckStateTree(ctx, store, nst, epoch, builtin0.TotalFilecoin)
	if err != nil {
		return cid.Undef, xerrors.Errorf("sanity check after ignition upgrade failed: %v", err)
	}

	return tree.Flush(ctx)
}

func (c *ChainFork) UpgradeRefuel(ctx context.Context, cache MigrationCache, root cid.Cid, epoch abi.ChainEpoch, ts *types.TipSet) (cid.Cid, error) {
	store := adt.WrapStore(ctx, c.ipldstore)
	tree, err := c.StateTree(ctx, root)
	if err != nil {
		return cid.Undef, xerrors.Errorf("getting state tree: %v", err)
	}

	err = resetMultisigVesting0(ctx, store, tree, builtin.SaftAddress, 0, 0, big.Zero())
	if err != nil {
		return cid.Undef, xerrors.Errorf("tweaking msig vesting: %v", err)
	}

	err = resetMultisigVesting0(ctx, store, tree, builtin.ReserveAddress, 0, 0, big.Zero())
	if err != nil {
		return cid.Undef, xerrors.Errorf("tweaking msig vesting: %v", err)
	}

	err = resetMultisigVesting0(ctx, store, tree, builtin.RootVerifierAddress, 0, 0, big.Zero())
	if err != nil {
		return cid.Undef, xerrors.Errorf("tweaking msig vesting: %v", err)
	}

	return tree.Flush(ctx)
}

func linksForObj(blk ipfsblock.Block, cb func(cid.Cid)) error {
	switch blk.Cid().Prefix().Codec {
	case cid.DagCBOR:
		err := cbg.ScanForLinks(bytes.NewReader(blk.RawData()), cb)
		if err != nil {
			return xerrors.Errorf("cbg.ScanForLinks: %v", err)
		}
		return nil
	case cid.Raw:
		// We implicitly have all children of raw blocks.
		return nil
	default:
		return xerrors.Errorf("vm flush copy method only supports dag cbor")
	}
}

func copyRec(from, to blockstore.Blockstore, root cid.Cid, cp func(ipfsblock.Block) error) error {
	if root.Prefix().MhType == 0 {
		// identity cid, skip
		return nil
	}

	blk, err := from.Get(root)
	if err != nil {
		return xerrors.Errorf("get %s failed: %v", root, err)
	}

	var lerr error
	err = linksForObj(blk, func(link cid.Cid) {
		if lerr != nil {
			// Theres no erorr return on linksForObj callback :(
			return
		}

		prefix := link.Prefix()
		if prefix.Codec == cid.FilCommitmentSealed || prefix.Codec == cid.FilCommitmentUnsealed {
			return
		}

		// We always have blocks inlined into CIDs, but we may not have their children.
		if prefix.MhType == mh.IDENTITY {
			// Unless the inlined block has no children.
			if prefix.Codec == cid.Raw {
				return
			}
		} else {
			// If we have an object, we already have its children, skip the object.
			has, err := to.Has(link)
			if err != nil {
				lerr = xerrors.Errorf("has: %v", err)
				return
			}
			if has {
				return
			}
		}

		if err := copyRec(from, to, link, cp); err != nil {
			lerr = err
			return
		}
	})
	if err != nil {
		return xerrors.Errorf("linksForObj (%x): %v", blk.RawData(), err)
	}
	if lerr != nil {
		return lerr
	}

	if err := cp(blk); err != nil {
		return xerrors.Errorf("copy: %v", err)
	}
	return nil
}

func Copy(ctx context.Context, from, to blockstore.Blockstore, root cid.Cid) error {
	ctx, span := trace.StartSpan(ctx, "vm.Copy") // nolint
	defer span.End()

	var numBlocks int
	var totalCopySize int

	const batchSize = 128
	const bufCount = 3
	freeBufs := make(chan []ipfsblock.Block, bufCount)
	toFlush := make(chan []ipfsblock.Block, bufCount)
	for i := 0; i < bufCount; i++ {
		freeBufs <- make([]ipfsblock.Block, 0, batchSize)
	}

	errFlushChan := make(chan error)

	go func() {
		for b := range toFlush {
			if err := to.PutMany(b); err != nil {
				close(freeBufs)
				errFlushChan <- xerrors.Errorf("batch put in copy: %v", err)
				return
			}
			freeBufs <- b[:0]
		}
		close(errFlushChan)
		close(freeBufs)
	}()

	var batch = <-freeBufs
	batchCp := func(blk ipfsblock.Block) error {
		numBlocks++
		totalCopySize += len(blk.RawData())

		batch = append(batch, blk)

		if len(batch) >= batchSize {
			toFlush <- batch
			var ok bool
			batch, ok = <-freeBufs
			if !ok {
				return <-errFlushChan
			}
		}
		return nil
	}

	if err := copyRec(from, to, root, batchCp); err != nil {
		return xerrors.Errorf("copyRec: %v", err)
	}

	if len(batch) > 0 {
		toFlush <- batch
	}
	close(toFlush)        // close the toFlush triggering the loop to end
	err := <-errFlushChan // get error out or get nil if it was closed
	if err != nil {
		return err
	}

	span.AddAttributes(
		trace.Int64Attribute("numBlocks", int64(numBlocks)),
		trace.Int64Attribute("copySize", int64(totalCopySize)),
	)

	return nil
}

func (c *ChainFork) UpgradeActorsV2(ctx context.Context, cache MigrationCache, root cid.Cid, epoch abi.ChainEpoch, ts *types.TipSet) (cid.Cid, error) {
	buf := blockstoreutil.NewTieredBstore(c.bs, blockstoreutil.NewTemporarySync())
	store := chain.ActorStore(ctx, buf)

	info, err := store.Put(ctx, new(vmstate.StateInfo0))
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to create new state info for actors v2: %v", err)
	}

	newHamtRoot, err := nv4.MigrateStateTree(ctx, store, root, epoch, nv4.DefaultConfig())
	if err != nil {
		return cid.Undef, xerrors.Errorf("upgrading to actors v2: %v", err)
	}

	newRoot, err := store.Put(ctx, &vmstate.StateRoot{
		Version: vmstate.StateTreeVersion1,
		Actors:  newHamtRoot,
		Info:    info,
	})
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to persist new state root: %v", err)
	}

	// perform some basic sanity checks to make sure everything still works.
	if newSm, err := vmstate.LoadState(ctx, store, newRoot); err != nil {
		return cid.Undef, xerrors.Errorf("state tree sanity load failed: %v", err)
	} else if newRoot2, err := newSm.Flush(ctx); err != nil {
		return cid.Undef, xerrors.Errorf("state tree sanity flush failed: %v", err)
	} else if newRoot2 != newRoot {
		return cid.Undef, xerrors.Errorf("state-root mismatch: %s != %s", newRoot, newRoot2)
	} else if _, _, err := newSm.GetActor(ctx, builtin0.InitActorAddr); err != nil {
		return cid.Undef, xerrors.Errorf("failed to load init actor after upgrade: %v", err)
	}

	{
		from := buf
		to := buf.Read()

		if err := Copy(ctx, from, to, newRoot); err != nil {
			return cid.Undef, xerrors.Errorf("copying migrated tree: %v", err)
		}
	}

	return newRoot, nil
}

func (c *ChainFork) UpgradeLiftoff(ctx context.Context, cache MigrationCache, root cid.Cid, epoch abi.ChainEpoch, ts *types.TipSet) (cid.Cid, error) {
	tree, err := c.StateTree(ctx, root)
	if err != nil {
		return cid.Undef, xerrors.Errorf("getting state tree: %v", err)
	}

	err = setNetworkName(ctx, adt.WrapStore(ctx, c.ipldstore), tree, "mainnet")
	if err != nil {
		return cid.Undef, xerrors.Errorf("setting network name: %v", err)
	}

	return tree.Flush(ctx)
}

func (c *ChainFork) UpgradeCalico(ctx context.Context, cache MigrationCache, root cid.Cid, epoch abi.ChainEpoch, ts *types.TipSet) (cid.Cid, error) {
	store := chain.ActorStore(ctx, c.bs)
	var stateRoot vmstate.StateRoot
	if err := store.Get(ctx, root, &stateRoot); err != nil {
		return cid.Undef, xerrors.Errorf("failed to decode state root: %v", err)
	}

	if stateRoot.Version != vmstate.StateTreeVersion1 {
		return cid.Undef, xerrors.Errorf(
			"expected state root version 1 for calico upgrade, got %d",
			stateRoot.Version,
		)
	}

	newHamtRoot, err := nv7.MigrateStateTree(ctx, store, stateRoot.Actors, epoch, nv7.DefaultConfig())
	if err != nil {
		return cid.Undef, xerrors.Errorf("running nv7 migration: %v", err)
	}

	newRoot, err := store.Put(ctx, &vmstate.StateRoot{
		Version: stateRoot.Version,
		Actors:  newHamtRoot,
		Info:    stateRoot.Info,
	})
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to persist new state root: %v", err)
	}

	// perform some basic sanity checks to make sure everything still works.
	if newSm, err := vmstate.LoadState(ctx, store, newRoot); err != nil {
		return cid.Undef, xerrors.Errorf("state tree sanity load failed: %v", err)
	} else if newRoot2, err := newSm.Flush(ctx); err != nil {
		return cid.Undef, xerrors.Errorf("state tree sanity flush failed: %v", err)
	} else if newRoot2 != newRoot {
		return cid.Undef, xerrors.Errorf("state-root mismatch: %s != %s", newRoot, newRoot2)
	} else if _, _, err := newSm.GetActor(ctx, builtin0.InitActorAddr); err != nil {
		return cid.Undef, xerrors.Errorf("failed to load init actor after upgrade: %v", err)
	}

	return newRoot, nil
}

func terminateActor(ctx context.Context, tree *vmstate.State, addr address.Address, epoch abi.ChainEpoch) error {
	a, found, err := tree.GetActor(context.TODO(), addr)
	if err != nil {
		return xerrors.Errorf("failed to get actor to delete: %v", err)
	}
	if !found {
		return types.ErrActorNotFound
	}

	if err := doTransfer(tree, addr, builtin.BurntFundsActorAddr, a.Balance); err != nil {
		return xerrors.Errorf("transferring terminated actor's balance: %v", err)
	}

	err = tree.DeleteActor(ctx, addr)
	if err != nil {
		return xerrors.Errorf("deleting actor from tree: %v", err)
	}

	ia, found, err := tree.GetActor(ctx, init_.Address)
	if err != nil {
		return xerrors.Errorf("loading init actor: %v", err)
	}
	if !found {
		return types.ErrActorNotFound
	}

	ias, err := init_.Load(&vmstate.AdtStore{IpldStore: tree.Store}, ia)
	if err != nil {
		return xerrors.Errorf("loading init actor state: %v", err)
	}

	if err := ias.Remove(addr); err != nil {
		return xerrors.Errorf("deleting entry from address map: %v", err)
	}

	nih, err := tree.Store.Put(ctx, ias)
	if err != nil {
		return xerrors.Errorf("writing new init actor state: %v", err)
	}

	ia.Head = nih

	return tree.SetActor(ctx, init_.Address, ia)
}

func (c *ChainFork) UpgradeActorsV3(ctx context.Context, cache MigrationCache, root cid.Cid, epoch abi.ChainEpoch, ts *types.TipSet) (cid.Cid, error) {
	// Use all the CPUs except 3.
	workerCount := runtime.NumCPU() - 3
	if workerCount <= 0 {
		workerCount = 1
	}

	cfg := nv10.Config{
		MaxWorkers:        uint(workerCount),
		JobQueueSize:      1000,
		ResultQueueSize:   100,
		ProgressLogPeriod: 10 * time.Second,
	}
	newRoot, err := c.upgradeActorsV3Common(ctx, cache, root, epoch, ts, cfg)
	if err != nil {
		return cid.Undef, xerrors.Errorf("migrating actors v3 state: %v", err)
	}

	tree, err := c.StateTree(ctx, newRoot)
	if err != nil {
		return cid.Undef, xerrors.Errorf("getting state tree: %v", err)
	}

	if c.networkType == constants.NetworkMainnet {
		err := terminateActor(ctx, tree, types.ZeroAddress, epoch)
		if err != nil && !xerrors.Is(err, types.ErrActorNotFound) {
			return cid.Undef, xerrors.Errorf("deleting zero bls actor: %v", err)
		}

		newRoot, err = tree.Flush(ctx)
		if err != nil {
			return cid.Undef, xerrors.Errorf("flushing state tree: %v", err)
		}
	}

	return newRoot, nil
}

func (c *ChainFork) PreUpgradeActorsV3(ctx context.Context, cache MigrationCache, root cid.Cid, epoch abi.ChainEpoch, ts *types.TipSet) error {
	log.Info("PreUpgradeActorsV3 ......")
	// Use half the CPUs for pre-migration, but leave at least 3.
	workerCount := runtime.NumCPU()
	if workerCount <= 4 {
		workerCount = 1
	} else {
		workerCount /= 2
	}
	cfg := nv10.Config{MaxWorkers: uint(workerCount)}
	_, err := c.upgradeActorsV3Common(ctx, cache, root, epoch, ts, cfg)
	return err
}

func (c *ChainFork) upgradeActorsV3Common(
	ctx context.Context, cache MigrationCache,
	root cid.Cid, epoch abi.ChainEpoch, ts *types.TipSet,
	config nv10.Config,
) (cid.Cid, error) {
	buf := blockstoreutil.NewTieredBstore(c.bs, blockstoreutil.NewTemporarySync())
	store := chain.ActorStore(ctx, buf)

	// Load the state root.
	var stateRoot vmstate.StateRoot
	if err := store.Get(ctx, root, &stateRoot); err != nil {
		return cid.Undef, xerrors.Errorf("failed to decode state root: %v", err)
	}

	if stateRoot.Version != vmstate.StateTreeVersion1 {
		return cid.Undef, xerrors.Errorf(
			"expected state root version 1 for actors v3 upgrade, got %d",
			stateRoot.Version,
		)
	}

	// Perform the migration
	newHamtRoot, err := nv10.MigrateStateTree(ctx, store, stateRoot.Actors, epoch, config, migrationLogger{}, cache)
	if err != nil {
		return cid.Undef, xerrors.Errorf("upgrading to actors v2: %v", err)
	}

	// Persist the result.
	newRoot, err := store.Put(ctx, &vmstate.StateRoot{
		Version: vmstate.StateTreeVersion2,
		Actors:  newHamtRoot,
		Info:    stateRoot.Info,
	})
	if err != nil {
		return cid.Undef, xerrors.Errorf("failed to persist new state root: %v", err)
	}

	// Persist the new tree.

	{
		from := buf
		to := buf.Read()

		if err := Copy(ctx, from, to, newRoot); err != nil {
			return cid.Undef, xerrors.Errorf("copying migrated tree: %v", err)
		}
	}

	return newRoot, nil
}

func (c *ChainFork) GetForkUpgrade() *config.ForkUpgradeConfig {
	return c.forkUpgrade
}

//func makeFakeMsg(from address.Address, to address.Address, amt abi.TokenAmount, nonce uint64) *types.Message {
//	return &types.Message{
//		From:  from,
//		To:    to,
//		Value: amt,
//		Nonce: nonce,
//	}
//}
//
//func makeFakeRct() *types.MessageReceipt {
//	return &types.MessageReceipt{
//		ExitCode:    0,
//		ReturnValue: nil,
//		GasUsed:     0,
//	}
//}
