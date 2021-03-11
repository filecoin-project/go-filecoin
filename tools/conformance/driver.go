package conformance

import (
	"context"
	"github.com/filecoin-project/venus/pkg/vm/gas"
	cbor "github.com/ipfs/go-ipld-cbor"
	gobig "math/big"
	"os"

	"github.com/filecoin-project/venus/app/node"
	"github.com/filecoin-project/venus/fixtures/networks"
	"github.com/filecoin-project/venus/pkg/chain"
	_ "github.com/filecoin-project/venus/pkg/crypto/sigs/bls"  // enable bls signatures
	_ "github.com/filecoin-project/venus/pkg/crypto/sigs/secp" // enable secp signatures
	"github.com/filecoin-project/venus/pkg/fork"
	"github.com/filecoin-project/venus/pkg/slashing"
	"github.com/filecoin-project/venus/pkg/state/tree"
	"github.com/filecoin-project/venus/pkg/vm"
	"github.com/filecoin-project/venus/pkg/vm/register"
	"github.com/filecoin-project/venus/pkg/vmsupport"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/crypto"
	"github.com/filecoin-project/test-vectors/schema"
	crypto2 "github.com/filecoin-project/venus/pkg/crypto"
	"github.com/filecoin-project/venus/pkg/types"
	"github.com/filecoin-project/venus/pkg/util/ffiwrapper"
	"github.com/filecoin-project/venus/tools/conformance/chaos"
	"github.com/ipfs/go-cid"
	ds "github.com/ipfs/go-datastore"
	blockstore "github.com/ipfs/go-ipfs-blockstore"
)

var (
	// DefaultCirculatingSupply is the fallback circulating supply returned by
	// the driver's CircSupplyCalculator function, used if the vector specifies
	// no circulating supply.
	DefaultCirculatingSupply = crypto2.TotalFilecoinInt

	// DefaultBaseFee to use in the VM, if one is not supplied in the vector.
	DefaultBaseFee = abi.NewTokenAmount(100)
)

type Driver struct {
	ctx      context.Context
	selector schema.Selector
	vmFlush  bool
}

type DriverOpts struct {
	// DisableVMFlush, when true, avoids calling VM.Flush(), forces a blockstore
	// recursive copy, from the temporary buffer blockstore, to the real
	// system's blockstore. Disabling VM flushing is useful when extracting test
	// vectors and trimming state, as we don't want to force an accidental
	// deep copy of the state tree.
	//
	// Disabling VM flushing almost always should go hand-in-hand with
	// LOTUS_DISABLE_VM_BUF=iknowitsabadidea. That way, state tree writes are
	// immediately committed to the blockstore.
	DisableVMFlush bool
}

func NewDriver(ctx context.Context, selector schema.Selector, opts DriverOpts) *Driver {
	return &Driver{ctx: ctx, selector: selector, vmFlush: !opts.DisableVMFlush}
}

type ExecuteTipsetResult struct {
	ReceiptsRoot  cid.Cid
	PostStateRoot cid.Cid

	// AppliedMessages stores the messages that were applied, in the order they
	// were applied. It includes implicit messages (cron, rewards).
	AppliedMessages []*vm.VmMessage
	// AppliedResults stores the results of AppliedMessages, in the same order.
	AppliedResults []*vm.Ret
}

// ExecuteTipset executes the supplied tipset on top of the state represented
// by the preroot CID.
//
// parentEpoch is the last epoch in which an actual tipset was processed. This
// is used by Lotus for null block counting and cron firing.
//
// This method returns the the receipts root, the poststate root, and the VM
// message results. The latter _include_ implicit messages, such as cron ticks
// and reward withdrawal per miner.
func (d *Driver) ExecuteTipset(bs blockstore.Blockstore, chainDs ds.Batching, preroot cid.Cid, parentEpoch abi.ChainEpoch, tipset *schema.Tipset, execEpoch abi.ChainEpoch) (*ExecuteTipsetResult, error) {
	ipldStore := cbor.NewCborStore(bs)
	chainStatusReporter := chain.NewStatusReporter()
	mainNetParams := networks.Mainnet()
	node.SetNetParams(&mainNetParams.Network)
	//chainstore
	chainStore := chain.NewStore(chainDs, ipldStore, bs, chainStatusReporter, mainNetParams.Network.ForkUpgradeParam, cid.Undef) //load genesis from car

	//drand
	/*genBlk, err := chainStore.GetGenesisBlock(context.TODO())
	if err != nil {
		return nil, err
	}

	drand, err := beacon.DefaultDrandIfaceFromConfig(genBlk.Timestamp)
	if err != nil {
		return nil, err
	}*/

	//chain fork
	chainFork, err := fork.NewChainFork(context.TODO(), chainStore, ipldStore, bs, &mainNetParams.Network)
	faultChecker := slashing.NewFaultChecker(chainStore, chainFork)
	syscalls := vmsupport.NewSyscalls(faultChecker, ffiwrapper.ProofVerifier)
	if err != nil {
		return nil, err
	}
	var (
		caculator = chain.NewCirculatingSupplyCalculator(bs, chainStore, mainNetParams.Network.ForkUpgradeParam)

		vmOption = vm.VmOption{
			CircSupplyCalculator: func(ctx context.Context, epoch abi.ChainEpoch, tree tree.Tree) (abi.TokenAmount, error) {
				dertail, err := caculator.GetCirculatingSupplyDetailed(ctx, epoch, tree)
				if err != nil {
					return abi.TokenAmount{}, err
				}
				return dertail.FilCirculating, nil
			},
			NtwkVersionGetter: chainFork.GetNtwkVersion,
			Rnd:               NewFixedRand(),
			BaseFee:           big.NewFromGo(&tipset.BaseFee),
			Fork:              chainFork,
			Epoch:             execEpoch,
			GasPriceSchedule:  gas.NewPricesSchedule(mainNetParams.Network.ForkUpgradeParam),
			PRoot:             preroot,
			Bsstore:           bs,
			SysCallsImpl:      syscalls,
		}
	)

	lvm, err := vm.NewVM(vmOption)
	if err != nil {
		return nil, err
	}
	//flush data to blockstore
	defer lvm.Flush() //nolint

	blocks := make([]types.BlockMessagesInfo, 0, len(tipset.Blocks))
	for _, b := range tipset.Blocks {
		sb := types.BlockMessagesInfo{
			Block: &types.BlockHeader{
				Miner: b.MinerAddr,
				ElectionProof: &types.ElectionProof{
					WinCount: b.WinCount,
				},
			},
		}
		for _, m := range b.Messages {
			msg, err := types.DecodeMessage(m)
			if err != nil {
				return nil, err
			}
			switch msg.From.Protocol() {
			case address.SECP256K1:
				sb.SecpkMessages = append(sb.SecpkMessages, &types.SignedMessage{
					Message: *msg,
					Signature: crypto.Signature{
						Type: crypto.SigTypeSecp256k1,
						Data: make([]byte, 65),
					},
				})
			case address.BLS:
				sb.BlsMessages = append(sb.BlsMessages, msg)
			default:
				// sneak in messages originating from other addresses as both kinds.
				// these should fail, as they are actually invalid senders.
				/*sb.SECPMessages = append(sb.SECPMessages, &types.SignedMessage{
					Message: *msg,
					Signature: crypto.Signature{
						Type: crypto.SigTypeSecp256k1,
						Data: make([]byte, 65),
					},
				})*/
				sb.BlsMessages = append(sb.BlsMessages, msg) //todo  use interface for message
				sb.BlsMessages = append(sb.BlsMessages, msg)
			}
		}
		blocks = append(blocks, sb)
	}

	var (
		messages []*vm.VmMessage
		results  []*vm.Ret
	)

	postcid, receipt, err := lvm.ApplyTipSetMessages(blocks, nil, parentEpoch, execEpoch, func(_ cid.Cid, msg vm.VmMessage, ret *vm.Ret) error {
		messages = append(messages, &msg)
		results = append(results, ret)
		return nil
	})
	if err != nil {
		return nil, err
	}
	receiptsroot, err := chain.GetReceiptRoot(receipt)
	if err != nil {
		return nil, err
	}

	/*	postcid, receiptsroot, err := sm.ApplyBlocks(context.Background(), parentEpoch, preroot, blocks, execEpoch, vmRand, func(_ cid.Cid, msg *types.ChainMsg, ret *vm.Ret) error {
		messages = append(messages, msg)
		results = append(results, ret)
		return nil
	}, basefee, nil)*/

	ret := &ExecuteTipsetResult{
		ReceiptsRoot:    receiptsroot,
		PostStateRoot:   postcid,
		AppliedMessages: messages,
		AppliedResults:  results,
	}
	return ret, nil
}

type ExecuteMessageParams struct {
	Preroot    cid.Cid
	Epoch      abi.ChainEpoch
	Message    *types.UnsignedMessage
	CircSupply abi.TokenAmount
	BaseFee    abi.TokenAmount

	Rand chain.RandomnessSource
}

// ExecuteMessage executes a conformance test vector message in a temporary VM.
func (d *Driver) ExecuteMessage(bs blockstore.Blockstore, params ExecuteMessageParams) (*vm.Ret, cid.Cid, error) {
	if !d.vmFlush {
		// do not flush the VM, just the state tree; this should be used with
		// LOTUS_DISABLE_VM_BUF enabled, so writes will anyway be visible.
		_ = os.Setenv("LOTUS_DISABLE_VM_BUF", "iknowitsabadidea")
	}
	actorBuilder := register.DefaultActorBuilder
	// register the chaos actor if required by the vector.
	if chaosOn, ok := d.selector["chaos_actor"]; ok && chaosOn == "true" {
		chaosActor := chaos.Actor{}
		actorBuilder.Add(nil, chaosActor)
	}

	coderLoader := actorBuilder.Build()

	if params.Rand == nil {
		params.Rand = NewFixedRand()
	}
	mainNetParams := networks.Mainnet()
	node.SetNetParams(&mainNetParams.Network)
	ipldStore := cbor.NewCborStore(bs)
	chainStatusReporter := chain.NewStatusReporter()
	chainDs := ds.NewMapDatastore() //just mock one
	//chainstore
	chainStore := chain.NewStore(chainDs, ipldStore, bs, chainStatusReporter, mainNetParams.Network.ForkUpgradeParam, cid.Undef) //load genesis from car

	//drand
	/*	genBlk, err := chainStore.GetGenesisBlock(context.TODO())
		if err != nil {
			return nil, cid.Undef, err
		}

		drand, err := beacon.DefaultDrandIfaceFromConfig(genBlk.Timestamp)
		if err != nil {
			return nil, cid.Undef, err
		}*/

	//chain fork
	chainFork, err := fork.NewChainFork(context.TODO(), chainStore, ipldStore, bs, &mainNetParams.Network)
	faultChecker := slashing.NewFaultChecker(chainStore, chainFork)
	syscalls := vmsupport.NewSyscalls(faultChecker, ffiwrapper.ProofVerifier)
	if err != nil {
		return nil, cid.Undef, err
	}
	var (
		vmOption = vm.VmOption{
			CircSupplyCalculator: func(ctx context.Context, epoch abi.ChainEpoch, tree tree.Tree) (abi.TokenAmount, error) {
				return params.CircSupply, nil
			},
			NtwkVersionGetter: chainFork.GetNtwkVersion,
			Rnd:               params.Rand,
			BaseFee:           params.BaseFee,
			Fork:              chainFork,
			ActorCodeLoader:   &coderLoader,
			Epoch:             params.Epoch,
			GasPriceSchedule:  gas.NewPricesSchedule(mainNetParams.Network.ForkUpgradeParam),
			PRoot:             params.Preroot,
			Bsstore:           bs,
			SysCallsImpl:      syscalls,
		}
	)

	lvm, err := vm.NewVM(vmOption)
	if err != nil {
		return nil, cid.Undef, err
	}

	ret, err := lvm.ApplyMessage(toChainMsg(params.Message))
	if err != nil {
		return nil, cid.Undef, err
	}

	var root cid.Cid
	if d.vmFlush {
		// flush the VM, committing the state tree changes and forcing a
		// recursive copy from the temporary blcokstore to the real blockstore.
		root, err = lvm.Flush()
		if err != nil {
			return nil, cid.Undef, err
		}
	} else {
		root, err = lvm.StateTree().Flush(d.ctx)
		if err != nil {
			return nil, cid.Undef, err
		}
	}

	return ret, root, err
}

// toChainMsg injects a synthetic 0-filled signature of the right length to
// messages that originate from secp256k senders, leaving all
// others untouched.
// TODO: generate a signature in the DSL so that it's encoded in
//  the test vector.
func toChainMsg(msg *types.UnsignedMessage) (ret types.ChainMsg) {
	ret = msg
	if msg.From.Protocol() == address.SECP256K1 {
		ret = &types.SignedMessage{
			Message: *msg,
			Signature: crypto.Signature{
				Type: crypto.SigTypeSecp256k1,
				Data: make([]byte, 65),
			},
		}
	}
	return ret
}

// BaseFeeOrDefault converts a basefee as passed in a test vector (go *big.Int
// type) to an abi.TokenAmount, or if nil it returns the DefaultBaseFee.
func BaseFeeOrDefault(basefee *gobig.Int) abi.TokenAmount {
	if basefee == nil {
		return DefaultBaseFee
	}
	return big.NewFromGo(basefee)
}

// CircSupplyOrDefault converts a circulating supply as passed in a test vector
// (go *big.Int type) to an abi.TokenAmount, or if nil it returns the
// DefaultCirculatingSupply.
func CircSupplyOrDefault(circSupply *gobig.Int) abi.TokenAmount {
	if circSupply == nil {
		return DefaultCirculatingSupply
	}
	return big.NewFromGo(circSupply)
}
