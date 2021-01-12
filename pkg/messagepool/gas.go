package messagepool

import (
	"context"
	"math"
	"math/rand"
	"sort"

	"github.com/filecoin-project/go-address"
	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/go-state-types/exitcode"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/venus/pkg/constants"
	"github.com/filecoin-project/venus/pkg/fork"
	"github.com/filecoin-project/venus/pkg/specactors/builtin"
	"github.com/filecoin-project/venus/pkg/specactors/builtin/paych"
	"github.com/filecoin-project/venus/pkg/types"
	"github.com/filecoin-project/venus/pkg/vm"
)

const MinGasPremium = 100e3

// const MaxSpendOnFeeDenom = 100

var bigZero = big.NewInt(0)

func (mp *MessagePool) GasEstimateFeeCap(
	ctx context.Context,
	msg *types.UnsignedMessage,
	maxqueueblks int64,
	tsk types.TipSetKey,
) (tbig.Int, error) {
	tsk block.TipSetKey,
) (big.Int, error) {
	if big.Cmp(mp.GetGasConfig().GasFeeGap, big.NewInt(0)) > 0 {
		return mp.GetGasConfig().GasFeeGap, nil
	}

	ts, err := mp.api.ChainHead()
	if err != nil {
		return types.NewGasFeeCap(0), err
	}

	parentBaseFee := ts.Blocks()[0].ParentBaseFee
	increaseFactor := math.Pow(1.+1./float64(constants.BaseFeeMaxChangeDenom), float64(maxqueueblks))

	feeInFuture := big.Mul(parentBaseFee, big.NewInt(int64(increaseFactor*(1<<8))))
	out := big.Div(feeInFuture, big.NewInt(1<<8))

	if !msg.GasPremium.Nil() && big.Cmp(msg.GasPremium, big.NewInt(0)) != 0 {
		out = big.Add(out, msg.GasPremium)
	}

	return out, nil
}

type gasMeta struct {
	price big.Int
	limit int64
}

func medianGasPremium(prices []gasMeta, blocks int) abi.TokenAmount {
	sort.Slice(prices, func(i, j int) bool {
		// sort desc by price
		return prices[i].price.GreaterThan(prices[j].price)
	})

	at := constants.BlockGasTarget * int64(blocks) / 2
	prev1, prev2 := big.Zero(), big.Zero()
	for _, price := range prices {
		prev1, prev2 = price.price, prev1
		at -= price.limit
		if at < 0 {
			break
		}
	}

	premium := prev1
	if prev2.Sign() != 0 {
		premium = big.Div(big.Add(prev1, prev2), big.NewInt(2))
	}

	return premium
}

func (mp *MessagePool) GasEstimateGasPremium(
	ctx context.Context,
	nblocksincl uint64,
	sender address.Address,
	gaslimit int64,
	_ types.TipSetKey,
) (tbig.Int, error) {
	if big.Cmp(mp.GetGasConfig().GasPremium, big.NewInt(0)) > 0 {
		return mp.GetGasConfig().GasPremium, nil
	}

	if nblocksincl == 0 {
		nblocksincl = 1
	}

	var prices []gasMeta
	var blocks int

	ts, err := mp.api.ChainHead()
	if err != nil {
		return big.Int{}, err
	}

	for i := uint64(0); i < nblocksincl*2; i++ {
		h := ts.Height()
		if h == 0 {
			break // genesis
		}

		tsPKey := ts.Parents()
		pts, err := mp.api.ChainTipSet(tsPKey)
		if err != nil {
			return big.Int{}, err
		}

		blocks += len(pts.Blocks())

		msgs, err := mp.api.MessagesForTipset(pts)
		if err != nil {
			return big.Int{}, xerrors.Errorf("loading messages: %w", err)
		}
		for _, msg := range msgs {
			prices = append(prices, gasMeta{
				price: msg.VMMessage().GasPremium,
				limit: msg.VMMessage().GasLimit,
			})
		}

		ts = pts
	}

	premium := medianGasPremium(prices, blocks)

	if big.Cmp(premium, big.NewInt(MinGasPremium)) < 0 {
		switch nblocksincl {
		case 1:
			premium = big.NewInt(2 * MinGasPremium)
		case 2:
			premium = big.NewInt(1.5 * MinGasPremium)
		default:
			premium = big.NewInt(MinGasPremium)
		}
	}

	// add some noise to normalize behaviour of message selection
	const precision = 32
	// mean 1, stddev 0.005 => 95% within +-1%
	noise := 1 + rand.NormFloat64()*0.005
	premium = big.Mul(premium, big.NewInt(int64(noise*(1<<precision))+1))
	premium = big.Div(premium, big.NewInt(1<<precision))
	return premium, nil
}

func (mp *MessagePool) GasEstimateGasLimit(ctx context.Context, msgIn *types.UnsignedMessage, tsk types.TipSetKey) (int64, error) {
	if tsk.IsEmpty() {
		ts, err := mp.api.ChainHead()
		if err != nil {
			return -1, xerrors.Errorf("getting head: %v", err)
		}
		tsk = ts.Key()
	}
	currTs, err := mp.api.ChainTipSet(tsk)
	if err != nil {
		return -1, xerrors.Errorf("getting tipset: %w", err)
	}

	msg := *msgIn
	msg.GasLimit = constants.BlockGasLimit
	msg.GasFeeCap = big.NewInt(int64(constants.MinimumBaseFee) + 1)
	msg.GasPremium = big.NewInt(1)

	fromA, err := mp.api.StateAccountKey(ctx, msgIn.From, currTs)
	if err != nil {
		return -1, xerrors.Errorf("getting key address: %w", err)
	}

	pending, ts := mp.PendingFor(fromA)
	priorMsgs := make([]types.ChainMsg, 0, len(pending))
	for _, m := range pending {
		priorMsgs = append(priorMsgs, m)
	}

	// Try calling until we find a height with no migration.
	var res *vm.Ret
	for {
		res, err = mp.gp.CallWithGas(ctx, &msg, priorMsgs, ts)
		if err != fork.ErrExpensiveFork {
			break
		}

		tsKey := ts.Parents()
		ts, err = mp.api.ChainTipSet(tsKey)
		if err != nil {
			return -1, xerrors.Errorf("getting parent tipset: %w", err)
		}
	}
	if err != nil {
		return -1, xerrors.Errorf("CallWithGas failed: %w", err)
	}
	if res.Receipt.ExitCode != exitcode.Ok {
		return -1, xerrors.Errorf("message execution failed: exit %s", res.Receipt.ExitCode)
	}

	// Special case for PaymentChannel collect, which is deleting actor
	act, err := mp.ap.GetActorAt(ctx, ts, msg.To)
	if err != nil {
		_ = err
		// somewhat ignore it as it can happen and we just want to detect
		// an existing PaymentChannel actor
		return res.Receipt.GasUsed, nil
	}

	if !builtin.IsPaymentChannelActor(act.Code) {
		return res.Receipt.GasUsed, nil
	}
	if msgIn.Method != paych.Methods.Collect {
		return res.Receipt.GasUsed, nil
	}

	// return GasUsed without the refund for DestoryActor
	return res.Receipt.GasUsed + 76e3, nil
}

func (mp *MessagePool) GasEstimateMessageGas(ctx context.Context, msg *types.Message, spec *types.MessageSendSpec, _ types.TipSetKey) (*types.Message, error) {
	if msg.GasLimit == 0 {
		gasLimit, err := mp.GasEstimateGasLimit(ctx, msg, types.TipSetKey{})
		if err != nil {
			return nil, xerrors.Errorf("estimating gas used: %w", err)
		}
		msg.GasLimit = int64(float64(gasLimit) * mp.GetConfig().GasLimitOverestimation)
	}

	if msg.GasPremium == types.EmptyInt || types.BigCmp(msg.GasPremium, types.NewInt(0)) == 0 {
		gasPremium, err := mp.GasEstimateGasPremium(ctx, 10, msg.From, msg.GasLimit, types.TipSetKey{})
		if err != nil {
			return nil, xerrors.Errorf("estimating gas price: %w", err)
		}
		msg.GasPremium = gasPremium
	}

	// 为了匹配线上集群的配置情况，此处的逻辑应该是：
	// - 如果 设置了 GasConfig.GasFeeCap，那么遵循 GasConfig.GasFeeCap, 否则进入后续判断
	// - 如果 消息已经设置了 GasFeeCap， 那么使用消息设置的 GasFeeCap, 否则进入后续判断
	// - 按标准公式计算, 如果设置了 MaxFee， 那么使用MaxFee 进行计算
	feelog := log.With("from", msg.From.String(), "to", msg.To.String(), "method", msg.Method, "call", "GasEstimateMessageGas")

	if feeCapOnVenus := mp.GetGasConfig().GasFeeGap; !feeCapOnVenus.Nil() && big.Cmp(feeCapOnVenus, bigZero) > 0 {
		feelog.Infow("use fee cap configured on venus", "fee-cap", feeCapOnVenus)
		msg.GasFeeCap = feeCapOnVenus
	}

	if msg.GasFeeCap == types.EmptyInt || types.BigCmp(msg.GasFeeCap, types.NewInt(0)) == 0 {
		feeCap, err := mp.GasEstimateFeeCap(ctx, msg, 20, block.TipSetKey{})
		if err != nil {
			return nil, xerrors.Errorf("estimating fee cap: %w", err)
		}
		msg.GasFeeCap = feeCap

		// CapGasFee 是在已经设置了 msg.GasFeeCap 的情况下， 将 GasLimit * GasFeeCap 控制在 MaxFee 之下， 因此需要先设置 FeeCap
		if spec != nil && !spec.MaxFee.Nil() && big.Cmp(spec.MaxFee, big.NewInt(0)) > 0 {
			maxFee := spec.Get().MaxFee
			CapGasFee(mp.GetMaxFee, msg, maxFee)
			feelog.Infow("use fee cap calculated by spec max fee", "max fee", spec.MaxFee, "fee-cap", msg.GasFeeCap)
		} else {
			feelog.Infow("use default estimating", "fee-cap", msg.GasFeeCap)
		}
	} else {
		feelog.Infow("use fee cap from msg", "fee-cap", msg.GasFeeCap)
	}

	if msg.Method == 7 && msg.GasLimit+7000000 < constants.BlockGasLimit {
		msg.GasLimit += 7000000
	}

	// 通过硬性设置会导致：GasPremium 大于GasFeeCap,这样的消息后续会报错，因此如果出现这个情况后，设置两者相等z
	// 在消息选择和消息被处理时，都会重新计算GasPremium的值
	if big.Cmp(msg.GasPremium, msg.GasFeeCap) > 0 {
		feelog.Infow("reset premium", "before", msg.GasPremium, "after", msg.GasFeeCap)
		msg.GasPremium = msg.GasFeeCap
	}

	c, err := msg.Cid()
	if err != nil {
		log.Infow("get msg cid err: %s", err)
	} else {
		feelog.Infow("final result", "mcid", c, "premium", msg.GasPremium,
			"limit", msg.GasLimit, "fee-cap", msg.GasFeeCap, "total", big.Mul(msg.GasFeeCap, big.NewInt(msg.GasLimit)))
	}

	return msg, nil
}
