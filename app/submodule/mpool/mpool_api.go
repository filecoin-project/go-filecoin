package mpool

import (
	"context"
	"encoding/json"
	"sync"

	"github.com/filecoin-project/go-state-types/crypto"

	"github.com/filecoin-project/go-address"
	"github.com/ipfs/go-cid"
	"golang.org/x/xerrors"

	"github.com/filecoin-project/go-state-types/abi"
	"github.com/filecoin-project/go-state-types/big"
	"github.com/filecoin-project/venus/pkg/messagepool"
	"github.com/filecoin-project/venus/pkg/types"
	"github.com/filecoin-project/venus/pkg/wallet"
)

type IMessagePool interface {
	DeleteByAdress(ctx context.Context, addr address.Address) error
	MpoolPublish(ctx context.Context, addr address.Address) error
	MpoolPush(ctx context.Context, smsg *types.SignedMessage) (cid.Cid, error)
	MpoolGetConfig(context.Context) (*messagepool.MpoolConfig, error)
	MpoolSetConfig(ctx context.Context, cfg *messagepool.MpoolConfig) error
	MpoolSelect(ctx context.Context, tsk types.TipSetKey, ticketQuality float64) ([]*types.SignedMessage, error)
	MpoolPending(ctx context.Context, tsk types.TipSetKey) ([]*types.SignedMessage, error)
	MpoolClear(ctx context.Context, local bool) error
	MpoolPushUntrusted(ctx context.Context, smsg *types.SignedMessage) (cid.Cid, error)
	MpoolPushMessage(ctx context.Context, msg *types.UnsignedMessage, spec *types.MessageSendSpec) (*types.SignedMessage, error)
	MpoolBatchPush(ctx context.Context, smsgs []*types.SignedMessage) ([]cid.Cid, error)
	MpoolBatchPushUntrusted(ctx context.Context, smsgs []*types.SignedMessage) ([]cid.Cid, error)
	MpoolBatchPushMessage(ctx context.Context, msgs []*types.UnsignedMessage, spec *types.MessageSendSpec) ([]*types.SignedMessage, error)
	MpoolGetNonce(ctx context.Context, addr address.Address) (uint64, error)
	MpoolSub(ctx context.Context) (<-chan messagepool.MpoolUpdate, error)
	SendMsg(ctx context.Context, from, to address.Address, method abi.MethodNum, value, maxFee abi.TokenAmount, params []byte) (cid.Cid, error)
	GasEstimateMessageGas(ctx context.Context, msg *types.UnsignedMessage, spec *types.MessageSendSpec, tsk types.TipSetKey) (*types.UnsignedMessage, error)
	GasEstimateFeeCap(ctx context.Context, msg *types.UnsignedMessage, maxqueueblks int64, tsk types.TipSetKey) (big.Int, error)
	GasEstimateGasPremium(ctx context.Context, nblocksincl uint64, sender address.Address, gaslimit int64, tsk types.TipSetKey) (big.Int, error)
	WalletSign(ctx context.Context, k address.Address, msg []byte) (*crypto.Signature, error)
	WalletHas(ctx context.Context, addr address.Address) (bool, error)
}

type MessagePoolAPI struct {
	pushLocks *messagepool.MpoolLocker
	lk        sync.Mutex

	mp *MessagePoolSubmodule
}

func (a *MessagePoolAPI) DeleteByAdress(ctx context.Context, addr address.Address) error {
	return a.mp.MPool.DeleteByAdress(addr)
}

func (a *MessagePoolAPI) MpoolPublish(ctx context.Context, addr address.Address) error {
	return a.mp.MPool.PublishMsgForWallet(addr)

}

func (a *MessagePoolAPI) MpoolPush(ctx context.Context, smsg *types.SignedMessage) (cid.Cid, error) {
	return a.mp.MPool.Push(smsg)
}

func (a *MessagePoolAPI) MpoolGetConfig(context.Context) (*messagepool.MpoolConfig, error) {
	return a.mp.MPool.GetConfig(), nil
}

func (a *MessagePoolAPI) MpoolSetConfig(ctx context.Context, cfg *messagepool.MpoolConfig) error {
	return a.mp.MPool.SetConfig(cfg)
}

func (a *MessagePoolAPI) MpoolGetGasConfig(context.Context) (*messagepool.MpoolGasConfig, error) {
	return a.mp.MPool.GetGasConfig(), nil
}

func (a *MessagePoolAPI) MpoolSetGasConfig(ctx context.Context, cfg *messagepool.MpoolGasConfig) error {
	return a.mp.MPool.SetGasConfig(cfg)
}

func (a *MessagePoolAPI) MpoolSelect(ctx context.Context, tsk block.TipSetKey, ticketQuality float64) ([]*types.SignedMessage, error) {
	ts, err := a.mp.chain.API().ChainGetTipSet(tsk)
	if err != nil {
		return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
	}

	return a.mp.MPool.SelectMessages(ts, ticketQuality)
}

func (a *MessagePoolAPI) MpoolPending(ctx context.Context, tsk types.TipSetKey) ([]*types.SignedMessage, error) {
	var ts *types.TipSet
	var err error
	if tsk.IsEmpty() {
		ts, err = a.mp.chain.API().ChainHead(ctx)
		if err != nil {
			return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
		}
	} else {
		ts, err = a.mp.chain.API().ChainGetTipSet(tsk)
		if err != nil {
			return nil, xerrors.Errorf("loading tipset %s: %w", tsk, err)
		}
	}

	pending, mpts := a.mp.MPool.Pending()

	haveCids := map[cid.Cid]struct{}{}
	for _, m := range pending {
		haveCids[m.Cid()] = struct{}{}
	}

	mptsH := mpts.Height()
	tsH := ts.Height()
	if ts == nil || mptsH > tsH {
		return pending, nil
	}

	for {
		mptsH = mpts.Height()
		tsH = ts.Height()
		if mptsH == tsH {
			if mpts.Equals(ts) {
				return pending, nil
			}
			// different blocks in tipsets

			have, err := a.mp.MPool.MessagesForBlocks(ts.Blocks())
			if err != nil {
				return nil, xerrors.Errorf("getting messages for base ts: %w", err)
			}

			for _, m := range have {
				haveCids[m.Cid()] = struct{}{}
			}
		}

		msgs, err := a.mp.MPool.MessagesForBlocks(ts.Blocks())
		if err != nil {
			return nil, xerrors.Errorf(": %w", err)
		}

		for _, m := range msgs {
			mc := m.Cid()
			if _, ok := haveCids[mc]; ok {
				continue
			}

			haveCids[mc] = struct{}{}
			pending = append(pending, m)
		}

		mptsH = mpts.Height()
		tsH = ts.Height()
		if mptsH >= tsH {
			return pending, nil
		}

		ts, err = a.mp.chain.API().ChainGetTipSet(ts.Parents())
		if err != nil {
			return nil, xerrors.Errorf("loading parent tipset: %w", err)
		}
	}
}

func (a *MessagePoolAPI) MpoolClear(ctx context.Context, local bool) error {
	a.mp.MPool.Clear(local)
	return nil
}

func (a *MessagePoolAPI) MpoolPushUntrusted(ctx context.Context, smsg *types.SignedMessage) (cid.Cid, error) {
	return a.mp.MPool.PushUntrusted(smsg)
}

func (a *MessagePoolAPI) MpoolPushMessage(ctx context.Context, msg *types.UnsignedMessage, spec *types.MessageSendSpec) (*types.SignedMessage, error) {
	cp := *msg
	msg = &cp
	inMsg := *msg
	ts, err := a.mp.chain.API().ChainHead(ctx)
	if err != nil {
		return nil, xerrors.Errorf("getting tipset error: %v", err)
	}
	fromA, err := a.mp.chain.API().ResolveToKeyAddr(ctx, msg.From, ts)
	if err != nil {
		return nil, xerrors.Errorf("getting key address: %w", err)
	}
	{
		done, err := a.pushLocks.TakeLock(ctx, fromA)
		if err != nil {
			return nil, xerrors.Errorf("taking lock: %w", err)
		}
		defer done()
	}

	if msg.Nonce != 0 {
		return nil, xerrors.Errorf("MpoolPushMessage expects message nonce to be 0, was %d", msg.Nonce)
	}

	msg, err = a.GasEstimateMessageGas(ctx, msg, spec, types.TipSetKey{})
	if err != nil {
		return nil, xerrors.Errorf("GasEstimateMessageGas error: %w", err)
	}

	if msg.GasPremium.GreaterThan(msg.GasFeeCap) {
		inJSON, err := json.Marshal(inMsg)
		if err != nil {
			return nil, err
		}
		outJSON, err := json.Marshal(msg)
		if err != nil {
			return nil, err
		}
		return nil, xerrors.Errorf("After estimation, GasPremium is greater than GasFeeCap, inmsg: %s, outmsg: %s",
			inJSON, outJSON)
	}

	if msg.From.Protocol() == address.ID {
		log.Warnf("Push from ID address (%s), adjusting to %s", msg.From, fromA)
		msg.From = fromA
	}

	b, err := a.mp.walletAPI.WalletBalance(ctx, msg.From)
	if err != nil {
		return nil, xerrors.Errorf("mpool push: getting origin balance: %w", err)
	}

	if b.LessThan(msg.Value) {
		return nil, xerrors.Errorf("mpool push: not enough funds: %s < %s", b, msg.Value)
	}

	// Todo Define SignMessage ???
	SignMessage := func(ctx context.Context, msg *types.UnsignedMessage, cb func(*types.SignedMessage) error) (*types.SignedMessage, error) {
		a.lk.Lock()
		defer a.lk.Unlock()

		nonce, err := a.mp.MPool.GetNonce(msg.From)
		if err != nil {
			return nil, err
		}

		// Sign the message with the nonce
		msg.Nonce = nonce

		mb, err := msg.ToStorageBlock()
		if err != nil {
			return nil, xerrors.Errorf("serializing message: %w", err)
		}

		sig, err := a.mp.walletAPI.WalletSign(ctx, msg.From, mb.Cid().Bytes(), wallet.MsgMeta{})
		if err != nil {
			return nil, xerrors.Errorf("failed to sign message: %w", err)
		}

		// Callback with the signed message
		smsg := &types.SignedMessage{
			Message:   *msg,
			Signature: *sig,
		}
		err = cb(smsg)
		if err != nil {
			return nil, err
		}

		return smsg, nil
	}

	// Sign and push the message
	return SignMessage(ctx, msg, func(smsg *types.SignedMessage) error {
		if _, err := a.MpoolPush(ctx, smsg); err != nil {
			return xerrors.Errorf("mpool push: failed to push message: %w", err)
		}
		return nil
	})
}

func (a *MessagePoolAPI) MpoolBatchPush(ctx context.Context, smsgs []*types.SignedMessage) ([]cid.Cid, error) {
	var messageCids []cid.Cid
	for _, smsg := range smsgs {
		smsgCid, err := a.mp.MPool.Push(smsg)
		if err != nil {
			return messageCids, err
		}
		messageCids = append(messageCids, smsgCid)
	}
	return messageCids, nil
}

func (a *MessagePoolAPI) MpoolBatchPushUntrusted(ctx context.Context, smsgs []*types.SignedMessage) ([]cid.Cid, error) {
	var messageCids []cid.Cid
	for _, smsg := range smsgs {
		smsgCid, err := a.mp.MPool.PushUntrusted(smsg)
		if err != nil {
			return messageCids, err
		}
		messageCids = append(messageCids, smsgCid)
	}
	return messageCids, nil
}

func (a *MessagePoolAPI) MpoolBatchPushMessage(ctx context.Context, msgs []*types.UnsignedMessage, spec *types.MessageSendSpec) ([]*types.SignedMessage, error) {
	var smsgs []*types.SignedMessage
	for _, msg := range msgs {
		smsg, err := a.MpoolPushMessage(ctx, msg, spec)
		if err != nil {
			return smsgs, err
		}
		smsgs = append(smsgs, smsg)
	}
	return smsgs, nil
}

func (a *MessagePoolAPI) MpoolGetNonce(ctx context.Context, addr address.Address) (uint64, error) {
	return a.mp.MPool.GetNonce(addr)
}

func (a *MessagePoolAPI) MpoolSub(ctx context.Context) (<-chan messagepool.MpoolUpdate, error) {
	return a.mp.MPool.Updates(ctx)
}

func (a *MessagePoolAPI) SendMsg(ctx context.Context, from, to address.Address, method abi.MethodNum, value, maxFee abi.TokenAmount, params []byte) (cid.Cid, error) {
	msg := types.UnsignedMessage{
		To:     to,
		From:   from,
		Value:  value,
		Method: method,
		Params: params,
	}

	smsg, err := a.MpoolPushMessage(ctx, &msg, &types.MessageSendSpec{MaxFee: maxFee})
	if err != nil {
		return cid.Undef, err
	}

	return smsg.Cid(), nil
}

func (a *MessagePoolAPI) GasEstimateMessageGas(ctx context.Context, msg *types.UnsignedMessage, spec *types.MessageSendSpec, tsk types.TipSetKey) (*types.UnsignedMessage, error) {
	return a.mp.MPool.GasEstimateMessageGas(ctx, msg, spec, tsk)
}

func (a *MessagePoolAPI) GasEstimateFeeCap(ctx context.Context, msg *types.UnsignedMessage, maxqueueblks int64, tsk types.TipSetKey) (big.Int, error) {
	return a.mp.MPool.GasEstimateFeeCap(ctx, msg, maxqueueblks, tsk)
}

func (a *MessagePoolAPI) GasEstimateGasPremium(ctx context.Context, nblocksincl uint64, sender address.Address, gaslimit int64, tsk types.TipSetKey) (big.Int, error) {
	return a.mp.MPool.GasEstimateGasPremium(ctx, nblocksincl, sender, gaslimit, tsk)
}

func (a *MessagePoolAPI) WalletSign(ctx context.Context, k address.Address, msg []byte) (*crypto.Signature, error) {
	head := a.mp.chain.ChainReader.GetHead()
	view, err := a.mp.chain.ChainReader.StateView(head)
	if err != nil {
		return nil, err
	}

	keyAddr, err := view.ResolveToKeyAddr(ctx, k)
	if err != nil {
		return nil, xerrors.Errorf("failed to resolve ID address: %v", keyAddr)
	}
	return a.mp.walletAPI.WalletSign(ctx, keyAddr, msg, wallet.MsgMeta{
		Type: wallet.MTUnknown,
	})
}

func (a *MessagePoolAPI) WalletHas(ctx context.Context, addr address.Address) (bool, error) {
	return a.mp.walletAPI.WalletHas(ctx, addr)
}
