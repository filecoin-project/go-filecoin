package state

import (
	"context"

	addr "github.com/filecoin-project/go-address"
	"github.com/pkg/errors"

	"github.com/filecoin-project/venus/pkg/crypto"
	"github.com/filecoin-project/venus/pkg/types"
)

type AccountStateView interface {
	AccountSignerAddress(ctx context.Context, a addr.Address) (addr.Address, error)
}

//
// SignatureValidator resolves account actor addresses to their pubkey-style address for signature validation.
//
type SignatureValidator struct {
	state AccountStateView
}

func NewSignatureValidator(state AccountStateView) *SignatureValidator {
	return &SignatureValidator{state: state}
}

func (v *SignatureValidator) ValidateSignature(ctx context.Context, data []byte, signer addr.Address, sig crypto.Signature) error {
	signerAddress, err := v.state.AccountSignerAddress(ctx, signer)
	if err != nil {
		return errors.Wrapf(err, "failed to load signer address for %v", signer)
	}
	return crypto.ValidateSignature(data, signerAddress, sig)
}

func (v *SignatureValidator) ValidateMessageSignature(ctx context.Context, msg *types.SignedMessage) error {
	mCid := msg.Message.Cid()
	return v.ValidateSignature(ctx, mCid.Bytes(), msg.Message.From, msg.Signature)
}

func (v *SignatureValidator) ValidateBLSMessageAggregate(ctx context.Context, msgs []*types.UnsignedMessage, sig *crypto.Signature) error {
	if sig == nil {
		if len(msgs) > 0 {
			return errors.New("Invalid empty BLS sig over messages")
		}
		return nil
	}

	if len(msgs) == 0 {
		return nil
	}

	pubKeys := [][]byte{}
	encodedMsgCids := [][]byte{}
	for _, msg := range msgs {
		signerAddress, err := v.state.AccountSignerAddress(ctx, msg.From)
		if err != nil {
			return errors.Wrapf(err, "failed to load signer address for %v", msg.From)
		}
		pubKeys = append(pubKeys, signerAddress.Payload())
		mCid := msg.Cid()
		encodedMsgCids = append(encodedMsgCids, mCid.Bytes())
	}

	if !crypto.VerifyBLSAggregate(pubKeys, encodedMsgCids, sig.Data) {
		return errors.New("BLS signature invalid")
	}
	return nil
}
