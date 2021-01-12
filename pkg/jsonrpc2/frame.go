package jsonrpc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"golang.org/x/xerrors"
)

const (
	frameMethodCancel    = "xrpc.cancel" // nolint
	frameMethodChanStart = "xrpc.ch.start"
	frameMethodChanValue = "xrpc.ch.val"
	frameMethodChanClose = "xrpc.ch.close"
)

const (
	protoVer = "2.0"
)

var (
	emptyValue reflect.Value
	nilValue   = reflect.ValueOf(nil)
)

type any struct {
	data json.RawMessage // from unmarshal

	v reflect.Value // to marshal
}

func (p *any) UnmarshalJSON(raw []byte) error {
	return p.data.UnmarshalJSON(raw)
}

func (p any) MarshalJSON() ([]byte, error) {
	if p.v == emptyValue || p.v == nilValue {
		return json.Marshal(nil)
	}

	return json.Marshal(p.v.Interface())
}

var _ error = &respError{}

type respError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

func (e *respError) Error() string {
	if e.Code >= -32768 && e.Code <= -32000 {
		return fmt.Sprintf("RPC error (%d): %s", e.Code, e.Message)
	}
	return e.Message
}

type frame struct {
	// common
	Jsonrpc string            `json:"jsonrpc"`
	Client  string            `json:"client,omitempty"`
	ID      uint64            `json:"id,omitempty"`
	Meta    map[string]string `json:"meta,omitempty"`

	// request
	Method string `json:"method,omitempty"`
	Params []any  `json:"params,omitempty"`

	// response
	Result any        `json:"result,omitempty"`
	Error  *respError `json:"error,omitempty"`
}

type frameOrErr struct {
	frame *frame
	err   error
}

func newFrameReader(r io.ReadCloser) *frameReader {
	return &frameReader{
		r: r,
	}
}

type frameReader struct {
	r io.ReadCloser
}

func (fr *frameReader) read(ctx context.Context) <-chan frameOrErr {
	out := make(chan frameOrErr)

	onErr := func(err error) {
		select {
		case <-ctx.Done():
			return

		case out <- frameOrErr{
			frame: nil,
			err:   xerrors.Errorf("unable to unmarshal data frame: %w", err),
		}:

		}
	}

	onFrame := func(f *frame) {
		select {
		case <-ctx.Done():
			return

		case out <- frameOrErr{
			frame: f,
			err:   nil,
		}:

		}
	}

	go func() {
		defer func() {
			close(out)
			fr.r.Close() // nolint: errcheck
		}()

		decoder := json.NewDecoder(fr.r)
		for {
			if ctxDone(ctx) {
				return
			}

			var f frame
			err := decoder.Decode(&f)
			if err == io.EOF {
				return
			}

			if err != nil {
				onErr(err)
				continue

			}

			onFrame(&f)
		}
	}()

	return out
}
