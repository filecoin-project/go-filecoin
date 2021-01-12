package jsonrpc

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/rand"
	"reflect"
	"sync/atomic"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"golang.org/x/xerrors"
)

var log = logging.Logger("jsonrpc2")

// common errors
var (
	ErrCtxDone       = fmt.Errorf("context done")
	ErrEmptyResponse = fmt.Errorf("empty response")
	ErrChanClosed    = fmt.Errorf("chan closed")
	ErrClientClosed  = fmt.Errorf("client closed")
)

var (
	errorType   = reflect.TypeOf(new(error)).Elem()
	contextType = reflect.TypeOf(new(context.Context)).Elem()
)

func randStr() string {
	var b [8]byte
	rand.New(rand.NewSource(time.Now().UnixNano())).Read(b[:])
	return base64.URLEncoding.EncodeToString(b[:])
}

func ctxDone(ctx context.Context) bool {
	select {
	case <-ctx.Done():
		return true

	default:
		return false
	}
}

type idgen interface {
	next() uint64
}

type simpleIDGen struct {
	id uint64
}

func (g *simpleIDGen) next() uint64 {
	return atomic.AddUint64(&g.id, 1)
}

// processFuncOut finds value and error Outs in function
func processFuncOut(funcType reflect.Type) (valOut int, errOut int, n int) {
	errOut = -1 // -1 if not found
	valOut = -1
	n = funcType.NumOut()

	switch n {
	case 0:
	case 1:
		if funcType.Out(0) == errorType {
			errOut = 0
		} else {
			valOut = 0
		}
	case 2:
		valOut = 0
		errOut = 1
		if funcType.Out(1) != errorType {
			panic("expected error as second return value")
		}
	default:
		errstr := fmt.Sprintf("too many return values: %s", funcType)
		panic(errstr)
	}

	return
}

func processsRPCMethod(ftyp reflect.Type, hasRecv int, name string, opt rpcCallOption) (*rpcMethod, error) {
	if ftyp.Kind() != reflect.Func {
		return nil, xerrors.New("method must be a func")
	}

	rm := &rpcMethod{
		name:     name,
		funcType: ftyp,
		opt:      opt,
	}

	rm.outValIdx, rm.outErrIdx, rm.outNum = processFuncOut(ftyp)

	hasCtx := 0
	if ftyp.NumIn() > hasRecv && ftyp.In(hasRecv) == contextType {
		hasCtx = 1
	}
	rm.inHasCtx = hasCtx == 1

	ins := ftyp.NumIn() - hasRecv - hasCtx
	rm.inTypes = make([]reflect.Type, ins)
	for i := 0; i < ins; i++ {
		rm.inTypes[i] = ftyp.In(i + hasRecv + hasCtx)
	}

	rm.returnChan = rm.outValIdx != -1 && ftyp.Out(rm.outValIdx).Kind() == reflect.Chan

	return rm, nil
}
