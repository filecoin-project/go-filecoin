package jsonrpc

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"net"
	// "io/ioutil"
	"net/http"
	"net/url"
	"reflect"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
	"golang.org/x/xerrors"
)

const (
	retryIntervalMin    = 4 * time.Second
	retryIntervalMax    = 60 * time.Second
	retryIntervalFactor = 2
)

// ClientCloser is a cancel func
type ClientCloser = context.CancelFunc

// NewMergeClient parses struct and set it's field with method
func NewMergeClient(ctx context.Context, addr string, namespace string, outs []interface{}, header http.Header, opts ...Option) (ClientCloser, error) {
	u, err := url.Parse(addr)
	if err != nil {
		return nil, xerrors.Errorf("invalid url: %w", err)
	}

	// be compatible with jsonrpc
	if u.Scheme == "ws" {
		u.Scheme = "http"
	}

	addr = u.String()

	rcli, closer := newRPCClient(ctx, namespace, addr, header, http.DefaultTransport)

	for _, handler := range outs {
		htyp := reflect.TypeOf(handler)
		if htyp.Kind() != reflect.Ptr {
			return nil, xerrors.New("expected handler to be a pointer")
		}
		typ := htyp.Elem()
		if typ.Kind() != reflect.Struct {
			return nil, xerrors.New("handler should be a struct")
		}

		val := reflect.ValueOf(handler)

		for i := 0; i < typ.NumField(); i++ {
			field := typ.Field(i)
			ftype := field.Type

			var opt rpcCallOption
			_, opt.failfast = field.Tag.Lookup("failfast")
			rm, err := processsRPCMethod(ftype, 0, field.Name, opt)
			if err != nil {
				return nil, err
			}

			log.Debugw("merged rpc method", "ns", namespace, "name", rm.name, "in", len(rm.inTypes), "chan", rm.returnChan)
			val.Elem().Field(i).Set(reflect.MakeFunc(ftype, rm.makeFunc(rcli)))
		}
	}

	return closer, nil
}

func newRPCClient(ctx context.Context, namespace, remote string, header http.Header, rt http.RoundTripper) (*rpcClient, ClientCloser) {
	running, cancel := context.WithCancel(context.Background())
	return &rpcClient{
		running:    running,
		identifier: randStr(),
		namespace:  namespace,
		remote:     remote,
		header:     header,
		idgen:      &simpleIDGen{},
		rt:         rt,
	}, cancel
}

type rpcClient struct {
	running    context.Context
	identifier string
	namespace  string
	remote     string
	header     http.Header
	idgen      idgen
	rt         http.RoundTripper
}

type rpcCallOption struct {
	// if we should fail fast when net failure occurs
	failfast bool
}

type rpcMethod struct {
	name string

	funcType reflect.Type

	inHasCtx bool
	inTypes  []reflect.Type

	outNum    int
	outValIdx int
	outErrIdx int

	returnChan bool

	opt rpcCallOption
}

func (rm *rpcMethod) makeFunc(cli *rpcClient) func([]reflect.Value) []reflect.Value {

	return func(args []reflect.Value) []reflect.Value {
		if ctxDone(cli.running) {
			return rm.processError(ErrClientClosed)
		}

		var ctx context.Context
		var span *trace.Span
		var meta map[string]string

		if rm.inHasCtx {
			ctx = args[0].Interface().(context.Context)
			ctx, span = trace.StartSpan(ctx, "api.call")
			defer span.End()

			span.AddAttributes(trace.StringAttribute("method", rm.name))

			meta = map[string]string{}
			meta["SpanContext"] = base64.StdEncoding.EncodeToString(
				propagation.Binary(span.SpanContext()))

			args = args[1:]

		} else {
			ctx = context.Background()
		}

		params := make([]any, len(args))
		for i := range args {
			params[i] = any{
				v: args[i],
			}
		}

		startTime := time.Now()
		defer func() {
			log.Debug(fmt.Sprintf("%s.%s", cli.namespace, rm.name), " time:", time.Now().Sub(startTime))
		}()

		rcall, err := newRPCCall(ctx, cli, rm.name, params, rm.opt)
		if err != nil {
			return rm.processError(err)
		}
		rcall.log.Debug("attempting")

		fch, first, err := rcall.waitForFirstFrame()
		if err != nil {
			rcall.close()
			return rm.processError(err)
		}

		if !rm.returnChan {
			return rm.processResponse(rcall, first)
		}

		return rm.processChan(rcall, fch)
	}
}

func (rm *rpcMethod) processResponse(rcall *rpcCall, f *frame) []reflect.Value {
	defer rcall.close()

	if rm.outValIdx == -1 {
		return rm.processReturnValue(nilValue)
	}

	val := reflect.New(rm.funcType.Out(rm.outValIdx))

	rcall.log.Debugf("rpc result type=%v", rm.funcType.Out(rm.outValIdx))
	if f.Result.data != nil {
		if err := json.Unmarshal(f.Result.data, val.Interface()); err != nil {
			return rm.processError(err)
		}
	}

	return rm.processReturnValue(val.Elem())
}

func (rm *rpcMethod) processChan(rcall *rpcCall, fch <-chan frameOrErr) []reflect.Value {
	rcall.log.Debug("chan handshake made")

	elemType := rm.funcType.Out(rm.outValIdx).Elem()
	chType := reflect.ChanOf(reflect.BothDir, elemType)
	outCh := reflect.MakeChan(chType, 1)

	go rcall.startChanLoop(fch, elemType, outCh)

	return rm.processReturnValue(outCh.Convert(rm.funcType.Out(rm.outValIdx)))
}

func (rm *rpcMethod) processReturnValue(val reflect.Value) []reflect.Value {
	out := make([]reflect.Value, rm.outNum)

	if rm.outValIdx != -1 {
		out[rm.outValIdx] = val
	}

	if rm.outErrIdx != -1 {
		out[rm.outErrIdx] = reflect.New(errorType).Elem()
	}

	return out
}

func (rm *rpcMethod) processError(err error) []reflect.Value {
	out := make([]reflect.Value, rm.outNum)

	if rm.outValIdx != -1 {
		out[rm.outValIdx] = reflect.New(rm.funcType.Out(rm.outValIdx)).Elem()
	}

	if rm.outErrIdx != -1 {
		out[rm.outErrIdx] = reflect.New(errorType).Elem()
		out[rm.outErrIdx].Set(reflect.ValueOf(err))
	}

	return out
}

func newRPCCall(ctx context.Context, cli *rpcClient, name string, params []any, opt rpcCallOption) (*rpcCall, error) {
	method := fmt.Sprintf("%s.%s", cli.namespace, name)
	id := cli.idgen.next()

	data, err := json.Marshal(frame{
		Jsonrpc: protoVer,
		Client:  cli.identifier,
		ID:      id,
		Method:  method,
		Params:  params,
	})

	if err != nil {
		return nil, err
	}

	log.Debug("request ", cli.remote, "body ", string(data))
	body := bytes.NewReader(data)
	req, err := http.NewRequest(http.MethodPost, cli.remote, body)
	if err != nil {
		return nil, err
	}

	if cli.header != nil {
		req.Header = cli.header
	}

	ctx, cancel := context.WithCancel(ctx)

	return &rpcCall{
		running: cli.running,
		ctx:     ctx,
		cancel:  cancel,

		req:      req,
		body:     body,
		cli:      cli,
		opt:      opt,
		log:      log.With("method", name, "id", id, "client", cli.identifier),
		interval: retryIntervalMin,
	}, nil
}

type rpcCall struct {
	running context.Context
	ctx     context.Context
	cancel  context.CancelFunc

	req      *http.Request
	body     *bytes.Reader
	cli      *rpcClient
	opt      rpcCallOption
	log      logging.StandardLogger
	interval time.Duration

	reqSent uint64
}

func (rc *rpcCall) waitForFirstFrame() (fch <-chan frameOrErr, first *frame, err error) {
	// we get the first frame or a unrecoverable error
	// either case, we should reset retry interval
	defer rc.reset()

	// first retryCh will be triggered immediately
	immediately := make(chan time.Time, 0)
	close(immediately)
	var retryCh <-chan time.Time = immediately

	setRetryWait := func() {
		retryCh = time.After(rc.interval)

		rc.interval *= retryIntervalFactor
		if rc.interval > retryIntervalMax {
			rc.interval = retryIntervalMax
		}
	}

CALL_LOOP:
	// we will enter next loop on any kind of error
	// and set retryCh before that
	for {

		select {
		case <-rc.running.Done():
			return nil, nil, ErrClientClosed

		case <-rc.ctx.Done():
			return nil, nil, ErrCtxDone

		case <-retryCh:
			fch, err = rc.roundTrip()
			if err != nil {
				if rc.checkForRetry(err) {
					//NOTICE just try when net error
					if rc.opt.failfast || ctxDone(rc.ctx) || ctxDone(rc.running) {
						return nil, nil, err
					}
					rc.log.Warnf("unable to make request, will retry after %s: %s", rc.interval, err)
					setRetryWait()
					continue CALL_LOOP
				} else {
					return nil, nil, err
				}
			}
		}

		select {
		case <-rc.ctx.Done():
			return nil, nil, ErrClientClosed

		case <-rc.ctx.Done():
			return nil, nil, ErrCtxDone

		case first, ok := <-fch:
			var firstErr error
			if !ok {
				firstErr = ErrEmptyResponse
			} else {
				firstErr = first.err
			}

			if firstErr != nil {
				if rc.opt.failfast || ctxDone(rc.ctx) || ctxDone(rc.running) {
					return nil, nil, firstErr
				}

				rc.log.Warnf("unable to get a valid frame, will retry after %s: %s", rc.interval, err)
				setRetryWait()

				continue CALL_LOOP
			}

			if first.frame.Error != nil {
				return nil, nil, first.frame.Error
			}

			return fch, first.frame, nil
		}
	}
}

func (rc *rpcCall) checkForRetry(err error) bool {
	_, ok := err.(net.Error)
	if ok {
		return true
	}

	if err == io.EOF || err == io.ErrUnexpectedEOF || err == io.ErrShortWrite || err == io.ErrShortBuffer || err == io.ErrNoProgress {
		return true
	}
	return false
}
func (rc *rpcCall) roundTrip() (<-chan frameOrErr, error) {
	rc.body.Seek(0, io.SeekStart) // nolint: errcheck

	req := rc.req.WithContext(rc.ctx)
	resp, err := rc.cli.rt.RoundTrip(req)
	if err != nil {
		return nil, err
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()                // nolint: errcheck
		return nil, &http.ProtocolError{ // nolint
			ErrorString: fmt.Sprintf("unexpected response code %d: %s", resp.StatusCode, http.StatusText(resp.StatusCode)),
		}
	}

	rc.reqSent++
	if rc.reqSent > 1 {
		rc.log.Warnf("request has been sent for %d times", rc.reqSent)
	}

	freader := newFrameReader(resp.Body)
	out := freader.read(rc.ctx)
	return out, nil
}

func (rc *rpcCall) startChanLoop(fch <-chan frameOrErr, elemType reflect.Type, outCh reflect.Value) {
	defer func() {
		outCh.Close()
		rc.close()
	}()

	refSelectCases := [3]reflect.SelectCase{}
	refSelectCases[0] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(rc.running.Done()),
	}
	refSelectCases[1] = reflect.SelectCase{
		Dir:  reflect.SelectRecv,
		Chan: reflect.ValueOf(rc.ctx.Done()),
	}

CHAN_LOOP:
	for {
		select {
		case <-rc.running.Done():
			rc.log.Warn("client closed")
			return

		case <-rc.ctx.Done():
			rc.log.Debug("request context done on receiving incoming chan value")
			return

		case incoming, ok := <-fch:
			var ierr error
			if !ok {
				ierr = ErrChanClosed
			} else {
				ierr = incoming.err
			}

			if ierr != nil {
				rc.log.Warnf("err captured on receiving chan value: %s", ierr)

				fch, _, ierr = rc.waitForFirstFrame()
				if ierr != nil {
					rc.log.Errorf("unrecoverable err captured on re-establishing the chan: %s", ierr)
					return
				}

				continue CHAN_LOOP
			}

			switch incoming.frame.Method {
			case frameMethodChanClose:
				rc.log.Debugf("%s frame received", incoming.frame.Method)
				return

			case frameMethodChanValue:

			default:
				rc.log.Warnf("unexpected frame %q", incoming.frame.Method)
				continue CHAN_LOOP
			}

			val := reflect.New(elemType)
			if incoming.frame.Result.data != nil {
				if err := json.Unmarshal(incoming.frame.Result.data, val.Interface()); err != nil {
					rc.log.Warnf("unable to unmarshal incoming chan value: %s", err)
					continue CHAN_LOOP
				}
			}

			refSelectCases[2] = reflect.SelectCase{
				Dir:  reflect.SelectSend,
				Chan: outCh,
				Send: val.Elem(),
			}

			choose, _, _ := reflect.Select(refSelectCases[:])
			switch choose {
			case 0:
				rc.log.Warn("client closed on sending new chan value")
				return

			case 1:
				rc.log.Debug("request context done on sending new chan value")
				return
			}
		}
	}

}

func (rc *rpcCall) reset() {
	rc.interval = retryIntervalMin
}

func (rc *rpcCall) close() {
	rc.cancel()
}
