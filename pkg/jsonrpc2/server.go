package jsonrpc

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"net/http"
	"reflect"
	"runtime/debug"
	"time"

	logging "github.com/ipfs/go-log/v2"
	"go.opencensus.io/trace"
	"go.opencensus.io/trace/propagation"
	"golang.org/x/xerrors"
)

const (
	rpcParseError     = -32700
	rpcMethodNotFound = -32601
	rpcInvalidParams  = -32602
)

type rpcHandler struct {
	*rpcMethod

	receiver    reflect.Value
	handlerFunc reflect.Value
}

func (rh *rpcHandler) call(args []reflect.Value) (out []reflect.Value, err error) {
	defer func() {
		if i := recover(); i != nil {
			err = xerrors.Errorf("panic in rpc method %q: %s", rh.name, i)
			log.Error(err)
			fmt.Printf("%s", debug.Stack())
		}
	}()

	out = rh.handlerFunc.Call(args)
	return
}

// NewServer creates a new RPCServer instance
func NewServer() *RPCServer {
	return &RPCServer{
		handlers: map[string]rpcHandler{},
	}
}

// RPCServer wraps local methods and serve as a http server
type RPCServer struct {
	handlers map[string]rpcHandler
}

func (rs *RPCServer) ServeHTTP(rw http.ResponseWriter, req *http.Request) {
	var reqFrame frame
	err := json.NewDecoder(req.Body).Decode(&reqFrame)
	req.Body.Close() // nolint: errcheck

	if err != nil {
		handleErrorResponse(nil, rw, rpcParseError, err)
		return
	}

	rw.Header().Set("Access-Control-Allow-Origin", "*")
	rs.handle(req.Context(), rw, reqFrame)
}

// Register registers rpc handlers
func (rs *RPCServer) Register(namespace string, handler interface{}) {
	log.Infof("registering handlers in %q", namespace)

	rv := reflect.ValueOf(handler)
	rt := rv.Type()

	for i := 0; i < rv.NumMethod(); i++ {
		meth := rt.Method(i)

		ftype := meth.Func.Type()
		rm, err := processsRPCMethod(ftype, 1, meth.Name, rpcCallOption{})
		if err != nil {
			log.Warnf("unable to register method %q: %s", meth.Name, err)
			continue
		}

		log.Debugw("handler registered", "ns", namespace, "meth", meth.Name, "in", len(rm.inTypes), "chan", rm.returnChan)
		rs.handlers[fmt.Sprintf("%s.%s", namespace, meth.Name)] = rpcHandler{
			rpcMethod:   rm,
			handlerFunc: meth.Func,
			receiver:    rv,
		}
	}
}

func (rs *RPCServer) handle(ctx context.Context, rw http.ResponseWriter, req frame) {
	rreq := newRPCRequest(ctx, rw, req)
	defer rreq.finish()

	//rpc命令可能卡死，记录进去的时间及方法名称
	now := time.Now()
	log.Debug("rpcserver handle ", "start:", now, " method:", req.Method)

	defer func() {
		if time.Since(now).Seconds() > 1 {
			log.Infow("rpcserver handle spent too long", "spent:", time.Since(now), " req.method:", req.Method)
		}
	}()
	handler, ok := rs.handlers[req.Method]
	if !ok {
		rreq.handleError(rpcMethodNotFound, fmt.Errorf("method %q not found", req.Method))
		return
	}

	nParams := len(handler.inTypes)

	if len(req.Params) != nParams {
		rreq.handleError(rpcInvalidParams, fmt.Errorf("expected %d params, got %d", nParams, len(req.Params)))
		return
	}

	nCallArgs := 1 + nParams
	paramStart := 1
	if handler.inHasCtx {
		nCallArgs++
		paramStart++
	}

	args := make([]reflect.Value, nCallArgs)
	args[0] = handler.receiver
	if handler.inHasCtx {
		args[1] = reflect.ValueOf(ctx)
	}

	for i := range handler.inTypes {
		rp := reflect.New(handler.inTypes[i])
		if err := json.Unmarshal(req.Params[i].data, rp.Interface()); err != nil {
			rreq.handleError(rpcParseError, err)
			return
		}

		args[paramStart+i] = reflect.ValueOf(rp.Elem().Interface())
	}

	ret, err := handler.call(args)
	if err != nil {
		rreq.handleError(1, xerrors.Errorf("fatal error calling: %w", err))
		return
	}

	if handler.outErrIdx != -1 {
		err := ret[handler.outErrIdx].Interface()
		if err != nil {
			log.Warnf("error in RPC call to '%s': %+v", req.Method, err)
			rreq.handleError(1, err.(error))
			return
		}
	}

	if handler.outValIdx == -1 {
		rreq.handleReturn(nilValue)
		return
	}

	if !handler.returnChan {
		rreq.handleReturn(ret[handler.outValIdx])
		return
	}

	rreq.handleChan(ret[handler.outValIdx])
}

func newRPCRequest(ctx context.Context, rw http.ResponseWriter, req frame) *rpcRequest {
	ctx, span := getSpan(ctx, &req)
	return &rpcRequest{
		ctx:  ctx,
		span: span,
		req:  req,
		rw:   rw,
		log:  log.With("method", req.Method, "id", req.ID, "from", req.Client),
	}
}

type rpcRequest struct {
	ctx  context.Context
	span *trace.Span
	req  frame
	rw   http.ResponseWriter
	log  logging.StandardLogger
}

func (rr *rpcRequest) handleError(code int, err error) {
	rr.log.Debug("handling error")
	handleErrorResponse(rr.log, rr.rw, code, err)
}

func (rr *rpcRequest) handleReturn(ret reflect.Value) {
	rr.log.Debugf("handling return")

	rr.rw.WriteHeader(http.StatusOK)
	f := frame{
		Jsonrpc: protoVer,
		Result: any{
			v: ret,
		},
	}
	b, _ := json.Marshal(f)
	rr.log.Debug(string(b))

	if err := json.NewEncoder(rr.rw).Encode(frame{
		Jsonrpc: protoVer,
		Result: any{
			v: ret,
		},
	}); err != nil {
		log.Warnf("unable to output response frame: %s", err)
		return
	}
}

func (rr *rpcRequest) handleChan(inCh reflect.Value) {
	rr.log.Debugf("handling chan")

	rr.rw.Header().Set("Transfer-Encoding", "chunked")
	rr.rw.WriteHeader(http.StatusOK)

	flusher, ok := rr.rw.(http.Flusher)
	if !ok {
		rr.log.Warnf("non-flusher http.ResponseWriter %v", rr.rw)
	}

	defer func() {
		rr.log.Debugf("rpc chan closed")

		if ok {
			flusher.Flush()
		}
	}()

	encoder := json.NewEncoder(rr.rw)
	if err := encoder.Encode(frame{
		Jsonrpc: protoVer,
		Method:  frameMethodChanStart,
	}); err != nil {
		log.Errorf("unable to output %q frame: %s", frameMethodChanStart, err)
		return
	}

	scases := []reflect.SelectCase{
		{
			Chan: reflect.ValueOf(rr.ctx.Done()),
			Dir:  reflect.SelectRecv,
		},
		{
			Chan: inCh,
			Dir:  reflect.SelectRecv,
		},
	}

	for {
		choise, val, vok := reflect.Select(scases)
		switch choise {
		case 0:
			return

		case 1:
			if !vok {
				if err := encoder.Encode(frame{
					Jsonrpc: protoVer,
					Method:  frameMethodChanClose,
				}); err != nil {
					log.Warnf("unable to output %s frame: %s", frameMethodChanClose, err)
				}

				return
			}

			if err := encoder.Encode(frame{
				Jsonrpc: protoVer,
				Method:  frameMethodChanValue,
				Result: any{
					v: val,
				},
			}); err != nil {
				log.Warnf("unable to output chan value frame: %s", err)
				return
			}

			if ok {
				flusher.Flush()
			}
		}
	}
}

func (rr *rpcRequest) finish() {
	rr.log.Debug("request done")
	if rr.span != nil {
		rr.span.End()
	}
}

func handleErrorResponse(l logging.StandardLogger, rw http.ResponseWriter, code int, err error) {
	if l == nil {
		l = log
	}

	l.Warnf("request failed %d: %s", code, err)

	rw.WriteHeader(http.StatusOK)
	if err := json.NewEncoder(rw).Encode(frame{
		Jsonrpc: protoVer,
		Error: &respError{
			Code:    code,
			Message: err.Error(),
		},
	}); err != nil {
		l.Warnf("unable to output error frame: %s", err)
		return
	}
}

func getSpan(ctx context.Context, req *frame) (context.Context, *trace.Span) {
	if req == nil || len(req.Meta) == 0 {
		return ctx, nil
	}

	if eSC, ok := req.Meta["SpanContext"]; ok {
		bSC := make([]byte, base64.StdEncoding.DecodedLen(len(eSC)))
		_, err := base64.StdEncoding.Decode(bSC, []byte(eSC))
		if err != nil {
			log.Warnw("SpanContext: decode", "error", err)
			return ctx, nil
		}
		sc, ok := propagation.FromBinary(bSC)
		if !ok {
			log.Warnw("SpanContext: could not create span", "data", bSC)
			return ctx, nil
		}
		ctx, span := trace.StartSpanWithRemoteParent(ctx, "api.handle", sc)
		span.AddAttributes(trace.StringAttribute("method", req.Method))
		return ctx, span
	}

	return ctx, nil
}
