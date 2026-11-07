package request

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"runtime/debug"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liquidgecka/blobby/internal/sloghelper"
	"github.com/liquidgecka/blobby/internal/tracing"
)

var (
	nextRequestID uint64
	requestPool   = sync.Pool{
		New: func() interface{} {
			return new(Request)
		},
	}
)

type Request struct {
	Context     context.Context
	ID          uint64
	Request     *http.Request
	start       time.Time
	trace       *tracing.Trace
	bodyWrapper bodyWrapper
	replyBytes  int64
	response    http.ResponseWriter
	statusCode  int
	Log         *slog.Logger
	baseLog     *slog.Logger
}

// Get acquires a Request from the pool and initializes it.
func Get(
	w http.ResponseWriter,
	req *http.Request,
	l *slog.Logger,
) *Request {
	r := requestPool.Get().(*Request)
	r.ID = atomic.AddUint64(&nextRequestID, 1)
	r.Context = req.Context()
	r.Request = req
	r.start = time.Now()
	r.bodyWrapper.in = req.Body
	r.bodyWrapper.size = 0
	r.response = w
	r.replyBytes = 0
	r.statusCode = 0
	r.trace = nil
	r.Log = nil
	r.baseLog = l
	if l != nil && l.Enabled(r.Context, slog.LevelDebug) {
		r.Log = l.With(sloghelper.Uint64("request-id", r.ID))
		uri := req.RequestURI
		if uri == "" && req.URL != nil {
			uri = req.URL.String()
		}
		r.Log.LogAttrs(
			r.Context,
			slog.LevelDebug,
			"Starting request processing.",
			sloghelper.String("uri", uri))
	}
	req.Body = &r.bodyWrapper
	return r
}

// Put cleans up and returns a Request to the pool.
func Put(r *Request) {
	if r == nil {
		return
	}
	r.Request = nil
	r.response = nil
	r.bodyWrapper.in = nil
	r.Log = nil
	r.baseLog = nil
	r.Context = nil
	if r.trace != nil {
		r.trace.Free()
		r.trace = nil
	}
	*r = Request{}
	requestPool.Put(r)
}

func New(
	w http.ResponseWriter,
	req *http.Request,
	l *slog.Logger,
) (
	r Request,
) {
	ir := Get(w, req, l)
	r = *ir
	// Detach pointers before recycling temporary instance
	ir.Request = nil
	ir.response = nil
	ir.bodyWrapper.in = nil
	*ir = Request{}
	requestPool.Put(ir)
	return
}

// Logger returns the logger enriched with the request ID, instantiating
// it lazily if it has not yet been initialized.
func (r *Request) Logger() *slog.Logger {
	if r.Log == nil {
		if r.baseLog != nil {
			r.Log = r.baseLog.With(sloghelper.Uint64("request-id", r.ID))
		} else {
			r.Log = slog.Default().With(sloghelper.Uint64("request-id", r.ID))
		}
	}
	return r.Log
}

// Logs the access log for this request to the given logger.
func (r *Request) AccessLog(l *slog.Logger) {
	duration := time.Since(r.start)
	uri := r.Request.RequestURI
	if uri == "" && r.Request.URL != nil {
		uri = r.Request.URL.String()
	}
	l.LogAttrs(
		r.Context,
		slog.LevelInfo,
		"request complete.",
		slog.Time("start", r.start),
		sloghelper.Uint64("request-id", r.ID),
		sloghelper.Int("status", r.statusCode),
		sloghelper.String("method", r.Request.Method),
		sloghelper.String("url", uri),
		sloghelper.Int64("bytes-read", r.bodyWrapper.size),
		sloghelper.Duration("request-duration", duration),
	)
}

// Enables tracing on this Request.
func (r *Request) AddTracer() {
	r.trace = tracing.New()
}

// Returns a validated Hash header from the Request.
func (r *Request) HashHeader() string {
	headers, ok := r.Request.Header["Hash"]
	if !ok || len(headers) == 0 {
		panic(&HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Missing 'Hash' header.",
		})
	} else if len(headers) > 1 {
		panic(&HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Multiple 'Hash' headers are not allowed.",
		})
	}
	return headers[0]
}

// Returns the underlying response headers to the caller.
func (r *Request) Header() http.Header {
	return r.response.Header()
}

// Panic handler. This can be called in a defer from the main request handler
// in order to catch and return panics raised as part of event flow processing.
//
// If a panic is received then this will sent a HTTP response to the caller
// indicating what type of error was received (if HTTPError) or a 500
// indicating that a completely unexpected error happened during the
// request processing cycle.
func (r *Request) PanicHandler(serveError bool) {
	if err := recover(); err != nil {
		if he, ok := err.(*HTTPError); ok {
			he.ServeError(r)
			if he.Err != nil {
				r.Logger().LogAttrs(
					r.Context,
					slog.LevelError,
					"Error while processing HTTP request.",
					sloghelper.Error("error", he),
					sloghelper.String("stack", string(debug.Stack())))
				if serveError {
					r.Write([]byte{'\n'})
					r.Write([]byte(he.Err.Error()))
					r.Write([]byte{'\n'})
					r.Write([]byte(debug.Stack()))
				}
			}
		} else {
			if pe, ok := err.(error); ok {
				r.Logger().LogAttrs(
					r.Context,
					slog.LevelError,
					"Unexpected panic while processing request.",
					sloghelper.Error("error", pe),
					sloghelper.String("stack", string(debug.Stack())))
			} else {
				r.Logger().LogAttrs(
					r.Context,
					slog.LevelError,
					"Unexpected error while processing request.",
					sloghelper.Interface("interface", err),
					sloghelper.String("stack", string(debug.Stack())))
			}
			r.Header().Add("Content-Type", "text/plain")
			r.WriteHeader(http.StatusInternalServerError)
			r.Write([]byte("Unexpected internal server error."))
			if serveError {
				r.Write([]byte(debug.Stack()))
			}
		}
	}
}

// Returns the tracer set using AddTracer()
func (r *Request) Tracer() *tracing.Trace {
	return r.trace
}

// Gets a header that is expected to be a uint64 value, returning an error
// if the header is either not present, or can not be parsed into a uint64
// value.
func (r *Request) Uint64Header(h string) uint64 {
	headers, ok := r.Request.Header[h]
	if !ok || len(headers) == 0 {
		panic(&HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Missing '" + h + "' header.",
		})
	} else if len(headers) > 1 {
		panic(&HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Multiple '" + h + "' headers are not allowed.",
		})
	}
	value, err := strconv.ParseUint(headers[0], 10, 64)
	if err != nil {
		panic(&HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid '" + h + "' header (expected an integer).",
			Err:      fmt.Errorf("Invalid '%s' header: %w", h, err),
		})
	}
	return value
}

// Writes a status code to to the caller.
func (r *Request) WriteHeader(h int) {
	r.statusCode = h
	r.response.WriteHeader(h)
}

// Writes data to the caller.
func (r *Request) Write(data []byte) (n int, err error) {
	n, err = r.response.Write(data)
	r.replyBytes += int64(n)
	return
}
