package request

import (
	"net/http"
	"runtime/debug"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"

	"github.com/liquidgecka/blobby/internal/logging"
	"github.com/liquidgecka/blobby/internal/tracing"
)

var nextRequestID uint64

type Request struct {
	ID          uint64
	Request     *http.Request
	start       time.Time
	trace       *tracing.Trace
	bodyWrapper bodyWrapper
	replyBytes  int64
	response    http.ResponseWriter
	statusCode  int
	log         *logging.Logger
}

func New(
	w http.ResponseWriter,
	req *http.Request,
	l *logging.Logger,
) (
	r Request,
) {
	r.ID = atomic.AddUint64(&nextRequestID, 1)
	r.Request = req
	r.start = time.Now()
	r.bodyWrapper.in = req.Body
	r.response = w
	r.log = l.NewChild().AddField("request-id", r.ID)
	if r.log.DebugEnabled() {
		r.log.Debug(
			"Starting request processing.",
			logging.NewField("uri", req.URL.String()))
	}
	req.Body = &r.bodyWrapper
	return
}

// Logs the access log for this request to the given logger.
func (r *Request) AccessLog(l *logging.Logger) {
	duration := time.Now().Sub(r.start) / time.Millisecond
	l.Info(
		"request complete.",
		logging.NewFieldIface("start", r.start.String()),
		logging.NewFieldIface("request-id", strconv.FormatUint(r.ID, 10)),
		logging.NewFieldIface("status", strconv.Itoa(r.statusCode)),
		logging.NewFieldIface("method", r.Request.Method),
		logging.NewFieldIface("url", r.Request.URL.String()),
		logging.NewFieldIface("bytes-read", strconv.FormatInt(
			r.bodyWrapper.size,
			10)),
		logging.NewFieldIface("request-duration-ms", strconv.FormatInt(
			int64(duration),
			10)),
	)
}

// Enables tracing on this Request.
func (r *Request) AddTracer() {
	r.trace = tracing.New()
}

// Generates a debug log against the request. This is useful when special
// information should be debugged for a specific request ID.
func (r *Request) Debug(msg string, fields ...logging.Field) {
	r.log.Debug(msg, fields...)
}

// Returns true if debug logging is enabled.
func (r *Request) DebugEnabled() bool {
	if r.log == nil {
		return false
	} else {
		return r.log.DebugEnabled()
	}
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
				r.log.Error(
					"Error while processing HTTP request.",
					logging.NewFieldIface("error", err),
					logging.NewField("stack", string(debug.Stack())))
				if serveError {
					r.Write([]byte{'\n'})
					r.Write([]byte(he.Err.Error()))
					r.Write([]byte{'\n'})
					r.Write([]byte(debug.Stack()))
				}
			}
		} else {
			r.log.Error(
				"Unexpected error while processing request.",
				logging.NewFieldIface("error", err),
				logging.NewField("stack", string(debug.Stack())))
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
			Err:      errors.Wrap(err, "Invalid '"+h+"' header: "),
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
