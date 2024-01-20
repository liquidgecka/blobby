package httpserver

import (
	"bytes"
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"log/slog"
	"net"
	"net/http"
	"net/http/pprof"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/liquidgecka/blobby/httpserver/request"
	"github.com/liquidgecka/blobby/internal/compat"
	"github.com/liquidgecka/blobby/internal/sloghelper"
	"github.com/liquidgecka/blobby/storage"
	"github.com/liquidgecka/blobby/storage/fid"
	"github.com/liquidgecka/blobby/storage/metrics"
)

// Used for tracking requests that are received by this server.
var requestID uint64

const (
	// A list of characters that are allowed in namespace names.
	validNameSpaceChars = "" +
		"abcdefghijklmnopqrstuvwxyz" +
		"ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
		"0123456789" +
		"_-"
)

// An implementation of Server that exposes the public functions.
type Server interface {
	Addr() string
	Listen() error
	Run() error
}

// Creates a new Server that is capable of serving HTTP requests.
func New(settings *Settings) Server {
	// Validate that the settings we were given is valid and
	// will work.
	if len(settings.NameSpaces) == 0 {
		panic("settings.NameSpaces can not be nil or empty.")
	} else if settings.Logger == nil {
		panic("settings.Logger is a required field.")
	} else if settings.WriteTimeout < 0 {
		panic("settings.WriteTimeout is negative.")
	} else if settings.ReadTimeout < 0 {
		panic("settings.ReadTimeout is negative.")
	} else if settings.IdleTimeout < 0 {
		panic("settings.IdleTimeout is negative.")
	} else if settings.MaxHeaderBytes < 0 {
		panic("settings.MaxHeaderBytes is negative.")
	}
	for ns := range settings.NameSpaces {
		if ns == "" {
			panic("settings.NameSpaces names can not be empty.")
		} else if strings.Trim(ns, validNameSpaceChars) != "" {
			panic(fmt.Sprintf(
				"%s is not a valid name space name, supported characters: %s",
				ns,
				validNameSpaceChars))
		} else if ns[0] == '_' {
			panic(fmt.Sprintf(
				"%s is an invalid namespace name, can not start with _.",
				ns))
		}
	}
	s := &server{
		context:  context.Background(), // FIXME
		settings: *settings,
		httpServer: compat.SetIdleTimeout(
			&http.Server{
				WriteTimeout:   settings.WriteTimeout,
				ReadTimeout:    settings.ReadTimeout,
				MaxHeaderBytes: settings.MaxHeaderBytes,
				ErrorLog:       log.New(ioutil.Discard, "", 0),
			},
			settings.IdleTimeout,
		),
		log: settings.Logger,
	}
	s.httpServer.Handler = s
	return s
}

type server struct {
	// A copy of the settings defined when the server was created. This is
	// a copy specifically so that the values can not be altered during
	// the operation of the HTTP server.
	settings Settings

	// If true then the contents of the errors will be written back to
	// the caller. This is not safe in production environments as it
	// may leak information back to the caller.
	ReplyWithError bool

	// The underlying HTTP server that is capable of serving requests
	// to a caller.
	httpServer *http.Server

	// The listener that this http server will serve on.
	listener net.Listener

	// The context that the server is running within.
	context context.Context

	// Set to one if the server is shutting down. This will cause all
	// non replica related requests to be closed via a header that the
	// server returns. This will ensure that clients disconnect and find
	// a new server to upload too.
	shuttingDown int32

	// The logger that is used for all internal logging.
	log *slog.Logger
}

// Returns the address that this server will listen on.
func (s *server) Addr() string {
	return fmt.Sprintf("%s:%d", s.settings.Addr, s.settings.Port)
}

// Starts the listener.
func (s *server) Listen() error {
	listener, err := net.Listen("tcp", fmt.Sprintf("%s:%d",
		s.settings.Addr,
		s.settings.Port))
	if err != nil {
		return err
	}
	s.listener = listener
	return nil
}

// Starts the HTTP server and runs it, returning an error only when it
// has stopped.
func (s *server) Run() error {
	l := s.listener
	if s.settings.TLSCerts != nil {
		tc := tls.Config{
			GetCertificate: s.cert,
		}
		l = tls.NewListener(s.listener, &tc)
	}
	return s.httpServer.Serve(l)
}

//
// HTTP Handler functions
//

// Internally exposed ServeHTTP method used as server implements the Muxer
// interface.
func (s *server) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	// We want to capture the response in order to put it in the log. As
	// such we actually wrap the ResponseWriter in an internal implementation
	// that captures details.
	ir := request.New(w, req, s.log)
	if s.settings.EnableTracing {
		ir.AddTracer()
	}
	w = &ir

	// Use the above object to generate a log line at the end of the
	// request.
	if s.settings.AccessLogger != nil {
		defer func() {
			ir.AccessLog(s.settings.AccessLogger)
		}()
	}

	// If there is an error presented via a panic we want to handle that
	// as cleanly as possible. Normally the http.Server will eat this
	// and turn it into an InternalServerError but we want more control
	// over that path.
	defer ir.PanicHandler(s.ReplyWithError)

	// Make sure the trace ends.
	defer ir.Tracer().End()

	// Add a shutting down header so any caller that needs to know is aware
	// that the server will terminate soon.
	if atomic.LoadInt32(&s.shuttingDown) != 0 {
		ir.Header().Add("Shutting-Down", "true")
	}

	// Mux to the right handled based on the method used.
	switch req.Method {
	// The following two functions are used by callers and are documented
	// as part of the API. Both of these methods can end up in several
	// different implementation paths as they also implement internal
	// functionality like health checks.
	case "GET":
		s.httpGetMuxer(&ir)
	case "POST":
		s.httpPostMuxer(&ir)

	// Protocol level operations. The underlying protocol uses methods to
	// define which operation is being used rather than trying to map into
	// a REST like semantic. This is purely for simplicity and to prevent
	// users from accidentally calling legitimate operations by accident.
	case "DELETE":
		s.httpDelete(&ir)
	case "HEARTBEAT":
		s.httpHeartBeat(&ir)
	case "INITIALIZE":
		s.httpInitialize(&ir)
	case "READ":
		// TODO: READ is deprecated but for now we keep it working so that
		// users have time to transition. In the future this will be used
		// for proxying Blobby GET requests between servers.
		parts := strings.Split(ir.Request.URL.Path, "/")
		s.httpGet(&ir, parts)
	case "REPLICATE":
		s.httpReplicate(&ir)

	// Blast Path requests which are used to dump whole files or limited
	// ranges out of the server before they get uploaded to S3.
	case "BLASTSTATUS":
		s.httpBlastStatus(&ir)
	case "BLASTGET":
		s.httpBlastRead(&ir)

	// Otherwise its an unsupported method.
	default:
		ir.Header().Add("Content-Type", "text/plan")
		ir.WriteHeader(http.StatusMethodNotAllowed)
		ir.Write([]byte("Unsupported method.\n"))
	}
}

// The GET handler must account for several internally provided GET URLs.
func (s *server) httpGetMuxer(ir *request.Request) {
	// We need to capture any internal or administrative URLS before
	// processing the generic Method based URLs that customers will use.
	parts := strings.Split(ir.Request.URL.Path, "/")
	if strings.HasPrefix(ir.Request.URL.Path, "/_") {
		switch parts[1] {
		case "_debug":
			s.settings.DebugPathsACL.Assert(ir)
			switch ir.Request.URL.Path {
			case "/_debug/allocs":
				pprof.Handler("allocs").ServeHTTP(ir, ir.Request)
			case "/_debug/block":
				pprof.Handler("block").ServeHTTP(ir, ir.Request)
			case "/_debug/cmdline":
				pprof.Handler("cmdline").ServeHTTP(ir, ir.Request)
			case "/_debug/goroutine":
				pprof.Handler("goroutine").ServeHTTP(ir, ir.Request)
			case "/_debug/heap":
				pprof.Handler("heap").ServeHTTP(ir, ir.Request)
			case "/_debug/mutex":
				pprof.Handler("mutex").ServeHTTP(ir, ir.Request)
			case "/_debug/profile":
				pprof.Handler("profile").ServeHTTP(ir, ir.Request)
			case "/_debug/stack":
				buffer := make([]byte, 10000)
				ir.Header().Add("Content-Type", "text/plain")
				ir.WriteHeader(http.StatusOK)
				ir.Write(buffer[0:runtime.Stack(buffer, true)])
			case "/_debug/threadcreate":
				pprof.Handler("threadcreate").ServeHTTP(ir, ir.Request)
			case "/_debug/pprof":
				pprof.Profile(ir, ir.Request)
			default:
				panic(&request.HTTPError{
					Status:   http.StatusNotFound,
					Response: "The URL you are requesting does not exist.",
				})
			}
		case "_health":
			s.settings.HealthCheckACL.Assert(ir)
			s.httpGetHealth(ir)
		case "_login":
			s.settings.WebAuthProvider.LoginGet(ir)
		case "_metrics":
			s.settings.StatusACL.Assert(ir)
			s.httpMetrics(ir)
		case "_saml":
			if len(parts) == 4 && parts[3] == "metadata" {
				s.httpSAMLMetadata(ir, parts)
			} else {
				panic(&request.HTTPError{
					Status:   http.StatusNotFound,
					Response: "The URL you are requesting does not exist.",
				})
			}
		case "_shutdown":
			s.settings.ShutDownACL.Assert(ir)
			s.httpShutDown(ir, parts)
		case "_status":
			s.settings.StatusACL.Assert(ir)
			s.httpStatus(ir)
		case "_id":
			s.settings.DebugPathsACL.Assert(ir)
			s.httpID(ir)
		default:
			panic(&request.HTTPError{
				Status:   http.StatusNotFound,
				Response: "The URL you are requesting does not exist.",
			})
		}
	} else {
		s.httpGet(ir, parts)
	}
}

// The POST handler must account for several internally provided POST URLs.
func (s *server) httpPostMuxer(ir *request.Request) {
	// We need to capture any internal or administrative URLS before
	// processing the generic Method based URLs that customers will use.
	parts := strings.Split(ir.Request.URL.Path, "/")
	if strings.HasPrefix(ir.Request.URL.Path, "/_") {
		switch parts[1] {
		case "_login":
			// If the server is shutting down then we need to indicate to
			// the client that they should close the TCP session once this
			// request completes.
			if atomic.LoadInt32(&s.shuttingDown) != 0 {
				ir.Header().Add("Connection", "close")
			}
			if s.settings.WebAuthProvider == nil {
				panic(&request.HTTPError{
					Status:   http.StatusNotFound,
					Response: "Web logins are disabled on this server.",
				})
			}
			// FIXME: permissions?
			s.settings.WebAuthProvider.LoginPost(ir)
		case "_saml":
			s.httpSAMLAuth(ir, parts)
		default:
			panic(&request.HTTPError{
				Status:   http.StatusNotFound,
				Response: "The URL you are requesting does not exist.",
			})
		}
	} else {
		s.httpInsert(ir, parts)
	}
}

// BLASTREAD requests are sent by a Blast Path server to get a specific
// portion of the primary file.
func (s *server) httpBlastRead(r *request.Request) {
	parts := strings.Split(r.Request.URL.Path, "/")
	if len(parts) != 5 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid BLASTREAD request.",
		})
	}

	// Convert the start byte string into an integer.
	start, err := strconv.ParseUint(parts[3], 10, 64)
	if err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Start byte is not valid.",
		})
	}

	// Convert the end byte string into an integer.
	end, err := strconv.ParseUint(parts[4], 10, 64)
	if err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "End byte is not valid.",
		})
	}

	// Obtain the namespace for the given path.
	ns, ok := s.settings.NameSpaces[parts[1]]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Name space does not exist.",
		})
	}

	// Verify that the caller is allowed to make this request.
	ns.BlastPathACL.Assert(r)

	// Fetch the data out of the Storage instance.
	content, err := ns.Storage.BlastPathRead(parts[2], start, end)
	if err != nil {
		if _, ok := err.(storage.ErrNotPossible); ok {
			r.Header().Add("Content-Type", "text/plain")
			r.WriteHeader(http.StatusBadRequest)
			r.Write([]byte("Can not fetch objects from a compressed source."))
			return
		} else {
			panic(err)
		}
	}
	defer content.Close()

	// Success!
	r.Header().Add("Content-type", "text/plain")
	r.WriteHeader(http.StatusOK)
	io.Copy(r, content)
}

// BLASTSTATUS requests are sent by a Blast Path server to get the current
// list of supported files so that they can be fetched as needed.
func (s *server) httpBlastStatus(r *request.Request) {
	parts := strings.Split(r.Request.URL.Path, "/")
	if len(parts) != 2 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid BLASTSTATUS path.",
		})
	}

	// Obtain the namespace for the given path.
	ns, ok := s.settings.NameSpaces[parts[1]]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Name space does not exist.",
		})
	}

	// Verify that the caller is allowed to make this request.
	ns.BlastPathACL.Assert(r)

	// Get the status from the Name Space.
	r.Header().Add("Content-Type", "application/json")
	r.WriteHeader(http.StatusOK)
	ns.Storage.BlastPathStatus(r)
}

// DELETE requests are sent by a Blobby server to another Blobby server
// in order to delete a replica file from disk.
func (s *server) httpDelete(r *request.Request) {
	parts := strings.Split(r.Request.URL.Path, "/")
	if len(parts) != 3 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid DELETE path.",
		})
	}

	// Obtain the namespace for the given path.
	ns, ok := s.settings.NameSpaces[parts[1]]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Name space does not exist.",
		})
	}

	// Verify that the caller is allowed to make this request.
	ns.PrimaryACL.Assert(r)

	// Perform the delete.
	if err := ns.Storage.ReplicaQueueDelete(r.Context, parts[2]); err != nil {
		if _, ok := err.(storage.ErrReplicaNotFound); ok {
			panic(&request.HTTPError{
				Status:   http.StatusNotFound,
				Response: "That replica does not exist.",
			})
		} else {
			panic(err)
		}
	}

	// Success!
	r.WriteHeader(http.StatusNoContent)
}

// GET requests are used to fetch the contents of an ID from a given namespace.
// This call may also be forwarded from another blobby server if it is
// attempting to route the request back to the server that created it so that
// it can be served locally.
func (s *server) httpGet(r *request.Request, parts []string) {
	// If the server is shutting down then we need to indicate to the client
	// that they should close the TCP session once this request completes.
	if atomic.LoadInt32(&s.shuttingDown) != 0 {
		r.Header().Add("Connection", "close")
	}

	// Check that the GET path is actually valid before continuing.
	if len(parts) != 3 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid GET path.",
		})
	}

	// Obtain the namespace for the given path.
	ns, ok := s.settings.NameSpaces[parts[1]]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Name space does not exist.",
		})
	}

	// Verify that the caller is allowed to make this request.
	ns.ReadACL.Assert(r)

	// Parse the ID given into the file id, start and length so that they
	// can be used in the readConfig object. If this errors then we can
	// safely reject the request without even sending it to the Storage.
	f, start, length, err := fid.ParseID(parts[2])
	if err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "The given ID is not valid.",
		})
	}

	// We need to setup the readConfig object that will pass information
	// into the Read implementation.
	log := s.settings.Logger.With(
		sloghelper.String("namespace", parts[1]),
		sloghelper.String("id", parts[2]),
		sloghelper.Uint64("start", start),
		sloghelper.Uint32("length", length),
	)
	rc := readConfig{
		nameSpace: parts[1],
		id:        parts[2],
		fid:       f,
		fidStr:    f.String(),
		start:     start,
		length:    length,
		machine:   f.Machine(),
		acl:       ns.ReadACL,
		request:   r.Request,
		logger:    log,
	}

	// If the request has the "Blobby-Local-Only" header set then we need
	// to configure the readConfig to ony perform local operations. This
	// is done when the request is being forwarded from the initial server
	// to read from the local file if present.
	if r.Request.Header.Get("Blobby-Local-Only") != "" {
		rc.localOnly = true
	}

	// Attempt to fetch the data from the Storage server.
	content, err := ns.Storage.Read(r.Context, &rc)
	if err != nil {
		if _, ok := err.(storage.ErrNotPossible); ok {
			r.Header().Add("Content-Type", "text/plain")
			r.WriteHeader(http.StatusBadRequest)
			r.Write([]byte("Can not fetch objects from a compressed source."))
			return
		} else if _, ok := err.(storage.ErrInvalidID); ok {
			r.Header().Add("Content-Type", "text/plain")
			r.WriteHeader(http.StatusBadRequest)
			r.Write([]byte("The provided ID is not valid."))
			return
		} else if _, ok := err.(storage.ErrNotFound); ok {
			r.Header().Add("Content-Type", "text/plain")
			r.WriteHeader(http.StatusNotFound)
			r.Write([]byte("The requested ID was not found."))
			return
		} else {
			panic(err)
		}
	}
	defer content.Close()

	// Success!
	r.Header().Add("Content-type", "text/plain")
	r.WriteHeader(http.StatusOK)
	io.Copy(r, content)
}

// Returns health status of the server. This is useful for load balancing
// and traffic management.
func (s *server) httpGetHealth(r *request.Request) {
	status := http.StatusOK
	output := bytes.Buffer{}
	for name, ns := range s.settings.NameSpaces {
		if ok, desc := ns.Storage.Health(); ok {
			output.WriteString(name)
			output.WriteString(": OK\n")
		} else {
			status = http.StatusInternalServerError
			output.WriteString(name)
			output.WriteString(": FAILED\n")
			for _, line := range strings.Split(desc, "\n") {
				output.WriteString("    ")
				output.WriteString(line)
				output.WriteByte('\n')
			}
		}
	}
	r.Header().Add("Content-Type", "text/plain")
	if atomic.LoadInt32(&s.shuttingDown) != 0 {
		r.WriteHeader(http.StatusInternalServerError)
		r.Write([]byte("Server is shutting down.\n"))
	} else {
		r.WriteHeader(status)
	}
	r.Write(output.Bytes())
}

// HEARTBEAT requests are sent by a Blobby server to another Blobby server
// in order to mark a replica file as not being orphaned.
func (s *server) httpHeartBeat(r *request.Request) {
	parts := strings.Split(r.Request.URL.Path, "/")
	if len(parts) != 3 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid HEARTBEAT path.",
		})
	}

	// Obtain the namespace for the given path.
	ns, ok := s.settings.NameSpaces[parts[1]]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Name space does not exist.",
		})
	}

	// Verify that the caller is allowed to make this request.
	ns.PrimaryACL.Assert(r)

	// Perform the heart beat.
	if err := ns.Storage.ReplicaHeartBeat(r.Context, parts[2]); err != nil {
		if _, ok := err.(storage.ErrReplicaNotFound); ok {
			panic(&request.HTTPError{
				Status:   http.StatusNotFound,
				Response: "That replica does not exist.",
			})
		} else if _, ok = err.(storage.ErrReplicaNotFound); ok {
			panic(&request.HTTPError{
				Status:   http.StatusConflict,
				Response: "The replica is not in a valid state.",
			})
		} else {
			panic(err)
		}
	}

	// Success!
	r.WriteHeader(http.StatusNoContent)
}

// The ID tool is a simple helper that will expose information about a specific
// passed in ID. This can be used to show the server that generated it, the
// status of the file (if local), the eventual S3 filename, etc.
func (s *server) httpID(r *request.Request) {
	// If the server is shutting down then we need to indicate to the client
	// that they should close the TCP session once this request completes.
	if atomic.LoadInt32(&s.shuttingDown) != 0 {
		r.Header().Add("Connection", "close")
	}

	parts := strings.Split(r.Request.URL.Path, "/")
	if len(parts) != 4 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Expected path: /_id/<namespace>/<id>",
		})
	}

	// Obtain the namespace for the given path.
	ns, ok := s.settings.NameSpaces[parts[2]]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Name space does not exist.",
		})
	}

	// Call the namespace looking for debug information about the ID given.
	r.Header().Add("Content-Type", "text/plain")
	r.WriteHeader(http.StatusOK)
	ns.Storage.DebugID(r, parts[3])
}

func (s *server) httpInitialize(r *request.Request) {
	// INITIALIZE requests are sent by a Blobby server to another Blobby
	// server. In order to initialize a new replica file.
	if atomic.LoadInt32(&s.shuttingDown) != 0 {
		r.Header().Add("Connection", "close")
		panic(&request.HTTPError{
			Status:   http.StatusForbidden,
			Response: "Unable to INITIALIZE, server is shutting down.",
		})
	}

	// See if the URL is even a valid replica path.
	parts := strings.Split(r.Request.URL.Path, "/")
	if len(parts) != 3 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid INITIALIZE path.",
		})
	}

	// Obtain the namespace for the given path.
	ns, ok := s.settings.NameSpaces[parts[1]]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Name space does not exist.",
		})
	}

	// Verify that the caller is allowed to make this request.
	ns.PrimaryACL.Assert(r)

	// Perform the initializing.
	if err := ns.Storage.ReplicaInitialize(r.Context, parts[2]); err != nil {
		panic(err)
	}

	// Success!
	r.WriteHeader(http.StatusNoContent)
}

func (s *server) httpInsert(r *request.Request, parts []string) {
	// If the server is shutting down then we need to indicate to the client
	// that they should close the TCP session once this request completes.
	if atomic.LoadInt32(&s.shuttingDown) != 0 {
		r.Header().Add("Connection", "close")
	}

	// Parse the path. The first segment is the namespace. For POST requests
	// there should be no other segments in the path. We split the URL out
	// on slashes to make sure of this.
	if len(parts) != 2 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid POST path.",
		})
	}

	// Obtain the namespace for the given path.
	ns, ok := s.settings.NameSpaces[parts[1]]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Name space does not exist.",
		})
	}

	// Verify that the caller is actually allowed to make this request.
	ns.InsertACL.Assert(r)

	// Attempt to insert the data into the Blobby instance.
	data := storage.InsertData{
		Source: r.Request.Body,
		Length: r.Request.ContentLength,
		Tracer: r.Tracer(),
	}
	id, err := ns.Storage.Insert(r.Context, &data)
	if err != nil {
		panic(err)
	}

	// Success!
	r.Header().Add("Content-type", "text/plain")
	r.WriteHeader(http.StatusOK)
	r.Write([]byte(id))
}

func (s *server) httpReplicate(r *request.Request) {
	// REPLICATE requests are sent by a Blobby server to another Blobby server.
	// The append data into a replica file. As such the path will require
	// a namespace, and a file name element.
	parts := strings.Split(r.Request.URL.Path, "/")
	if len(parts) != 3 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid REPLICATE path.",
		})
	}

	// Obtain the namespace for the given path.
	ns, ok := s.settings.NameSpaces[parts[1]]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Name space does not exist.",
		})
	}

	// Verify that the caller is allowed to make this request.
	ns.PrimaryACL.Assert(r)

	// Create the replicator from the values provided in the request headers.
	rc := remoteReplicatorConfig{
		body:      r.Request.Body,
		end:       r.Uint64Header("End"),
		fid:       parts[2],
		hash:      r.HashHeader(),
		namespace: parts[1],
		start:     r.Uint64Header("Start"),
	}

	// Perform the replicate call.
	if err := ns.Storage.ReplicaReplicate(r.Context, parts[2], &rc); err != nil {
		if _, ok := err.(storage.ErrReplicaNotFound); ok {
			panic(&request.HTTPError{
				Status:   http.StatusNotFound,
				Response: "That replica does not exist.",
			})
		} else {
			panic(err)
		}
	}

	// Success.
	r.WriteHeader(http.StatusNoContent)
}

// The receiver side of a SAML authentication loop. This is where the user
// will end up landing once they have completed SAML authentication against
// the IDP.
func (s *server) httpSAMLAuth(r *request.Request, parts []string) {
	// If the server is shutting down then we need to indicate to the client
	// that they should close the TCP session once this request completes.
	if atomic.LoadInt32(&s.shuttingDown) != 0 {
		r.Header().Add("Connection", "close")
	}

	// If there are no SAML authentication configurations then we
	// can just return a 404 right out of the gate.
	if len(parts) != 4 {
		// We expect SAML return paths to be in the form of /_saml/<name>/acs
		// which means that the given URL is completely invalid.
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Invalid SAML authentication return path.",
		})
	} else if s.settings.SAMLAuth == nil {
		// If there is not any SAML endpoints even configured then we
		// can just 404 here as well.
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "SAML Logins are not configured on this server.",
		})
	} else if saml, ok := s.settings.SAMLAuth[parts[2]]; !ok {
		// The given SAML destination is not configured, we can 404
		// the request.
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "That is not a configured SAML destination.",
		})
	} else {
		// Use the configured SAML destination to serve the request.
		saml.Post(r)
	}
}

// This renders the SAML Service PRovider meta data.
func (s *server) httpSAMLMetadata(r *request.Request, parts []string) {
	// If the server is shutting down then we need to indicate to the client
	// that they should close the TCP session once this request completes.
	if atomic.LoadInt32(&s.shuttingDown) != 0 {
		r.Header().Add("Connection", "close")
	}

	// If there are no SAML authentication configurations then we
	// can just return a 404 right out of the gate.
	if len(parts) != 4 {
		// We expect SAML metadata to be in the form of
		// /_saml/<name>/metadata which means that the given URL is
		// completely invalid.
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "Invalid SAML meta data path.",
		})
	} else if s.settings.SAMLAuth == nil {
		// If there is not any SAML endpoints even configured then we
		// can just 404 here as well.
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "SAML Logins are not configured on this server.",
		})
	} else if saml, ok := s.settings.SAMLAuth[parts[2]]; !ok {
		// The given SAML destination is not configured, we can 404
		// the request.
		panic(&request.HTTPError{
			Status:   http.StatusNotFound,
			Response: "That is not a configured SAML destination.",
		})
	} else {
		// Use the configured SAML destination to serve the request.
		saml.MetaData(r)
	}
}

// Sets this server into shutting down mode which will attempt to push traffic
// off the server so it can be safely restarted.
func (s *server) httpShutDown(r *request.Request, parts []string) {
	switch {
	case len(parts) == 3 && parts[2] == "status":
		r.Header().Add("Content-Type", "text/plain")
		r.WriteHeader(http.StatusOK)
		status := atomic.LoadInt32(&s.shuttingDown)
		if status == 0 {
			fmt.Fprintf(r, "server is not shutting down.\n")
		} else {
			fmt.Fprintf(r, "server is shutting down.\n")
		}
		return
	case len(parts) == 3 && parts[2] == "stop":
		r.Header().Add("Content-Type", "text/plain")
		r.WriteHeader(http.StatusOK)
		old := atomic.SwapInt32(&s.shuttingDown, 0)
		if old == 0 {
			fmt.Fprintf(r, "server was not shutting down.\n")
		} else {
			fmt.Fprintf(r, "server is no longer shutting down.\n")
		}
		return
	case len(parts) == 2 || (len(parts) == 3 && parts[2] == "start"):
		r.Header().Add("Content-Type", "text/plain")
		r.WriteHeader(http.StatusOK)
		old := atomic.SwapInt32(&s.shuttingDown, 1)
		if old == 0 {
			fmt.Fprintf(r, "shutting down.\n")
		} else {
			fmt.Fprintf(r, "already shutting down.\n")
		}
	default:
		r.Header().Add("Content-Type", "text/plain")
		r.WriteHeader(http.StatusBadRequest)
		fmt.Fprintf(r, "Invalid request.\n")
	}
}

// Returns the current status of the Storage implementations.
func (s *server) httpStatus(r *request.Request) {
	r.WriteHeader(http.StatusOK)
	if atomic.LoadInt32(&s.shuttingDown) != 0 {
		fmt.Fprintf(r, "This server is shutting down.\n\n")
	}
	nameSpaces := make([]string, 0, len(s.settings.NameSpaces))
	for name := range s.settings.NameSpaces {
		nameSpaces = append(nameSpaces, name)
	}
	sort.Strings(nameSpaces)
	for _, name := range nameSpaces {
		fmt.Fprintf(r, "%s:\n", name)
		s.settings.NameSpaces[name].Storage.Status(r)
	}
}

// Returns the current status of the Storage implementations.
func (s *server) httpMetrics(r *request.Request) {
	r.Header().Add("Content-Type", "text/plain; version=0.0.4")
	r.WriteHeader(http.StatusOK)

	// allNameSpaceMetrics holds the Metrics structs for every namespace:
	allNameSpaceMetrics := make(
		map[string]metrics.Metrics,
		len(s.settings.NameSpaces))

	fmt.Fprintf(r, "# TYPE shutting_down gauge\n")
	fmt.Fprintf(r, "# HELP shutting_down Is blobby shutting down\n")
	fmt.Fprintf(r, "shutting_down %d\n\n", atomic.LoadInt32(&s.shuttingDown))

	fmt.Fprintf(r, "# TYPE namespaces_healthy gauge\n")
	fmt.Fprintf(r, "# HELP namespaces_healthy Number of healhty namespaces\n")
	for name, ns := range s.settings.NameSpaces {
		healthy := 0
		if ok, _ := ns.Storage.Health(); ok {
			healthy = 1
		}
		// Populate allNameSpaceMetrics with each namespace's metrics struct:
		allNameSpaceMetrics[name] = ns.Storage.GetMetrics()
		fmt.Fprintf(
			r,
			`namespaces_healthy{%snamespace="%s"} %d`,
			s.settings.PrometheusTagPrefix,
			name,
			healthy)
		r.Write([]byte{'\n'})

	}
	r.Write([]byte{'\n'})

	// Generate all the storage specific prometheus metrics.
	metrics.RenderPrometheus(
		r,
		s.settings.PrometheusTagPrefix,
		allNameSpaceMetrics)
}

// Gets the current certificate from the CertLoader and returns it to the
// tls.Listen interface.
func (s *server) cert(*tls.ClientHelloInfo) (*tls.Certificate, error) {
	return s.settings.TLSCerts.Cert(s.context)
}
