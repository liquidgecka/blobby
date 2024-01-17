package httpserver

import (
	"net/http"

	"github.com/iterable/blobby/httpserver/access"
	"github.com/iterable/blobby/internal/logging"
	"github.com/iterable/blobby/storage/fid"
)

// Passed to the Read() call to highlight which ID is being fetched.
type readConfig struct {
	nameSpace string
	id        string
	fid       fid.FID
	fidStr    string
	start     uint64
	length    uint32
	machine   uint32
	localOnly bool
	logger    *logging.Logger
	acl       *access.ACL
	request   *http.Request
}

func (r *readConfig) NameSpace() string {
	return r.nameSpace
}

func (r *readConfig) ID() string {
	return r.id
}

func (r *readConfig) FID() fid.FID {
	return r.fid
}

func (r *readConfig) FIDString() string {
	return r.fidStr
}

func (r *readConfig) Machine() uint32 {
	return r.machine
}

func (r *readConfig) Start() uint64 {
	return r.start
}

func (r *readConfig) Length() uint32 {
	return r.length
}

func (r *readConfig) LocalOnly() bool {
	return r.localOnly
}

func (r *readConfig) Logger() *logging.Logger {
	return r.logger
}

func (r *readConfig) Context() interface{} {
	return r.proxy
}

func (r *readConfig) proxy(dest *http.Request) {
	r.acl.Proxy(r.request, dest)
}
