package storage

import (
	"io"

	"github.com/iterable/blobby/internal/logging"
	"github.com/iterable/blobby/storage/fid"
)

// When performing a read the arguments can be kind of fluid due to the way
// that requests are made. Authentication may need to be proxied through
// to a remote in a way that we do not do with normal Inserts. As such this
// allows the upstream caller to provide Remote specific details that the
// storage implementation is simply unaware of.
type ReadConfig interface {
	// The NameSpace and ID being requested from this storage instance.
	NameSpace() string
	ID() string

	// Returns some FID specific fields as a helper.
	FID() fid.FID
	FIDString() string
	Machine() uint32
	Start() uint64
	Length() uint32

	// If this returns true then the read request will not attempt to make
	// any remote calls. It will purely attempt to fetch the file from the
	// local cache and return 404 if its not found locally.
	LocalOnly() bool

	// Returns the Logger that is associated with this Read operation. If
	// this returns nil then a logger will be created from the BAseLogger
	// in the Storage object.
	Logger() *logging.Logger

	// Since the request may need to be forwarded on to a Remote we need to
	// allow a way for request contexts to be proxied. Since the Storage
	// implementation does not know or care what those contexts are they
	// are simply provided as an interface.
	Context() interface{}
}

// All of the arguments to Replicate() are bundled up here in order to make
// it easier to pass objects in and around. This vastly simplifies the function
// footprint.
type RemoteReplicateConfig interface {
	FileName() string
	GetBody() io.ReadCloser
	Hash() string
	NameSpace() string
	Offset() uint64
	Size() uint64
}

// An interface that the Storage object will use when interfacing with remote
// instances. This implementation allows remotes to use any number of protocols
// or access methods vs statically defining them in the storage module.
type Remote interface {
	Delete(namespace, fn string) error
	HeartBeat(namespace, fn string) (bool, error)
	Initialize(namespace, fn string) error
	Read(rc ReadConfig) (io.ReadCloser, error)
	Replicate(rc RemoteReplicateConfig) (bool, error)
	String() string
}
