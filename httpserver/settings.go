package httpserver

import (
	"time"

	"github.com/iterable/blobby/httpserver/access"
	"github.com/iterable/blobby/httpserver/secretloader"
	"github.com/iterable/blobby/internal/logging"
	"github.com/iterable/blobby/storage"
)

type Settings struct {
	// The address and port that should be listened on for this server.
	Addr string
	Port int

	// A list of namespaces, mapped by name, that should be served
	// by this HTTP server.
	NameSpaces map[string]*NameSpaceSettings

	// Enable tracing for requests. Adds debugging but impacts performance
	// slightly.
	EnableTracing bool

	// If TLS is desired then this loader should be non nil and it should
	// return certificates to be used for serving on the TLS ports.
	TLSCerts *secretloader.Certificate

	// Debugging ACL
	EnableDebugPaths bool
	DebugPathsACL    *access.ACL

	// Health checking ACL
	HealthCheckACL *access.ACL

	// Status endpoint ACL
	StatusACL *access.ACL

	// Shut down endpoint ACL
	ShutDownACL *access.ACL

	// The Logger that will be used for all logs.
	Logger *logging.Logger

	// HTTP requests will be logged to this logger for access/request
	// logging. This is optional, if its left nil then no access logging
	// will be processed.
	AccessLogger *logging.Logger

	// These settings will be mapped into the underlying http.Server
	// object.
	WriteTimeout   time.Duration
	ReadTimeout    time.Duration
	IdleTimeout    time.Duration
	MaxHeaderBytes int

	// The prefix for the namespace= tag; a value of blobby_ for this field
	// would give blobby_namespace as the tag key in the rendered Prometheus
	// metrics:
	PrometheusTagPrefix string

	// If defined then web authentication will be enabled and users will
	// be allowed to authenticate to Blobby using a static web login form
	// server from _login.
	WebAuthProvider *access.WebAuthProvider

	// We allow SAML authentication from multiple sources. Each needs to be
	// configured and named. This stores a mapping of name to SAML
	// name to authentication details.
	SAMLAuth map[string]*access.SAML
}

// Each name space is given a specific security ACL configuration that allows
// it to be protected.
type NameSpaceSettings struct {
	// The Storage implementation that is serving this specific name space
	// within the server.
	Storage *storage.Storage

	// Protections around the Blast Path API access for this specific
	// name space.
	BlastPathACL *access.ACL

	// Protections around replica related operations. Servers in this Access
	// Control List will be able to create, update, and destroy replicas
	// on this server.
	PrimaryACL *access.ACL

	// Protections around read access for this specific name space.
	ReadACL *access.ACL

	// Protections around insert access for this specific name space.
	InsertACL *access.ACL
}
