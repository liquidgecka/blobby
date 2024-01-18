package config

import (
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/liquidgecka/blobby/httpserver/remotes"
	"github.com/liquidgecka/blobby/storage"
)

var (
	defaultRemoteTLS          = false
	defaultInsecureSkipVerify = false
	defaultMaxIdleConns       = 1000
	defaultInvalidString      = "invalid"
)

type remote struct {
	// The name of this remote (optional). This will be used in logging
	// to identify the host. If not specified then Host:Port will be used
	// instead.
	Name *string `toml:"name"`

	// The hostname or IP to connect to when using this remote.
	Host *string `toml:"host"`

	// The port to connect to when using this remote.
	Port value `toml:"port"`
	port int

	// The unique ID given to this remote.
	ID *uint32 `toml:"id"`

	// Use TLS when communicating
	TLS *bool `toml:"tls"`

	// When set the TLS validation will not be done for this remote.
	InsecureSkipVerify *bool `toml:"tls_insecure_skip_verify"`

	// When using TLS its possible that the remote machine will be using
	// a SSL cert that does not match the exact host address above. The
	// primary example of this is when a IP is used to talk to a specific
	// machine even though the machine is serving a cert for a generic
	// hostname. When that configuration is used then verity_host can be set
	// to the name of a host that the cert should match.
	VerifyHost *string `toml:"verify_host"`

	// The maximum number of idle connections to this host that will be
	// allowed. If not set this defaults to 100.
	MaxIdleConns *int `toml:"max_idle_conns"`

	// A reference to the top of the config file. Needed so we can get
	// back to the list of all remotes configured.
	top *top
}

func (r *remote) Remote() storage.Remote {
	client := r.top.Client.HTTPClient()
	if r.VerifyHost != nil {
		client.Transport = &http.Transport{
			// The default MaxIdleConns is 2 per host, but since this client
			// will only ever talk to a single host we want to allow far more
			// where possible. If we are going to initialize a connection for
			// an ephemeral operation its likely that we will need it again
			// soon, and if not then we want to just let it naturally
			// timeout and die rather than closing it by force.
			MaxIdleConns:        *r.MaxIdleConns,
			MaxIdleConnsPerHost: *r.MaxIdleConns,
			IdleConnTimeout:     time.Second * 90,
			DisableCompression:  true,
			TLSClientConfig: &tls.Config{
				ServerName:         *r.VerifyHost,
				InsecureSkipVerify: *r.InsecureSkipVerify,
			},
		}
	}
	useS := ""
	if *r.TLS {
		useS = "s"
	}
	return &remotes.Remote{
		Client: client,
		ID:     *r.ID,
		Name:   *r.Name,
		URL:    fmt.Sprintf("http%s://%s:%d", useS, *r.Host, r.port),
	}
}

func (r *remote) validate(t *top) []string {
	var errors []string
	var err error

	// Save the top for later.
	r.top = t

	// Host
	if r.Host == nil {
		errors = append(errors, "remotes.host is a required field.")
	} else if net.ParseIP(*r.Host) == nil {
		errors = append(errors, "remotes.host is not a valid ip.")
	}

	// Port
	if !r.Port.set {
		r.port = defaultPort
	} else if r.port, err = r.Port.Int(); err != nil {
		errors = append(errors, "remotes.port must be an integer.")
	} else if r.port < 1 || r.port > 65535 {
		errors = append(errors, "remotes.port is not a valid port number.")
	}

	// Name
	if r.Name != nil && *r.Name == "" {
		errors = append(errors, "remotes.name can not be empty.")
	} else if r.Name == nil {
		if r.Host == nil {
			r.Name = &defaultInvalidString
		} else if r.Port.set {
			s := fmt.Sprintf("%s:%d", *r.Host, r.port)
			r.Name = &s
		} else {
			r.Name = r.Host
		}
	}

	// MachineID
	if r.ID == nil {
		errors = append(errors, "remotes.id must be provided.")
	}

	// TLS
	if r.TLS == nil {
		r.TLS = &defaultRemoteTLS
	}

	// InsecureSkipVerify
	if r.InsecureSkipVerify == nil {
		r.InsecureSkipVerify = &defaultInsecureSkipVerify
	} else if *r.InsecureSkipVerify && !*r.TLS {
		errors = append(
			errors,
			"remotes.tls_insecure_skip_verify requires tls be true")
	}

	// VerifyHost
	if *r.TLS == false && r.VerifyHost != nil {
		errors = append(
			errors,
			"remotes.verify_host can only be used if tls is true.")
	}

	// MaxIdleConns
	if r.MaxIdleConns == nil {
		r.MaxIdleConns = &defaultMaxIdleConns
	} else if *r.MaxIdleConns < 0 {
		errors = append(
			errors,
			"remotes.max_idle_conns must be 0 or greater.")
	}

	// Return any errors encountered.
	return errors
}
