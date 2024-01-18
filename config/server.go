package config

import (
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"fmt"
	"net"
	"time"

	"github.com/liquidgecka/blobby/httpserver"
	"github.com/liquidgecka/blobby/httpserver/access"
	"github.com/liquidgecka/blobby/httpserver/cookie"
	"github.com/liquidgecka/blobby/httpserver/secretloader"
)

var (
	defaultCookieDomain        = ""
	defaultDebugPathsEnable    = false
	defaultEnableTracing       = false
	defaultIdleTimeout         = time.Minute * 5
	defaultMaxHeaderBytes      = int(1 << 20)
	defaultPort                = 1091
	defaultPrometheusTagPrefix = ""
	defaultReadHeaderTimeout   = time.Minute
	defaultReadTimeout         = time.Minute
	defaultTLS                 = false
	defaultWebAuthCookieName   = "ba"
	defaultWebLoginDuration    = time.Hour * 24
	defaultWriteTimeout        = time.Minute
)

var (
	localHostOnlyIPRequired = true
	localHostOnlyACL        = acl{
		WhiteListCIDRs:    []string{"127.0.0.1/32"},
		WhiteListRequired: &localHostOnlyIPRequired,
		cidrs: []net.IPNet{
			net.IPNet{
				IP:   net.IPv4(127, 0, 0, 1),
				Mask: net.IPv4Mask(255, 255, 255, 255),
			},
		},
	}
)

type server struct {
	// The IP address to bind too.
	Addr value `toml:"addr"`

	// When encrypting and decrypting http cookies we need to use an AES
	// secret. This is to ensure that the value of the cookie is not readable
	// by callers. AES keys can be loaded two different ways. We allow them
	// to be specified manually as a map for simplicity if the config doesn't
	// require security, otherwise they can be loaded as a JSON element via
	// the secrets manager loader that we use for certificates as well.
	//
	// Its valid to use one of these and not both.
	AESKeys       []string `toml:"aes_keys"`
	AESKeysURL    *string  `toml:"aes_keys_url"`
	aesKeysLoader *secretloader.AESKeys

	// The port to listen on.
	Port value `toml:"port"`
	port int

	// The Host name that this Blobby server will report back as. This is
	// primarily used for any situation where a full return URL needs to
	// be generated.
	HostName *string `toml:"host_name"`

	// TLS related configuration.
	TLS        *bool   `toml:"tls"`
	TLSCertURL *string `toml:"tls_certificate_url"`
	TLSKeyURL  *string `toml:"tls_private_key_url"`

	// Refreshes certificates on an interface.
	// FIXME: Deprecated, this is not used anymore.
	TLSRefreshInterval *time.Duration `toml:"tls_refresh_interval"`

	// Timeouts for Reads and Writes.
	ReadTimeout       *time.Duration `toml:"read_timeout"`
	ReadHeaderTimeout *time.Duration `toml:"read_header_timeout"`
	WriteTimeout      *time.Duration `toml:"write_timeout"`
	IdleTimeout       *time.Duration `toml:"idle_timeout"`

	// The prefix for the namespace= tag; a value of blobby_ for this field
	// would give blobby_namespace as the tag key in the rendered Prometheus
	// metrics.
	PrometheusTagPrefix *string `toml:"prometheus_tag_prefix"`

	// Maximum header sizes.
	MaxHeaderBytes value `toml:"max_header_bytes"`
	maxHeaderBytes int

	// Enable debug HTTP URLs and the ACL that controls access to those
	// functions.
	DebugPathsEnable *bool `toml:"debug_paths_enable"`
	DebugPathsACL    *acl  `toml:"debug_paths_acl"`

	// Likewise we allow setting up an ACL on who can access the health check
	// and metrics endpoints.
	HealthCheckACL *acl `toml:"health_check_acl"`

	// Controls who has access to the /_status endpoint.
	StatusACL *acl `toml:"status_acl"`

	// Controls who has access to the /_shutdown endpoint.
	ShutDownACL *acl `toml:"shut_down_acl"`

	// The access log configuration.
	AccessLog *log `toml:"access_log"`

	// Enable tracing on incoming requests for better insight on performance.
	EnableTracing *bool `toml:"enable_tracing"`

	// If enabled then a list of users will be loaded from the given HTPasswd
	// secrets URL. These users will be able to login via the _login URL
	// available on the server. We also keep a local WebAuthProvider object
	// that handles user management for the whole server.
	WebUsersHTPasswdURL *string `toml:"web_users_htpasswd_url"`
	webAuthProvider     *access.WebAuthProvider
	webUsersHTPasswd    *secretloader.HTPasswd

	// When a web user logins in they will be granted access to the system
	// for this much time before they are required to authenticate again.
	WebLoginDuration *time.Duration `toml:"web_login_duration"`

	// If using web logins then this controls what the name of the cookie
	// will be. If this is not set then the server will fall back to the
	// default value.
	WebAuthCookieName *string `toml:"web_auth_cookie_name"`

	// Some settings related to cookies that will be used with Web Logins or
	// SAML. These control properties which domain the cookies will be set
	// to use, and if they require SSL. UseSecureCookies will default to
	// the value of TLS, and leaving CookieDomain unset will default to not
	// setting domain on the cookie which will cause the browser to only send
	// it to the host that is serving the request. In both cases the default
	// values should be sufficient.
	UseSecureCookies *bool   `toml:"use_secure_cookies"`
	CookieDomain     *string `roml:"cookie_domain"`

	// The cookie encrypting/decrypting tool that will be used for web based
	// logins.
	cookieTool *cookie.CookieTool

	// The top object attached to this.
	top *top

	// A cache of the created httpserver.Server object.
	server httpserver.Server

	// A certificate loader that will be used for initializing TLS
	// configuration.
	tlsCerts *secretloader.Certificate
}

func (s *server) initLogging() {
	if s.AccessLog != nil {
		s.AccessLog.initLogging()
	}
	if s.DebugPathsACL != nil {
		s.DebugPathsACL.initLogging()
	}
	if s.HealthCheckACL != nil {
		s.HealthCheckACL.initLogging()
	}
	if s.tlsCerts != nil {
		s.tlsCerts.Logger = s.top.Log.logger.
			NewChild().
			AddField("component", "tls-loader").
			AddField("certificate-url", *s.TLSCertURL).
			AddField("private-key-url", *s.TLSKeyURL)
	}
	if s.webUsersHTPasswd != nil {
		s.webUsersHTPasswd.Logger = s.top.Log.logger.
			NewChild().
			AddField("component", "htpasswd-loader").
			AddField("url", *s.WebUsersHTPasswdURL)
	}
	if s.aesKeysLoader != nil {
		s.aesKeysLoader.Logger = s.top.Log.logger.
			NewChild().
			AddField("url", *s.AESKeysURL).
			AddField("component", "aes-keys-loader")
	}
}

// Returns the WebAuthProvider that should be used for web based
// authentication.
func (s *server) WebAuthProvider() *access.WebAuthProvider {
	if s.webAuthProvider == nil {
		s.webAuthProvider = &access.WebAuthProvider{
			CookieDomain:   *s.CookieDomain,
			CookieName:     *s.WebAuthCookieName,
			CookieTool:     s.cookieTool,
			CookieValidity: *s.WebLoginDuration,
			Users:          s.webUsersHTPasswd,
		}
	}
	return s.webAuthProvider
}

// Returns the HTTP server that was created. This must be done after
// logging is initialized!
func (s *server) Server() httpserver.Server {
	if s.server == nil {
		nss := make(
			map[string]*httpserver.NameSpaceSettings,
			len(s.top.NameSpace))
		for name, ns := range s.top.NameSpace {
			nss[name] = ns.getNameSpaceSettings()
		}
		var samlMap map[string]*access.SAML
		if len(s.top.SAML) > 0 {
			samlMap = make(map[string]*access.SAML, len(s.top.SAML))
			for name, s := range s.top.SAML {
				samlMap[name] = s.Provider()
			}
		}

		logger := s.top.Log.logger.
			NewChild().
			AddField("component", "http-server")
		settings := &httpserver.Settings{
			Addr:                s.Addr.String(),
			DebugPathsACL:       s.DebugPathsACL.access(),
			EnableDebugPaths:    s.debugging(),
			EnableTracing:       *s.EnableTracing,
			HealthCheckACL:      s.HealthCheckACL.access(),
			IdleTimeout:         *s.IdleTimeout,
			Logger:              logger,
			MaxHeaderBytes:      s.maxHeaderBytes,
			NameSpaces:          nss,
			Port:                s.port,
			PrometheusTagPrefix: *s.PrometheusTagPrefix,
			ReadTimeout:         *s.ReadTimeout,
			SAMLAuth:            samlMap,
			ShutDownACL:         s.ShutDownACL.access(),
			StatusACL:           s.StatusACL.access(),
			TLSCerts:            s.tlsCerts,
			WriteTimeout:        *s.WriteTimeout,
		}
		if s.webUsersHTPasswd != nil {
			settings.WebAuthProvider = s.WebAuthProvider()
			if s.cookieTool == nil {
				panic("Can not setup web auth without cookie encryption config")
			}
		}
		s.server = httpserver.New(settings)
	}
	return s.server
}

func (s *server) debugging() bool {
	return *s.DebugPathsEnable
}

func (s *server) validate(t *top) []string {
	var errors []string
	var err error

	// Set the top object.
	s.top = t

	// Addr
	if !s.Addr.set {
		s.Addr.raw = []byte{}
	} else if net.ParseIP(s.Addr.String()) == nil {
		errors = append(errors, "server.addr is not a valid ip.")
	}

	// AESKeys / AESKeysURL
	if s.AESKeys != nil && s.AESKeysURL != nil {
		errors = append(errors, fmt.Sprintf(
			"server.aes_keys and server.aes_keys_url are mutually exclusive."))
	} else if s.AESKeys != nil {
		// The keys are configured directly in the text file. Each needs to
		// be validated.
		list := make(aesKeyList, len(s.AESKeys))
		for i, k := range s.AESKeys {
			switch len(k) * 4 {
			case 128:
			case 192:
			case 256:
			default:
				errors = append(errors, fmt.Sprintf(
					"server.aes_keys[%d] is not a valid AES key length.",
					i))
				continue
			}
			raw, err := hex.DecodeString(k)
			if err != nil {
				errors = append(errors, fmt.Sprintf(
					"server.aes_keys[%d] is not a valid HEx string: %s",
					i,
					err.Error()))
				continue
			}
			list[i], err = aes.NewCipher(raw)
			if err != nil {
				errors = append(errors, fmt.Sprintf(
					"server.aes_keys[%d] is not a valid AES key: %s",
					i,
					err.Error()))
				continue
			}
		}
		s.cookieTool = &cookie.CookieTool{
			AESKeys: list.Get,
		}
	} else if s.AESKeysURL != nil {
		// The keys are configured to be loaded out of secrets manager.
		l, err := secretloader.NewLoader(
			*s.AESKeysURL,
			s.top.getProfiles())
		if err != nil {
			errors = append(errors, fmt.Sprintf(
				"server.aes_keys_url is not valid: %s", err.Error()))
		} else {
			s.aesKeysLoader = &secretloader.AESKeys{
				Source: l,
			}
		}
		s.cookieTool = &cookie.CookieTool{
			AESKeys: s.aesKeysLoader.Keys,
		}
	} else if s.WebUsersHTPasswdURL != nil {
		errors = append(errors, fmt.Sprintf(""+
			"server.aes_keys or server.aes_keys_url must be used if web "+
			"logins are enabled."))
	}

	// Port
	if !s.Port.set {
		s.port = defaultPort
	} else if s.port, err = s.Port.Int(); err != nil {
		errors = append(errors, "server.port must be an integer.")
	} else if s.port < 1 || s.port > 65535 {
		errors = append(errors, "server.port is not a valid port.")
	}

	// HostName
	if s.HostName != nil && !isDomainName(*s.HostName) {
		errors = append(
			errors,
			"server.host_name is not a valid host name.")
	}

	// TLS
	if s.TLS == nil {
		s.TLS = &defaultTLS
	}
	if *s.TLS {
		s.tlsCerts = &secretloader.Certificate{}
	}

	// TLSCertURL
	if !*s.TLS && s.TLSCertURL != nil {
		errors = append(errors, ""+
			"server.tls_certificate_url can not be used when server.tls "+
			"is false")
	} else if *s.TLS && s.TLSKeyURL == nil {
		errors = append(
			errors,
			"server.tls_certificate_url is required when server.tls is true.")
	} else if *s.TLS {
		l, err := secretloader.NewLoader(
			*s.TLSCertURL,
			s.top.getProfiles())
		if err != nil {
			errors = append(errors, fmt.Sprintf(
				"server.tls_certificate_url is not valid: %s", err.Error()))
		} else {
			s.tlsCerts.Certificate = l
		}
	}

	// TLSKeyURL
	if !*s.TLS && s.TLSKeyURL != nil {
		errors = append(errors, ""+
			"server.tls_private_key_url can not be used when server.tls "+
			"is false")
	} else if *s.TLS && s.TLSKeyURL == nil {
		errors = append(
			errors,
			"server.tls_private_key_url is required when server.tls is true.")
	} else if *s.TLS {
		l, err := secretloader.NewLoader(
			*s.TLSKeyURL,
			s.top.getProfiles())
		if err != nil {
			errors = append(errors, fmt.Sprintf(
				"server.tls_private_key_url is not valid: %s", err.Error()))
		} else {
			s.tlsCerts.Private = l
		}
	}

	// ReadTimeout
	if s.ReadTimeout == nil {
		s.ReadTimeout = &defaultReadTimeout
	} else if *s.ReadTimeout < time.Second {
		errors = append(errors, "server.read_timeout must be larger than 1s.")
	}

	// ReadHeaderTimeout
	if s.ReadHeaderTimeout == nil {
		s.ReadHeaderTimeout = &defaultReadHeaderTimeout
	} else if *s.ReadTimeout < time.Second {
		errors = append(
			errors,
			"server.read_header_timeout must be larger than 1s.")
	}

	// WriteTimeout
	if s.WriteTimeout == nil {
		s.WriteTimeout = &defaultWriteTimeout
	} else if *s.WriteTimeout < time.Second {
		errors = append(errors, "server.read_timeout must be larger than 1s.")
	}

	// IdleTimeout
	if s.IdleTimeout == nil {
		s.IdleTimeout = &defaultIdleTimeout
	} else if *s.ReadTimeout < time.Second {
		errors = append(errors, "server.idle_timeout must be larger than 1s.")
	}

	// MaxHeaderBytes
	if !s.MaxHeaderBytes.set {
		s.maxHeaderBytes = defaultMaxHeaderBytes
	} else if m, err := s.MaxHeaderBytes.Bytes(); err != nil {
		errors = append(
			errors,
			fmt.Sprintf("server.max_header_bytes %s.", err.Error()))
	} else if m < 4096 {
		errors = append(
			errors,
			"server.max_header_bytes can not be less than 4KB.")
	} else if m > 1<<30 {
		errors = append(
			errors,
			"server.max_header_bytes can not be greater than 1GB.")
	} else {
		s.maxHeaderBytes = int(m)
	}

	// DebugPathsEnable
	if s.DebugPathsEnable == nil {
		s.DebugPathsEnable = &defaultDebugPathsEnable
	}

	// DebugPathsACL
	if s.DebugPathsACL == nil {
		s.DebugPathsACL = &localHostOnlyACL
	} else {
		errors = append(
			errors,
			s.DebugPathsACL.validate(t, "server.debug_paths_acl")...)
	}

	// StatusACL
	if s.StatusACL == nil {
		s.StatusACL = &localHostOnlyACL
	} else {
		errors = append(
			errors,
			s.StatusACL.validate(t, "server.status_acl")...)
	}

	// ShutDownACL
	if s.ShutDownACL == nil {
		s.ShutDownACL = &localHostOnlyACL
	} else {
		errors = append(
			errors,
			s.ShutDownACL.validate(t, "server.shut_down_acl")...)
	}

	// HealthCheckACL
	if s.HealthCheckACL == nil {
		s.HealthCheckACL = &localHostOnlyACL
	} else {
		errors = append(
			errors,
			s.HealthCheckACL.validate(t, "server.health_check_acl")...)
	}

	// AccessLog
	if s.AccessLog != nil {
		errors = append(
			errors,
			s.AccessLog.validate(t, "server.access_log")...,
		)
	}

	// NameSpaceTagKeyPrefix
	if s.PrometheusTagPrefix == nil {
		s.PrometheusTagPrefix = &defaultPrometheusTagPrefix
	}

	// EnableTracing
	if s.EnableTracing == nil {
		s.EnableTracing = &defaultEnableTracing
	}

	// WebUsersHTPasswdURL
	if s.WebUsersHTPasswdURL != nil {
		l, err := secretloader.NewLoader(
			*s.WebUsersHTPasswdURL,
			s.top.getProfiles())
		if err != nil {
			errors = append(errors, fmt.Sprintf(
				"server.web_users_htpsswd_url: invalid url: %s",
				err.Error()))
		} else {
			s.webUsersHTPasswd = &secretloader.HTPasswd{
				Source: l,
			}
		}
	}

	// WebLoginDuration
	if s.WebLoginDuration == nil {
		s.WebLoginDuration = &defaultWebLoginDuration
	} else if *s.WebLoginDuration < 0 {
		errors = append(
			errors,
			"server.web_login_duration: can not be negative.")
	} else if *s.WebLoginDuration < time.Minute {
		errors = append(
			errors,
			"servier.web_login_duration: must be at least 1 minute.")
	}

	// WebAuthCookieName
	if s.WebAuthCookieName == nil {
		s.WebAuthCookieName = &defaultWebAuthCookieName
	} else if !isValidCookieName(*s.WebAuthCookieName) {
		errors = append(
			errors,
			"server.web_auth_cookie_name: not a valid cookie name.")
	}

	// UseSecureCookies
	if s.UseSecureCookies == nil {
		s.UseSecureCookies = s.TLS
	}

	// CookieDomain
	if s.CookieDomain != nil && !isValidCookieDomain(*s.CookieDomain) {
		errors = append(
			errors,
			"server.cookie_domain: invalid domain.")
	} else if s.CookieDomain == nil {
		s.CookieDomain = &defaultCookieDomain
	}

	// Return any errors encountered.
	return errors
}

// The HTTP server requires a function that returns a list of AES Keys which
// works well for the loader, however for static configured keys we need a way
// to convert a list into a function which is what this wrapper does.
type aesKeyList []cipher.Block

func (a aesKeyList) Get() ([]cipher.Block, error) {
	return []cipher.Block(a), nil
}

// This initially came from the golang source code which is released under
// a MIT style license that requires attribution:
//   https://golang.org/src/net/dnsclient.go
//   Copyright (c) 2009 The Go Authors. All rights reserved.
//   https://golang.org/LICENSE
//
// Its inclusion here is purely because its not public in the net package,
// and I needed a way to validate that a host name given as a configuration
// parameter is valid.
func isDomainName(s string) bool {
	// See RFC 1035, RFC 3696.
	// Presentation format has dots before every label except the first, and the
	// terminal empty label is optional here because we assume fully-qualified
	// (absolute) input. We must therefore reserve space for the first and last
	// labels' length octets in wire format, where they are necessary and the
	// maximum total length is 255.
	// So our _effective_ maximum is 253, but 254 is not rejected if the last
	// character is a dot.
	l := len(s)
	if l == 0 || l > 254 || l == 254 && s[l-1] != '.' {
		return false
	}

	last := byte('.')
	nonNumeric := false // true once we've seen a letter or hyphen
	partlen := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		switch {
		default:
			return false
		case 'a' <= c && c <= 'z' || 'A' <= c && c <= 'Z' || c == '_':
			nonNumeric = true
			partlen++
		case '0' <= c && c <= '9':
			// fine
			partlen++
		case c == '-':
			// Byte before dash cannot be dot.
			if last == '.' {
				return false
			}
			partlen++
			nonNumeric = true
		case c == '.':
			// Byte before dot cannot be dot, dash.
			if last == '.' || last == '-' {
				return false
			}
			if partlen > 63 || partlen == 0 {
				return false
			}
			partlen = 0
		}
		last = c
	}
	if last == '-' || partlen > 63 {
		return false
	}

	return nonNumeric
}
