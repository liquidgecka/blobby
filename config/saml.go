package config

import (
	"fmt"
	"net/url"
	"time"

	"github.com/liquidgecka/blobby/httpserver/access"
	"github.com/liquidgecka/blobby/httpserver/secretloader"
	"github.com/liquidgecka/blobby/internal/sloghelper"
)

var (
	defaultSAMLCookieName     = "bsa"
	defaultSAMLLoginDuration  = time.Hour
	defaultSAMLCookieValidity = time.Hour * 24
	defaultUserNameAttribute  = "uid"
)

type saml struct {
	// The URL that will return the metadata for the Identity Provider.
	IDPMetaDataURL *string `toml:"idp_metadata_url"`

	// The URL of the Identity Provider that will be serving SAML
	// authentication requests. This is a required field and must
	// be a valid URL.
	IDPURL *string `toml:"idp_url"`

	// The Certificate and Private Key URL that will be used for loading
	// the certificate for the SAML provider.
	CertificateURL *string `toml:"certificate_url"`
	PrivateKeyURL  *string `toml:"private_key_url"`

	// The domain to attach to the cookie. If this is not provided then it
	// will default to the host being accessed by the browser.
	CookieDomain *string `toml:"cookie_domain"`

	// The name of the cookie that will be used for storing SAML authentication
	// details.
	CookieName *string `toml:"cookie_name"`

	// When a user successfully authenticates via SAML the cookie will be
	// valid for this amount of time, after which the user will be forced
	// to log in again.
	CookieValidity *time.Duration `toml:"cookie_validity"`

	// How long to give a user to login after being passed off to the SAML
	// IDP before the login attempt is considered invalid.
	LoginDuration *time.Duration `toml:"login_duration"`

	// The attribute name that we can extract the user name from.
	UserNameAttribute *string `toml:"user_name_attribute"`

	// A reference to the secret loader that should be used for loading
	// the certificate and private key.
	certs *secretloader.Certificate

	// The generated SAML that this SAML configuration creates.
	provider *access.SAML

	// A link back to the top of the configuration tree.
	top *top

	// Keep a reference to the name of this SAML configuration.
	name string
}

func (s *saml) initLogging() {
	s.certs.Logger = s.top.Log.logger.With(
		sloghelper.String("component", "saml-cert-loader"),
		sloghelper.String("certificate-url", *s.CertificateURL),
		sloghelper.String("private-key-url", *s.PrivateKeyURL))
}

// Generates a SAML return URL that should be used for the given resource.
func (s *saml) URL(r string) string {
	var proto string
	var port string
	if *s.top.Server.TLS {
		proto = "https"
		if p, _ := s.top.Server.Port.Int(); p != 443 {
			port = fmt.Sprintf(":%d", p)
		}
	} else {
		proto = "http"
		if p, _ := s.top.Server.Port.Int(); p != 80 {
			port = fmt.Sprintf(":%d", p)
		}
	}
	return fmt.Sprintf(
		"%s://%s%s/_saml/%s/%s",
		proto,
		*s.top.Server.HostName,
		port,
		s.name,
		r)
}

func (s *saml) Provider() *access.SAML {
	if s.provider == nil {
		s.provider = &access.SAML{
			Provider: &secretloader.SAMLProvider{
				ACSURL:         s.URL("acs"),
				Certificate:    *s.certs,
				IDPMetaDataURL: *s.IDPMetaDataURL,
				IDPURL:         *s.IDPURL,
				MetaDataURL:    s.URL("metadata"),
				SLOURL:         s.URL("slo"),
			},
			CookieValidity:    *s.CookieValidity,
			CookieName:        *s.CookieName,
			CookieDomain:      *s.CookieDomain,
			CookieSecure:      *s.top.Server.TLS,
			CookieTool:        s.top.Server.cookieTool,
			LoginDuration:     *s.LoginDuration,
			UserNameAttribute: *s.UserNameAttribute,
		}
	}
	return s.provider
}

func (s *saml) validate(top *top, name string) []string {
	var errors []string
	s.top = top
	s.name = name
	s.certs = &secretloader.Certificate{}

	// Server.HostName (used for SAML hostname generation.)
	if s.top.Server.HostName == nil {
		errors = append(errors, "server.host_name is required to use saml.")
	}

	// Server.AESKeys (used for cookie configuration.)
	if s.top.Server.cookieTool == nil {
		errors = append(errors, "server.aes_keys* is required to use SAML.")
	}

	// IDPMetaDataURL
	if s.IDPMetaDataURL == nil {
		errors = append(
			errors,
			"saml."+name+".idp_meta_data_url is required.")
	} else if u, err := url.Parse(*s.IDPMetaDataURL); err != nil {
		errors = append(
			errors,
			"saml."+name+".idp_metadata_url is not valid: "+err.Error())
	} else if u.Scheme != "http" && u.Scheme != "https" {
		errors = append(
			errors,
			"saml."+name+".idp_metadata_url is not a http/https url.")
	} else if u.Host == "" {
		errors = append(
			errors,
			"saml."+name+".idp_metadata_url requires a host.")
	}

	// IDPURL
	if s.IDPURL == nil {
		errors = append(
			errors,
			"saml."+name+".idp_url is required.")
	} else if u, err := url.Parse(*s.IDPURL); err != nil {
		errors = append(
			errors,
			"saml."+name+".idp_url is not a valid url: "+err.Error())
	} else if u.Scheme != "http" && u.Scheme != "https" {
		errors = append(
			errors,
			"saml."+name+".idp_url is not a http/https url.")
	} else if u.Host == "" {
		errors = append(
			errors,
			"saml."+name+".idp_url requires a host.")
	}

	// CertificateURL
	if s.CertificateURL == nil {
		errors = append(
			errors,
			"saml."+name+".certificate_url is required.")
	} else {
		l, err := secretloader.NewLoader(
			*s.CertificateURL,
			s.top.getProfiles())
		if err != nil {
			errors = append(
				errors,
				"saml."+name+".certificate_url is invalid: "+err.Error())
		} else {
			s.certs.Certificate = l
		}
	}

	// PrivateKeyURL
	if s.PrivateKeyURL == nil {
		errors = append(
			errors,
			"saml."+name+".pivate_key_url is required.")
	} else {
		l, err := secretloader.NewLoader(
			*s.PrivateKeyURL,
			s.top.getProfiles())
		if err != nil {
			errors = append(
				errors,
				"saml."+name+".private_key_url is invalid: "+err.Error())
		} else {
			s.certs.Private = l
		}
	}

	// CookieDomain
	if s.CookieDomain != nil && !isValidCookieDomain(*s.CookieDomain) {
		errors = append(
			errors,
			"saml."+name+".cookie_domain: invalid domain.")
	} else if s.CookieDomain == nil {
		s.CookieDomain = &defaultCookieDomain
	}

	// CookieName
	if s.CookieName == nil {
		s.CookieName = &defaultSAMLCookieName
	} else if !isValidCookieName(*s.CookieName) {
		errors = append(
			errors,
			"saml."+name+".cookie_name: not a valid cookie name.")
	}

	// CookieValidity
	if s.CookieValidity == nil {
		s.CookieValidity = &defaultSAMLCookieValidity
	} else if *s.CookieValidity < 0 {
		errors = append(
			errors,
			"saml."+name+".cookie_validity: can not be negative.")
	} else if *s.CookieValidity < time.Minute {
		errors = append(
			errors,
			"saml."+name+".cookie_validity: must be at least 1 minute.")
	}

	// LoginDuration
	if s.LoginDuration == nil {
		s.LoginDuration = &defaultSAMLLoginDuration
	} else if *s.LoginDuration < 0 {
		errors = append(
			errors,
			"saml."+name+".login_duration: can not be negative.")
	} else if *s.LoginDuration < time.Minute {
		errors = append(
			errors,
			"saml."+name+".login_duration: must be at least 1 minute.")
	}

	// UserNameAttribute
	if s.UserNameAttribute == nil {
		s.UserNameAttribute = &defaultUserNameAttribute
	} else if *s.UserNameAttribute == "" {
		errors = append(
			errors,
			"saml."+name+".user_name_attribute can not be empty.")
	}

	// Return any errors encountered.
	return errors
}
