package access

import (
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"log/slog"
	"math/rand"
	"net/http"
	"strings"
	"time"

	"github.com/crewjam/saml"

	"github.com/liquidgecka/blobby/httpserver/cookie"
	"github.com/liquidgecka/blobby/httpserver/request"
	"github.com/liquidgecka/blobby/httpserver/secretloader"
	"github.com/liquidgecka/blobby/internal/sloghelper"
)

const (
	defaultSAMLCookieName = "sua"
)

// A structure used for representing the auth cookie.
type samlAuthCookie struct {
	User    string      `json:"u"`
	Tags    []string    `json:"t"`
	Expires cookie.Time `json:"e"`
}

// The contents of the cookie that will be set when the user initiates a
// SAML authentication. This needs to contain enough information for us
// to return the user back to where they came from on the other side of the
// SAML request.
type samlRequestToken struct {
	// We don't want a user to initiate a SAML request and then return
	// days or months later only to see things work in an unexpected way.
	Expires cookie.Time `json:"e"`

	// The URL that should be redirected too once the user returns having
	// successfully authenticated.
	ReturnURL string `json:"r"`

	// We also verify that the request matches the one the user successfully
	// authenticated with. This prevents the request from being associated
	// with the wrong authentication request.
	ID string `json:"id"`
}

// Handle all of configuration for a SAML provider that can be used with
// access control lists.
type SAML struct {
	// The Secret Loader that manages access to the SAML Identity Provider.
	Provider *secretloader.SAMLProvider

	// How long to allow SAML based authentication to be valid for. Setting
	// this too high will make it impossible to expunge a logged in user from
	// the system without removing the user completely. Setting it too low
	// will be annoying for users as they will have to login constantly.
	CookieValidity time.Duration

	// The name of the cookie to use for authentication. This name should
	// not conflict with any other possible cookies used on this domain.
	CookieName string

	// The domain that the cookie will be limited too.
	CookieDomain string

	// Used for encrypting the contents of the cookie.
	CookieTool *cookie.CookieTool

	// Set to secure if the cookie should only ever be returned over https.
	CookieSecure bool

	// The amount of time that a user has after being directed to the
	// Identity Provider before the login attempt is considered
	// expired.
	LoginDuration time.Duration

	// The name of the SAML attribute that will be used when assigning login
	// names. This is useful if your provider uses displayName over say
	// uid. If not set this will default to "uid"
	UserNameAttribute string

	// The name of the SAML attribute that will be used when setting up
	// tags. Each tag within this attribute will be usable with the
	// UserTags portion of the SAMLAuth object type.
	TagAttribute string
}

// Writes the meta data for the service provider out to the given
// request object.
func (s *SAML) MetaData(ir *request.Request) {
	md, err := s.Provider.MetaData(ir.Context)
	if err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusInternalServerError,
			Response: "Internal Server Error.",
			Err:      err,
		})
	} else if md == nil {
		panic(&request.HTTPError{
			Status:   http.StatusInternalServerError,
			Response: "Internal Server Error.",
			Err:      fmt.Errorf("No metadata returned from provider."),
		})
	} else {
		ir.Header().Set("Content-Type", "application/samlmetadata+xml")
		encoder := xml.NewEncoder(ir)
		encoder.Encode(md)
	}
}

// Called when an IDP response is issued to the httpserver. This will take
// the response, validate it, and if its valid then sets a cookie that will
// be used for future requests.
func (s *SAML) Post(ir *request.Request) {
	// Parse out the values from the form. This will return metadata
	// and values that we need to validate that the user successfully
	// authenticated against our SAML provider.
	ir.Request.ParseForm()
	c := samlRequestToken{}
	if rs := ir.Request.Form.Get("RelayState"); rs == "" {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Malformed SAML response (Missing RelayState value).",
		})
	} else if cookie, err := ir.Request.Cookie(rs); err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Malformed SAML response (Missing RelayState cookie).",
		})
	} else if err := s.CookieTool.Decode(ir.Context, cookie.Value, &c); err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Malformed SAML response (Invalid RelayState cookie).",
		})
	}

	// We have received the form details. Now we need to validate that all
	// of the details in the request match as expected. This will return the
	// assertions made by the identity provider about the user that
	// authenticated.
	atn, err := s.Provider.ParseResponse(ir.Context, ir.Request, c.ID)
	if err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusForbidden,
			Response: "SAML Response not valid.",
			Err:      err,
		})
	}

	// The next step is to extract the user name and tags from the SAML
	// response and store then in the cookie that we are going to set to
	// authenticate against requests later.
	ac := samlAuthCookie{}
	var userAttrib string
	if s.UserNameAttribute == "" {
		userAttrib = "uid"
	} else {
		userAttrib = s.UserNameAttribute
	}
	if attribs, ok := s.getAttribute(atn, userAttrib); !ok {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid SAML Response.",
			Err: fmt.Errorf(
				"SAML Response does not contain a user attribute (%s)",
				userAttrib),
		})
	} else {
		ac.User = attribs[0]
	}
	if s.TagAttribute != "" {
		if a, ok := s.getAttribute(atn, s.TagAttribute); ok && len(a) > 0 {
			ac.Tags = a
		}
	}

	// Success! The user has successfully authenticated via the Identity
	// Provider. We can now set a session cookie for them and redirect them
	// to the place they intended to go in the first place.
	ac.Expires.Time = time.Now().Add(s.CookieValidity)
	ctext, err := s.CookieTool.Encode(ir.Context, &ac)
	if err != nil {
		panic(err)
	}
	http.SetCookie(ir, &http.Cookie{
		Name:    s.cookieName(),
		Value:   ctext,
		Path:    "/",
		Domain:  s.CookieDomain,
		Expires: ac.Expires.Time,
		Secure:  s.CookieSecure,
	})

	// And finally, redirect the user back to the place that they wanted
	// to go in the very first place.
	if c.ReturnURL == "" {
		ir.Header().Add("Location", "/")
	} else {
		ir.Header().Add("Location", c.ReturnURL)
	}
	ir.WriteHeader(http.StatusFound)

	// Log the quthentication.
	if ir.Log.Enabled(ir.Context, slog.LevelDebug) {
		ir.Log.LogAttrs(
			ir.Context,
			slog.LevelDebug,
			"User  successfully authenticated.",
			sloghelper.String("user", ac.User),
			sloghelper.String("tags", strings.Join(ac.Tags, ",")))
	}
}

// Returns the cookie name if set, or the default value if not set.
func (s *SAML) cookieName() string {
	if s.CookieName == "" {
		return defaultSAMLCookieName
	} else {
		return s.CookieName
	}
}

// Parses a saml.Assertion to look for a given attribute. This will walk
// down the tree looking for an attribute with the given friendly name. If
// found it returns it and true, otherwise it returns nil and
// false.
func (s *SAML) getAttribute(a *saml.Assertion, name string) ([]string, bool) {
	if a == nil {
		return nil, false
	}
	var r []string
	for _, as := range a.AttributeStatements {
		for _, a := range as.Attributes {
			if a.Name == name || a.FriendlyName == name {
				for _, v := range a.Values {
					r = append(r, v.Value)
				}
			}
		}
	}
	return r, len(r) > 0
}

// A specific implementation of a SAMLAuth that will validate that the
// user has logged in.
type SAMLAuth struct {
	// The reference to the common provider for this SAML instance.
	Source *SAML

	// If provided then users must have all of the given tags to be able
	// to authenticate to this service.
	UserTags []string
}

// If triggered then this will generate a response to the request. For SAML
// this specifically will redirect to the appropriate login page.
func (s *SAMLAuth) assert(ir *request.Request) {
	// Create a request that can be forwarded to the identify provider.
	req, err := s.Source.Provider.AuthenticationRequest(ir.Context)
	if err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusInternalServerError,
			Response: "Internal Server Error.",
			Err:      err,
		})
	}

	// We need to create a cookie that contains information about the
	// SAML request being generated so that we can authorize it once the
	// caller returns. This will be stored in a cookie with a randomly
	// generated name so that we don't have to worry about size limits.
	// The name of the cookie is what will be stored in the request to
	// the IDP.
	c := samlRequestToken{
		ReturnURL: ir.Request.URL.String(),
		ID:        req.ID,
	}
	c.Expires.Time = time.Now().Add(s.Source.LoginDuration)

	// We set the cookie with a random name and store that in the SAML request
	// so we can read it later.
	rawCookieName := make([]byte, 8)
	rand.Read(rawCookieName)
	cookieName := hex.EncodeToString(rawCookieName)
	value, err := s.Source.CookieTool.Encode(ir.Context, &c)
	if err != nil {
		panic(err)
	}
	http.SetCookie(ir, &http.Cookie{
		Name:    cookieName,
		Value:   value,
		Path:    "/",
		Domain:  s.Source.CookieDomain,
		Expires: time.Now().Add(s.Source.LoginDuration),
		Secure:  s.Source.CookieSecure,
	})

	// For now we just use redirection based SAML auth.
	ir.Header().Add("Location", req.Redirect(cookieName).String())
	panic(&request.HTTPError{Status: http.StatusFound})
}

// Checks to see if the given request is allowed to access the given resource.
func (s *SAMLAuth) check(ir *request.Request) bool {
	v := samlAuthCookie{}
	if cook, err := ir.Request.Cookie(s.Source.CookieName); err != nil {
		if ir.Log.Enabled(ir.Context, slog.LevelDebug) {
			ir.Log.LogAttrs(
				ir.Context,
				slog.LevelDebug,
				"Authentication cookie not set.")
		}
		return false
	} else if err := s.Source.CookieTool.Decode(ir.Context, cook.Value, &v); err != nil {
		if ir.Log.Enabled(ir.Context, slog.LevelDebug) {
			ir.Log.LogAttrs(
				ir.Context,
				slog.LevelDebug,
				"Authentication cookie is set, but is invalid.")
		}
		return false
	} else if v.Expires.Before(time.Now()) {
		if ir.Log.Enabled(ir.Context, slog.LevelDebug) {
			ir.Log.LogAttrs(
				ir.Context,
				slog.LevelDebug,
				"Authentication cookie is expired.",
				sloghelper.String("user", v.User),
				sloghelper.String("tags", strings.Join(v.Tags, ",")),
				sloghelper.String("expires", v.Expires.String()))
		}
		return false
	} else if len(s.UserTags) == 0 {
		if ir.Log.Enabled(ir.Context, slog.LevelDebug) {
			ir.Log.LogAttrs(
				ir.Context,
				slog.LevelDebug,
				"User is authenticated and valid. No tags are required.",
				sloghelper.String("user", v.User),
				sloghelper.String("tags", strings.Join(v.Tags, ",")))
		}
		return true
	} else {
		for _, want := range s.UserTags {
			for _, have := range v.Tags {
				if have == want {
					ir.Log.LogAttrs(
						ir.Context,
						slog.LevelDebug,
						"User is authenticated and a tag matches.",
						sloghelper.String("tag", want),
						sloghelper.String("user", v.User),
						sloghelper.String("tags", strings.Join(v.Tags, ",")))
					return true
				}
			}
		}
		ir.Log.LogAttrs(
			ir.Context,
			slog.LevelDebug,
			"Authentication cookie is valid, but tags do not match.",
			sloghelper.String("user", v.User),
			sloghelper.String("user", v.User))
		return false
	}
}

func (s *SAMLAuth) proxy(source, dest *http.Request) {
	cook, err := source.Cookie(s.Source.CookieName)
	if err != nil {
		return
	}
	dest.AddCookie(cook)
}
