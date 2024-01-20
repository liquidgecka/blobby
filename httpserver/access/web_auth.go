package access

import (
	"fmt"
	"math/rand"
	"net/http"
	"net/url"
	"time"

	"github.com/liquidgecka/blobby/httpserver/cookie"
	"github.com/liquidgecka/blobby/httpserver/request"
	"github.com/liquidgecka/blobby/httpserver/secretloader"
)

// A structure used for representing the auth cookie.
type webAuthCookie struct {
	User    string      `json:"u"`
	Expires cookie.Time `json:"e"`
}

// When logging in via /_login the browser is given a token value that
// contains an encrypted set of parameters which are used to ensure that
// attackers are not able to CSRF and such.
type loginFormToken struct {
	// The time that this login cookie expires. This prevents cookie replays
	// by copying the contents of a previously generated cookie.
	Expires cookie.Time `json:"e"`

	// The name of the `user` form field
	UserField string `json:"u"`

	// The name of the `password` form field.
	PassField string `json:"p"`

	// The URL that the user should be returned too once they have
	// successfully authenticated.
	ReturnURL string
}

// If a Request can be authenticated with Web Auth then this will handle
// the authentication cycle.
type WebAuthProvider struct {
	// Access to the htpasswd data.
	Users *secretloader.HTPasswd

	// How long to allow the web authentication to be valid for. Setting this
	// too high will make it impossible to expunge a logged in user from the
	// system without removing the user entry completely. Setting it too low
	// will be annoying for users as they will have to login in constantly.
	CookieValidity time.Duration

	// The Name of the cookie to use when setting the authorized cookie.
	CookieName string

	// The domain that web login cookies should be restricted too.
	CookieDomain string

	// Used for encrypting the contents of the cookie.
	CookieTool *cookie.CookieTool
}

// Handles requests to /_login (GET)
func (w *WebAuthProvider) LoginGet(ir *request.Request) {
	token := loginFormToken{
		Expires: cookie.Time{
			Time: time.Now().Add(w.CookieValidity),
		},
		ReturnURL: func() string {
			r, ok := ir.Request.URL.Query()["return_url"]
			if !ok || len(r) != 1 {
				return ""
			}
			return r[0]
		}(),
		PassField: fmt.Sprintf("pass%d", rand.Int()),
		UserField: fmt.Sprintf("user%d", rand.Int()),
	}
	raw, err := w.CookieTool.Encode(ir.Context, &token)
	if err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusInternalServerError,
			Response: "Internal error encoding data.",
			Err:      err,
		})
	}
	ir.Header().Add("Content-type", "text/html")
	ir.WriteHeader(http.StatusOK)
	ir.Write([]byte(`<html>`))
	ir.Write([]byte(`<head><title>Blobby Login</title></head>`))
	ir.Write([]byte(`<body>`))
	ir.Write([]byte(`<form name="login" method="post" action="/_login">`))
	fmt.Fprintf(ir, `<input type="text" size=50 name="%s">`, token.UserField)
	ir.Write([]byte(`<td>`))
	fmt.Fprintf(
		ir,
		`<input type="password" size=50 name="%s">`,
		token.PassField)
	ir.Write([]byte(`<td>`))
	fmt.Fprintf(ir, `<input type="hidden" name="token" value="%s">`, raw)
	ir.Write([]byte(`<input type="submit" value="login">`))
	ir.Write([]byte(`</body>`))
	ir.Write([]byte(`</html>`))
}

// Handles request to /_login (POST)
func (w *WebAuthProvider) LoginPost(ir *request.Request) {
	// We set a security token which contains the form fill field names
	// so that attackers can not auto populate variables.
	var err error
	switch ir.Request.Header.Get("Content-Type") {
	case "application/x-www-form-urlencoded":
		err = ir.Request.ParseForm()
	case "multipart/form-data":
		err = ir.Request.ParseMultipartForm(1024 * 1024)
	default:
		ir.WriteHeader(http.StatusBadRequest)
		ir.Write([]byte("Request does not contain form data."))
		return
	}
	if err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Request content is not form data.",
			Err:      err,
		})
	}

	// Next we need to parse out the token as it will contain important
	// information about the fields we expect to see.
	token := loginFormToken{}
	if raw, ok := ir.Request.Form["token"]; !ok {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Missing token form field.",
		})
	} else if err := w.CookieTool.Decode(ir.Context, raw[0], &token); err != nil {
		// FIXME: Detect the difference between an internal error and bad data
		// being passed to the request.
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Token is not valid." + err.Error(),
		})
	} else if token.Expires.Before(time.Now()) {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Login form expired.",
		})
	} else if token.UserField == "" || token.PassField == "" {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Invalid login token.",
		})
	}

	// Get the user name.
	user, ok := ir.Request.Form[token.UserField]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Missing user field.",
		})
	} else if len(user) != 1 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "there must be exactly one user field.",
		})
	}

	// Get the password.
	pass, ok := ir.Request.Form[token.PassField]
	if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "Missing password field.",
		})
	} else if len(pass) != 1 {
		panic(&request.HTTPError{
			Status:   http.StatusBadRequest,
			Response: "there must be exactly one pass field.",
		})
	}

	// Validate that the provided user and password credentials are valid.
	ok, err = w.Users.Verify(ir.Context, user[0], pass[0], nil)
	if err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusInternalServerError,
			Response: "Error loading user database.",
			Err:      err,
		})
	} else if !ok {
		panic(&request.HTTPError{
			Status:   http.StatusUnauthorized,
			Response: "Login failed.",
		})
	}

	// The login was successful. Now we need to set the login cookie and
	// return the user back to where they came from.
	cookie := webAuthCookie{
		User: user[0],
		Expires: cookie.Time{
			Time: time.Now().Add(w.CookieValidity),
		},
	}
	raw, err := w.CookieTool.Encode(ir.Context, &cookie)
	if err != nil {
		panic(&request.HTTPError{
			Status:   http.StatusInternalServerError,
			Response: "Error generating the authentication cookie.",
			Err:      err,
		})
	}
	http.SetCookie(ir, &http.Cookie{
		Domain:  w.CookieDomain,
		Expires: cookie.Expires.Time,
		Name:    w.CookieName,
		Path:    "/",
		Value:   raw,
	})
	if token.ReturnURL != "" {
		ir.Header().Add("Location", token.ReturnURL)
	} else {
		ir.Header().Add("Location", "/")
	}
	ir.WriteHeader(http.StatusFound)
	return
}

// An acl.Method implementation of WebAuthProvider that uses the provider
// but allows individual resources to configure Tags required by users.
type WebAuth struct {
	// The Provider that configures users.
	Provider *WebAuthProvider

	// A list of tags that users must have in order to access this
	// resource. All tags must be present in the user profile for this
	// to work. Alternatively if this is left nil then all users
	// will be allowed access.
	UserTags []string
}

func (w *WebAuth) assert(ir *request.Request) {
	q := url.Values{}
	q.Add("return_url", ir.Request.URL.RequestURI())
	ir.Header().Add("Location", "/_login?"+q.Encode())
	panic(&request.HTTPError{
		Status: http.StatusFound,
	})
}

// Checks a given Request and sees if it should be allowed.
func (w *WebAuth) check(ir *request.Request) bool {
	cook, err := ir.Request.Cookie(w.Provider.CookieName)
	if err != nil {
		return false
	}
	v := webAuthCookie{}
	if err = w.Provider.CookieTool.Decode(ir.Context, cook.Value, &v); err != nil {
		return false
	} else if v.Expires.Before(time.Now()) {
		return false
	}
	ok, err := w.Provider.Users.HasTags(ir.Context, v.User, w.UserTags)
	if err != nil {
		return false
	}
	return ok
}

func (w *WebAuth) proxy(source, dest *http.Request) {
	cook, err := source.Cookie(w.Provider.CookieName)
	if err != nil {
		return
	}
	dest.AddCookie(cook)
}
