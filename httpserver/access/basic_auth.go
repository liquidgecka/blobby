package access

import (
	"fmt"
	"net/http"

	"github.com/liquidgecka/blobby/httpserver/request"
	"github.com/liquidgecka/blobby/httpserver/secretloader"
)

// If a Request can be authenticated with Basic Auth then this will handle
// the authentication cycle.
type BasicAuth struct {
	// Access to the htpasswd data.
	Users *secretloader.HTPasswd

	// The realm that will be returned to the user if they fail to auth.
	Realm string

	// The tags required for this resource. If no tags are required then
	// all users will be allowed.
	UserTags []string
}

// Checks a given Request and sees if it should be allowed.
func (b *BasicAuth) check(ir *request.Request) bool {
	if b.Users == nil {
		return false
	} else if user, pass, ok := ir.Request.BasicAuth(); !ok {
		return false
	} else if ok, err := b.Users.Verify(ir.Context, user, pass, b.UserTags); err != nil {
		panic(err)
	} else {
		return ok
	}
}

func (b *BasicAuth) assert(ir *request.Request) {
	realm := b.Realm
	if len(realm) == 0 {
		realm = "Authentication is required"
	}
	ir.Header().Add(
		"WWW-Authenticate",
		fmt.Sprintf(`Basic realm="%s"`, realm))
	panic(&request.HTTPError{
		Status:   http.StatusUnauthorized,
		Response: "Authentication is required.",
	})
}

func (b *BasicAuth) proxy(source, dest *http.Request) {
	for _, value := range source.Header.Values("WWW-Authenticate") {
		dest.Header.Add("WWW-Authenticate", value)
	}
}
