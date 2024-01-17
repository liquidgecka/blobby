package access

import (
	"net/http"

	"github.com/iterable/blobby/httpserver/request"
)

// A tool used to validate that a given request is allowed to proceed. It
// is expected that this gets implemented by the various authentication
// methods in the access package.
type Method interface {
	check(*request.Request) bool
	assert(*request.Request)
	proxy(source, dest *http.Request)
}

// For some request times access can be limited in several different ways
// including: IP white listing and authentication. This object allows those
// methods to be configured.
type ACL struct {
	// A list of Required accessors. These must be true for all requests that
	// pass through this resource.
	Required []Method

	// A list of optional accessors. At least one of these must be true
	// in order for a request to flow through this resource. If this is not
	// defined then only Required will be used.
	Any []Method
}

// Checks a given Request and sees if it should be allowed.
func (a *ACL) Assert(ir *request.Request) {
	if a == nil {
		return
	}

	// Walk the required checks and make sure that they all are valid.
	for _, r := range a.Required {
		if !r.check(ir) {
			r.assert(ir)
		}
	}

	// If there are 'Any' checks defined then we need to make sure that
	// at least one is true.
	if len(a.Any) > 0 {
		func() {
			for _, a := range a.Any {
				if a.check(ir) {
					return
				}
			}
			a.Any[0].assert(ir)
		}()
	}
}

// When proxying a request forward to a different blobby server this
// will walk through each access control method to make sure that it can
// set the appropriate values on the request so that it gets auth
// details forwarded.
func (a *ACL) Proxy(source, dest *http.Request) {
	if a == nil {
		return
	}

	for _, r := range a.Required {
		r.proxy(source, dest)
	}
	for _, r := range a.Any {
		r.proxy(source, dest)
	}
}
