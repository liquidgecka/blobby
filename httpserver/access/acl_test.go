package access

import (
	"bytes"
	"net/http"
	"testing"

	"github.com/liquidgecka/blobby/httpserver/request"
	"github.com/liquidgecka/testlib"
)

// Nil receivers should be safe to use
func TestNilACLSafe(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	var a *ACL = nil
	buf := bytes.NewBuffer([]byte{})
	req, err := http.NewRequest("GET", "http://localhost", buf)
	T.ExpectSuccess(err)

	ir := &request.Request{
		Request: &http.Request{
			RemoteAddr: "1.1.1.1:1000",
		},
	}
	a.Assert(ir)
	a.Proxy(req, req)
}
