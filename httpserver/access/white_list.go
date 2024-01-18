package access

import (
	"net"
	"net/http"

	"github.com/liquidgecka/blobby/httpserver/request"
)

// Handles CIDR white listing requests.
type WhiteList struct {
	// A list of IP/Mask combinations that can be used to limit who can
	// access a given resource within the HTTP server.
	CIDRs []net.IPNet

	// Allow the following IPs to set the value of RemoteAddr via the
	// X-Forwarded-For header. This should be restricted to a list of
	// trusted hosts as they will effectively be able to circumvent the
	// IP white listing process.
	AllowXForwardedForFrom []net.IPNet
}

// Checks a given request and sees if it should be allowed.
func (w *WhiteList) check(ir *request.Request) bool {
	ipStr, _, err := net.SplitHostPort(ir.Request.RemoteAddr)
	if err != nil {
		// Any error returned from this function would be a code error or
		// major change to the golang library since the http.Server
		// implementation always set RemoteAddr to IP:Port.
		panic(err)
	}
	ip := net.ParseIP(ipStr)
	if xff := ir.Request.Header.Get("X-Forwarded-For"); xff != "" {
		for _, ipnet := range w.AllowXForwardedForFrom {
			if ipnet.Contains(ip) {
				ipStr = xff
				ip = net.ParseIP(xff)
				break
			}
		}
	}
	for _, ipnet := range w.CIDRs {
		if ipnet.Contains(ip) {
			return true
		}
	}
	return false
}

func (w *WhiteList) assert(ir *request.Request) {
	panic(&request.HTTPError{
		Status:   http.StatusUnauthorized,
		Response: "IP is not allowed to access this resource.",
	})
}

func (w *WhiteList) proxy(source, dest *http.Request) {
	ipStr, _, err := net.SplitHostPort(source.RemoteAddr)
	if err == nil {
		dest.Header.Set("X-Forwarded-For", ipStr)
	}
}
