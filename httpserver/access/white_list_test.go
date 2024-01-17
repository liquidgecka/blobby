package access

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"testing"

	"github.com/liquidgecka/testlib"

	"github.com/iterable/blobby/httpserver/request"
)

func makeIPNet(s string) net.IPNet {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		panic(fmt.Sprintf("%s is not a valid ip/mask", s))
	}
	ip := net.ParseIP(parts[0])
	bits, err := strconv.ParseInt(parts[1], 10, 63)
	if err != nil {
		panic(err)
	}
	if bits > 32 {
		panic(fmt.Sprintf("%d is not a valid bit mask.", bits))
	}
	mask := net.CIDRMask(int(bits), 32)
	return net.IPNet{
		IP:   ip,
		Mask: mask,
	}
}

func TestWhiteList_Check(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	w := &WhiteList{
		CIDRs: []net.IPNet{
			makeIPNet("1.1.0.0/16"),
			makeIPNet("2.0.0.0/8"),
		},
	}
	ir := &request.Request{
		Request: &http.Request{
			RemoteAddr: "1.1.1.1:1000",
		},
	}
	T.Equal(w.check(ir), true)
	ir.Request.RemoteAddr = "9.9.9.9:9000"
	T.Equal(w.check(ir), false)
}

func TestWhiteList_Check_XForwardedFor(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	w := &WhiteList{
		CIDRs: []net.IPNet{
			makeIPNet("1.1.0.0/16"),
			makeIPNet("2.0.0.0/8"),
		},
		AllowXForwardedForFrom: []net.IPNet{
			makeIPNet("255.255.255.0/24"),
		},
	}
	ir := &request.Request{
		Request: &http.Request{
			Header: http.Header{
				"X-Forwarded-For": []string{"1.1.1.1"},
			},
			RemoteAddr: "255.255.255.0:1000",
		},
	}
	T.Equal(w.check(ir), true)
	ir.Request.RemoteAddr = "9.9.9.9:9000"
	T.Equal(w.check(ir), false)
}

func TestWhiteList_Check_Panic(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	w := WhiteList{
		CIDRs: []net.IPNet{
			makeIPNet("1.1.0.0/16"),
			makeIPNet("2.0.0.0/8"),
		},
	}
	ir := &request.Request{
		Request: &http.Request{
			RemoteAddr: "invalid",
		},
	}
	T.ExpectPanic(func() {
		T.Equal(w.check(ir), true)
	}, &net.AddrError{Err: "missing port in address", Addr: "invalid"})
}

func TestWhiteList_Assert(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	w := &WhiteList{
		CIDRs: []net.IPNet{
			makeIPNet("1.1.0.0/16"),
			makeIPNet("2.0.0.0/8"),
		},
	}
	ir := &request.Request{
		Request: &http.Request{
			RemoteAddr: "1.1.1.1:1000",
		},
	}
	T.ExpectPanic(
		func() { w.assert(ir) },
		&request.HTTPError{
			Status:   http.StatusUnauthorized,
			Response: "IP is not allowed to access this resource.",
		})
}
