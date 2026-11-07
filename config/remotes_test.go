package config

import (
	"net/http"
	"testing"
	"time"

	"github.com/liquidgecka/blobby/httpserver/remotes"
	"github.com/liquidgecka/testlib"
)

func TestRemote_TransportConfigured(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	host := "127.0.0.1"
	id := uint32(1)
	name := "remote-1"
	tlsFalse := false
	maxIdle := 50

	timeout := time.Minute
	topCfg := &top{
		Client: client{
			Timeout: &timeout,
		},
	}

	r := &remote{
		Host:         &host,
		Port:         value{set: true, raw: []byte("8080")},
		ID:           &id,
		Name:         &name,
		TLS:          &tlsFalse,
		MaxIdleConns: &maxIdle,
		top:          topCfg,
	}

	errors := r.validate(topCfg)
	T.Equal(len(errors), 0)

	rem := r.Remote()
	T.NotEqual(rem, nil)

	rActual, ok := rem.(*remotes.Remote)
	T.Equal(ok, true)
	T.NotEqual(rActual.Client.Transport, nil)

	tr, ok := rActual.Client.Transport.(*http.Transport)
	T.Equal(ok, true)
	T.Equal(tr.MaxIdleConns, 50)
	T.Equal(tr.MaxIdleConnsPerHost, 50)
	T.Equal(tr.DisableCompression, true)
}
