// +build !go1.8

package compat

import (
	"net/http"
	"time"
)

func SetIdleTimeout(s *http.Server, timeout time.Duration) *http.Server {
	return s
}
