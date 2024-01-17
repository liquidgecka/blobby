package config

import (
	"net/http"
	"time"
)

var (
	defaultTimeout = time.Minute
)

type client struct {
	Timeout *time.Duration `toml:"timeout"`
}

func (c *client) HTTPClient() *http.Client {
	return &http.Client{
		// We do not want to follow redirects in the client that we use to
		// talk to remotes. They shouldn't ever 3xx and if they do they could
		// send us off site which is not ideal.
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		},

		// Timeout requests after the configured amount of time.
		Timeout: *c.Timeout,
	}
}

func (c *client) validate() []string {
	var errors []string

	// Timeout
	if c.Timeout == nil {
		c.Timeout = &defaultTimeout
	} else if *c.Timeout < time.Second {
		errors = append(errors, "client.timeout must be larger than 1s.")
	}

	// Return any errors encountered.
	return errors
}
