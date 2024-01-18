package secretloader

import (
	"crypto/tls"
	"fmt"
	"time"

	"github.com/liquidgecka/blobby/internal/logging"
)

// A Generic interface around certificate loading.
type Certificate struct {
	// The Loader that will fetch the bytes needed for the public portion of
	// the certificate.
	Certificate Loader

	// The loader that will fetch the bytes needed for the private portion
	// of the certificate.
	Private Loader

	// All logging for the certificate manager will be done via this Logger
	// object.
	Logger *logging.Logger

	// A cache of the certificate that was generated via the prior Load()
	// call.
	cert *tls.Certificate
}

// Returns the certificate loaded via the Load() call.
func (c *Certificate) Cert() (*tls.Certificate, error) {
	if c.Certificate.IsStale() || c.Private.IsStale() {
		if err := c.load(); err != nil {
			return nil, err
		}
	}
	return c.cert, nil
}

// Returns true if this secret is expected to be pre-loaded at startup.
func (c *Certificate) PreLoad() error {
	if c == nil {
		return nil
	} else if !c.Certificate.PreLoad() && !c.Private.PreLoad() {
		return nil
	}
	return c.load()
}

// Starts a goroutine that will periodically refresh the data in the secret
// if configured to do so. This routine will stop processing if the passed
// in channel is closed.
func (c *Certificate) StartRefresher(stop <-chan struct{}) {
	switch {
	case c == nil:
	case c.Certificate != nil && c.Certificate.Stale():
	case c.Private != nil && c.Private.Stale():
	default:
		dur := c.Certificate.CacheDuration()
		if dur2 := c.Private.CacheDuration(); dur2 < dur {
			dur = dur2
		}
		if dur > 0 {
			go c.refresher(dur, stop)
		}
	}
}

// Reloads the secret on an interval until the stop channel is closed. This
// is expected to be run as a goroutine.
func (c *Certificate) refresher(dur time.Duration, stop <-chan struct{}) {
	timer := time.NewTimer(dur)
	defer func() {
		if !timer.Stop() {
			<-timer.C
		}
	}()
	for {
		select {
		case <-stop:
			return
		case <-timer.C:
		}
		c.Logger.Debug("Refreshing the certificate data.")
		if err := c.load(); err != nil {
			c.Logger.Error(
				"Error refreshing the certificate data.",
				logging.NewFieldIface("error", err))
		}
	}
}

// Loads the certificate from the loaders and parses it. If this returns
// an error then the existing certificate will not be changed.
func (c *Certificate) load() error {
	// Get the raw certificate bytes.
	certRaw, err := c.Certificate.Fetch()
	if err != nil {
		return err
	}

	// Get the raw private key bytes.
	keyRaw, err := c.Private.Fetch()
	if err != nil {
		return err
	}

	// Parse the bytes into a certificate.
	if cert, err := tls.X509KeyPair(certRaw, keyRaw); err != nil {
		return fmt.Errorf(
			"Error loading certificate from '%s'/'%s': %s'",
			c.Certificate.URL(),
			c.Private.URL(),
			err.Error())
	} else {
		c.cert = &cert
	}

	// Success
	return nil
}
