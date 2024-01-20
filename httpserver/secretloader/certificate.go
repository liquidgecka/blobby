package secretloader

import (
	"context"
	"crypto/tls"
	"fmt"
	"log/slog"
	"time"

	"github.com/liquidgecka/blobby/internal/sloghelper"
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
	Logger *slog.Logger

	// A cache of the certificate that was generated via the prior Load()
	// call.
	cert *tls.Certificate
}

// Returns the certificate loaded via the Load() call.
func (c *Certificate) Cert(ctx context.Context) (*tls.Certificate, error) {
	if c.Certificate.IsStale(ctx) || c.Private.IsStale(ctx) {
		if err := c.load(ctx); err != nil {
			return nil, err
		}
	}
	return c.cert, nil
}

// Returns true if this secret is expected to be pre-loaded at startup.
func (c *Certificate) PreLoad(ctx context.Context) error {
	if c == nil {
		return nil
	} else if !c.Certificate.PreLoad(ctx) && !c.Private.PreLoad(ctx) {
		return nil
	}
	return c.load(ctx)
}

// Starts a goroutine that will periodically refresh the data in the secret
// if configured to do so. This routine will stop processing if the passed
// in context is canceled.
func (c *Certificate) StartRefresher(ctx context.Context) {
	switch {
	case c == nil:
	case c.Certificate != nil && c.Certificate.Stale(ctx):
	case c.Private != nil && c.Private.Stale(ctx):
	default:
		dur := c.Certificate.CacheDuration()
		if dur2 := c.Private.CacheDuration(); dur2 < dur {
			dur = dur2
		}
		if dur > 0 {
			go c.refresher(dur, ctx)
		}
	}
}

// Reloads the secret on an interval until the context is canceled. This
// is expected to be run as a goroutine.
func (c *Certificate) refresher(dur time.Duration, ctx context.Context) {
	timer := time.NewTimer(dur)
	defer func() {
		if !timer.Stop() {
			<-timer.C
		}
	}()
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}
		c.Logger.LogAttrs(
			ctx,
			slog.LevelDebug,
			"Refreshing the certificate data.")
		if err := c.load(ctx); err != nil {
			c.Logger.LogAttrs(
				ctx,
				slog.LevelError,
				"Error refreshing the certificate data.",
				sloghelper.Error("error", err))
		}
	}
}

// Loads the certificate from the loaders and parses it. If this returns
// an error then the existing certificate will not be changed.
func (c *Certificate) load(ctx context.Context) error {
	// Get the raw certificate bytes.
	certRaw, err := c.Certificate.Fetch(ctx)
	if err != nil {
		return err
	}

	// Get the raw private key bytes.
	keyRaw, err := c.Private.Fetch(ctx)
	if err != nil {
		return err
	}

	// Parse the bytes into a certificate.
	if cert, err := tls.X509KeyPair(certRaw, keyRaw); err != nil {
		return fmt.Errorf(
			"Error loading certificate from '%s'/'%s': %s'",
			c.Certificate.URL(ctx),
			c.Private.URL(ctx),
			err.Error())
	} else {
		c.cert = &cert
	}

	// Success
	return nil
}
