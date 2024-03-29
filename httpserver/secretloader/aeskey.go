package secretloader

import (
	"context"
	"crypto/aes"
	"crypto/cipher"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/liquidgecka/blobby/internal/sloghelper"
)

// An AES Key loader implementation.
type AESKeys struct {
	// The source for AES key data.
	Source Loader

	// All logging for the certificate manager will be done via this Logger
	// object.
	Logger *slog.Logger

	// A cache of the certificate that was generated via the prior load()
	// call.
	keys     []cipher.Block
	keysLock sync.Mutex
}

// Returns the current list of keys loaded from the secret.
func (a *AESKeys) Keys(ctx context.Context) ([]cipher.Block, error) {
	if a.Source.IsStale(ctx) {
		if keys, err := a.load(ctx); err != nil {
			return nil, err
		} else {
			a.keysLock.Lock()
			defer a.keysLock.Unlock()
			a.keys = keys
			return keys, nil
		}
	} else {
		a.keysLock.Lock()
		defer a.keysLock.Unlock()
		return a.keys, nil
	}
}

func (a *AESKeys) PreLoad(ctx context.Context) error {
	if a == nil {
		return nil
	} else if a.Source == nil {
		return nil
	} else if !a.Source.PreLoad(ctx) {
		return nil
	}
	_, err := a.load(ctx)
	return err
}

// Starts the cache refresher.
func (a *AESKeys) StartRefresher(ctx context.Context) {
	if a.Source.Stale(ctx) {
		dur := a.Source.CacheDuration()
		go a.refresher(dur, ctx)
	}
}

// Loads the AES keys from the secret.
func (a *AESKeys) load(ctx context.Context) ([]cipher.Block, error) {
	// Get the raw source data.
	aesRaw, err := a.Source.Fetch(ctx)
	if err != nil {
		return nil, err
	}

	// The contents of the secret needs to be a json list of hex encoded
	// string values.
	results := []string{}
	if err := json.Unmarshal(aesRaw, &results); err != nil {
		return nil, err
	}

	// Walk through each of the strings in results and parse them into
	// AES keys. We want to be able to return all of the errors in one
	// message so that the user doesn't need to edit, reload, edit, reload,
	// etc.
	var errs []string
	keys := make([]cipher.Block, len(results))
	for i, raw := range results {
		decoded, err := hex.DecodeString(raw)
		if err != nil {
			errs = append(errs, fmt.Sprintf(
				"key [%d] is not a valid hex value: %s",
				i,
				err.Error()))
			continue
		}
		switch len(decoded) * 8 {
		case 128:
		case 192:
		case 256:
		default:
			errs = append(errs, fmt.Sprintf(
				"key [%d] is not a valid size (128, 192, or 256 bits)",
				i))
			continue
		}
		keys[i], err = aes.NewCipher(decoded)
		if err != nil {
			errs = append(errs, fmt.Sprintf(
				"key [%d] is not a valid AES key: %s",
				i,
				err.Error()))
			continue
		}
	}

	// If there were errors then format and return them.
	if errs != nil {
		return nil, errors.New(strings.Join(errs, ", "))
	}

	// Save the results if there were no errors.
	a.keysLock.Lock()
	a.keysLock.Unlock()
	a.keys = keys
	return a.keys, nil
}

func (a *AESKeys) refresher(dur time.Duration, ctx context.Context) {
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
		a.Logger.LogAttrs(
			ctx,
			slog.LevelDebug,
			"Refreshing the AES keys.")
		if _, err := a.load(ctx); err != nil {
			a.Logger.LogAttrs(
				ctx,
				slog.LevelError,
				"Error refreshing the AES keys.",
				sloghelper.Error("error", err))
		}
	}
}
