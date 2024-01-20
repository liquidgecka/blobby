package secretloader

import (
	"context"
	"fmt"
	"net/url"
	"strings"
	"time"
)

type Loader interface {
	// How long the data in the cache should be kept before its refreshed.
	CacheDuration() time.Duration

	// Fetches the secret from the underlying store.
	Fetch(context.Context) ([]byte, error)

	// Returns true if the data in the secret is stale and needs to be
	// refreshed before the next use.
	IsStale(context.Context) bool

	// True if the data should be preloaded on startup.
	PreLoad(context.Context) bool

	// True if the data is allowed to be stale.
	Stale(context.Context) bool

	// A string representing the URL that was used to load this secret.
	URL(context.Context) string
}

func NewLoader(u string, p Profiles) (Loader, error) {
	// Parse the URL into components.
	ud, err := url.Parse(u)
	if err != nil {
		return nil, err
	}

	// Parse the query so we can get some common query parameters.
	preload := true
	cache := time.Duration(time.Hour)
	stale := true
	qv := ud.Query()
	if v, ok := qv["preload"]; ok {
		if len(v) != 1 {
			return nil, fmt.Errorf("preload can only have one value.")
		}
		switch v[0] {
		case "true", "1":
			preload = true
		case "false", "0":
			preload = false
		default:
			return nil, fmt.Errorf("unknown value for preload.")
		}
		delete(qv, "preload")
	}
	if v, ok := qv["cache"]; ok {
		if len(v) != 1 {
			return nil, fmt.Errorf("cache can only have one value.")
		}
		if d, err := time.ParseDuration(v[0]); err != nil {
			return nil, fmt.Errorf("cache value is invalid: %s", err.Error())
		} else {
			cache = d
		}
		delete(qv, "cache")
	}
	if v, ok := qv["stale"]; ok {
		if len(v) != 1 {
			return nil, fmt.Errorf("stale can only have one value.")
		}
		switch v[0] {
		case "true", "1":
			stale = true
		case "false", "0":
			stale = false
		default:
			return nil, fmt.Errorf("unknown value for stale.")
		}
		delete(qv, "stale")
	}

	// If the scheme is "sm" then its a secrets manager url and needs to be
	// parsed as such.
	switch ud.Scheme {
	case "sm":
		for v := range qv {
			return nil, fmt.Errorf("Unknown query '%s'.", v)
		}
		switch {
		case ud.User != nil:
			return nil, fmt.Errorf("User names are not valid on sm: urls.")
		case ud.Opaque != "":
			return nil, fmt.Errorf("Opaque paths are not valid on sm: urls.")
		case ud.Host == "":
			return nil, fmt.Errorf("The profile name is required for sm: urls.")
		case ud.Path == "":
			return nil, fmt.Errorf("The secret name is required for sm: urls.")
		case ud.Fragment != "":
			return nil, fmt.Errorf("Fragments are not allowed on sm: urls.")
		case !p.CheckProfile(ud.Host):
			return nil, fmt.Errorf("No AWS client named '%s'.", ud.Host)
		default:
			return &secretsManagerLoader{
				cache:       cache,
				preload:     preload,
				profileName: ud.Host,
				profiles:    p,
				secret:      strings.TrimPrefix(ud.Path, "/"),
				sourceURL:   u,
				stale:       stale,
			}, nil
		}
	case "":
		fallthrough
	case "file":
		for v := range qv {
			return nil, fmt.Errorf("Unknown query '%s'.", v)
		}
		switch {
		case ud.User != nil:
			return nil, fmt.Errorf("User names are not allowed in file: urls.")
		case ud.Host != "":
			return nil, fmt.Errorf("Hosts are not allowed in file: urls.")
		case ud.Fragment != "":
			return nil, fmt.Errorf("Fragments are not allowed in file: urls.")
		case ud.Path != "":
			return &fileLoader{
				sourceURL: u,
				file:      ud.Path,
			}, nil
		case ud.Opaque != "":
			return &fileLoader{
				cache:     cache,
				file:      ud.Opaque,
				preload:   preload,
				sourceURL: u,
				stale:     stale,
			}, nil
		}
		return nil, fmt.Errorf("Badly formatted file url.")
	default:
		return nil, fmt.Errorf("Unknown URL scheme: %s", ud.Scheme)
	}
}
