package secretloader

import (
	"io/ioutil"
	"time"
)

// A Loader that will fetch raw bytes from the secret given.
type fileLoader struct {
	// The source URL used to define the loader.
	sourceURL string

	// The file that will be loaded from disk.
	file string

	// The expiration time of the data returned by the last call to Fetch()
	expires time.Time

	// Cache related values.
	preload bool
	cache   time.Duration
	stale   bool
}

// How long to keep cached data.
func (f *fileLoader) CacheDuration() time.Duration {
	return f.cache
}

// Fetch the raw data from the secret.
func (f *fileLoader) Fetch() ([]byte, error) {
	data, err := ioutil.ReadFile(f.file)
	if err != nil {
		return nil, err
	}
	f.expires = time.Now().Add(f.cache)
	return data, nil
}

// Returns true if the data in the cache is stale and needs to be refreshed
// before it can be loaded.
func (f *fileLoader) IsStale() bool {
	if !f.stale || time.Now().After(f.expires) {
		return true
	}
	return false
}

// Returns true if the secret should be loaded at startup.
func (f *fileLoader) PreLoad() bool {
	return f.preload
}

// Returns true if the secret is allowed to be served stale.
func (f *fileLoader) Stale() bool {
	return f.stale
}

// Returns the URL used to generate this Loader.
func (f *fileLoader) URL() string {
	return f.sourceURL
}
