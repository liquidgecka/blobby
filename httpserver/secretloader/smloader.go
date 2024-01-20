package secretloader

import (
	"context"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go/service/secretsmanager"
)

// A Loader that will fetch raw bytes from the secret given.
type secretsManagerLoader struct {
	// sourceURL is just a string for URL() to return.
	sourceURL string

	// The name of the profile parsed out of the url.
	profileName string

	// The AWS session fetcher that is used to fetch profiles for use
	// with this loader.
	profiles Profiles

	// The name of the secret to fetch.
	secret string

	// The expiration time of the data last returned by Fetch()
	expires time.Time

	// Cache related values.
	preload bool
	cache   time.Duration
	stale   bool
}

// How long to keep cached data.
func (s *secretsManagerLoader) CacheDuration() time.Duration {
	return s.cache
}

// Fetch the raw data from the secret.
func (s *secretsManagerLoader) Fetch(ctx context.Context) ([]byte, error) {
	ses := s.profiles.GetSession(s.profileName)
	if ses == nil {
		return nil, fmt.Errorf(
			"AWS profile named %s does not exist.",
			s.profileName)
	}
	gsvi := secretsmanager.GetSecretValueInput{
		SecretId: &s.secret,
	}
	gsvo, err := secretsmanager.New(ses).GetSecretValue(&gsvi)
	if err != nil {
		return nil, fmt.Errorf(
			"Error fetching '%s': %s",
			s.sourceURL,
			err.Error())
	}
	if gsvo.SecretBinary != nil {
		s.expires = time.Now().Add(s.cache)
		return gsvo.SecretBinary, nil
	} else if gsvo.SecretString != nil {
		s.expires = time.Now().Add(s.cache)
		return []byte(*gsvo.SecretString), nil
	} else {
		return nil, fmt.Errorf(
			"No content found for '%s'.",
			s.sourceURL)
	}
}

// Returns true if the data in the cache needs to be updated before it can
// be used.
func (s *secretsManagerLoader) IsStale(ctx context.Context) bool {
	if !s.stale || time.Now().After(s.expires) {
		return true
	}
	return false
}

// Returns true if this secret should be loaded at startup.
func (s *secretsManagerLoader) PreLoad(ctx context.Context) bool {
	return s.preload
}

// Returns true if the data is allowed to be served stale.
func (s *secretsManagerLoader) Stale(ctx context.Context) bool {
	return s.stale
}

// Returns the URL used to generate this Loader.
func (s *secretsManagerLoader) URL(ctx context.Context) string {
	return s.sourceURL
}
