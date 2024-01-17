package secretloader

import (
	"github.com/aws/aws-sdk-go/aws/session"
)

type Profiles interface {
	// Returns true if a profile has been configured (but perhaps not
	// initialized yet). Used for configuration.
	CheckProfile(name string) bool

	// Gets the actual AWS session that will be used for fetching secrets
	// if needed.
	GetSession(name string) *session.Session
}
