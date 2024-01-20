package delayqueue

import (
	"context"
	"time"
)

// Used to change the timer that was initially set. This is a helper
// to make timer adjustment much easier to the caller and to eliminate the
// need to search the data store for a given value.
type Token struct {
	// The function to call once this timer element has triggered.
	f func(context.Context)

	// If set to true then the function will be launched in line rather
	// than as a background goroutine.
	inLine bool

	// The time at which this timer should trigger.
	t time.Time

	// Set to true if this Token is already in a list.
	inList bool

	// Pointers to the next and previous Tokens in the list.
	next *Token
	prev *Token
}

// Returns true if this token is in a list. This can be used to see if the
// token has already fired.
func (t *Token) InList() bool {
	return t.inList
}
