package backoff

import (
	"sync"
	"time"
)

// Tracks errors and automatically backs off if there has been too many
// issues in a short period of time. This implements a exponential back
// off when too many errors have been encountered over a short period.
type BackOff struct {
	// The period for which failures will be evaluated.
	Period time.Duration

	// The amount of delay to add after each additional failure (can not
	// be zero).
	X time.Duration

	// The Maximum back off allowed.
	Max time.Duration

	// A queue used for tracking failures, an offset that tracks the start
	// and end of the current data. This allows us to use the queue as a
	// ring buffer and therefor never copy the contents as items get
	// removed from the queue.
	queue  []time.Time
	end    int
	length int

	// A lock that protects all operations so they can be run in parallel.
	lock sync.Mutex
}

// Inserts an error into the BackOff queue.
func (b *BackOff) Failure() {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.queue == nil {
		b.allocate()
	}
	qlen := len(b.queue)
	b.queue[b.end] = time.Now()
	b.end = (b.end + 1) % qlen
	b.length += 1
	if b.length > qlen {
		b.length = qlen
	}
}

func (b *BackOff) Healthy() bool {
	b.lock.Lock()
	defer b.lock.Unlock()
	return b.length == 0
}

// Returns the amount of time that needs to be waited in order to slow down
// operations due to the back off function.
func (b *BackOff) Wait() time.Duration {
	b.lock.Lock()
	defer b.lock.Unlock()
	if b.queue == nil {
		b.allocate()
	}
	cutoff := time.Now().Add(-b.Period)
	for b.length > 0 {
		qlen := len(b.queue)
		index := (b.end + qlen - b.length) % qlen
		if b.queue[index].After(cutoff) {
			break
		}
		b.length -= 1
	}
	delay := b.X * time.Duration(b.length-1)
	if delay < 0 {
		return 0
	} else if delay > b.Max {
		return b.Max
	} else {
		return delay
	}
}

func (b *BackOff) allocate() {
	b.queue = make([]time.Time, int(b.Period/b.X))
}
