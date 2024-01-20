package delayqueue

import (
	"context"
	"sync"
	"time"
)

// Used as basically a more efficient timer implementation that eliminates
// the need to track timers across multiple goroutines. Generally this is
// used with state machines to allow state transitions that can be preempted
// without overhead (where possible).
//
// Generally the assumptions for use in this queue processor is that it will
// work with a few thousands nodes which will be constantly transitioning
// times but not firing frequently, and the typical delays is seconds to a few
// minutes. This is common in a state machine where there is a "timeout" path.
// This is designed to be very efficient and to not use a lot of CPU managing
// these trigger events. This is specifically setup to avoid having thousands
// of goroutines running individual timers writing to individual channels.
//
// Since its going to be "uncommon" for the next queued item to be triggered
// and far more common for the items within the list to be jostled about
// as they change deadlines we use a simple unordered list here.
type DelayQueue struct {
	// Locking that prevents race conditions with the data in the queue.
	lock sync.Mutex

	// A timer that is used for tracking when the next element in the
	// queue should trigger. When this triggers it will signal a channel
	// that is read by the DelayQueue.process routine. This timer is purely
	// a hint that the next item may be available for processing.
	timer *time.Timer

	// We keep track of only the next token in the list. This is what is
	// checked against when performing sleep or update operations. All other
	// tokens are simply inserted into the list unordered.
	next *Token

	// The head of the list of tokens that will be processed. This is not
	// ordered so inserts will just push to the head. Generally its cheaper
	// to do a O(n) scan each time a timer fires then it is to constantly
	// keep the list completely ordered every time. Hence this is just a
	// pointer to the head of the list.
	list *Token

	// A channel used to signal that the DelayQueue processor needs to stop.
	stop chan struct{}
}

// Alters the given token to be in the list with the given fields.
func (d *DelayQueue) Alter(tok *Token, t time.Time, f func(context.Context)) {
	d.lock.Lock()
	defer d.lock.Unlock()

	if f == nil {
		panic("DelayQueue.Alter with a nil function.")
	} else if tok == nil {
		panic("DelayQueue.Alter with a nil token.")
	} else {
		tok.f = f
	}
	tok.t = t

	// If this token is the next token that will trigger then we need
	// to update the time in the token, then force a reset of the next value
	// in the DelayQueue.
	if tok == d.next {
		d.findNext()
		return
	}

	// If the token is not in the list already then it needs to be added back
	// in. Since the list is unordered we just add it back in at the head of
	// the list.
	if !tok.inList {
		tok.inList = true
		tok.next = d.list
		tok.prev = nil
		d.list = tok
		if tok.next != nil {
			// There are items already in the list, we just need to add
			// them and then fall through to the next clause.
			tok.next.prev = tok
		} else {
			// The list is empty, we can just set the next item to this
			// one and then reset the timer.
			d.next = tok
			d.resetTimer()
			return
		}
	}

	// If this token is now set to expire sooner than our current "next" token
	// then we need to reset the next token and timer to trigger when this
	// token is due.
	if t.Before(d.next.t) {
		d.next = tok
		d.resetTimer()
	}
}

// Cancels a token. This is used to remove a token from the queue all together.
// This does basically nothing if the token is not in the list.
func (d *DelayQueue) Cancel(tok *Token) {
	d.lock.Lock()
	defer d.lock.Unlock()

	// Check to see if the token is even in the list.
	if !tok.inList {
		return
	}

	// Remove tok from the list.
	if tok.prev == nil {
		d.list = tok.next
	} else {
		tok.prev.next = tok.next
	}
	if tok.next != nil {
		tok.next.prev = tok.prev
	}
	tok.next = nil
	tok.prev = nil
	tok.inList = false

	// If tok was the token expected to fire next then we also need to reset
	// the timer to the next firing token.
	if d.next == tok {
		if d.list != nil {
			d.findNext()
		} else {
			d.next = nil
			d.longSleepTimer()
		}
	}
}

// Starts the DelayQueue processing.
func (d *DelayQueue) Start() {
	d.stop = make(chan struct{})
	d.timer = time.NewTimer(time.Hour)
	d.timer.Reset(time.Hour)
	go d.process()
}

// Stops the DelayQueue processing.
func (d *DelayQueue) Stop() {
	close(d.stop)
}

// Finds the next lowest item in the list and sets the next pointer to it.
func (d *DelayQueue) findNext() {
	next := d.list
	for p := next.next; p != nil; p = p.next {
		if p.t.Before(next.t) {
			next = p
		}
	}
	d.next = next
	d.resetTimer()
}

// Runs in the background to start the next trigger event in a loop. This will
// be run as a goroutine.
func (d *DelayQueue) process() {
	for {
		// The next section needs to be run with a lock so its done in an
		// embedded function.
		func() {
			d.lock.Lock()
			defer d.lock.Unlock()

			// Make sure there is an item to trigger, if not setup a long
			// sleep timer so that we will sleep until something major
			// changes.
			if d.next == nil {
				d.longSleepTimer()
				return
			}

			// If the next item is not in the past then we need to reset the
			// timer and then return so that the sleep will wait the right
			// amount of time.
			if time.Now().Before(d.next.t) {
				d.resetTimer()
				return
			}

			// Capture the next item and run the function associated with it.
			tok := d.next
			if tok.inLine {
				tok.f(context.Background()) // FIXME
			} else {
				go tok.f(context.Background()) // FIXME
			}

			// Remove the next item from the list.
			if tok.prev == nil {
				d.list = tok.next
			} else {
				tok.prev.next = tok.next
			}
			if tok.next != nil {
				tok.next.prev = tok.prev
			}
			tok.next = nil
			tok.prev = nil
			tok.inList = false
			if d.list != nil {
				d.findNext()
			} else {
				d.next = nil
				d.longSleepTimer()
			}
		}()

		// Wait until the timer triggers for the next item to be processed.
		select {
		case <-d.timer.C:
		case <-d.stop:
			return
		}
	}
}

// Called to reset the timer to the current next objects time.
func (d *DelayQueue) resetTimer() {
	if !d.timer.Stop() {
		select {
		case <-d.timer.C:
		default:
		}
	}
	d.timer.Reset(time.Until(d.next.t))
}

// Called to ensure that the timer is setup to sleep for a really long time.
func (d *DelayQueue) longSleepTimer() {
	if !d.timer.Stop() {
		select {
		case <-d.timer.C:
		default:
		}
	}
	d.timer.Reset(time.Hour * 24 * 365)
}
