package workqueue

import (
	"sync"
)

// WorkQueue is a channel like interface specifically designed to get
// around some limitations of the typical golang channel interface. The
// largest is that it will not block on insertion of data no matter
// how much information gets inserted. However this is also designed to
// run functions so it will start and run them in a goroutine up until
// a limit.
type WorkQueue struct {
	// A lock that protects operations being performed inside of this
	// work queue.
	lock sync.Mutex

	// A WaitGroup that tracks all of the goroutines that are spawning
	// work from the work queue.
	wg sync.WaitGroup

	// The number of items currently stored in this workqueue.
	length int

	// The maximum number of parallel operations that can be performed
	// on this WorkQueue.
	parallel int

	// Tracks how many running processors there currently are. This is used
	// to ensure that more processors than are allowed do not get
	// started.
	running int

	// A pointer to the next workList in the queue as well as the current
	// index into the list that is being processed.
	next      *workList
	nextIndex int

	// A pointer to the last workList in the queue as well as the current
	// index being written too.
	last      *workList
	lastIndex int
}

func New(parallel int) *WorkQueue {
	w := &WorkQueue{
		parallel: parallel,
	}
	w.next = getWorkList()
	w.last = w.next
	return w
}

// Inserts a function into the work queue.
func (w *WorkQueue) Insert(f func()) {
	if f == nil {
		return
	}
	w.lock.Lock()
	defer w.lock.Unlock()
	w.last.work[w.lastIndex] = f
	w.lastIndex += 1
	w.length += 1

	// Check that we need to extend the work list to a new object.
	if w.lastIndex >= len(w.last.work) {
		w.last.next = getWorkList()
		w.last = w.last.next
		w.lastIndex = 0
	}

	// Start any work processors that are needed.
	if w.running < w.parallel {
		w.running += 1
		w.wg.Add(1)
		go w.process()
	}
}

// Gets the length of the current work queue.
func (w *WorkQueue) Len() int {
	w.lock.Lock()
	defer w.lock.Unlock()
	return w.length
}

// Processes items from the queue. This will work until there is nothing left
// in the queue and then it will shut down.
func (w *WorkQueue) process() {
	defer func() {
		w.running -= 1
		w.wg.Done()
	}()
	for {
		// Get the next item from the queue.
		next, ok := func() (func(), bool) {
			w.lock.Lock()
			defer w.lock.Unlock()
			for w.next == w.last && w.nextIndex == w.lastIndex {
				return nil, false
			}

			// There is an item available. Consume it.
			next := w.next.work[w.nextIndex]
			w.next.work[w.nextIndex] = nil

			// And move the queue pointer over one, releasing it if needed.
			w.nextIndex += 1
			if w.nextIndex >= len(w.next.work) {
				w.nextIndex = 0
				tmp := w.next
				w.next = w.next.next
				tmp.next = nil
				wlPool.Put(tmp)
			}

			// And lastly we handle a special case. If the read pointer and
			// the write pointer are equal and pointing at the same
			// list then we can set them back to zero so we don't waste
			// space at the start of the list.
			if w.next == w.last && w.nextIndex == w.lastIndex {
				w.nextIndex = 0
				w.lastIndex = 0
			}

			// Decrement the length.
			w.length -= 1

			// And return.
			return next, true
		}()
		if !ok {
			return
		} else {
			next()
		}
	}
}
