package storage

import (
	"sync"
)

// An internal type that is used to manage lists of objects.
// This is used for keeping track of files that are waiting for actions
// to be taken (written to, uploaded, deleted, etc)
type list struct {
	head     *primary
	length   int
	waiting  int
	lock     sync.Mutex
	cond     sync.Cond
	headCond sync.Cond
}

// Obtain the next idle file and return it. This supports passing in a
// check function that will be run (holding the lock) to ensure that
// there are objects available if needed.
func (l *list) Get(check func()) *primary {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.waiting += 1
	for l.head == nil {
		if check != nil {
			check()
		}
		if l.cond.L == nil {
			l.cond.L = &l.lock
		}
		l.cond.Wait()
	}
	next := l.head
	l.head = next.next
	l.length -= 1
	l.waiting -= 1
	next.next = nil
	if l.headCond.L == nil {
		l.headCond.L = &l.lock
	}
	l.headCond.Broadcast()
	return next
}

// Puts a object into the list.
func (l *list) Put(p *primary) {
	l.lock.Lock()
	defer l.lock.Unlock()
	l.length += 1
	np := &l.head
	for *np != nil {
		if p.expires < (*np).expires {
			break
		}
		np = &(*np).next
	}
	p.next = *np
	*np = p
	l.signal()
	if l.head == p {
		if l.headCond.L == nil {
			l.headCond.L = &l.lock
		}
		l.headCond.Broadcast()
	}
}

// Removes an item from the list and returns true if the removal
// was successful.
func (l *list) Remove(p *primary) bool {
	if p == nil {
		return false
	}
	l.lock.Lock()
	defer l.lock.Unlock()
	if l.head == p {
		l.head = p.next
		p.next = nil
		l.length -= 1
		return true
	}
	for pp := l.head; pp != nil; pp = pp.next {
		if pp.next == p {
			pp.next = p.next
			p.next = nil
			l.length -= 1
			return true
		}
	}
	return false
}

// Returns the number of callers currently waiting for data.
func (l *list) Waiting() int {
	l.lock.Lock()
	defer l.lock.Unlock()
	return l.waiting
}

// Signals the list to indicate that it should check for updates.
func (l *list) signal() {
	if l.cond.L == nil {
		l.cond.L = &l.lock
	}
	l.cond.Signal()
}
