package workqueue

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/liquidgecka/testlib"
)

func TestWorkQueue_Insert_Nil(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	w := New(0)
	w.Insert(nil)
	T.Equal(w.nextIndex, 0)
	T.Equal(w.lastIndex, 0)
}

func TestWorkQueue_Insert_Simple(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a WorkQueue that is not allowed to actually run anything.
	w := New(0)
	ran := int32(0)

	// Making sure that inserting 10 items results in 10 items being in
	// the list.
	for i := 0; i < 10; i++ {
		w.Insert(func(context.Context) { atomic.AddInt32(&ran, 1) })
	}

	// Check that all ten items were added.
	T.Equal(ran, int32(0))
	T.Equal(w.nextIndex, 0)
	T.Equal(w.lastIndex, 10)
	for i := 0; i < 10; i++ {
		T.NotEqual(w.next.work[i], nil)
	}
	for i := 10; i < len(w.next.work); i++ {
		T.Equal(w.next.work[i], nil)
	}

	// Next we manually run a process() call which should clear out
	// all of the items and then terminate.
	w.wg.Add(1)
	w.process()
	T.Equal(w.nextIndex, 0)
	T.Equal(ran, int32(10))
}

func TestWorkQueue_Insert_Long(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a WorkQueue that is not allowed to actually run anything.
	w := New(0)
	ran := int32(0)

	// Making sure that inserting 10 items results in 10 items being in
	// the list.
	for i := 0; i < 5000; i++ {
		w.Insert(func(context.Context) { atomic.AddInt32(&ran, 1) })
	}
	T.Equal(ran, int32(0))
	T.Equal(w.nextIndex, 0)
	w.wg.Add(1)
	w.process()
	T.Equal(w.nextIndex, 0)
	T.Equal(ran, int32(5000))
}

func TestWorkQueue_Cycle(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	tests := int32(1024)
	block := sync.Mutex{}
	block.Lock()
	parallel := 50
	running := int32(0)
	ran := int32(0)
	tooMany := false
	w := New(parallel)
	for i := int32(0); i < tests; i++ {
		w.Insert(func(context.Context) {
			if atomic.AddInt32(&running, 1) > int32(parallel) {
				tooMany = true
			}
			defer atomic.AddInt32(&running, -1)
			block.Lock()
			defer block.Unlock()
			ran += 1
		})
	}

	// Since the lock is locked we shouldn't have any more than 1
	// running function.
	T.TryUntil(func() bool { return running == int32(parallel) }, time.Second)
	T.Equal(running, int32(parallel))
	T.Equal(w.running, parallel)
	T.NotEqual(w.next, w.last)
	T.Equal(tooMany, false, "Too many calls ran in parallel.")
	T.Equal(ran, int32(0))

	// Unlock the lock and let the functions process. Since this happens
	// in parallel we need to wait until it finishes.
	block.Unlock()
	T.TryUntil(func() bool { return ran == tests }, time.Second)
}

func TestWorkQueue_Parallel(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Create a WorkQueue that can run 10 things in parallel.
	w := New(10)
	wg := sync.WaitGroup{}
	run := int64(0)

	// Start 10 inserters that will each insert 100,000 items.
	for i := 0; i < 10; i++ {
		for i := 0; i < 100000; i++ {
			wg.Add(1)
			w.Insert(func(context.Context) {
				atomic.AddInt64(&run, 1)
				wg.Done()
			})
		}
	}

	// Wait for all the work to complete.
	wg.Wait()

	// And verify that all the tests ran.
	T.Equal(run, int64(1000000))
}
