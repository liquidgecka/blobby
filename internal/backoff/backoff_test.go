package backoff

import (
	"fmt"
	"testing"
	"time"

	"github.com/liquidgecka/testlib"
)

func TestBackOff_Failure(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a new BackOff object.
	b := BackOff{
		Period: time.Second,
		X:      time.Millisecond,
		Max:    time.Second,
		queue:  make([]time.Time, 100),
	}

	// Add 200 items and ensure that all items are updated in the queue.
	// This ensures that the write will wrap around.
	now := time.Now()
	for i := 0; i < 200; i++ {
		b.Failure()
	}
	for i, t := range b.queue {
		T.Equal(now.Before(t), true, fmt.Sprintf("item %d", i))
	}
}

func TestBackOff_Healthy(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	b := BackOff{}
	T.Equal(b.Healthy(), true)
	b.length += 1
	T.Equal(b.Healthy(), false)
}

func TestBackOff_Wait_Allocate(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	b := BackOff{
		Period: time.Second,
		X:      time.Millisecond,
		Max:    time.Hour,
	}
	T.Equal(b.Wait(), time.Duration(0))
	T.NotEqual(b.queue, nil)
	T.Equal(len(b.queue), 1000)
}

func TestBackOff_Wait_MaxWait(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	b := BackOff{
		Period: time.Hour,
		X:      time.Second,
		Max:    time.Second,
	}
	for i := 0; i < 100; i++ {
		b.Failure()
	}
	T.Equal(b.Wait(), time.Second)
}

func TestBackOff_Wait(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a new BackOff object.
	b := BackOff{
		Period: time.Second,
		X:      time.Millisecond,
		Max:    time.Hour,
		queue:  make([]time.Time, 100),
	}

	// This should return 0 when 0 items have been added.
	T.Equal(b.Wait(), time.Duration(0))

	// And once all the queue items have been marked as a failure then
	// this should return X * 99 (since the first failure is not delayed).
	for range b.queue {
		b.Failure()
	}
	T.Equal(b.Wait(), b.X*99)

	// Next we check and see what happens when the first item in the
	// queue has expired.
	b.queue[0] = time.Time{}
	T.Equal(b.Wait(), b.X*98)
	T.Equal(b.length, 99)

	// Next we can check that 50 items being expired does the same.
	for i := 0; i < 50; i++ {
		b.queue[i] = time.Time{}
	}
	T.Equal(b.Wait(), b.X*49)
	T.Equal(b.length, 50)

	// And finally we can ensure that the same thing happens when all items
	// are expired.
	for i := 0; i < len(b.queue); i++ {
		b.queue[i] = time.Time{}
	}
	T.Equal(b.Wait(), time.Duration(0))
	T.Equal(b.length, 0)
}
