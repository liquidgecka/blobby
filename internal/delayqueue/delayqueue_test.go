package delayqueue

import (
	"testing"
	"time"

	"github.com/liquidgecka/testlib"
)

// Benchmarks

func BenchmarkAlter(b *testing.B) {
	d := DelayQueue{}
	d.Start()
	defer d.Stop()

	// Create 10k tokens.
	tokens := make([]Token, 10000)
	for i := range tokens {
		d.Alter(
			&tokens[i],
			time.Now().Add(time.Millisecond),
			func() {})
	}

	// Runs through b.N iterations resetting the time on each
	// token.
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Alter(
			&tokens[i%len(tokens)],
			time.Now().Add(time.Millisecond),
			func() {})
	}
	b.StopTimer()
}

// High level tests of multiple elements of functionality.

func TestDelayQueue_Fires(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	d := DelayQueue{}
	d.Start()
	defer d.Stop()
	tok := &Token{}
	done := make(chan struct{})
	d.Alter(
		tok,
		time.Now().Add(time.Second/10),
		func() { close(done) })
	timer := time.NewTimer(time.Second * 5)
	select {
	case <-timer.C:
		T.Fatalf("Timer did not trigger function as expected.")
	case <-done:
	}
}

func TestDelayQueue_Ordering(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	d := DelayQueue{}
	d.Start()
	defer d.Stop()
	series := make(chan int, 100)
	start := time.Now()
	for i := 0; i < cap(series); i++ {
		instanceI := i
		d.Alter(
			&Token{inLine: true},
			start.Add(time.Millisecond*time.Duration(i)),
			func() {
				series <- instanceI
			},
		)
	}

	// Verify that the objects come out of the channel in the right order.
	cutoff := time.NewTimer(time.Second * 30)
	for i := 0; i < cap(series); i++ {
		select {
		case <-cutoff.C:
			T.Fatalf("Cutoff reached for actions to be executed.")
		case have := <-series:
			T.Equal(have, i)
		}
	}
	if !cutoff.Stop() {
		<-cutoff.C
	}
}

// Specific function tests

func TestDelayQueue_Alter(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	d := DelayQueue{}
	d.Start()
	defer d.Stop()

	// Expect a panic if f is nil.
	T.ExpectPanic(func() {
		t := Token{}
		d.Alter(&t, time.Now().Add(time.Second), nil)
	}, "DelayQueue.Alter with a nil function.")

	// For the first step we want to not fire a token.
	stepOne := time.Now().Add(time.Hour * 1000)

	// First token should panic and only fire in ages.
	tOne := Token{}
	d.Alter(
		&tOne,
		stepOne,
		func() {
			panic("STEP ONE SHOULDN'T EVER FIRE")
		})
	T.Equal(tOne.inList, true)
	T.Equal(d.list, &tOne)
	T.Equal(d.next, &tOne)
	T.Equal(tOne.next, nil)
	T.Equal(tOne.prev, nil)
	T.NotEqual(d.timer, nil)

	// Alter a new token that is not currently in the list.
	tTwo := Token{}
	d.Alter(
		&tTwo,
		stepOne.Add(time.Minute),
		func() {
			panic("STEP TWO SHOULDN'T EVER FIRE.")
		})
	T.Equal(tTwo.inList, true)
	T.Equal(d.list, &tTwo)
	T.Equal(d.next, &tOne)
	T.Equal(tOne.next, nil)
	T.Equal(tOne.prev, &tTwo)
	T.Equal(tTwo.next, &tOne)
	T.Equal(tTwo.prev, nil)
	T.NotEqual(d.timer, nil)

	// Adding an item before tOne should change the trigger time
	// on the timer.
	tZero := Token{}
	d.Alter(
		&tZero,
		stepOne.Add(-time.Minute),
		func() {
			panic("STEP ZERO SHOULDN'T EVER FIRE.")
		})
	T.Equal(tZero.inList, true)
	T.Equal(d.list, &tZero)
	T.Equal(d.next, &tZero)
	T.Equal(tOne.next, nil)
	T.Equal(tOne.prev, &tTwo)
	T.Equal(tTwo.next, &tOne)
	T.Equal(tTwo.prev, &tZero)
	T.Equal(tZero.next, &tTwo)
	T.Equal(tZero.prev, nil)
	T.NotEqual(d.timer, nil)

	// Alter each token one by one to ensure that they fire.
	fire := make(chan struct{}, 3)
	d.Alter(
		&tOne,
		time.Now(),
		func() {
			fire <- struct{}{}
		})
	select {
	case <-fire:
	case <-time.NewTimer(time.Second).C:
		T.Fatalf("Expected function firing didn't happen.")
	}
	T.Equal(tOne.inList, false)
	T.Equal(tOne.next, nil)
	T.Equal(tOne.prev, nil)
	T.Equal(d.list, &tZero)
	T.Equal(d.next, &tZero)
	T.Equal(tTwo.next, nil)
	T.Equal(tTwo.prev, &tZero)
	T.Equal(tZero.next, &tTwo)
	T.Equal(tZero.prev, nil)
	T.NotEqual(d.timer, nil)
	d.Alter(
		&tTwo,
		time.Now(),
		func() {
			fire <- struct{}{}
		})
	select {
	case <-fire:
	case <-time.NewTimer(time.Second).C:
		T.Fatalf("Expected function firing didn't happen.")
	}
	T.Equal(tTwo.inList, false)
	T.Equal(tTwo.next, nil)
	T.Equal(tTwo.prev, nil)
	T.Equal(d.list, &tZero)
	T.Equal(d.next, &tZero)
	T.Equal(tZero.next, nil)
	T.Equal(tZero.prev, nil)
	T.NotEqual(d.timer, nil)
	d.Alter(
		&tZero,
		time.Now(),
		func() {
			fire <- struct{}{}
		})
	select {
	case <-fire:
	case <-time.NewTimer(time.Second).C:
		T.Fatalf("Expected function firing didn't happen.")
	}
	T.Equal(tZero.inList, false)
	T.Equal(tZero.next, nil)
	T.Equal(tZero.prev, nil)
	T.Equal(d.list, nil)
	T.Equal(d.next, nil)
	T.NotEqual(d.timer, nil)
}

func TestDelayQueue_Cancel(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	d := DelayQueue{}
	d.Start()
	defer d.Stop()

	f := func() {
		panic("NOT EXPECTED")
	}

	// Add three tokens to the list.
	tok1, tok2, tok3 := Token{}, Token{}, Token{}
	d.Alter(&tok1, time.Now().Add(time.Hour), f)
	d.Alter(&tok2, time.Now().Add(time.Hour*2), f)
	d.Alter(&tok3, time.Now().Add(time.Hour*3), f)
	T.Equal(d.list, &tok3)
	T.Equal(d.list.next, &tok2)
	T.Equal(d.list.next.next, &tok1)
	T.Equal(d.next, &tok1)

	// Make sure that canceling a token that is not in the list does
	// nothing.
	tokVoid := Token{}
	d.Cancel(&tokVoid)
	T.Equal(&tokVoid, &Token{})

	// Remove the last items in the list.
	d.Cancel(&tok3)
	T.Equal(d.list, &tok2)
	T.Equal(d.list.next, &tok1)
	T.Equal(d.list.next.next, nil)
	T.Equal(d.next, &tok1)

	// Remove the head of the list.
	d.Cancel(&tok1)
	T.Equal(d.list, &tok2)
	T.Equal(d.list.next, nil)
	T.Equal(d.next, &tok2)

	// And finally remove the last item.
	d.Cancel(&tok2)
	T.Equal(d.list, nil)
	T.Equal(d.next, nil)
}
