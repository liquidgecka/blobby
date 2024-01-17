package storage

import (
	"math/rand"
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestList_Get(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	sentinal := primary{
		expires: rand.Int63(),
	}

	// Test out a simple get where there are nodes available already.
	l := list{
		head: &sentinal,
	}
	l.cond.L = &l.lock
	T.Equal(l.Get(nil), &sentinal)

	// Try again using a check function that sets a value to ensure that
	// the check was run. The check function will also trigger a goroutine
	// that will add the node to the list to be picked up later.
	checkRan := false
	l = list{head: nil}
	T.Equal(l.Get(func() {
		checkRan = true
		go func() {
			l.Put(&sentinal)
		}()
	}), &sentinal)
	T.Equal(checkRan, true)
}

func TestList_Put(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Create a series of nodes so that we can test that the order is correct
	// once added.
	n1 := primary{expires: 1}
	n2 := primary{expires: 2}
	n3 := primary{expires: 3}
	n4 := primary{expires: 4}
	n5 := primary{expires: 5}

	// Put the nodes in a non sorted order.
	l := list{}
	l.Put(&n5)
	l.Put(&n1)
	l.Put(&n4)
	l.Put(&n2)
	l.Put(&n3)

	// Verify that the order is correct.
	i := int64(1)
	for p := l.head; p != nil; p = p.next {
		T.Equal(p.expires, i)
		i += 1
	}
}

func TestList_Remove(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Add 5 items.
	items := make([]primary, 100)
	for i := range items {
		items[i].expires = int64(i)
	}

	// Put all the items into the list.
	l := list{}
	for i := range items {
		l.Put(&items[i])
	}

	// Test that removing an item doesn't fail if the item is not in the
	// list.
	T.Equal(l.Remove(&primary{}), false)

	// Randomly generate a list containing indexes so that we can
	// remove all the items in a functionally random order.
	order := make([]int, len(items))
	for i := range order {
		order[i] = i
	}
	rand.Shuffle(len(order), func(i, j int) {
		order[i], order[j] = order[j], order[i]
	})

	// Remove the items in this order.
	for _, i := range order {
		T.Equal(l.Remove(&items[i]), true)
	}

	// And finally, remove an item when the list is empty.
	T.Equal(l.Remove(&primary{}), false)

	// As one last check we make sure that removing nil doesn't crash.
	T.Equal(l.Remove(nil), false)
}

func TestList_Waiting(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	l := list{}
	T.Equal(l.Waiting(), l.waiting)
}
