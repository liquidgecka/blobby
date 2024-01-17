package storage

import (
	"math/rand"
	"sort"
	"strings"
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestPrimarySlice(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Randomize an array.
	p := primarySlice{
		&primary{fidStr: "1"},
		&primary{fidStr: "2"},
		&primary{fidStr: "3"},
		&primary{fidStr: "4"},
		&primary{fidStr: "5"},
		&primary{fidStr: "6"},
	}
	rand.Shuffle(len(p), func(i, j int) { p[i], p[j] = p[j], p[i] })

	// Sort the array.
	sort.Sort(p)

	// Verify that it is sorted.
	for i := 1; i < len(p); i++ {
		if strings.Compare(p[i-1].fidStr, p[i].fidStr) > 0 {
			T.Fatal("the slice was not sorted properly!")
		}
	}
}

func TestReplicaSlice(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Randomize an array.
	r := replicaSlice{
		&replica{fidStr: "1"},
		&replica{fidStr: "2"},
		&replica{fidStr: "3"},
		&replica{fidStr: "4"},
		&replica{fidStr: "5"},
		&replica{fidStr: "6"},
	}
	rand.Shuffle(len(r), func(i, j int) { r[i], r[j] = r[j], r[i] })

	// Sort the array.
	sort.Sort(r)

	// Verify that it is sorted.
	for i := 1; i < len(r); i++ {
		if strings.Compare(r[i-1].fidStr, r[i].fidStr) > 0 {
			T.Fatal("the slice was not sorted properly!")
		}
	}
}
