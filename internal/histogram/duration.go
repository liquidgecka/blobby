package histogram

import (
	"math/rand"
	"sort"
	"sync/atomic"
	"time"
)

type Duration struct {
	raw   []int64
	added int64
}

func (d *Duration) Add(v time.Duration) {
	atomic.StoreInt64(&d.raw[rand.Intn(len(d.raw))], int64(v))
}

func (d *Duration) Histogram() (p50, p90, p99, p999 time.Duration) {
	// Make a copy of the data and sort it.
	rCopy := make([]int64, len(d.raw))
	for i := range rCopy {
		rCopy[i] = atomic.LoadInt64(&d.raw[i])
	}
	sort.Sort(int64Slice(rCopy))

	// Discard zero's.
	for {
		if len(rCopy) == 0 {
			return 0, 0, 0, 0
		} else if rCopy[0] == 0 {
			rCopy = rCopy[1:]
		} else {
			break
		}
	}

	// Calculate the values
	p50 = time.Duration(rCopy[(len(rCopy)*500)/1000])
	p90 = time.Duration(rCopy[(len(rCopy)*900)/1000])
	p99 = time.Duration(rCopy[(len(rCopy)*990)/1000])
	p999 = time.Duration(rCopy[(len(rCopy)*999)/1000])
	return
}
