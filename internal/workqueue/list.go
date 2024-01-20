package workqueue

import (
	"context"
	"sync"
)

var (
	// A pool of workList objects that are used to reduce malloc churn.
	wlPool = sync.Pool{
		New: func() interface{} { return new(workList) },
	}
)

func getWorkList() *workList {
	return wlPool.Get().(*workList)
}

type workList struct {
	// A list of functions that are stored in this workList object. This
	// will be used in a rolling list to ensure that items are read
	// in order.
	work [64]func(context.Context)

	// A pointer to the next workList object in the WorkQueue.
	next *workList
}
