package tracing

import (
	"sync"
	"time"

	"github.com/liquidgecka/blobby/internal/compat"
)

var (
	tracePool = sync.Pool{
		New: func() interface{} {
			return new(Trace)
		},
	}
)

type Trace struct {
	// The name of this trace.
	name string

	// The time that this trace was started.
	start time.Time

	// The time that this trace ended.
	end time.Time

	// A linked list of all children of this trace. These children
	// were called in order of appearance in the list.
	children  *Trace
	lastChild *Trace

	// A next pointer that is used when this Trace is in a linked list.
	next *Trace
}

func New() *Trace {
	nt := tracePool.Get().(*Trace)
	nt.name = "top"
	nt.start = time.Now()
	return nt
}

func (t *Trace) End() {
	if t != nil {
		t.end = time.Now()
	}
}

func (t *Trace) Free() {
	t.free()
	*t = Trace{}
	tracePool.Put(t)
}

func (t *Trace) free() {
	var f *Trace
	for n := t.children; n != nil; {
		n.free()
		f, n = n, n.next
		*f = Trace{}
		tracePool.Put(f)
	}
	t.children = nil
	t.lastChild = nil
}

func (t *Trace) NewChild(name string) *Trace {
	if t == nil {
		return nil
	}
	nt := tracePool.Get().(*Trace)
	nt.name = name
	nt.start = time.Now()
	if t.children == nil {
		t.children = nt
		t.lastChild = nt
	} else {
		t.lastChild.next = nt
		t.lastChild = nt
	}
	return nt
}

func (t *Trace) String() string {
	s := compat.Builder{}
	s.WriteString(t.name)
	s.WriteByte(' ')
	s.WriteString(t.end.Sub(t.start).String())
	s.WriteByte('\n')
	if t.children != nil {
		stack := make([]*Trace, 1, 10)
		stack[0] = t.children
		for level := 0; level >= 0; {
			if stack[level] == nil {
				level -= 1
				continue
			}
			for i := -1; i < level-1; i++ {
				s.WriteString("| ")
			}
			if stack[level].next == nil {
				s.WriteString(`\-`)
			} else {
				s.WriteString(`+-`)
			}
			s.WriteString(stack[level].name)
			s.WriteByte(':')
			s.WriteByte(' ')
			s.WriteString(stack[level].end.Sub(stack[level].start).String())
			s.WriteByte('\n')
			if len(stack) < level+2 {
				stack = append(stack, nil)
			}
			stack[level+1] = stack[level].children
			stack[level] = stack[level].next
			level += 1
		}
	}
	return s.String()
}
