package tracing

import (
	"testing"
	"time"

	"github.com/liquidgecka/testlib"
)

func TestTrace_String(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Sets end to start + 1m for testing
	fix := func(t *Trace) *Trace {
		t.end = t.start.Add(time.Millisecond)
		return t
	}

	expected := `top 1ms
+-c1: 1ms
| +-c1_1: 1ms
| | \-c1_1_1: 1ms
| \-c1_2: 1ms
\-c2: 1ms
`

	// Make a trace tree.
	top := fix(New())
	c1 := fix(top.NewChild("c1"))
	c1_1 := fix(c1.NewChild("c1_1"))
	fix(c1_1.NewChild("c1_1_1"))
	fix(c1.NewChild("c1_2"))
	fix(top.NewChild("c2"))
	T.Equal(top.String(), expected)
	top.Free()
}

func TestTrace_NilSafety(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	var nilTrace *Trace
	nilTrace.End()
	nilTrace.Free()
	T.Equal(nilTrace.NewChild("child"), (*Trace)(nil))
	T.Equal(nilTrace.String(), "")
}

func BenchmarkTrace_String(b *testing.B) {
	fix := func(t *Trace) *Trace {
		t.end = t.start.Add(time.Millisecond)
		return t
	}
	top := fix(New())
	c1 := fix(top.NewChild("c1"))
	c1_1 := fix(c1.NewChild("c1_1"))
	fix(c1_1.NewChild("c1_1_1"))
	fix(c1.NewChild("c1_2"))
	fix(top.NewChild("c2"))

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = top.String()
	}
	top.Free()
}
