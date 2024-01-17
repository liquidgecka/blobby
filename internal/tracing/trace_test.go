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
}
