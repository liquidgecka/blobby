package logging

import (
	"bufio"
	"bytes"
	"testing"
	"time"

	"github.com/liquidgecka/testlib"
)

func TestNewANSIOutput(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	b := bufio.NewWriter(bytes.NewBuffer(nil))
	T.Equal(NewANSIOutput(b), &Output{renderer: renderANSI, buffer: b})
}

func TestNewJSONOutput(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	b := bufio.NewWriter(bytes.NewBuffer(nil))
	T.Equal(NewJSONOutput(b), &Output{renderer: renderJSON, buffer: b})
}

func TestNewPlainOutput(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	b := bufio.NewWriter(bytes.NewBuffer(nil))
	T.Equal(NewPlainOutput(b), &Output{renderer: renderPlain, buffer: b})
}

func TestOutput_TeeOutput(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	o1 := Output{}
	o2 := Output{}
	o1.TeeOutput(&o2)
	T.Equal(o1.next, &o2)
	T.Equal(o2.next, nil)
}

func TestOutput_Write(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a Output object and a default log line.
	buffer := bytes.NewBuffer(nil)
	o := Output{
		buffer: bufio.NewWriter(buffer),
	}
	rd := renderData{
		time:      time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC),
		level:     dbg,
		message:   "log message",
		tail:      nil,
		optFields: nil,
	}

	// Attempt a plain write and verify the line was written.
	o.renderer = renderPlain
	o.Write(&rd)
	T.Equal(buffer.String(), "2000-01-02T03:04:05Z: DBG log message\n")
	buffer.Truncate(0)

	// Attempt a JSON write.
	o.renderer = renderJSON
	o.Write(&rd)
	T.Equal(buffer.String(), ``+
		`{"timestamp":"2000-01-02T03:04:05Z","message":"log message",`+
		`"level"="debug"}`+"\n",
	)
	buffer.Truncate(0)

	// Attempt a ANSI write.
	o.renderer = renderANSI
	o.Write(&rd)
	T.Equal(
		buffer.String(),
		"\x1b[90m2000-01-02T03:04:05Z \x1b[33mDBG \x1b[0mlog message\x1b[0m\n",
	)
	buffer.Truncate(0)

	// Attempt a Test write.
	o.renderer = renderTest
	o.Write(&rd)
	T.Equal(buffer.String(), "DBG log message\n")
	buffer.Truncate(0)
}
