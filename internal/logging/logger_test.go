package logging

import (
	"bufio"
	"bytes"
	"io/ioutil"
	"regexp"
	"runtime"
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestNewLogger(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	o := &Output{}
	T.Equal(NewLogger(o), &Logger{output: o})
}

func TestLogger_AddField(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	start := &Logger{}
	T.Equal(start.AddField("key", "value"), start)
	T.Equal(start.fields, &Field{
		name:  "key",
		value: "value",
		level: 1,
		prev:  nil,
	})
	T.Equal(start.AddField("second key", "second value"), start)
	T.Equal(start.fields, &Field{
		name:        "second key",
		nameQuoted:  true,
		value:       "second value",
		valueQuoted: true,
		level:       2,
		prev: &Field{
			name:  "key",
			value: "value",
			level: 1,
		},
	})
}

func TestLogger_DisableDebug(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer to receive the output.
	buffer := bytes.Buffer{}
	l := Logger{
		enableDbg: true,
		output: &Output{
			buffer: bufio.NewWriter(&buffer),
		},
	}
	l.Debug("test")
	T.NotEqual(buffer.Len(), 0)
	buffer.Reset()
	l.DisableDebug()
	T.Equal(l.DebugEnabled(), false)
	l.Debug("test")
	T.Equal(buffer.Len(), 0)
}

func TestLogger_EnableDebug(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer to receive the output.
	buffer := bytes.Buffer{}
	l := Logger{
		output: &Output{
			buffer: bufio.NewWriter(&buffer),
		},
	}
	l.Debug("test")
	T.Equal(buffer.Len(), 0)
	l.EnableDebug()
	T.Equal(l.DebugEnabled(), true)
	l.Debug("test")
	T.NotEqual(buffer.Len(), 0)
}

func TestLogger_NewChild(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	l := Logger{
		output: &Output{
			buffer: bufio.NewWriter(ioutil.Discard),
		},
		enableDbg: true,
	}
	T.Equal(&l, l.NewChild())
}

func TestLogger_SetOutput(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	l := Logger{}
	o := &Output{}
	l.SetOutput(o)
	T.Equal(l.output, o)
}

func TestLogger_Debug(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer to receive the output.
	buffer := bytes.Buffer{}
	l := Logger{
		enableDbg: true,
		output: &Output{
			renderer: renderTest,
			buffer:   bufio.NewWriter(&buffer),
		},
	}
	l.Debug("test")
	T.NotEqual(buffer.Len(), 0)
	T.Equal(buffer.String(), "DBG test\n")
}

// This test ensures that we have not accidentally made a mistake that will
// force every log line to suddenly start allocating memory. A typical
// call to a logging function should not introduce any forced allocations.
func TestLogger_Debug_NoAllocates(t *testing.T) {
	// FIXME: This test no longer works in go 1.20/1.21
	t.Skip("Test doesn't work in modern golang.")

	// This test doesn't work on go1.11 or earlier because ReadMemStats()
	// allocates memory. As such we check the version of go and if it is
	// older than go1.10 we skip the test.
	v := runtime.Version()
	pattern := "^go1\\.([0-9]|10|11)(\\.[0-9]+)?(-.*)?$"
	if m, err := regexp.MatchString(pattern, v); m {
		t.Skip("This test requires a newer go release.")
	} else if err != nil {
		t.Skip(err.Error())
	}
	t.Logf("Running on" + v)

	// Otherwise perform the test.
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer to receive the output. Note that we want debug
	// logging specifically disabled because we do not want to exercise
	// any of the follow up logic since it might introduce an allocation
	// in the output cycle.
	o := Output{buffer: bufio.NewWriterSize(ioutil.Discard, 4096)}
	l := Logger{
		enableDbg: false,
		output:    &o,
	}

	// Get the starting memory allocations.
	start := runtime.MemStats{}
	runtime.ReadMemStats(&start)

	// Run the test with additional fields to ensure that nothing gets
	// allocated.
	l.Debug("test", NewField("test", "test"))

	// Get the ending memory allocations.
	end := runtime.MemStats{}
	runtime.ReadMemStats(&end)

	// Calculate the number of allocations.
	T.Equal(start.TotalAlloc, end.TotalAlloc, "allocations were made.")
}

func TestLogger_Info(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer to receive the output.
	buffer := bytes.Buffer{}
	l := Logger{
		enableDbg: true,
		output: &Output{
			renderer: renderTest,
			buffer:   bufio.NewWriter(&buffer),
		},
	}
	l.Info("test")
	T.NotEqual(buffer.Len(), 0)
	T.Equal(buffer.String(), "INF test\n")
}

func TestLogger_Warning(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer to receive the output.
	buffer := bytes.Buffer{}
	l := Logger{
		enableDbg: true,
		output: &Output{
			renderer: renderTest,
			buffer:   bufio.NewWriter(&buffer),
		},
	}
	l.Warning("test")
	T.NotEqual(buffer.Len(), 0)
	T.Equal(buffer.String(), "WRN test\n")
}

func TestLogger_Error(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer to receive the output.
	buffer := bytes.Buffer{}
	l := Logger{
		enableDbg: true,
		output: &Output{
			renderer: renderTest,
			buffer:   bufio.NewWriter(&buffer),
		},
	}
	l.Error("test")
	T.NotEqual(buffer.Len(), 0)
	T.Equal(buffer.String(), "ERR test\n")
}
