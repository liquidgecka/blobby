package logging

import (
	"bufio"
	"sync"
)

// Implements outputting to a specific writer using a specific rendering
// function. Note that we can not use an interface here as it will require
// that all calls to Write() use heap allocated field data, which in turn
// will make all calls to logging functions use heap allocated Field data
// which in turn increases the GC footprint of this logging library
// heavily.
type Output struct {
	renderer int
	buffer   *bufio.Writer
	lock     sync.Mutex
	next     *Output
}

// Sets up a new Output object that will write to the given buffer using
// an ANSI writer. This will inject ANSI coloring between log elements.
func NewANSIOutput(buf *bufio.Writer) *Output {
	return &Output{
		renderer: renderANSI,
		buffer:   buf,
	}
}

// Sets up a new Output object that will write to the given buffer using
// a JSON output. Each line will be a single json object containing all
// of the fields.
func NewJSONOutput(buf *bufio.Writer) *Output {
	return &Output{
		renderer: renderJSON,
		buffer:   buf,
	}
}

// Sets up a new Output object that will write to the given buffer using
// a plain output. Plain is single line text elements.
func NewPlainOutput(buf *bufio.Writer) *Output {
	return &Output{
		renderer: renderPlain,
		buffer:   buf,
	}
}

// Adds a second output path to this Output.
func (o *Output) TeeOutput(n *Output) {
	// FIXME: What to do if multiple things are added?
	o.next = n
}

// Writes a log line to the output.
func (o *Output) Write(rd *renderData) {
	for o != nil {
		func() {
			o.lock.Lock()
			defer o.lock.Unlock()
			switch o.renderer {
			case renderJSON:
				rJSON(o.buffer, rd)
			case renderANSI:
				rANSI(o.buffer, rd)
			case renderTest:
				rTest(o.buffer, rd)
			case renderPlain:
				fallthrough
			default:
				rPlain(o.buffer, rd)
			}
			o.buffer.WriteByte('\n')
			writeReturn(o.buffer)
			o.buffer.Flush()
			o = o.next
		}()
	}
}
