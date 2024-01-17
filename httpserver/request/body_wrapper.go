package request

import (
	"io"
)

// An internal implementation of io.Reader that tracks how many total bytes
// have passed through it. This is useful for logging the total amount of
// data read and written during a request and response cycle.
type bodyWrapper struct {
	in   io.ReadCloser
	size int64
}

func (b *bodyWrapper) Read(data []byte) (n int, err error) {
	n, err = b.in.Read(data)
	b.size += int64(n)
	return
}

func (b *bodyWrapper) Close() error {
	return b.in.Close()
}
