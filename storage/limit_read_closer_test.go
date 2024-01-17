package storage

import (
	"bytes"
	"io"
	"testing"

	"github.com/liquidgecka/testlib"
)

type tlrc struct {
	C func() error
	R func([]byte) (int, error)
}

func (t *tlrc) Close() error {
	return t.C()
}

func (t *tlrc) Read(data []byte) (int, error) {
	return t.R(data)
}

func TestLimitReadCloser_Close(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	closed := false
	l := limitReadCloser{
		RC: &tlrc{
			C: func() error {
				closed = true
				return nil
			},
		},
	}
	T.ExpectSuccess(l.Close())
	T.Equal(closed, true)
}

func TestLimitReadCloser_Read(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Happy path, read is allowed.
	expect := "Happy path."
	buffer := bytes.NewBuffer(nil)
	buffer.WriteString(expect)
	l := limitReadCloser{
		N: 1000,
		RC: &tlrc{
			C: func() error { return nil },
			R: func(data []byte) (int, error) { return buffer.Read(data) },
		},
	}
	raw := make([]byte, 1000)
	n, err := l.Read(raw)
	T.ExpectSuccess(err)
	T.Equal(n, len(expect))
	T.Equal(raw[0:n], []byte(expect))
	T.Equal(l.N, int64(1000-len(expect)))

	// Less happy path, limited read.
	buffer = bytes.NewBuffer(nil)
	buffer.WriteString(expect)
	l = limitReadCloser{
		N: 5,
		RC: &tlrc{
			C: func() error { return nil },
			R: func(data []byte) (int, error) { return buffer.Read(data) },
		},
	}
	raw = make([]byte, 1000)
	n, err = l.Read(raw)
	T.ExpectSuccess(err)
	T.Equal(n, 5)
	T.Equal(raw[0:n], []byte(expect)[0:5])
	T.Equal(l.N, int64(0))

	// No bytes left for reading, ensure Read() is never even called.
	read := false
	l = limitReadCloser{
		N: 0,
		RC: &tlrc{
			C: func() error { return nil },
			R: func(data []byte) (int, error) {
				read = true
				return 0, nil
			},
		},
	}
	n, err = l.Read(raw)
	T.Equal(err, io.EOF)
	T.Equal(n, 0)
	T.Equal(l.N, int64(0))
	T.Equal(read, false)
}
