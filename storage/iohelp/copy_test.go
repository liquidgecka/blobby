package iohelp

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"testing"

	"github.com/liquidgecka/testlib"
)

type readwriter struct {
	read  func([]byte) (int, error)
	write func([]byte) (int, error)
}

func (r *readwriter) Read(data []byte) (int, error) {
	return r.read(data)
}

func (r *readwriter) Write(data []byte) (int, error) {
	return r.write(data)
}

func TestCopyBuffer(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	contents := make([]byte, 1024*16+100)
	rand.Read(contents)
	source := bytes.Buffer{}
	source.Write(contents)
	dest := bytes.Buffer{}
	buffer := make([]byte, 1024)
	n, err1, err2 := CopyBuffer(&dest, &source, buffer)
	T.Equal(n, int64(len(contents)))
	T.ExpectSuccess(err1)
	T.ExpectSuccess(err2)
	T.Equal(dest.Bytes(), contents)
}

func TestCopyBuffer_SourceError(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	source := readwriter{
		read: func(data []byte) (int, error) {
			return 0, fmt.Errorf("EXPECTED")
		},
	}
	dest := bytes.Buffer{}
	buffer := make([]byte, 1024)
	n, err1, err2 := CopyBuffer(&dest, &source, buffer)
	T.Equal(n, int64(0))
	T.ExpectSuccess(err1)
	T.ExpectErrorMessage(err2, "EXPECTED")
}

func TestCopyBuffer_DestError(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	contents := make([]byte, 1024*16+100)
	rand.Read(contents)
	source := bytes.Buffer{}
	source.Write(contents)
	dest := readwriter{
		write: func(data []byte) (int, error) {
			return 0, fmt.Errorf("EXPECTED")
		},
	}
	buffer := make([]byte, 1024)
	n, err1, err2 := CopyBuffer(&dest, &source, buffer)
	T.Equal(n, int64(0))
	T.ExpectErrorMessage(err1, "EXPECTED")
	T.ExpectSuccess(err2)
}

func TestCopyBuffer_ShortWrite(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	contents := make([]byte, 1024*16+100)
	rand.Read(contents)
	source := bytes.Buffer{}
	source.Write(contents)
	dest := readwriter{
		write: func(data []byte) (int, error) {
			return len(data) / 2, nil
		},
	}
	buffer := make([]byte, 1024)
	n, err1, err2 := CopyBuffer(&dest, &source, buffer)
	T.Equal(n, int64(512))
	T.Equal(err1, io.ErrShortWrite)
	T.ExpectSuccess(err2)
}
