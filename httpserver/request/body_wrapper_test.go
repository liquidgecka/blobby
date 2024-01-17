package request

import (
	"bytes"
	"io/ioutil"
	"math/rand"
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestBodyWrapper_Read(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	buffer := bytes.NewBuffer(nil)
	b := bodyWrapper{
		in:   ioutil.NopCloser(buffer),
		size: 0,
	}
	in := make([]byte, 1000)
	rand.Read(in)
	buffer.Write(in)
	out := make([]byte, 1000)
	n, err := b.Read(out)
	T.ExpectSuccess(err)
	T.Equal(n, len(in))
	T.Equal(out, in)
	T.Equal(b.size, int64(len(out)))
}

type closeTester struct {
	closed bool
}

func (c *closeTester) Read(data []byte) (int, error) {
	panic("not used")
}

func (c *closeTester) Close() error {
	c.closed = true
	return nil
}

func TestBodyWrapper_Close(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	c := &closeTester{}
	b := &bodyWrapper{in: c}
	T.ExpectSuccess(b.Close())
	T.Equal(c.closed, true)
}
