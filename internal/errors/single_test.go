package errors

import (
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestSimpleError(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	s := New("test")
	T.Equal(s.Error(), "test")
}
