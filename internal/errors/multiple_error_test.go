package errors

import (
	"fmt"
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestMultipleError(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Make a list of errors.
	errs := make([]error, 3)
	for i := range errs {
		errs[i] = fmt.Errorf("error %d", i)
	}
	err := NewMultipleError("description", errs)
	T.NotEqual(err, nil)
	T.Equal(err.Error(), "description: error 0, error 1, error 2")
}
