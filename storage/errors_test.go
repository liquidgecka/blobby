package storage

import (
	"testing"

	"github.com/liquidgecka/testlib"
)

func TestErrInvalidID_Error(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := ErrInvalidID{}
	T.Equal(r.Error(), "The provided ID is not valid.")
}

func TestErrNotFound_Error(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := ErrNotFound("test")
	T.Equal(r.Error(), "test was not found.")
}

func TestErrNotPossible_Error(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := ErrNotPossible{}
	T.Equal(r.Error(), "The requested operation is not possible.")
}

func TestErrReplicaNotFound_Error(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := ErrReplicaNotFound("test")
	T.Equal(r.Error(), "test is not a known replica.")
}

func TestErrWrongReplicaState(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := ErrWrongReplicaState{}
	T.Equal(r.Error(), "The replica is in the wrong state for this operation.")
}
