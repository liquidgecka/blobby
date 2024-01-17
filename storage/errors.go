package storage

import (
	"fmt"
)

type ErrInvalidID struct{}

func (e ErrInvalidID) Error() string {
	return "The provided ID is not valid."
}

type ErrNotFound string

func (e ErrNotFound) Error() string {
	return fmt.Sprintf("%s was not found.", string(e))
}

type ErrNotPossible struct{}

func (ErrNotPossible) Error() string {
	return "The requested operation is not possible."
}

type ErrReplicaNotFound string

func (e ErrReplicaNotFound) Error() string {
	return fmt.Sprintf("%s is not a known replica.", string(e))
}

type ErrWrongReplicaState struct{}

func (e ErrWrongReplicaState) Error() string {
	return "The replica is in the wrong state for this operation."
}
