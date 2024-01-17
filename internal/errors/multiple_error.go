package errors

import (
	"strings"
)

type MultipleError struct {
	Errors      []error
	Description string
}

func NewMultipleError(desc string, errs []error) error {
	// Make a copy of the errors so that the caller won't have its error
	// list suddenly allocated on heap.
	errCopy := make([]error, len(errs))
	copy(errCopy, errs)
	return &MultipleError{
		Errors:      errCopy,
		Description: desc,
	}
}

func (m *MultipleError) Error() string {
	strs := make([]string, len(m.Errors))
	for i, e := range m.Errors {
		strs[i] = e.Error()
	}
	return m.Description + ": " + strings.Join(strs, ", ")
}
