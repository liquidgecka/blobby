package errors

// A simple, single string error.
type simpleError string

func New(s string) error {
	return simpleError(s)
}

func (s simpleError) Error() string {
	return string(s)
}
