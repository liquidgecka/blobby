package logging

import (
	"fmt"
	"strconv"
)

type Field struct {
	// The underlying string values of the field. The string is the encoded
	// value stored for writing later. The *Quoted bools are used to
	// tell the rendering functions later if the string had special characters
	// that need quoting. This does not impact the JSON renderer, but for
	// Plain and ANSI rendering we need to know this so foo=bar does not
	// get quotes, while foo="foo bar" does. the valueRaw field is used for
	// JSON rendering to let it know that the value can be saved without
	// quotes.
	name        string
	nameQuoted  bool
	value       string
	valueRaw    bool
	valueQuoted bool

	// Keeps track of how deep the field list is. This is used with fields
	// that are attached to loggers.
	level int

	// Should this field have a special color? This is used for special
	// field types like errors.
	red bool

	// A reference to the previous value in the context. Since context are
	// built as an appending list of fields this will never be duplicated
	// unless the user does something broken.
	prev *Field
}

// Generates a field that can be used with the logging functions. This is
// designed to be a no allocation call so that it will be fast and efficient.
// Where possible use this verses NewFieldIface since that call will force
// memory allocations due to its use of the Stringer interface{}.
func NewField(name, value string) (f Field) {
	f.set(name, value)
	return
}

// Like NewField except this will do everything possible to render the
// data to a string. In turn this means that this will use the Stringer
// interface which will require than any argument passed here be allocated
// on the heap. Use NewField() where possible.
func NewFieldIface(name string, value interface{}) (f Field) {
	switch o := value.(type) {
	case (string):
		f.set(name, o)
	case (int):
		f.set(name, strconv.FormatInt(int64(o), 10))
		f.valueRaw = true
	case (int8):
		f.set(name, strconv.FormatInt(int64(o), 10))
		f.valueRaw = true
	case (int16):
		f.set(name, strconv.FormatInt(int64(o), 10))
		f.valueRaw = true
	case (int32):
		f.set(name, strconv.FormatInt(int64(o), 10))
		f.valueRaw = true
	case (int64):
		f.set(name, strconv.FormatInt(int64(o), 10))
		f.valueRaw = true
	case uint:
		f.set(name, strconv.FormatUint(uint64(o), 10))
		f.valueRaw = true
	case uint8:
		f.set(name, strconv.FormatUint(uint64(o), 10))
		f.valueRaw = true
	case uint16:
		f.set(name, strconv.FormatUint(uint64(o), 10))
		f.valueRaw = true
	case uint32:
		f.set(name, strconv.FormatUint(uint64(o), 10))
		f.valueRaw = true
	case uint64:
		f.set(name, strconv.FormatUint(uint64(o), 10))
		f.valueRaw = true
	case (bool):
		f.valueRaw = true
		if o {
			f.set(name, "true")
		} else {
			f.set(name, "false")
		}
	case (error):
		f.set(name, o.Error())
		f.red = true
	case fmt.Stringer:
		f.set(name, o.String())
	default:
		return NewField(name, fmt.Sprintf("%#v", value))
	}
	return
}

// Creates a new Field using the given int32 value.
func NewFieldInt32(name string, value int32) (f Field) {
	f.set(name, strconv.FormatInt(int64(value), 10))
	f.valueRaw = true
	return
}

// Creates a new Field using the given int64 value.
func NewFieldInt64(name string, value int64) (f Field) {
	f.set(name, strconv.FormatInt(value, 10))
	f.valueRaw = true
	return
}

// Creates a new Field using the given uint32 value.
func NewFieldUint32(name string, value uint32) (f Field) {
	f.set(name, strconv.FormatUint(uint64(value), 10))
	f.valueRaw = true
	return
}

// Creates a new Field using the given uint64 value.
func NewFieldUint64(name string, value uint64) (f Field) {
	f.set(name, strconv.FormatUint(value, 10))
	f.valueRaw = true
	return
}

// Inner function that is used by both NewField versions to set the
// actual data inside of the field object.
func (f *Field) set(name, value string) {
	if !shouldEscape(name) {
		f.name = name
	} else {
		f.name = encodeJSONString(name)
		f.nameQuoted = true
	}
	if !shouldEscape(value) {
		f.value = value
	} else {
		f.value = encodeJSONString(value)
		f.valueQuoted = true
	}
}
