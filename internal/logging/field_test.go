package logging

import (
	"fmt"
	"testing"

	"github.com/liquidgecka/testlib"

	"github.com/liquidgecka/blobby/internal/compat"
)

func TestNewField(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	have := NewField("key", "value")
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "value",
		valueQuoted: false,
		valueRaw:    false,
		red:         false,
		prev:        nil,
	})

	// Escaped values.
	have = NewField("a\nb", "c\nd")
	T.Equal(have, Field{
		name:        "a\\nb",
		nameQuoted:  true,
		value:       "c\\nd",
		valueQuoted: true,
		valueRaw:    false,
		red:         false,
		prev:        nil,
	})
}

func TestNewFieldInt32(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	have := NewFieldInt32("key", -1)
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "-1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})
}

func TestNewFieldInt64(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	have := NewFieldInt64("key", -1)
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "-1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})
}

func TestNewFieldUint32(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	have := NewFieldUint32("key", 1)
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})
}

func TestNewFieldUint64(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	have := NewFieldUint64("key", 1)
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})
}

func TestNewFieldIface_String(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// string (simple)
	have := NewFieldIface("key", "value")
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "value",
		valueQuoted: false,
		valueRaw:    false,
		red:         false,
		prev:        nil,
	})

	// string (complex)
	have = NewFieldIface("key", "escaped value")
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "escaped value",
		valueQuoted: true,
		valueRaw:    false,
		red:         false,
		prev:        nil,
	})
}

func TestNewFieldIface_Ints(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// int: negative
	have := NewFieldIface("key", -1)
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "-1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// int: positive
	have = NewFieldIface("key", 1)
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// int8: negative
	have = NewFieldIface("key", int8(-1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "-1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// int8: positive
	have = NewFieldIface("key", int8(1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// int16: negative
	have = NewFieldIface("key", int16(-1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "-1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// int16: positive
	have = NewFieldIface("key", int16(1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// int32: negative
	have = NewFieldIface("key", int32(-1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "-1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// int32: positive
	have = NewFieldIface("key", int32(1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// int32: negative
	have = NewFieldIface("key", int32(-1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "-1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// int64: positive
	have = NewFieldIface("key", int64(1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// uint
	have = NewFieldIface("key", uint(1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// uint8
	have = NewFieldIface("key", uint8(1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// uint16
	have = NewFieldIface("key", uint16(1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// uint32
	have = NewFieldIface("key", uint32(1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// int64
	have = NewFieldIface("key", uint64(1))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "1",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})
}

func TestNewFieldIface_Bools(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// true
	have := NewFieldIface("key", true)
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "true",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})

	// false
	have = NewFieldIface("key", false)
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "false",
		valueQuoted: false,
		valueRaw:    true,
		red:         false,
		prev:        nil,
	})
}

func TestNewFieldIface_Stringer(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	b := compat.Builder{}
	b.WriteString("test")
	have := NewFieldIface("key", &b)
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "test",
		valueQuoted: false,
		valueRaw:    false,
		red:         false,
		prev:        nil,
	})
}

func TestNewFieldIface_Default(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	v := struct{}{}

	have := NewFieldIface("key", v)
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "struct {}{}",
		valueQuoted: true,
		valueRaw:    false,
		red:         false,
		prev:        nil,
	})
}

func TestNewFieldIface_Error(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()
	have := NewFieldIface("key", fmt.Errorf("value"))
	T.Equal(have, Field{
		name:        "key",
		nameQuoted:  false,
		value:       "value",
		valueQuoted: false,
		valueRaw:    false,
		red:         true,
		prev:        nil,
	})
}
