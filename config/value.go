package config

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Since the TOML parser will error the moment that it sees an invalid value
// without parsing the rest of the file we provide a mechanism that will
// parse the values _after_ they all read. This gives us cleaner validation
// when it comes to value types.
//
// This also allows much richer value definitions for values that require
// bytes. For example: 1, "1k" and "1mib" are all accepted.
type value struct {
	set bool
	raw []byte
}

// Accepts the raw text and stores it for later parsing.
func (v *value) UnmarshalText(raw []byte) error {
	v.set = true
	v.raw = make([]byte, len(raw))
	copy(v.raw, raw)
	return nil
}

// Accept a byte value.
func (v *value) Bytes() (int64, error) {
	// We want to allow commas in values, but we don't want to have
	// to work about parsing around them so for internal evaluation we
	// just strip them out. We also want to find the first location of
	// a NON integer value.
	num := strings.Replace(string(v.raw), ",", "", -1)
	num = strings.TrimSpace(num)
	suffix := ""

	// The value should start with a number, and then end with a
	// suffix. Start by finding the first value after a suffix.
	for i, r := range num {
		if r < '0' || r > '9' {
			suffix = strings.ToLower(strings.TrimSpace(num[i:]))
			num = num[0:i]
			break
		}
	}

	// Parse the number.
	val, err := strconv.ParseInt(num, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid numerical value (%s)", num)
	} else if val < 0 {
		return 0, errors.New("byte values can not be negative.")
	}

	// Multiply it if needed.
	switch suffix {
	case "", "b":
	case "k", "kb":
		val *= 1000
	case "kib":
		val *= 1024
	case "m", "mb":
		val *= 1000000
	case "mib":
		val *= 1024 * 1024
	case "g", "gb":
		val *= 1000000000
	case "gib":
		val *= 1024 * 1024 * 1024
	case "t", "tb":
		val *= 1000000000000
	case "tib":
		val *= 1024 * 1024 * 1024 * 1024
	case "p", "pb":
		val *= 1000000000000000
	case "pib":
		val *= 1024 * 1024 * 1024 * 1024 * 1024
	case "e", "eb":
		val *= 1000000000000000000
	case "eib":
		val *= 1024 * 1024 * 1024 * 1024 * 1024 * 1024
	default:
		return 0, fmt.Errorf("unknown byte suffix (%s)", suffix)
	}
	return val, nil
}

// Accept an integer value.
func (v *value) Int() (int, error) {
	return strconv.Atoi(string(v.raw))
}

// Accept a string value.
func (v *value) String() string {
	return string(v.raw)
}
