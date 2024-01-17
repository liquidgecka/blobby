package cookie

import (
	"strconv"
	"time"
)

// Acts like a time.Time object, but encodes to and from a unix epoch
// to save space. Because this converts to float and back it will lose
// precision in the sub-second values, typically at most in the micro second
// range.
type Time struct {
	time.Time
}

func (t *Time) MarshalText() ([]byte, error) {
	f := float64(t.Time.UnixNano()) / float64(time.Second)
	return []byte(strconv.FormatFloat(f, 'f', -1, 64)), nil
}

func (t *Time) UnmarshalText(raw []byte) error {
	// Parse the raw values as a floating point number.
	f, err := strconv.ParseFloat(string(raw), 64)
	if err != nil {
		return err
	}
	sec := int64(f)
	nsec := int64(f*float64(time.Second)) % int64(time.Second)
	t.Time = time.Unix(sec, nsec)
	return nil
}
