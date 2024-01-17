package human

import (
	"strconv"
)

// A simple wrapper to make formatting easier to parse.
func atoi(u uint64) string {
	return strconv.FormatUint(u, 10)
}

// Returns a human readable version of the bytes. This will reduce a large
// value into a smaller one (eg 123001 into 123k).
func Bytes(size uint64) string {
	// The whole number, and the remainder that will be used when
	// generating the number to display.
	var whole uint64
	var dec uint64

	// We also want to track the expected precision of the decimal portion
	// so we set that here. This represents the expected length so that
	// we can zero pad it later if needed.
	var decLen int

	// The label that will be applied to the number.
	label := ""

	// Start by calculating the major size of the uint given to us.
	switch {
	case size < 1000:
		// xB, xxB, xxxB
		whole = size
		label = "B"
	case size < 10000:
		// x.xxkB
		whole = size / 1000
		dec = (size % 1000) / 10
		decLen = 2
		label = "kB"
	case size < 100000:
		// xx.xkB
		whole = size / 1000
		dec = (size % 1000) / 100
		decLen = 1
		label = "kB"
	case size < 1000000:
		// xxxkB
		whole = size / 1000
		label = "kB"
	case size < 10000000:
		// x.xxmB
		whole = size / 1000000
		dec = (size % 1000000) / 10000
		decLen = 2
		label = "MB"
	case size < 100000000:
		// xx.xMB
		whole = size / 1000000
		dec = (size % 1000000) / 100000
		decLen = 1
		label = "MB"
	case size < 1000000000:
		// xxxMB
		whole = size / 1000000
		label = "MB"
	case size < 10000000000:
		// x.xxGB
		whole = size / 1000000000
		dec = (size % 1000000000) / 10000000
		decLen = 2
		label = "GB"
	case size < 100000000000:
		// xx.xGB
		whole = size / 1000000000
		dec = (size % 1000000000) / 100000000
		decLen = 1
		label = "GB"
	case size < 1000000000000:
		// xxxGB
		whole = size / 1000000000
		label = "GB"
	case size < 10000000000000:
		// x.xxTB
		whole = size / 1000000000000
		dec = (size % 1000000000000) / 10000000000
		decLen = 2
		label = "TB"
	case size < 100000000000000:
		// xx.xTB
		whole = size / 1000000000000
		dec = (size % 1000000000000) / 100000000000
		decLen = 1
		label = "TB"
	case size < 1000000000000000:
		// xxxTB
		whole = size / 1000000000000
		label = "TB"
	case size < 10000000000000000:
		// x.xxPB
		whole = size / 1000000000000000
		dec = (size % 1000000000000000) / 10000000000000
		decLen = 2
		label = "PB"
	case size < 100000000000000000:
		// xx.xPB
		whole = size / 1000000000000000
		dec = (size % 1000000000000000) / 100000000000000
		decLen = 1
		label = "PB"
	case size < 1000000000000000000:
		// xxxPB
		whole = size / 1000000000000000
		label = "PB"
	case size < 10000000000000000000:
		// x.xxEB
		whole = size / 1000000000000000000
		dec = (size % 1000000000000000000) / 10000000000000000
		decLen = 2
		label = "EB"
	default:
		// xx.xEB
		whole = size / 1000000000000000000
		dec = (size % 1000000000000000000) / 100000000000000000
		decLen = 1
		label = "EB"
	}

	if dec == 0 {
		return atoi(whole) + label
	} else {
		decStr := atoi(dec)
		decStrLen := len(decStr)
		if decLen == 2 && decStrLen == 1 {
			decStr = "0" + decStr
		} else if decStrLen == 2 && decStr[1] == '0' {
			decStr = decStr[0:1]
		}
		return atoi(whole) + "." + decStr + label
	}
}
