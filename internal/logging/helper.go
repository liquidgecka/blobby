package logging

import (
	"bytes"
	"unicode"
)

var (
	// A quick hex converter for values.
	hexChars = []byte{
		'0', '1', '2', '3', '4', '5', '6', '7',
		'8', '9', 'A', 'B', 'C', 'D', 'E', 'F',
	}
)

// Encodes a JSON string into a byte buffer.
func encodeJSONString(s string) string {
	buffer := bytes.Buffer{}
	buffer.Grow(len(s) * 2)
	for _, r := range s {
		switch r {
		case '\\':
			buffer.WriteByte('\\')
			buffer.WriteByte('\\')
		case '"':
			buffer.WriteByte('\\')
			buffer.WriteByte('"')
		case '\t':
			buffer.WriteByte('\\')
			buffer.WriteByte('t')
		case '\n':
			buffer.WriteByte('\\')
			buffer.WriteByte('n')
		case '\r':
			buffer.WriteByte('\\')
			buffer.WriteByte('r')
		case 0x2028:
			buffer.WriteByte('\\')
			buffer.WriteByte('u')
			buffer.WriteByte('2')
			buffer.WriteByte('0')
			buffer.WriteByte('2')
			buffer.WriteByte('8')
		case 0x2029:
			buffer.WriteByte('\\')
			buffer.WriteByte('u')
			buffer.WriteByte('2')
			buffer.WriteByte('0')
			buffer.WriteByte('2')
			buffer.WriteByte('9')
		default:
			if !unicode.IsPrint(r) {
				buffer.WriteByte('\\')
				buffer.WriteByte('u')
				buffer.WriteByte(hexChars[int(r>>12&0x0F)])
				buffer.WriteByte(hexChars[int(r>>8&0x0F)])
				buffer.WriteByte(hexChars[int(r>>4&0x0F)])
				buffer.WriteByte(hexChars[int(r&0x0F)])
			} else {
				buffer.WriteRune(r)
			}
		}
	}
	return buffer.String()
}

// Returns true if the string value needs to be escaped.
func shouldEscape(str string) bool {
	if len(str) == 0 {
		return true
	}
	for _, r := range str {
		switch {
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case r >= '0' && r <= '9':
		case r == '_':
		case r == '-':
		default:
			return true
		}
	}
	return false
}
