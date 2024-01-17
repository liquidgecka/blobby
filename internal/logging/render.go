package logging

import (
	"bufio"
	"time"
)

// When an output attempts to render data it will use one of these types
// to actually convert it to bytes.
const (
	renderPlain = int(iota)
	renderJSON
	renderANSI
	renderTest
)

// When rendering a log line this will be passed around to eliminate as much
// data copying as possible.
type renderData struct {
	time      time.Time
	level     level
	message   string
	tail      *Field
	optFields []Field
}

// Plain render

func rPlain(buffer *bufio.Writer, data *renderData) {
	ts := data.time.Format(time.RFC3339)
	buffer.WriteString(ts)
	buffer.WriteByte(':')
	buffer.WriteByte(' ')
	switch data.level {
	case dbg:
		buffer.WriteString("DBG")
	case inf:
		buffer.WriteString("INF")
	case wrn:
		buffer.WriteString("WRN")
	case err:
		buffer.WriteString("ERR")
	default:
		buffer.WriteString("UNK")
	}
	buffer.WriteByte(' ')
	buffer.WriteString(data.message)
	for f := data.tail; f != nil; f = f.prev {
		buffer.WriteByte(' ')
		if f.nameQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.name)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.name)
		}
		buffer.WriteByte('=')
		if f.valueQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.value)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.value)
		}
	}
	for _, f := range data.optFields {
		buffer.WriteByte(' ')
		if f.nameQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.name)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.name)
		}
		buffer.WriteByte('=')
		if f.valueQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.value)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.value)
		}
	}
}

//
// JSON render
//

func rJSON(buffer *bufio.Writer, data *renderData) {
	ts := data.time.Format(time.RFC3339)
	buffer.WriteByte('{')
	buffer.WriteString(`"timestamp":"`)
	buffer.WriteString(ts)
	buffer.WriteString(`","message":"`)
	buffer.WriteString(encodeJSONString(data.message))
	buffer.WriteByte('"')
	switch data.level {
	case dbg:
		buffer.WriteString(`,"level"="debug"`)
	case inf:
		buffer.WriteString(`,"level"="info"`)
	case wrn:
		buffer.WriteString(`,"level"="warning"`)
	case err:
		buffer.WriteString(`,"level"="error"`)
	default:
		buffer.WriteString(`,"level"="unknown"`)
	}
	for f := data.tail; f != nil; f = f.prev {
		buffer.WriteByte(',')
		buffer.WriteByte('"')
		buffer.WriteString(f.name)
		buffer.WriteString(`":`)
		if !f.valueRaw {
			buffer.WriteByte('"')
			buffer.WriteString(f.value)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.value)
		}
	}
	for _, f := range data.optFields {
		buffer.WriteByte(',')
		buffer.WriteByte('"')
		buffer.WriteString(f.name)
		buffer.WriteString(`":`)
		if !f.valueRaw {
			buffer.WriteByte('"')
			buffer.WriteString(f.value)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.value)
		}
	}
	buffer.WriteByte('}')
}

//
// ANSI render
//

// ANSI colors.
var (
	colorBold     = []byte("\x1b[1m")
	colorBlue     = []byte("\x1b[34m")
	colorCyan     = []byte("\x1b[36m")
	colorDarkGrey = []byte("\x1b[90m")
	colorGreen    = []byte("\x1b[32m")
	colorMagenta  = []byte("\x1b[35m")
	colorRed      = []byte("\x1b[31m")
	colorWhite    = []byte("\x1b[37m")
	colorYellow   = []byte("\x1b[33m")
	colorReset    = []byte("\x1b[0m")
)

// Generates an ANSI stylized log line.
func rANSI(buffer *bufio.Writer, data *renderData) {
	ts := data.time.Format(time.RFC3339)
	buffer.Write(colorDarkGrey)
	buffer.WriteString(ts)
	buffer.WriteByte(' ')
	switch data.level {
	case dbg:
		buffer.Write(colorYellow)
		buffer.WriteString("DBG")
	case inf:
		buffer.Write(colorGreen)
		buffer.WriteString("INF")
	case wrn:
		buffer.Write(colorRed)
		buffer.WriteString("WRN")
	case err:
		buffer.Write(colorBold)
		buffer.Write(colorRed)
		buffer.WriteString("ERR")
	default:
		buffer.Write(colorBlue)
		buffer.WriteString("UNK")
	}
	buffer.WriteByte(' ')
	buffer.Write(colorReset)
	buffer.WriteString(data.message)
	for f := data.tail; f != nil; f = f.prev {
		buffer.WriteByte(' ')
		if f.red {
			buffer.Write(colorRed)
		} else {
			buffer.Write(colorDarkGrey)
		}
		if f.nameQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.name)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.name)
		}
		buffer.WriteByte('=')
		if f.valueQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.value)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.value)
		}
	}
	for _, f := range data.optFields {
		buffer.WriteByte(' ')
		if f.red {
			buffer.Write(colorRed)
		} else {
			buffer.Write(colorDarkGrey)
		}
		if f.nameQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.name)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.name)
		}
		buffer.WriteByte('=')
		if f.valueQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.value)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.value)
		}
	}
	buffer.Write(colorReset)
}

//
// Test
//

// Like rPlain except this excludes the timestamp to make testing easier.
func rTest(buffer *bufio.Writer, data *renderData) {
	switch data.level {
	case dbg:
		buffer.WriteString("DBG")
	case inf:
		buffer.WriteString("INF")
	case wrn:
		buffer.WriteString("WRN")
	case err:
		buffer.WriteString("ERR")
	default:
		buffer.WriteString("UNK")
	}
	buffer.WriteByte(' ')
	buffer.WriteString(data.message)
	for f := data.tail; f != nil; f = f.prev {
		buffer.WriteByte(' ')
		if f.nameQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.name)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.name)
		}
		buffer.WriteByte('=')
		if f.valueQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.value)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.value)
		}
	}
	for _, f := range data.optFields {
		buffer.WriteByte(' ')
		if f.nameQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.name)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.name)
		}
		buffer.WriteByte('=')
		if f.valueQuoted {
			buffer.WriteByte('"')
			buffer.WriteString(f.value)
			buffer.WriteByte('"')
		} else {
			buffer.WriteString(f.value)
		}
	}
}
