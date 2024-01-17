package logging

import (
	"bufio"
	"bytes"
	"testing"
	"time"

	"github.com/liquidgecka/testlib"
)

func TestRANSI(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer and some data that we want to log.
	buffer := bytes.NewBuffer(nil)
	writer := bufio.NewWriter(buffer)
	rd := renderData{
		time:    time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC),
		level:   dbg,
		message: "log message",
		tail: &Field{
			name:  "fname",
			value: "fvalue",
			red:   true,
			prev: &Field{
				name:        "fname_quoted",
				nameQuoted:  true,
				value:       "fvalue_quoted",
				valueQuoted: true,
			},
		},
		optFields: []Field{
			Field{
				name:  "oname",
				value: "ovalue",
				red:   true,
			},
			Field{
				name:        "oname_quoted",
				nameQuoted:  true,
				value:       "ovalue_quoted",
				valueQuoted: true,
			},
		},
	}

	// Attempt a plain write and verify the line was written.
	rANSI(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		string(colorDarkGrey)+"2000-01-02T03:04:05Z "+
		string(colorYellow)+"DBG "+
		string(colorReset)+"log message "+
		string(colorRed)+"fname=fvalue "+
		string(colorDarkGrey)+`"fname_quoted"="fvalue_quoted" `+
		string(colorRed)+"oname=ovalue "+
		string(colorDarkGrey)+`"oname_quoted"="ovalue_quoted"`+
		string(colorReset),
	)

	// Do the same for Info/Warning/Error level log types.
	rd.level = inf
	buffer.Truncate(0)
	rANSI(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		string(colorDarkGrey)+"2000-01-02T03:04:05Z "+
		string(colorGreen)+"INF "+
		string(colorReset)+"log message "+
		string(colorRed)+"fname=fvalue "+
		string(colorDarkGrey)+`"fname_quoted"="fvalue_quoted" `+
		string(colorRed)+"oname=ovalue "+
		string(colorDarkGrey)+`"oname_quoted"="ovalue_quoted"`+
		string(colorReset),
	)
	rd.level = wrn
	buffer.Truncate(0)
	rANSI(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		string(colorDarkGrey)+"2000-01-02T03:04:05Z "+
		string(colorRed)+"WRN "+
		string(colorReset)+"log message "+
		string(colorRed)+"fname=fvalue "+
		string(colorDarkGrey)+`"fname_quoted"="fvalue_quoted" `+
		string(colorRed)+"oname=ovalue "+
		string(colorDarkGrey)+`"oname_quoted"="ovalue_quoted"`+
		string(colorReset),
	)
	rd.level = err
	buffer.Truncate(0)
	rANSI(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		string(colorDarkGrey)+"2000-01-02T03:04:05Z "+
		string(colorBold)+string(colorRed)+"ERR "+
		string(colorReset)+"log message "+
		string(colorRed)+"fname=fvalue "+
		string(colorDarkGrey)+`"fname_quoted"="fvalue_quoted" `+
		string(colorRed)+"oname=ovalue "+
		string(colorDarkGrey)+`"oname_quoted"="ovalue_quoted"`+
		string(colorReset),
	)
	rd.level = err + 1
	buffer.Truncate(0)
	rANSI(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		string(colorDarkGrey)+"2000-01-02T03:04:05Z "+
		string(colorBlue)+"UNK "+
		string(colorReset)+"log message "+
		string(colorRed)+"fname=fvalue "+
		string(colorDarkGrey)+`"fname_quoted"="fvalue_quoted" `+
		string(colorRed)+"oname=ovalue "+
		string(colorDarkGrey)+`"oname_quoted"="ovalue_quoted"`+
		string(colorReset),
	)
}

func TestRJSON(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer and some data that we want to log.
	buffer := bytes.NewBuffer(nil)
	writer := bufio.NewWriter(buffer)
	rd := renderData{
		time:    time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC),
		level:   dbg,
		message: "log message",
		tail: &Field{
			name:  "fname",
			value: "fvalue",
			prev: &Field{
				name:     "fname_raw",
				value:    "5",
				valueRaw: true,
			},
		},
		optFields: []Field{
			Field{
				name:  "oname",
				value: "ovalue",
			},
			Field{
				name:     "oname_raw",
				value:    "6",
				valueRaw: true,
			},
		},
	}

	// Attempt a plain write and verify the line was written.
	rJSON(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`{"timestamp":"2000-01-02T03:04:05Z","message":"log message",`+
		`"level"="debug","fname":"fvalue","fname_raw":5,`+
		`"oname":"ovalue","oname_raw":6}`,
	)

	// Do the same for Info/Warning/Error level log types.
	rd.level = inf
	buffer.Truncate(0)
	rJSON(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`{"timestamp":"2000-01-02T03:04:05Z","message":"log message",`+
		`"level"="info","fname":"fvalue","fname_raw":5,`+
		`"oname":"ovalue","oname_raw":6}`,
	)
	rd.level = wrn
	buffer.Truncate(0)
	rJSON(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`{"timestamp":"2000-01-02T03:04:05Z","message":"log message",`+
		`"level"="warning","fname":"fvalue","fname_raw":5,`+
		`"oname":"ovalue","oname_raw":6}`,
	)
	rd.level = err
	buffer.Truncate(0)
	rJSON(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`{"timestamp":"2000-01-02T03:04:05Z","message":"log message",`+
		`"level"="error","fname":"fvalue","fname_raw":5,`+
		`"oname":"ovalue","oname_raw":6}`,
	)
	rd.level = err + 1
	buffer.Truncate(0)
	rJSON(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`{"timestamp":"2000-01-02T03:04:05Z","message":"log message",`+
		`"level"="unknown","fname":"fvalue","fname_raw":5,`+
		`"oname":"ovalue","oname_raw":6}`,
	)
}

func TestRPlain(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer and some data that we want to log.
	buffer := bytes.NewBuffer(nil)
	writer := bufio.NewWriter(buffer)
	rd := renderData{
		time:    time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC),
		level:   dbg,
		message: "log message",
		tail: &Field{
			name:  "fname",
			value: "fvalue",
			prev: &Field{
				name:        "fname_quoted",
				nameQuoted:  true,
				value:       "fvalue_quoted",
				valueQuoted: true,
			},
		},
		optFields: []Field{
			Field{
				name:  "oname",
				value: "ovalue",
			},
			Field{
				name:        "oname_quoted",
				nameQuoted:  true,
				value:       "ovalue_quoted",
				valueQuoted: true,
			},
		},
	}

	// Attempt a plain write and verify the line was written.
	rPlain(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`2000-01-02T03:04:05Z: DBG log message `+
		`fname=fvalue "fname_quoted"="fvalue_quoted" `+
		`oname=ovalue "oname_quoted"="ovalue_quoted"`,
	)

	// Do the same for Info/Warning/Error level log types.
	rd.level = inf
	buffer.Truncate(0)
	rPlain(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`2000-01-02T03:04:05Z: INF log message `+
		`fname=fvalue "fname_quoted"="fvalue_quoted" `+
		`oname=ovalue "oname_quoted"="ovalue_quoted"`,
	)
	rd.level = wrn
	buffer.Truncate(0)
	rPlain(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`2000-01-02T03:04:05Z: WRN log message `+
		`fname=fvalue "fname_quoted"="fvalue_quoted" `+
		`oname=ovalue "oname_quoted"="ovalue_quoted"`,
	)
	rd.level = err
	buffer.Truncate(0)
	rPlain(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`2000-01-02T03:04:05Z: ERR log message `+
		`fname=fvalue "fname_quoted"="fvalue_quoted" `+
		`oname=ovalue "oname_quoted"="ovalue_quoted"`,
	)
	rd.level = err + 1
	buffer.Truncate(0)
	rPlain(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`2000-01-02T03:04:05Z: UNK log message `+
		`fname=fvalue "fname_quoted"="fvalue_quoted" `+
		`oname=ovalue "oname_quoted"="ovalue_quoted"`,
	)
}

func TestRTest(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a buffer and some data that we want to log.
	buffer := bytes.NewBuffer(nil)
	writer := bufio.NewWriter(buffer)
	rd := renderData{
		time:    time.Date(2000, 1, 2, 3, 4, 5, 6, time.UTC),
		level:   dbg,
		message: "log message",
		tail: &Field{
			name:  "fname",
			value: "fvalue",
			prev: &Field{
				name:        "fname_quoted",
				nameQuoted:  true,
				value:       "fvalue_quoted",
				valueQuoted: true,
			},
		},
		optFields: []Field{
			Field{
				name:  "oname",
				value: "ovalue",
			},
			Field{
				name:        "oname_quoted",
				nameQuoted:  true,
				value:       "ovalue_quoted",
				valueQuoted: true,
			},
		},
	}

	// Attempt a plain write and verify the line was written.
	rTest(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`DBG log message `+
		`fname=fvalue "fname_quoted"="fvalue_quoted" `+
		`oname=ovalue "oname_quoted"="ovalue_quoted"`,
	)

	// Do the same for Info/Warning/Error level log types.
	rd.level = inf
	buffer.Truncate(0)
	rTest(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`INF log message `+
		`fname=fvalue "fname_quoted"="fvalue_quoted" `+
		`oname=ovalue "oname_quoted"="ovalue_quoted"`,
	)
	rd.level = wrn
	buffer.Truncate(0)
	rTest(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`WRN log message `+
		`fname=fvalue "fname_quoted"="fvalue_quoted" `+
		`oname=ovalue "oname_quoted"="ovalue_quoted"`,
	)
	rd.level = err
	buffer.Truncate(0)
	rTest(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`ERR log message `+
		`fname=fvalue "fname_quoted"="fvalue_quoted" `+
		`oname=ovalue "oname_quoted"="ovalue_quoted"`,
	)
	rd.level = err + 1
	buffer.Truncate(0)
	rTest(writer, &rd)
	writer.Flush()
	T.Equal(buffer.String(), ``+
		`UNK log message `+
		`fname=fvalue "fname_quoted"="fvalue_quoted" `+
		`oname=ovalue "oname_quoted"="ovalue_quoted"`,
	)
}
