package logging

import (
	"time"
)

// A logging structure that can be used for sending logs to an output.
type Logger struct {
	output    *Output
	fields    *Field
	enableDbg bool
}

// Creates a new top level logger.
func NewLogger(o *Output) *Logger {
	return &Logger{
		output: o,
	}
}

// Adds a string field to the logger. To simplify code this will return a
// reference to the logger it was called against so that calls can be
// chained together.
func (l *Logger) AddField(key string, value interface{}) *Logger {
	newField := NewFieldIface(key, value)
	if l.fields == nil {
		newField.level = 1
	} else {
		newField.level = l.fields.level + 1
	}
	newField.prev = l.fields
	l.fields = &newField
	return l
}

// Disables debug logging on this Logging output.
func (l *Logger) DisableDebug() {
	l.enableDbg = false
}

// Returns true if this logger has debug logging enabled.
func (l *Logger) DebugEnabled() bool {
	return l.enableDbg
}

// Enables debug logging on this Logging output, as well as all loggers
// created from it moving forward. Its best to enable this early or else
// Loggers can be created from this one that will not be enabled once this
// is set.
func (l *Logger) EnableDebug() {
	l.enableDbg = true
}

// Returns a copy of this logger that can be used for new logging paths.
func (l *Logger) NewChild() *Logger {
	return &Logger{
		output:    l.output,
		fields:    l.fields,
		enableDbg: l.enableDbg,
	}
}

// Set the output of this Logger to the given Output object.
func (l *Logger) SetOutput(o *Output) {
	l.output = o
}

//
// Logging functions
//

// Log a Debug level message with additional fields.
func (l *Logger) Debug(msg string, fields ...Field) {
	if l.enableDbg {
		data := renderData{
			time:      time.Now(),
			level:     dbg,
			message:   msg,
			tail:      l.fields,
			optFields: fields,
		}
		l.output.Write(&data)
	}
}

// Log a Info level message with additional fields.
func (l *Logger) Info(msg string, fields ...Field) {
	data := renderData{
		time:      time.Now(),
		level:     inf,
		message:   msg,
		tail:      l.fields,
		optFields: fields,
	}
	l.output.Write(&data)
}

// Log a Warning level message with additional fields.
func (l *Logger) Warning(msg string, fields ...Field) {
	data := renderData{
		time:      time.Now(),
		level:     wrn,
		message:   msg,
		tail:      l.fields,
		optFields: fields,
	}
	l.output.Write(&data)
}

// Log a Error level message with additional fields.
func (l *Logger) Error(msg string, fields ...Field) {
	data := renderData{
		time:      time.Now(),
		level:     err,
		message:   msg,
		tail:      l.fields,
		optFields: fields,
	}
	l.output.Write(&data)
}
