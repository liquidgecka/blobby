package config

import (
	"bufio"
	"fmt"
	"os"

	"github.com/liquidgecka/blobby/internal/logging"
)

var (
	defaultLogFormat = "plain"
	defaultLogDebug  = false
)

type log struct {
	// A log file to log too.
	File *string `toml:"file"`

	// Which format to use when logging to the file, valid option are
	// "plain" and "json". Default is plain.
	Format *string `toml:"format"`

	// Enable debug logging for this channel.
	Debug *bool `toml:"debug"`

	// A reference to the "top" object.
	top *top

	// The name passed in to validate() initially.
	name string

	// A reference to the logging.Logger that was created for this
	// log operation.
	logger *logging.Logger

	// A reference to the Rotator that is used to manage the output for
	// this log configuration.
	rotator *logging.Rotator
}

func (l *log) initLogging() error {
	var err error
	var output *logging.Output
	switch *l.Format {
	case "plain":
		l.rotator, output, err = logging.NewPlainRotator(*l.File)
	case "json":
		l.rotator, output, err = logging.NewJSONRotator(*l.File)
	}
	if err != nil {
		return fmt.Errorf(
			"%s had an error initializing: %s",
			l.name,
			err.Error())
	}
	if console != nil && *console {
		output.TeeOutput(logging.NewANSIOutput(bufio.NewWriter(os.Stdout)))
	}
	if &l.top.Log == l {
		l.logger = logging.NewLogger(output)
	} else {
		l.logger = l.top.Log.logger.NewChild()
		l.logger.SetOutput(output)
	}
	if (debug != nil && *debug) || *l.Debug {
		l.logger.EnableDebug()
	}
	return nil
}

func (l *log) validate(t *top, name string) []string {
	var errors []string

	// Store some information for referencing later.
	l.top = t
	l.name = name

	// File
	if l.File == nil {
		errors = append(errors, name+".file is a required field.")
	}
	// FIXME: Check that the file is openable?

	// Format
	if l.Format == nil {
		l.Format = &defaultLogFormat
	} else if *l.Format != "plain" && *l.Format != "json" {
		errors = append(errors, name+".format must be 'plain' or 'json'.")
	}

	// Debug
	if l.Debug == nil {
		l.Debug = &defaultLogDebug
	}

	// Return any errors encountered.
	return errors
}
