package config

import (
	"context"
	"log/slog"

	"github.com/liquidgecka/blobby/internal/sloghelper"
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

	// A reference to the slog.Logger that was created for this
	// log operation.
	logger *slog.Logger

	// A reference to the Rotator that is used to manage the output for
	// this log configuration.
	rotator *sloghelper.Rotator

	// Controls the log level that is currently being
	// logged.
	leveler *sloghelper.Leveler
}

func (l *log) initLogging(ctx context.Context) error {
	if l.File != nil {
		var err error
		l.rotator, err = sloghelper.NewRotator(ctx, *l.File)
		if err != nil {
			return err
		}
	}
	l.leveler = &sloghelper.Leveler{}
	if l.Debug != nil && *l.Debug {
		l.leveler.SetLevel(slog.LevelDebug)
	}
	switch *l.Format {
	case "plain":
		l.logger = slog.New(slog.NewTextHandler(
			l.rotator,
			&slog.HandlerOptions{
				Level: l.leveler,
			}))
	case "json":
		l.logger = slog.New(slog.NewJSONHandler(
			l.rotator,
			&slog.HandlerOptions{
				Level: l.leveler,
			}))
	}
	//	if console != nil && *console {
	//		output.TeeOutput(logging.NewANSIOutput(bufio.NewWriter(os.Stdout)))
	//	}
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
