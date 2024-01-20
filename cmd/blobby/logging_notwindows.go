// build: !windows

package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/liquidgecka/blobby/internal/sloghelper"
)

func SetupRotation(ctx context.Context) {
	schan := make(chan os.Signal, 1)
	signal.Notify(schan, syscall.SIGHUP)
	log.LogAttrs(
		ctx,
		slog.LevelDebug,
		"Starting signal handler for SIGHUP.")
	go func(c chan os.Signal) {
		for range schan {
			for _, r := range Rotators {
				if err := r.Rotate(ctx); err != nil {
					log.LogAttrs(
						ctx,
						slog.LevelWarn,
						"Log rotation failed.",
						sloghelper.Error("error", err))
				}
			}
			log.LogAttrs(
				ctx,
				slog.LevelDebug,
				"logs rotated.")
		}
	}(schan)
}
