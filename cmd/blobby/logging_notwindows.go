// build: !windows

package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/iterable/blobby/internal/logging"
)

func SetupRotation() {
	schan := make(chan os.Signal, 1)
	signal.Notify(schan, syscall.SIGHUP)
	log.Debug("Starting signal handler for SIGHUP.")
	go func(c chan os.Signal) {
		for range schan {
			for _, r := range Rotators {
				if err := r.Rotate(); err != nil {
					log.Warning(
						"Log rotation failed.",
						logging.NewFieldIface("error", err))
				}
			}
			log.Debug("logs rotated.")
		}
	}(schan)
}
