// build: windows

package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/iterable/blobby/internal/logging"
)

func SetupRotation() {
	log.Info("Log rotation does not currently work on windows.")
}
