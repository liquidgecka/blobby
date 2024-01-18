// build: windows

package main

import (
	"os"
	"os/signal"
	"syscall"
)

func SetupRotation() {
	log.Info("Log rotation does not currently work on windows.")
}
