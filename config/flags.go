package config

import (
	"flag"
)

var (
	console *bool
	debug   *bool
)

func SetupFlags() {
	console = flag.Bool(
		"console",
		false,
		"Also log to the system console.")

	debug = flag.Bool(
		"debug",
		false,
		"Enable debug logging.")
}
