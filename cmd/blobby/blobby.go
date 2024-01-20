package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"runtime"
	"strconv"

	"github.com/liquidgecka/blobby/config"
	"github.com/liquidgecka/blobby/httpserver"
	"github.com/liquidgecka/blobby/internal/delayqueue"
	"github.com/liquidgecka/blobby/internal/sloghelper"
)

// Common arguments.
var (
	Config = flag.String(
		"c",
		"",
		"Path to the config file.")

	VersionFlag = flag.Bool(
		"V",
		false,
		"Display the build version and then exit.")
)

// Common variables that are held for the life of the binary.
var (
	DelayQueue *delayqueue.DelayQueue
	Server     httpserver.Server
	Rotators   []*sloghelper.Rotator
	log        *slog.Logger
)

// Expected to be set via -ldflags/-X by the linker
var BuildVersion string
var BuildTimeEpoch string

func Version() string {
	if BuildVersion == "" {
		BuildVersion = "Unknown"
		BuildTimeEpoch = "unknown"
	}
	return fmt.Sprintf(
		"blobby: %s ts=%s go=%s\n",
		BuildVersion,
		BuildTimeEpoch,
		runtime.Version())
}

func WritePIDFile(ctx context.Context, file string) {
	log.LogAttrs(
		ctx,
		slog.LevelDebug,
		"Writing pid file.",
		sloghelper.String("file", file))
	fd, err := os.OpenFile(
		file,
		os.O_CREATE|os.O_TRUNC|os.O_WRONLY,
		0644)
	if err != nil {
		log.LogAttrs(
			ctx,
			slog.LevelError,
			"Error opening PID file.",
			sloghelper.String("file", file),
			sloghelper.Error("error", err))
		os.Exit(1)
	}
	defer fd.Close()
	if _, err = fd.WriteString(strconv.Itoa(os.Getpid()) + "\n"); err != nil {
		log.LogAttrs(
			ctx,
			slog.LevelError,
			"Error writing pid file.",
			sloghelper.String("file", file),
			sloghelper.Error("error", err))
		os.Exit(1)
	}
}

// All of the initialization work happens in this function in order to allow
// a limited scope of variables so that startup temporary data can be purged
// once the server is fully running.
func configure(ctx context.Context) {
	// Parse the config file.
	cnf, err := config.Parse(*Config)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	} else if err := cnf.InitializeLogging(ctx); err != nil {
		fmt.Fprintf(os.Stderr, "%s\n", err.Error())
		os.Exit(1)
	} else {
		DelayQueue = cnf.GetDelayQueue()
		Rotators = cnf.GetRotators(ctx)
		Server = cnf.GetServer(ctx)
		log = cnf.GetLogger(ctx)
	}

	// Log some build information.
	log.LogAttrs(
		ctx,
		slog.LevelError,
		"Server initializing.",
		sloghelper.String("build-version", BuildVersion),
		sloghelper.String("build-time", BuildTimeEpoch))

	// Start the log rotator.
	SetupRotation(ctx)

	// Start the delay queue.
	DelayQueue.Start()

	// Start and log each configured namespace.
	for name, ns := range cnf.GetNameSpaces(ctx) {
		if err := ns.Start(ctx); err != nil {
			log.LogAttrs(
				ctx,
				slog.LevelError,
				"Unable to start namespace.",
				sloghelper.String("namespace", name),
				sloghelper.Error("error", err))
			os.Exit(4)
		}
	}

	log.LogAttrs(
		ctx,
		slog.LevelInfo,
		"Loading secrets (if configured).")
	if err := cnf.PreLoadSecrets(ctx); err != nil {
		log.LogAttrs(
			ctx,
			slog.LevelError,
			"Error loading secrets.",
			sloghelper.Error("error", err))
		os.Exit(3)
	}

	// Start the HTTP listener. This is used to ensure that only one
	// Blobby instance is serving on the machine which helps us ensure
	// that we don't have two instances attempting to work on the files
	// on the file system at the same time.
	if err := Server.Listen(); err != nil {
		log.LogAttrs(
			ctx,
			slog.LevelError,
			"Unable to listen to the network address.",
			sloghelper.String("server", Server.Addr()),
			sloghelper.Error("error", err))
		os.Exit(2)
	}

	// Log a line so its clear that the server is serving traffic beyond
	// this point.
	log.LogAttrs(
		ctx,
		slog.LevelInfo,
		"HTTP Server is serving.",
		sloghelper.String("addr", Server.Addr()))

	// Write the pid file (if configured).
	if pidfile := cnf.GetPIDFile(); pidfile != "" {
		WritePIDFile(ctx, pidfile)
	}

	// Start the secret refreshers that will automatically update the
	// data as configured. For now we do nothing with the stop channel.
	cnf.StartSecretRefreshers(ctx)

	// FIXME: Health checkers!
}

func main() {
	// Add config arguments
	config.SetupFlags()

	// Check that a config file was provided.
	flag.Parse()
	if *VersionFlag {
		fmt.Print(Version())
		os.Exit(0)
	}
	if *Config == "" {
		fmt.Fprintf(os.Stderr, Version())
		flag.PrintDefaults()
		fmt.Fprintf(os.Stderr, "-c is a required parameter.\n")
		os.Exit(1)
	}

	// Run through the configuration work.
	configure(context.Background())

	// Start the HTTP server and run it. This is the primary work processor
	// so its run in the main thread.
	log.LogAttrs(
		context.Background(),
		slog.LevelError,
		"HTTP Server exited!",
		sloghelper.Error("error", Server.Run()))
	os.Exit(1)
}
