package config

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"

	toml "github.com/pelletier/go-toml"

	"github.com/liquidgecka/blobby/httpserver"
	"github.com/liquidgecka/blobby/internal/delayqueue"
	"github.com/liquidgecka/blobby/internal/sloghelper"
	"github.com/liquidgecka/blobby/storage"
)

type Config struct {
	top *top

	// A series of "Once" objects that ensure that the various stages
	// of initialization are all run.
	initializeOnce sync.Once
}

// Parses a file and validates its contents, returning the objects that can
// be used for configuration later.
func Parse(filename string) (*Config, error) {
	// Open the file.
	fd, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer fd.Close()

	// Read the contents of the file into a toml parser.
	top := &top{}
	decoder := toml.NewDecoder(fd).Strict(true)
	if err := decoder.Decode(top); err != nil {
		return nil, err
	}

	// Success.. The toml was read, now we need to validate that it is
	// correct and that all of the values are valid.
	errors := top.validate()
	if errors != nil {
		return nil, fmt.Errorf("%s\n", strings.Join(errors, "\n"))
	}

	// Success!
	return &Config{top: top}, nil
}

// Initializes the logging system.
func (c *Config) InitializeLogging(ctx context.Context) (err error) {
	c.initializeOnce.Do(func() {
		err = c.initializeLogging(ctx)
	})
	return
}

// Returns the DelayQueue that will be used for state transitions.
func (c *Config) GetDelayQueue() *delayqueue.DelayQueue {
	return c.top.getDelayQueue()
}

// Returns the top level logger that was generated during initialization.
func (c *Config) GetLogger(ctx context.Context) *slog.Logger {
	c.initializeOnce.Do(func() {
		if err := c.initializeLogging(ctx); err != nil {
			panic(err)
		}
	})
	return c.top.Log.logger
}

// Returns the pid file (if configured). If not configured this returns
// and empty string.
func (c *Config) GetPIDFile() string {
	if c.top.PIDFile == nil {
		return ""
	} else {
		return *c.top.PIDFile
	}
}

// Returns all log rotators created as part of the configuration.
func (c *Config) GetRotators(ctx context.Context) []*sloghelper.Rotator {
	c.initializeOnce.Do(func() {
		if err := c.initializeLogging(ctx); err != nil {
			panic(err)
		}
	})
	r := make([]*sloghelper.Rotator, 0, 2)
	if c.top.Server.AccessLog != nil {
		r = append(r, c.top.Server.AccessLog.rotator)
	}
	r = append(r, c.top.Log.rotator)
	return r
}

// Returns a map of all namespaces mapped into the Storage structure
// that will be serving them.
func (c *Config) GetNameSpaces(ctx context.Context) map[string]*storage.Storage {
	c.initializeOnce.Do(func() {
		if err := c.initializeLogging(ctx); err != nil {
			panic(err)
		}
	})
	return c.top.getNameSpaces()
}

// Returns the httpserver.Server for this config.
func (c *Config) GetServer(ctx context.Context) httpserver.Server {
	c.initializeOnce.Do(func() {
		if err := c.initializeLogging(ctx); err != nil {
			panic(err)
		}
	})
	return c.top.Server.Server()
}

// Pre-loads all of the secrets in the configuration. If any secrets were
// configured (certificates, passwords, aes keys, htpasswd files, etc) then
// this will perform the initial load of those resources.
func (c *Config) PreLoadSecrets(ctx context.Context) error {
	if err := c.top.Server.DebugPathsACL.preLoad(ctx); err != nil {
		return err
	}
	if err := c.top.Server.HealthCheckACL.preLoad(ctx); err != nil {
		return err
	}
	if err := c.top.Server.aesKeysLoader.PreLoad(ctx); err != nil {
		return err
	}
	if err := c.top.Server.tlsCerts.PreLoad(ctx); err != nil {
		return err
	}
	if err := c.top.Server.webUsersHTPasswd.PreLoad(ctx); err != nil {
		return err
	}
	for _, ns := range c.top.NameSpace {
		if err := ns.BlastPathACL.preLoad(ctx); err != nil {
			return err
		}
		if err := ns.InsertACL.preLoad(ctx); err != nil {
			return err
		}
		if err := ns.PrimaryACL.preLoad(ctx); err != nil {
			return err
		}
		if err := ns.ReadACL.preLoad(ctx); err != nil {
			return err
		}
	}
	for _, saml := range c.top.SAML {
		if err := saml.certs.PreLoad(ctx); err != nil {
			return err
		}
	}
	return nil
}

// If configured to do so this will setup a logger and start the secret
// refresher goroutine. This routine will run until the given context
// is canceled.
func (c *Config) StartSecretRefreshers(ctx context.Context) {
	c.initializeOnce.Do(func() {
		if err := c.initializeLogging(ctx); err != nil {
			panic(err)
		}
	})
	if c.top.Server.tlsCerts != nil {
		c.top.Server.tlsCerts.StartRefresher(ctx)
	}
	if c.top.Server.aesKeysLoader != nil {
		c.top.Server.aesKeysLoader.StartRefresher(ctx)
	}
	c.top.Server.DebugPathsACL.startRefresher(ctx)
	c.top.Server.HealthCheckACL.startRefresher(ctx)
	if c.top.Server.webUsersHTPasswd != nil {
		c.top.Server.webUsersHTPasswd.StartRefresher(ctx)
	}
	for _, ns := range c.top.NameSpace {
		ns.BlastPathACL.startRefresher(ctx)
		ns.InsertACL.startRefresher(ctx)
		ns.PrimaryACL.startRefresher(ctx)
		ns.ReadACL.startRefresher(ctx)
	}
	for _, saml := range c.top.SAML {
		saml.certs.StartRefresher(ctx)
	}
}

// initializes logging exactly one time.
func (c *Config) initializeLogging(ctx context.Context) error {
	if err := c.top.Log.initLogging(ctx); err != nil {
		return err
	}
	c.top.Server.initLogging(ctx)
	for _, saml := range c.top.SAML {
		saml.initLogging()
	}
	return nil
}
