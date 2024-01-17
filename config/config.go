package config

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/iterable/blobby/httpserver"
	"github.com/iterable/blobby/internal/delayqueue"
	"github.com/iterable/blobby/internal/logging"
	"github.com/iterable/blobby/storage"

	toml "github.com/pelletier/go-toml"
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
func (c *Config) InitializeLogging() (err error) {
	c.initializeOnce.Do(func() {
		err = c.initializeLogging()
	})
	return
}

// Returns the DelayQueue that will be used for state transitions.
func (c *Config) GetDelayQueue() *delayqueue.DelayQueue {
	return c.top.getDelayQueue()
}

// Returns the top level logger that was generated during initialization.
func (c *Config) GetLogger() *logging.Logger {
	c.initializeOnce.Do(func() {
		if err := c.initializeLogging(); err != nil {
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
func (c *Config) GetRotators() []*logging.Rotator {
	c.initializeOnce.Do(func() {
		if err := c.initializeLogging(); err != nil {
			panic(err)
		}
	})
	r := make([]*logging.Rotator, 0, 2)
	if c.top.Server.AccessLog != nil {
		r = append(r, c.top.Server.AccessLog.rotator)
	}
	r = append(r, c.top.Log.rotator)
	return r
}

// Returns a map of all namespaces mapped into the Storage structure
// that will be serving them.
func (c *Config) GetNameSpaces() map[string]*storage.Storage {
	c.initializeOnce.Do(func() {
		if err := c.initializeLogging(); err != nil {
			panic(err)
		}
	})
	return c.top.getNameSpaces()
}

// Returns the httpserver.Server for this config.
func (c *Config) GetServer() httpserver.Server {
	c.initializeOnce.Do(func() {
		if err := c.initializeLogging(); err != nil {
			panic(err)
		}
	})
	return c.top.Server.Server()
}

// Pre-loads all of the secrets in the configuration. If any secrets were
// configured (certificates, passwords, aes keys, htpasswd files, etc) then
// this will perform the initial load of those resources.
func (c *Config) PreLoadSecrets() error {
	if err := c.top.Server.DebugPathsACL.preLoad(); err != nil {
		return err
	}
	if err := c.top.Server.HealthCheckACL.preLoad(); err != nil {
		return err
	}
	if err := c.top.Server.aesKeysLoader.PreLoad(); err != nil {
		return err
	}
	if err := c.top.Server.tlsCerts.PreLoad(); err != nil {
		return err
	}
	if err := c.top.Server.webUsersHTPasswd.PreLoad(); err != nil {
		return err
	}
	for _, ns := range c.top.NameSpace {
		if err := ns.BlastPathACL.preLoad(); err != nil {
			return err
		}
		if err := ns.InsertACL.preLoad(); err != nil {
			return err
		}
		if err := ns.PrimaryACL.preLoad(); err != nil {
			return err
		}
		if err := ns.ReadACL.preLoad(); err != nil {
			return err
		}
	}
	for _, saml := range c.top.SAML {
		if err := saml.certs.PreLoad(); err != nil {
			return err
		}
	}
	return nil
}

// If configured to do so this will setup a logger and start the secret
// refresher goroutine. This routine will run until the given channel is
// closed and will refresh certificates on the interval configured.
func (c *Config) StartSecretRefreshers() chan<- struct{} {
	c.initializeOnce.Do(func() {
		if err := c.initializeLogging(); err != nil {
			panic(err)
		}
	})
	stop := make(chan struct{})
	if c.top.Server.tlsCerts != nil {
		c.top.Server.tlsCerts.StartRefresher(stop)
	}
	if c.top.Server.aesKeysLoader != nil {
		c.top.Server.aesKeysLoader.StartRefresher(stop)
	}
	c.top.Server.DebugPathsACL.startRefresher(stop)
	c.top.Server.HealthCheckACL.startRefresher(stop)
	if c.top.Server.webUsersHTPasswd != nil {
		c.top.Server.webUsersHTPasswd.StartRefresher(stop)
	}
	for _, ns := range c.top.NameSpace {
		ns.BlastPathACL.startRefresher(stop)
		ns.InsertACL.startRefresher(stop)
		ns.PrimaryACL.startRefresher(stop)
		ns.ReadACL.startRefresher(stop)
	}
	for _, saml := range c.top.SAML {
		saml.certs.StartRefresher(stop)
	}
	return stop
}

// initializes logging exactly one time.
func (c *Config) initializeLogging() error {
	if err := c.top.Log.initLogging(); err != nil {
		return err
	}
	c.top.Server.initLogging()
	for _, saml := range c.top.SAML {
		saml.initLogging()
	}
	return nil
}
