package secretloader

import (
	"bytes"
	"fmt"
	"sync"
	"time"

	htpasswd "github.com/tg123/go-htpasswd"

	"github.com/liquidgecka/blobby/internal/logging"
)

// Loads an .htpasswd file style file from the secret.
type HTPasswd struct {
	// The source for htpasswd data.
	Source Loader

	// All logging for this loader will be done via this logger.
	Logger *logging.Logger

	// A list of users and associated groups.
	usersLock sync.Mutex
	users     map[string][]htpasswdLine
}

// Preloads the htpasswd file if configured to do so.
func (h *HTPasswd) PreLoad() error {
	if h == nil {
		return nil
	} else if h.Source == nil {
		return nil
	} else if !h.Source.PreLoad() {
		return nil
	}
	_, err := h.load()
	return err
}

// Starts the cache refresher.
func (h *HTPasswd) StartRefresher(stop <-chan struct{}) {
	if h.Source.Stale() {
		dur := h.Source.CacheDuration()
		go h.refresher(dur, stop)
	}
}

// Verifies that the given user name has the tags given. The return
// values represent true if the user exists, true if the user exists and
// has the given tags, and an error if something goes wrong during the
// secret fetching process.
func (h *HTPasswd) HasTags(user string, tags []string) (bool, error) {
	var candidates []htpasswdLine
	if h.Source.IsStale() {
		if all, err := h.load(); err != nil {
			return false, err
		} else {
			candidates = all[user]
			func() {
				h.usersLock.Lock()
				defer h.usersLock.Unlock()
				h.users = all
			}()
		}
	} else {
		candidates = func() []htpasswdLine {
			h.usersLock.Lock()
			defer h.usersLock.Unlock()
			return h.users[user]
		}()
	}
	for _, line := range candidates {
		if line.HasTags(tags) {
			return true, nil
		}
	}
	return false, nil
}

// Verifies that a user with with the given password exists in the
// hapassword map, and that the user has all of the tags provided.
func (h *HTPasswd) Verify(user, pass string, tags []string) (bool, error) {
	var candidates []htpasswdLine
	if h.Source.IsStale() {
		if all, err := h.load(); err != nil {
			return false, err
		} else {
			candidates = all[user]
			func() {
				h.usersLock.Lock()
				defer h.usersLock.Unlock()
				h.users = all
			}()
		}
	} else {
		candidates = func() []htpasswdLine {
			h.usersLock.Lock()
			defer h.usersLock.Unlock()
			return h.users[user]
		}()
	}
	for _, line := range candidates {
		if line.IsPassword(pass) && line.HasTags(tags) {
			return true, nil
		}
	}
	return false, nil
}

// Loads the htpasswd file from the secret.
func (h *HTPasswd) load() (map[string][]htpasswdLine, error) {
	// Get the raw source data.
	raw, err := h.Source.Fetch()
	if err != nil {
		return nil, err
	}

	// The file is a colon delimited list of user:pass:tag pairings. Split
	// it into lines and use that as a hint for how large the map should be.
	lines := bytes.Split(raw, []byte{'\n'})
	users := make(map[string][]htpasswdLine, len(lines))
	for i, line := range lines {
		if len(line) == 0 || line[0] == '#' {
			continue
		}
		parts := bytes.Split(line, []byte{':'})
		if len(parts) < 2 || len(parts[0]) == 0 {
			return nil, fmt.Errorf(
				"file is badly formatted at line %d: %s",
				i,
				line)
		}
		data := htpasswdLine{
			user: string(parts[0]),
			tags: make(map[string]struct{}, len(parts)),
		}
		for j := 2; j < len(parts); j++ {
			s := string(parts[j])
			if len(s) == 0 {
				return nil, fmt.Errorf("Invalid tag `%s` at line %d", s, i)
			} else if _, ok := data.tags[s]; ok {
				return nil, fmt.Errorf("Duplicate tag `%s` at line %d", s, i)
			} else {
				data.tags[s] = struct{}{}
			}
		}
		if err := data.Parse(i, string(parts[1])); err != nil {
			return nil, err
		}
		users[data.user] = append(users[data.user], data)
	}

	// Save the results if there were no errors.
	h.Logger.Debug(
		"Loaded htpasswd file.",
		logging.NewFieldIface("users", len(users)))
	h.usersLock.Lock()
	defer h.usersLock.Unlock()
	h.users = users
	return users, nil
}

// Reloads the secret on an interval until the stop channel is closed. This
// is expected to be run as a goroutine.
func (h *HTPasswd) refresher(dur time.Duration, stop <-chan struct{}) {
	timer := time.NewTimer(dur)
	defer func() {
		if !timer.Stop() {
			<-timer.C
		}
	}()
	for {
		select {
		case <-stop:
			return
		case <-timer.C:
		}
		h.Logger.Debug("Refreshing the htpasswd data.")
		if _, err := h.load(); err != nil {
			h.Logger.Error(
				"Error refreshing the htpasswd data.",
				logging.NewFieldIface("error", err))
		}
	}
}

// Represents a single line in the htpasswd file.
type htpasswdLine struct {
	user string
	pass htpasswd.EncodedPasswd
	tags map[string]struct{}
}

func (h *htpasswdLine) HasTags(tags []string) bool {
	if tags == nil {
		return true
	}
	for _, tag := range tags {
		if _, ok := h.tags[tag]; !ok {
			return false
		}
	}
	return true
}

func (h *htpasswdLine) IsPassword(pass string) bool {
	return h.pass.MatchesPassword(pass)
}

// Parses a given password hash into a htpass.EncodedPassword or returns
// an error if the password is not understood.
func (h *htpasswdLine) Parse(line int, hash string) (err error) {
	for _, parser := range htpasswd.DefaultSystems {
		h.pass, err = parser(hash)
		if err != nil {
			return fmt.Errorf(
				"Invalid password on line %d: %s",
				line,
				err.Error())
		} else if h.pass != nil {
			return
		}
	}
	return fmt.Errorf("Unknown password hash on line %d", line)
}
