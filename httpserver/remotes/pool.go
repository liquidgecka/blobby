package remotes

import (
	"fmt"
	"io"
	"sync"

	"github.com/iterable/blobby/storage"
)

// Manages a pool of Remotes that can be used for operations with the
// storage implementation. The pool manages all the backends and supports
// the "AssignRemotes" functionality as well which is used when selecting
// remotes to use for new files.
type Pool struct {
	// The list of Remotes configured for this cluster.
	Remotes []storage.Remote

	// We also store a mapping of Remotes by the MachineID associated with
	// them. This gives us quick access when performing Read() calls.
	RemotesByMachineID map[uint32]storage.Remote

	// When assigning new Remotes for a primary this will be used to
	// round robin.
	NextRemote     int
	NextRemoteLock sync.Mutex

	// FIXME: Add support for health checks?
}

// Picks a list of Remotes that should be used for a new master file.
func (p *Pool) AssignRemotes(r int) ([]storage.Remote, error) {
	// If r is zero then return nil, no point doing any work. If r is
	// greater than 0 and there are no Remotes assigned then error out.
	if r == 0 {
		return nil, nil
	} else if r > len(p.Remotes) {
		return nil, fmt.Errorf(
			"There are %d remotes defined, can not assign %d replicas.",
			len(p.Remotes),
			r)
	}

	// We need to pick remotes randomly. In order to do this efficiently
	// we just sort the Remotes and then do a "round robin" approach to
	// items, bubbling each item to the rear of the list.
	remotes := make([]storage.Remote, 0, r)
	func() {
		p.NextRemoteLock.Lock()
		defer p.NextRemoteLock.Unlock()
		for i := 0; i < r; i++ {
			remotes = append(remotes, p.Remotes[p.NextRemote])
			p.NextRemote = (p.NextRemote + 1) % len(p.Remotes)
		}
	}()
	if len(remotes) < r {
		return nil, fmt.Errorf("There are not enough replicas to assign.")
	}
	return remotes, nil
}

// When a HTTP caller performs a GET against a token it will be processed
// internally if possible (the file was created on the local machine and is
// still present, or the replica is hosted on this server) however if the
// replica was created on a remote machine Blobby will attempt to fetch from
// that machine since it may still have the file locally. That server will
// look it up locally and if its not present return an 404 error. However
// in order to do that properly we need to be able to pass context from the
// HTTP request that started this operation.
func (p *Pool) Read(rc storage.ReadConfig) (io.ReadCloser, error) {
	// Check to see if the given machine id exists in the pool and if so
	// attempts to call it to perform an operation, otherwise this will
	// return an error.
	if r, ok := p.RemotesByMachineID[rc.Machine()]; !ok {
		return nil, fmt.Errorf("There is no machine with id %d", rc.Machine())
	} else {
		return r.Read(rc)
	}
}
