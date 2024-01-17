package config

import (
	"fmt"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/iterable/blobby/httpserver/remotes"
	"github.com/iterable/blobby/httpserver/secretloader"
	"github.com/iterable/blobby/internal/delayqueue"
	"github.com/iterable/blobby/internal/workqueue"
	"github.com/iterable/blobby/storage"
)

var (
	defaultMaximumParallelCompressions  = int(10)
	defaultMaximumParallelUploads       = int(10)
	defaultMaximumParallelLocalDeletes  = int(10)
	defaultMaximumParallelRemoteDeletes = int(10)
)

type top struct {
	// A list of remotes that will be allowed to serve as replicas
	// for masters hosted by this machine.
	Remotes []remote `toml:"remote"`

	// The list of name spaces that this server should handle.
	NameSpace map[string]*nameSpace `toml:"namespace"`

	// A mapping of AWS profile configurations by profile name.
	AWSProfiles map[string]*AWS `toml:"aws"`

	// HTTP Server configuration
	Server server `toml:"server"`

	// HTTP Client Configuration
	Client client `toml:"client"`

	// The unique, system wide machine ID for this machine.
	MachineID *uint32 `toml:"machine_id"`

	// The maximum number of parallel compression operations that can be
	// run at a single time. Setting this too low can cause file uploads
	// to back log as compressions get run, but setting it to high might
	// cause io contention.
	MaximumParallelCompressions *int `toml:"maximum_parallel_compressions"`

	// The maximum number of parallel delete operations that can be run.
	// Setting this too low will cause delete operations to back lock
	// but setting this too high might waste resources.
	MaximumParallelDeletes *int `toml:"maximum_parallel_deletes"`

	// The maximum number of parallel remote delete operations that can
	// be run. Setting this too low will cause primary objects to become
	// backlogged and may cause the disk to fill up.
	MaximumParallelRemoteDeletes *int `toml:"maximum_parallel_remote_deletes"`

	// Maximum number of parallel uploads that the process is allowed to
	// perform regardless of which uploader initiates the upload.
	MaximumParallelUploads *int `toml:"maximum_parallel_uploads"`

	// Log configuration for the process.
	Log log `toml:"log"`

	// If configured then the process id will be written to this file.
	PIDFile *string `toml:"pidfile"`

	// SAML configuration. Each saml provider should be given a named
	// profile here.
	SAML map[string]*saml

	// The remotes.Pool implementation that is capable of assigning remotes
	// for new master files. This is generated as part of the configuration
	// cycle since that is where we have access to both the Remotes and the
	// NameSpaces.
	remotePool *remotes.Pool

	// A delay queue used to manage delayed events within primary and replica
	// objects.
	delayQueue *delayqueue.DelayQueue

	// A work queue used for controlling compression events.
	compressWorkQueue *workqueue.WorkQueue

	// A work queue used for processing upload events.
	uploadWorkQueue *workqueue.WorkQueue

	// A work queue used for processing of deletes of remotes data.
	deleteRemotesWorkQueue *workqueue.WorkQueue

	// A work queue used for processing deletes of files on disk.
	deleteLocalWorkQueue *workqueue.WorkQueue

	// A cache of calculated name spaces.
	nameSpaces map[string]*storage.Storage

	// A cache of the getProfiles() call output.
	profiles profiles
}

func (t *top) getAWSSession(name string) (*session.Session, error) {
	if a, ok := t.AWSProfiles[name]; !ok {
		return nil, fmt.Errorf("AWS profile %s does not exist.", name)
	} else if sess := a.GetSession(); sess == nil {
		return nil, fmt.Errorf("AWS profile %s is not initialized.", name)
	} else {
		return sess, nil
	}
}

func (t *top) getCompressWorkQueue() *workqueue.WorkQueue {
	if t.compressWorkQueue == nil {
		t.compressWorkQueue = workqueue.New(*t.MaximumParallelCompressions)
	}
	return t.compressWorkQueue
}

func (t *top) getDelayQueue() *delayqueue.DelayQueue {
	if t.delayQueue == nil {
		t.delayQueue = &delayqueue.DelayQueue{}
	}
	return t.delayQueue
}

func (t *top) getDeleteLocalWorkQueue() *workqueue.WorkQueue {
	if t.deleteLocalWorkQueue == nil {
		t.deleteLocalWorkQueue = workqueue.New(*t.MaximumParallelDeletes)
	}
	return t.deleteLocalWorkQueue
}

func (t *top) getDeleteRemotesWorkQueue() *workqueue.WorkQueue {
	if t.deleteRemotesWorkQueue == nil {
		t.deleteRemotesWorkQueue = workqueue.New(
			*t.MaximumParallelRemoteDeletes)
	}
	return t.deleteRemotesWorkQueue
}

func (t *top) getNameSpaces() map[string]*storage.Storage {
	if t.nameSpaces == nil {
		t.nameSpaces = make(
			map[string]*storage.Storage,
			len(t.NameSpace)*2+1)
		for name, ns := range t.NameSpace {
			t.nameSpaces[name] = ns.Storage()
		}
	}
	return t.nameSpaces
}

func (t *top) getProfiles() secretloader.Profiles {
	return t.profiles
}

func (t *top) getUploadWorkQueue() *workqueue.WorkQueue {
	if t.uploadWorkQueue == nil {
		t.uploadWorkQueue = workqueue.New(*t.MaximumParallelUploads)
	}
	return t.uploadWorkQueue
}

func (t *top) validate() []string {
	var errors []string

	// The minimum number of remotes is defined as the largest replica value
	// from a namespace.
	minRemotes := 0

	// since profiles are used all over the config we initialise this map
	// earlier and then populate it as we initialize aws clients. We start
	// with this as nil since the key is the only thing used to check to
	// see if a given profile is actually valid, and then after that we
	// populate it with the correct clients.
	t.profiles = make(map[string]*session.Session, len(t.AWSProfiles))
	for name := range t.AWSProfiles {
		t.profiles[name] = nil
	}

	// NameSpace
	if len(t.NameSpace) == 0 {
		errors = append(errors, "At least one namespace must be defined.")
	} else {
		for name := range t.NameSpace {
			errors = append(errors, t.NameSpace[name].validate(t, name)...)
			switch {
			case t.NameSpace[name].Replicas == nil:
			case *t.NameSpace[name].Replicas > minRemotes:
				minRemotes = *t.NameSpace[name].Replicas
			}
		}
	}

	// Ensure that a pool is setup and assigned. This will get referenced
	// when setting up namespaces which is okay, we can populate it later.
	t.remotePool = &remotes.Pool{
		RemotesByMachineID: make(map[uint32]storage.Remote, len(t.Remotes)*2),
	}

	// Remotes
	if len(t.Remotes) < minRemotes {
		errors = append(errors, "Not enough remote servers defined.")
	} else {
		for i := range t.Remotes {
			errors = append(errors, t.Remotes[i].validate(t)...)
			if len(errors) == 0 {
				remote := t.Remotes[i].Remote()
				t.remotePool.Remotes = append(t.remotePool.Remotes, remote)
				t.remotePool.RemotesByMachineID[*t.Remotes[i].ID] = remote
			}
		}
	}

	// Validate that two remotes are not generated with the same machine
	// id. We can only check this if there were no errors processing
	// the existing remotes configuration.
	if len(errors) == 0 {
		seen := make(map[uint32]string, len(t.Remotes))
		for _, r := range t.Remotes {
			id := fmt.Sprintf("%s:%d", *r.Host, r.port)
			if old, ok := seen[*r.ID]; ok {
				errors = append(
					errors,
					fmt.Sprintf(""+
						"remote machine id's must be unique, "+
						"%s and %s share %d",
						old,
						id,
						*r.ID))
			}
		}
	}

	// AWSProfiles
	if len(t.AWSProfiles) == 0 {
		errors = append(errors, "At least one aws profile needs to be defined.")
	} else {
		for name, profile := range t.AWSProfiles {
			errors = append(errors, profile.validate(name)...)
			t.profiles[name] = profile.GetSession()
		}
	}

	// Server
	errors = append(errors, t.Server.validate(t)...)

	// Client
	errors = append(errors, t.Client.validate()...)

	// MachineID
	if t.MachineID == nil {
		errors = append(errors, "machine_id is a required value.")
	}

	// MaximumParallelCompressions
	if t.MaximumParallelCompressions == nil {
		t.MaximumParallelCompressions = &defaultMaximumParallelCompressions
	} else if *t.MaximumParallelCompressions < 1 {
		errors = append(
			errors,
			"maximum_parallel_compresses can not be less than 1.")
	}

	// MaximumParallelDeletes
	if t.MaximumParallelDeletes == nil {
		t.MaximumParallelDeletes = &defaultMaximumParallelLocalDeletes
	} else if *t.MaximumParallelDeletes < 1 {
		errors = append(
			errors,
			"maximum_parallel_deletes can not be less than 1.")
	}

	// MaximumParallelUploads
	if t.MaximumParallelUploads == nil {
		t.MaximumParallelUploads = &defaultMaximumParallelUploads
	} else if *t.MaximumParallelUploads < 1 {
		errors = append(
			errors,
			"maximum_aws_uploads can not be less than 1.")
	}

	// MaximumParallelRemoteDeletes
	if t.MaximumParallelRemoteDeletes == nil {
		t.MaximumParallelRemoteDeletes = &defaultMaximumParallelRemoteDeletes
	} else if *t.MaximumParallelRemoteDeletes < 1 {
		errors = append(
			errors,
			"maximum_parallel_remote_deletes can not be less than 1.")
	}

	// Log
	errors = append(errors, t.Log.validate(t, "log")...)

	// PIDFile
	if t.PIDFile != nil {
		// TODO: Ideally we will do some basic checks to make sure that this
		// path is valid here. For now we just accept any string value and
		// fail out when writing the PID file later.
	}

	// SAML
	for name, s := range t.SAML {
		if name == "" {
			errors = append(errors, "saml profiles must be named.")
		} else if s == nil {
			errors = append(errors, "saml profiles can not be nil.")
		} else {
			errors = append(errors, s.validate(t, name)...)
		}
	}

	// Return any errors found.
	return errors
}
