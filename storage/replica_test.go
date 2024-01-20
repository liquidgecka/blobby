package storage

import (
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"log/slog"
	"os"
	"path/filepath"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/liquidgecka/testlib"

	"github.com/liquidgecka/blobby/internal/delayqueue"
	"github.com/liquidgecka/blobby/internal/workqueue"
	"github.com/liquidgecka/blobby/storage/fid"
)

func TestReplica_Compress(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := &replica{
		settings: &Settings{
			BaseDirectory: T.TempDir(),
		},
		fd:     T.TempFile(),
		fidStr: "not_exist",
		offset: uint64(len("test data")),
		log:    NewTestLogger(),
	}
	_, err := r.fd.WriteString("test data")
	T.ExpectSuccess(err)
	setStateRun := 0
	defer monkey.Patch(
		(*replica).setState,
		func(r *replica, ctx context.Context, n int32) {
			switch setStateRun {
			case 0:
				T.Equal(n, replicaStateCompressing)
			case 1:
				T.Equal(n, replicaStatePendingUpload)
			default:
				T.Fatalf("r.setState called too many times.")
			}
			setStateRun += 1
		},
	).Unpatch()
	r.Compress(context.Background())
	T.Equal(setStateRun, 2)

	// Check that the zip file contains the proper contents for the file
	// that generated earlier.
	_, err = r.compressFd.Seek(0, io.SeekStart)
	T.ExpectSuccess(err)
	gr, err := gzip.NewReader(r.compressFd)
	T.ExpectSuccess(err)
	data, err := ioutil.ReadAll(gr)
	T.ExpectSuccess(err)
	T.Equal(data, []byte("test data"))
}

func TestReplica_Compress_CopyBufferFails(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := &replica{
		settings: &Settings{
			BaseDirectory: T.TempDir(),
		},
		fd:     T.TempFile(),
		fidStr: "not_exist",
		offset: 100,
		log:    NewTestLogger(),
	}
	setStateRun := 0
	defer monkey.Patch(
		(*replica).setState,
		func(r *replica, ctx context.Context, n int32) {
			switch setStateRun {
			case 0:
				T.Equal(n, replicaStateCompressing)
			case 1:
				T.Equal(n, replicaStatePendingCompression)
			default:
				T.Fatalf("r.setState called too many times.")
			}
			setStateRun += 1
		},
	).Unpatch()
	defer monkey.Patch(
		(*os.File).Read,
		func(f *os.File, d []byte) (int, error) {
			return 0, fmt.Errorf("EXPECTED")
		},
	).Unpatch()
	r.Compress(context.Background())
	T.Equal(setStateRun, 2)
}

func TestReplica_Compress_CreateGZFails(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := &replica{
		settings: &Settings{
			BaseDirectory: filepath.Join(T.TempDir(), "not_exist"),
		},
		fidStr: "not_exist",
		offset: 100,
		log:    NewTestLogger(),
	}
	setStateRun := 0
	defer monkey.Patch(
		(*replica).setState,
		func(r *replica, ctx context.Context, n int32) {
			switch setStateRun {
			case 0:
				T.Equal(n, replicaStateCompressing)
			case 1:
				T.Equal(n, replicaStatePendingCompression)
			default:
				T.Fatalf("r.setState called too many times.")
			}
			setStateRun += 1
		},
	).Unpatch()
	r.Compress(context.Background())
	T.Equal(setStateRun, 2)
}

func TestReplica_Compress_EmptyFile(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := &replica{
		log: NewTestLogger(),
	}
	setStateRun := 0
	defer monkey.Patch(
		(*replica).setState,
		func(r *replica, ctx context.Context, n int32) {
			switch setStateRun {
			case 0:
				T.Equal(n, replicaStatePendingDelete)
			default:
				T.Fatalf("r.setState called too many times.")
			}
			setStateRun += 1
		},
	).Unpatch()
	r.Compress(context.Background())
	T.Equal(setStateRun, 1)
}

func TestReplica_Compress_NewGzipWriterFails(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := &replica{
		settings: &Settings{
			BaseDirectory: T.TempDir(),
			CompressLevel: -10000,
		},
		fd:     T.TempFile(),
		fidStr: "not_exist",
		offset: 100,
		log:    NewTestLogger(),
	}
	setStateRun := 0
	defer monkey.Patch(
		(*replica).setState,
		func(r *replica, ctx context.Context, n int32) {
			switch setStateRun {
			case 0:
				T.Equal(n, replicaStateCompressing)
			default:
				T.Fatalf("r.setState called too many times.")
			}
			setStateRun += 1
		},
	).Unpatch()
	T.ExpectPanic(
		func() {
			r.Compress(context.Background())
		},
		fmt.Errorf("gzip: invalid compression level: -10000"))
	T.Equal(setStateRun, 1)
}

func TestReplica_Compress_ZipperCloseFails(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := &replica{
		settings: &Settings{
			BaseDirectory: T.TempDir(),
		},
		fd:     T.TempFile(),
		fidStr: "not_exist",
		offset: 100,
		log:    NewTestLogger(),
	}
	setStateRun := 0
	defer monkey.Patch(
		(*replica).setState,
		func(r *replica, ctx context.Context, n int32) {
			switch setStateRun {
			case 0:
				T.Equal(n, replicaStateCompressing)
			case 1:
				T.Equal(n, replicaStatePendingCompression)
			default:
				T.Fatalf("r.setState called too many times.")
			}
			setStateRun += 1
		},
	).Unpatch()
	defer monkey.Patch(
		(*gzip.Writer).Close,
		func(f *gzip.Writer) error {
			return fmt.Errorf("EXPECTED")
		},
	).Unpatch()
	r.Compress(context.Background())
	T.Equal(setStateRun, 2)
}

func TestReplica_HeartBeat(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := replica{
		settings: &Settings{
			DelayQueue:    &delayqueue.DelayQueue{},
			HeartBeatTime: time.Minute,
		},
		log: NewTestLogger(),
	}
	r.settings.DelayQueue.Start()
	defer r.settings.DelayQueue.Stop()

	// First pass sets the timer up.
	now := time.Now()
	r.HeartBeat(context.Background())
	T.Equal(r.heartBeatToken.InList(), true)
	T.Equal(r.heartBeatLast.After(now), true)

	// A second pass should change the timer as well.
	now = time.Now()
	r.HeartBeat(context.Background())
	T.Equal(r.heartBeatToken.InList(), true)
	T.Equal(r.heartBeatLast.After(now), true)
}

func TestReplica_Compress_SeekFails(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := &replica{
		settings: &Settings{
			BaseDirectory: T.TempDir(),
		},
		fd:     T.TempFile(),
		fidStr: "not_exist",
		offset: 100,
		log:    NewTestLogger(),
	}
	setStateRun := 0
	defer monkey.Patch(
		(*replica).setState,
		func(r *replica, ctx context.Context, n int32) {
			switch setStateRun {
			case 0:
				T.Equal(n, replicaStateCompressing)
			case 1:
				T.Equal(n, replicaStatePendingCompression)
			default:
				T.Fatalf("r.setState called too many times.")
			}
			setStateRun += 1
		},
	).Unpatch()
	defer monkey.Patch(
		(*os.File).Seek,
		func(f *os.File, o int64, x int) (int64, error) {
			return 0, fmt.Errorf("EXPECTED")
		},
	).Unpatch()
	r.Compress(context.Background())
	T.Equal(setStateRun, 2)
}

func TestReplica_Open(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// We have to monkey patch out the setState function because it calls
	// a bunch of other places that make setting up this test very
	// complicated.
	setStateRun := 0
	defer monkey.Patch(
		(*replica).setState,
		func(r *replica, ctx context.Context, n int32) {
			switch setStateRun {
			case 0:
				T.Equal(n, replicaStateOpening)
			case 1:
				T.Equal(n, replicaStateWaiting)
			default:
				T.Fatalf("r.setState called too many times.")
			}
			setStateRun += 1
		},
	).Unpatch()

	// Also prevent the DelayQueue.Alter from actually doing anything
	// so that it doesn't attempt to call anything in the background.
	defer monkey.Patch(
		(*delayqueue.DelayQueue).Alter,
		func(*delayqueue.DelayQueue, *delayqueue.Token, time.Time, func(context.Context)) {},
	).Unpatch()

	// Setup a replica and call open on it.
	r := replica{
		fidStr: "test",
		settings: &Settings{
			BaseDirectory: T.TempDir(),
			DelayQueue:    &delayqueue.DelayQueue{},
		},
		log: NewTestLogger(),
	}
	r.settings.DelayQueue.Start()
	defer r.settings.DelayQueue.Stop()
	T.ExpectSuccess(r.Open(context.Background()))
	T.NotEqual(r.fd, nil)
	defer r.fd.Close()

	// Check that the file exists.
	_, err := os.Stat(filepath.Join(r.settings.BaseDirectory, "r-test"))
	T.ExpectSuccess(err)
}

func TestReplica_Open_Fails(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// We have to monkey patch out the setState function because it calls
	// a bunch of other places that make setting up this test very
	// complicated.
	setStateRun := 0
	defer monkey.Patch(
		(*replica).setState,
		func(r *replica, ctx context.Context, n int32) {
			switch setStateRun {
			case 0:
				T.Equal(n, replicaStateOpening)
			case 1:
				T.Equal(n, replicaStateCompleted)
			default:
				T.Fatalf("r.setState called too many times.")
			}
			setStateRun += 1
		},
	).Unpatch()

	// Setup a replica and call open on it.
	r := replica{
		fidStr: "test",
		settings: &Settings{
			BaseDirectory: "/some/path/that/doesn't/exist",
			DelayQueue:    &delayqueue.DelayQueue{},
		},
		log: NewTestLogger(),
	}
	r.settings.DelayQueue.Start()
	defer r.settings.DelayQueue.Stop()
	T.ExpectErrorMessage(
		r.Open(context.Background()),
		"open /some/path/that/doesn't/exist/r-test: no such file or directory")
	T.Equal(r.fd, nil)
}

func TestReplica_Replicate_WrongState(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	r := replica{
		state: replicaStateCompleted,
		log:   NewTestLogger(),
	}
	T.ExpectErrorMessage(
		r.Replicate(context.Background(), nil),
		"Attempt to replicate to a replica that is not waiting for updates. "+
			"The replica is in the completed state.",
	)
}

func TestReplica_Upload(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a replica that we can work with for our state transitions.
	r := replica{
		fd:         T.TempFile(),
		compressFd: T.TempFile(),
		log:        NewTestLogger(),
		state:      replicaStateCompleted,
		storage:    &Storage{},
		s3key:      "test_s3_key",
		settings: &Settings{
			CompressWorkQueue:    workqueue.New(0),
			DelayQueue:           &delayqueue.DelayQueue{},
			DeleteLocalWorkQueue: workqueue.New(0),
			UploadWorkQueue:      workqueue.New(0),
		},
	}
	r.settings.DelayQueue.Start()
	r.storage.replicas = map[string]*replica{
		"test": &r,
	}
	defer r.settings.DelayQueue.Stop()

	// If the offset is zero then just expect to get moved to PendingDelete.
	r.offset = 0
	r.Upload(context.Background())
	T.Equal(r.state, replicaStatePendingDelete)

	// Patch out the upload function since we don't actually need it for this
	// unit test.
	success := true
	defer monkey.Patch(
		uploadToS3,
		func(
			ctx context.Context,
			fd *os.File,
			id fid.FID,
			key string,
			s *Settings,
			l *slog.Logger,
		) bool {
			T.NotEqual(l, nil)
			T.Equal(s, r.settings)
			T.Equal(id, r.fid)
			T.Equal(key, r.s3key)
			if success {
				T.Equal(fd, r.fd)
			} else {
				T.Equal(fd, r.compressFd)
			}
			return success
		},
	).Unpatch()

	// Check that a successful upload increments Total and Successes.
	r.offset = 1
	r.Upload(context.Background())
	T.Equal(r.state, replicaStatePendingDelete)
	T.Equal(r.storage.metrics.ReplicaUploads.Total, int64(1))
	T.Equal(r.storage.metrics.ReplicaUploads.Successes, int64(1))

	// And a failure increments Total and Failures.
	success = false
	r.settings.Compress = true
	r.Upload(context.Background())
	T.Equal(r.state, replicaStatePendingUpload)
	T.Equal(r.storage.metrics.ReplicaUploads.Total, int64(2))
	T.Equal(r.storage.metrics.ReplicaUploads.Successes, int64(1))
}

func TestReplica_Event(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a replica that we can work with for our state transitions.
	r := replica{
		log:     NewTestLogger(),
		state:   replicaStateCompleted,
		storage: &Storage{},
		settings: &Settings{
			CompressWorkQueue:    workqueue.New(0),
			DelayQueue:           &delayqueue.DelayQueue{},
			DeleteLocalWorkQueue: workqueue.New(0),
			UploadWorkQueue:      workqueue.New(0),
		},
	}
	r.settings.DelayQueue.Start()
	r.storage.replicas = map[string]*replica{
		"test": &r,
	}
	defer r.settings.DelayQueue.Stop()

	// A list of states that shouldn't cause any action to be executed when
	// the state triggers.
	ignoreStates := []int32{
		replicaStateNew,
		replicaStateOpening,
		replicaStateAppending,
		replicaStatePendingCompression,
		replicaStateCompressing,
		replicaStatePendingUpload,
		replicaStateUploading,
		replicaStatePendingDelete,
		replicaStateDeletingCompressed,
		replicaStateClosingCompressed,
		replicaStateDeleting,
		replicaStateClosing,
		replicaStateCompleted,
	}
	for _, state := range ignoreStates {
		tName := "state - " + replicaStateStrings[state]
		r.state = state
		r.event(context.Background())
		T.Equal(r.storage.metrics.ReplicaOrphaned, int64(0), tName)
		T.Equal(r.state, state, tName)
	}

	// Offset > 0, Compress = false should be moved to PendingUpload
	r.settings.Compress = false
	r.offset = 1
	r.state = replicaStateWaiting
	r.event(context.Background())
	T.Equal(r.storage.metrics.ReplicaOrphaned, int64(1))
	T.Equal(r.state, replicaStatePendingUpload)

	// Offset > 0, Compress = true should be moved to PendingCompress
	r.settings.Compress = true
	r.offset = 1
	r.state = replicaStateWaiting
	r.event(context.Background())
	T.Equal(r.storage.metrics.ReplicaOrphaned, int64(2))
	T.Equal(r.state, replicaStatePendingCompression)

	// Offset = 0 should be moved to pendingDelete.
	r.offset = 0
	r.state = replicaStateWaiting
	r.event(context.Background())
	T.Equal(r.storage.metrics.ReplicaOrphaned, int64(3))
	T.Equal(r.state, replicaStatePendingDelete)
}

func TestReplica_SetState(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Make sure that the heart beat timer is set way way in the future so
	// it never actually.
	start := time.Now()
	future := time.Date(3000, 1, 1, 0, 0, 0, 0, time.UTC)

	// Setup a replica that we can work with for our state transitions.
	r := replica{
		fidStr:  "test",
		log:     NewTestLogger(),
		storage: &Storage{},
		settings: &Settings{
			CompressWorkQueue:    workqueue.New(0),
			DelayQueue:           &delayqueue.DelayQueue{},
			DeleteLocalWorkQueue: workqueue.New(0),
			UploadWorkQueue:      workqueue.New(0),
		},
	}
	r.settings.DelayQueue.Start()
	r.storage.replicas = map[string]*replica{
		"test": &r,
	}
	defer r.settings.DelayQueue.Stop()

	// replicaStateNew
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateNew)
	T.Equal(r.state, replicaStateNew)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 0)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 0)
	T.Equal(r.settings.UploadWorkQueue.Len(), 0)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateOpening
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateOpening)
	T.Equal(r.state, replicaStateOpening)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 0)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 0)
	T.Equal(r.settings.UploadWorkQueue.Len(), 0)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateWaiting
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateWaiting)
	T.Equal(r.state, replicaStateWaiting)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), true)
	T.Equal(r.settings.CompressWorkQueue.Len(), 0)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 0)
	T.Equal(r.settings.UploadWorkQueue.Len(), 0)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateAppending
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateAppending)
	T.Equal(r.state, replicaStateAppending)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), true)
	T.Equal(r.settings.CompressWorkQueue.Len(), 0)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 0)
	T.Equal(r.settings.UploadWorkQueue.Len(), 0)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateFailed
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateFailed)
	T.Equal(r.state, replicaStateFailed)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), true)
	T.Equal(r.settings.CompressWorkQueue.Len(), 0)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 0)
	T.Equal(r.settings.UploadWorkQueue.Len(), 0)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStatePendingCompression
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStatePendingCompression)
	T.Equal(r.state, replicaStatePendingCompression)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 1)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 0)
	T.Equal(r.settings.UploadWorkQueue.Len(), 0)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateCompressing
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateCompressing)
	T.Equal(r.state, replicaStateCompressing)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 1)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 0)
	T.Equal(r.settings.UploadWorkQueue.Len(), 0)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStatePendingUpload
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStatePendingUpload)
	T.Equal(r.state, replicaStatePendingUpload)
	T.NotEqual(r.queuedForUpload, time.Time{})
	T.Equal(start.Before(r.queuedForUpload), true)
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 1)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 0)
	T.Equal(r.settings.UploadWorkQueue.Len(), 1)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateUploading
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateUploading)
	T.Equal(r.state, replicaStateUploading)
	T.NotEqual(r.queuedForUpload, time.Time{})
	T.Equal(start.Before(r.queuedForUpload), true)
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 1)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 0)
	T.Equal(r.settings.UploadWorkQueue.Len(), 1)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStatePendingDelete
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStatePendingDelete)
	T.Equal(r.state, replicaStatePendingDelete)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 1)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 1)
	T.Equal(r.settings.UploadWorkQueue.Len(), 1)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateDeletingCompressed
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateDeletingCompressed)
	T.Equal(r.state, replicaStateDeletingCompressed)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 1)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 1)
	T.Equal(r.settings.UploadWorkQueue.Len(), 1)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateClosingCompressed
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateClosingCompressed)
	T.Equal(r.state, replicaStateClosingCompressed)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 1)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 1)
	T.Equal(r.settings.UploadWorkQueue.Len(), 1)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateDeleting
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateDeleting)
	T.Equal(r.state, replicaStateDeleting)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 1)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 1)
	T.Equal(r.settings.UploadWorkQueue.Len(), 1)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateClosing
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateClosing)
	T.Equal(r.state, replicaStateClosing)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 1)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 1)
	T.Equal(r.settings.UploadWorkQueue.Len(), 1)
	T.Equal(len(r.storage.replicas), 1)

	// replicaStateCompleted
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		future,
		func(context.Context) {},
	)
	r.setState(context.Background(), replicaStateCompleted)
	T.Equal(r.state, replicaStateCompleted)
	T.Equal(r.queuedForUpload, time.Time{})
	T.Equal(r.heartBeatToken.InList(), false)
	T.Equal(r.settings.CompressWorkQueue.Len(), 1)
	T.Equal(r.settings.DeleteLocalWorkQueue.Len(), 1)
	T.Equal(r.settings.UploadWorkQueue.Len(), 1)
	T.Equal(len(r.storage.replicas), 0)
}
