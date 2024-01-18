package storage

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/liquidgecka/testlib"

	"github.com/liquidgecka/blobby/internal/backoff"
	"github.com/liquidgecka/blobby/internal/delayqueue"
	"github.com/liquidgecka/blobby/internal/workqueue"
	"github.com/liquidgecka/blobby/storage/fid"
)

func TestNew_PanicConditions(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	ar := func(int) ([]Remote, error) {
		return nil, nil
	}
	nilRead := func(ReadConfig) (io.ReadCloser, error) {
		return nil, nil
	}
	uploader := &s3manager.Uploader{}
	client := &s3.S3{}

	// Validate that various settings that are required generate
	// a panic as expected.
	T.ExpectPanic(func() {
		New(&Settings{
			AWSUploader:   uploader,
			BaseDirectory: "test",
			Compress:      true,
			CompressLevel: 1,
			DelayQueue:    &delayqueue.DelayQueue{},
			Read:          nilRead,
			S3Bucket:      "test",
			S3Client:      client,
		})
	}, "settings.AssignRemotes is required.")
	T.ExpectPanic(func() {
		New(&Settings{
			AssignRemotes: ar,
			BaseDirectory: "test",
			Compress:      true,
			CompressLevel: 1,
			DelayQueue:    &delayqueue.DelayQueue{},
			Read:          nilRead,
			S3Bucket:      "test",
			S3Client:      client,
		})
	}, "settings.AWSUploader is required.")
	T.ExpectPanic(func() {
		New(&Settings{
			AssignRemotes: ar,
			AWSUploader:   uploader,
			Compress:      true,
			CompressLevel: 1,
			DelayQueue:    &delayqueue.DelayQueue{},
			Read:          nilRead,
			S3Bucket:      "test",
			S3Client:      client,
		})
	}, "settings.BaseDirectory is required.")
	T.ExpectPanic(func() {
		New(&Settings{
			AssignRemotes: ar,
			AWSUploader:   uploader,
			BaseDirectory: "test",
			Compress:      true,
			CompressLevel: -2,
			DelayQueue:    &delayqueue.DelayQueue{},
			Read:          nilRead,
			S3Bucket:      "test",
			S3Client:      client,
		})
	}, "settings.CompressLevel can not be less than -1.")
	T.ExpectPanic(func() {
		New(&Settings{
			AssignRemotes: ar,
			AWSUploader:   uploader,
			BaseDirectory: "test",
			Compress:      true,
			CompressLevel: 100,
			DelayQueue:    &delayqueue.DelayQueue{},
			Read:          nilRead,
			S3Bucket:      "test",
			S3Client:      client,
		})
	}, "settings.CompressLevel can not be greater than 9.")
	T.ExpectPanic(func() {
		New(&Settings{
			AssignRemotes: ar,
			AWSUploader:   uploader,
			BaseDirectory: "test",
			Compress:      true,
			CompressLevel: 1,
			Read:          nilRead,
			S3Bucket:      "test",
			S3Client:      client,
		})
	}, "settings.DelayQueue is required.")
	T.ExpectPanic(func() {
		New(&Settings{
			AssignRemotes: ar,
			AWSUploader:   uploader,
			BaseDirectory: "test",
			Compress:      true,
			CompressLevel: 1,
			DelayQueue:    &delayqueue.DelayQueue{},
			S3Bucket:      "test",
			S3Client:      client,
		})
	}, "settings.Read is required.")
	T.ExpectPanic(func() {
		New(&Settings{
			AssignRemotes: ar,
			AWSUploader:   uploader,
			BaseDirectory: "test",
			Compress:      true,
			CompressLevel: 1,
			DelayQueue:    &delayqueue.DelayQueue{},
			Read:          nilRead,
			S3Client:      client,
		})
	}, "settings.S3Bucket is required.")
	T.ExpectPanic(func() {
		New(&Settings{
			AssignRemotes: ar,
			AWSUploader:   uploader,
			BaseDirectory: "test",
			Compress:      true,
			CompressLevel: 1,
			DelayQueue:    &delayqueue.DelayQueue{},
			Read:          nilRead,
			S3Bucket:      "test",
		})
	}, "settings.S3Client is required.")
}

func TestNew(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	ar := func(int) ([]Remote, error) {
		return nil, nil
	}
	nilRead := func(ReadConfig) (io.ReadCloser, error) {
		return nil, nil
	}
	uploader := &s3manager.Uploader{}
	client := &s3.S3{}

	// Mostly default configuration.
	settings := &Settings{
		AssignRemotes: ar,
		AWSUploader:   uploader,
		BaseDirectory: "test",
		Compress:      true,
		CompressLevel: -1,
		DelayQueue:    &delayqueue.DelayQueue{},
		Read:          nilRead,
		S3Bucket:      "test",
		S3Client:      client,
	}
	s := New(settings)
	T.NotEqual(s, nil)
	T.NotEqual(s.newFileBackOff.Max, time.Duration(0))
	T.NotEqual(s.newFileBackOff.Period, time.Duration(0))
	T.NotEqual(s.newFileBackOff.X, time.Duration(0))
	T.Equal(s.settings.HeartBeatTime, defaultHeartBeatTime)
	T.Equal(s.settings.NameSpace, "default")
	T.Equal(s.settings.OpenFilesMaximum, defaultOpenFilesMaximum)
	T.Equal(s.settings.OpenFilesMinimum, defaultOpenFilesMinimum)
	T.Equal(s.settings.UploadLargerThan, defaultUploadLargerThan)
	T.Equal(s.settings.UploadOlder, defaultUploadOlder)
	T.Equal(s.settings.CompressLevel, gzip.NoCompression)

	// Do the same but with 0 for CompressLevel
	settings = &Settings{
		AssignRemotes: ar,
		AWSUploader:   uploader,
		BaseDirectory: "test",
		Compress:      true,
		CompressLevel: 0,
		DelayQueue:    &delayqueue.DelayQueue{},
		Read:          nilRead,
		S3Bucket:      "test",
		S3Client:      client,
	}
	s = New(settings)
	T.NotEqual(s, nil)
	T.NotEqual(s.newFileBackOff.Max, time.Duration(0))
	T.NotEqual(s.newFileBackOff.Period, time.Duration(0))
	T.NotEqual(s.newFileBackOff.X, time.Duration(0))
	T.Equal(s.settings.HeartBeatTime, defaultHeartBeatTime)
	T.Equal(s.settings.NameSpace, "default")
	T.Equal(s.settings.OpenFilesMaximum, defaultOpenFilesMaximum)
	T.Equal(s.settings.OpenFilesMinimum, defaultOpenFilesMinimum)
	T.Equal(s.settings.UploadLargerThan, defaultUploadLargerThan)
	T.Equal(s.settings.UploadOlder, defaultUploadOlder)
	T.Equal(s.settings.CompressLevel, gzip.DefaultCompression)
}

func TestStorage_BlastPathStatus(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Setup a storage object with a few primaries we can use for validation.
	s := Storage{
		primaries: map[string]*primary{
			"test1": &primary{fidStr: "test1", offset: 100},
			"test2": &primary{fidStr: "test2", offset: 200},
			"test3": &primary{fidStr: "test3", offset: 300},
			"test4": &primary{fidStr: "test4", offset: 400},
		},
	}

	// Make a buffer to store the results and call the function.
	buffer := bytes.NewBuffer(nil)
	s.BlastPathStatus(buffer)

	// The output will be an array of _UNSORTED_ json data about each
	// primary so a simple comparison here will not be sufficient for us
	// to test this. To solve this we create a set of expected elements.
	// and then sort the ones we were given.
	want := []string{
		`{"fid":"test1","size":100}`,
		`{"fid":"test2","size":200}`,
		`{"fid":"test3","size":300}`,
		`{"fid":"test4","size":400}`,
	}
	haveRaw := []json.RawMessage{}
	err := json.NewDecoder(buffer).Decode(&haveRaw)
	T.ExpectSuccess(err)
	have := make([]string, len(haveRaw))
	for i, raw := range haveRaw {
		have[i] = string(raw)
	}
	sort.Strings(have)
	T.Equal(have, want)
}

func TestStorage_DebugID(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Simple storage object to call.
	formatter, err := fid.NewFormatter(
		"test/path/%y-%m-%d/%H:%M:%S/%K/%L",
	)
	T.ExpectSuccess(err)
	s := Storage{
		settings: Settings{
			NameSpace:   "NSTEST",
			S3Bucket:    "s3_bucket",
			S3KeyFormat: formatter,
		},
	}

	// Generate a FID for use with testing.
	mid := rand.Uint32()
	f := fid.FID{}
	f.Generate(mid)

	// Patch out time.Now() so that it returns an expected value.
	now := time.Unix(0, 0)
	defer monkey.Patch(
		time.Now,
		func() time.Time { return now },
	).Unpatch()

	test := func(start uint64, length uint32, p, r bool) {
		extra := ""
		if p {
			extra = strings.Join([]string{
				"",
				"This ID is served local by a primary that is",
				"in the waiting state.",
			}, "\n")
		} else {
			extra = strings.Join([]string{
				"",
				"This ID is served local by a replica that is",
				"in the waiting state.",
			}, "\n")
		}
		str := f.ID(start, length)
		buf := bytes.Buffer{}
		s.DebugID(&buf, str)
		T.Equal(
			strings.Split("\n", buf.String()),
			strings.Split("\n", fmt.Sprintf(
				`Name space: NSTEST
File ID: %s
Machine ID: %d
Start Offset: %d
Length: %d
S3 Desintation: s3://s3_bucket/test/path/1970-01-01/00:00:00/0/%d%s`,
				f.String(),
				mid,
				start,
				length,
				mid,
				extra,
			)))
	}

	// Run a hundred tests where the file is not a primary.
	for i := 0; i < 100; i++ {
		test(rand.Uint64(), rand.Uint32(), false, false)
	}

	// Run the tests again where the file is a primary.
	s.primaries = map[string]*primary{
		f.String(): &primary{state: primaryStateWaiting},
	}
	for i := 0; i < 100; i++ {
		test(rand.Uint64(), rand.Uint32(), true, false)
	}
	s.primaries = nil

	// Run the tests again where the file is a replicas.
	s.replicas = map[string]*replica{
		f.String(): &replica{state: replicaStateWaiting},
	}
	for i := 0; i < 100; i++ {
		test(rand.Uint64(), rand.Uint32(), true, false)
	}
	s.replicas = nil

	// Make sure that an invalid FID returns an error.
	b := bytes.Buffer{}
	s.DebugID(&b, "INVALID")
	T.Equal(b.String(), "Invalid ID: Not a valid ID token.\n")
}

func TestStorage_GetMetrics(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Mock out time.Now() since we need the return value to be
	// consistent and testable.
	now := time.Date(2020, 12, 31, 1, 2, 3, 4, time.UTC)
	defer monkey.Patch(
		time.Now,
		func() time.Time { return now },
	).Unpatch()

	// Mock out Since() so it returns a consistent value as well.
	defer monkey.Patch(
		time.Since,
		func(time.Time) time.Duration { return time.Second },
	).Unpatch()

	// Setup a storage to work with.
	s := Storage{
		primaries: map[string]*primary{
			// Oldest firstInsert, youngest queuedForUpload
			"test1": &primary{
				firstInsert: time.Date(
					2000, 1, 2, 3, 4, 5, 6, time.UTC,
				),
				queuedForUpload: time.Date(
					2020, 1, 2, 3, 4, 5, 6, time.UTC,
				),
			},
			// Youngest firstInsert, oldest queuedForUpload
			"test2": &primary{
				firstInsert: time.Date(
					2020, 1, 2, 3, 4, 5, 6, time.UTC,
				),
				queuedForUpload: time.Date(
					2000, 1, 2, 3, 4, 5, 6, time.UTC,
				),
			},
			// default firstInsert, defualt queuedForUpload
			"test3": &primary{},
		},
		replicas: map[string]*replica{
			// Even older queuedForUpload
			"test2": &replica{
				queuedForUpload: time.Date(
					1999, 1, 2, 3, 4, 5, 6, time.UTC,
				),
			},
			// default firstInsert, defualt queuedForUpload
			"test3": &replica{},
		},
	}

	// Run the actual test.
	want := s.metrics
	want.OldestQueuedUpload = 1.0
	want.OldestUnUploadedData = 1.0
	have := s.GetMetrics()
	T.Equal(have, want)
}

func TestStorage_Health(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Happy path, all is healthy.
	s := Storage{
		newFileBackOff: backoff.BackOff{
			Max:    time.Second * 1,
			Period: time.Second * 30,
			X:      time.Millisecond * 100,
		},
	}
	h, out := s.Health()
	T.Equal(h, true)
	T.Equal(out, "New file creation: SUCCEEDED\n")

	// New file creation is unhealthy.
	s.newFileBackOff.Failure()
	h, out = s.Health()
	T.Equal(h, false)
	T.Equal(out, "New file creation: FAILED\n")
}

func TestStorage_ReplicaHeartBeat(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// A replica that we will use to operate against.
	r := replica{
		fidStr:  "test",
		log:     NewTestLogger(),
		storage: &Storage{},
	}
	s := Storage{
		replicas: map[string]*replica{
			"test": &r,
		},
		settings: Settings{
			BaseDirectory:          T.TempDir(),
			BaseLogger:             NewTestLogger(),
			CompressWorkQueue:      workqueue.New(0),
			DelayQueue:             &delayqueue.DelayQueue{},
			DeleteLocalWorkQueue:   workqueue.New(0),
			DeleteRemotesWorkQueue: workqueue.New(0),
			UploadWorkQueue:        workqueue.New(0),
		},
	}
	r.settings = &s.settings
	s.settings.DelayQueue.Start()
	defer s.settings.DelayQueue.Stop()

	// If the replica doesn't exist then this should return an error.
	T.ExpectErrorMessage(
		s.ReplicaHeartBeat("not_found"),
		"not_found is not a known replica.",
	)
	T.Equal(s.metrics.ReplicaHeartBeats.Total, int64(1))
	T.Equal(s.metrics.ReplicaHeartBeats.Failures, int64(1))

	// And if it does exist we expect success.
	T.ExpectSuccess(s.ReplicaHeartBeat("test"))
	T.Equal(s.metrics.ReplicaHeartBeats.Total, int64(2))
	T.Equal(s.metrics.ReplicaHeartBeats.Successes, int64(1))
}

func TestStorage_ReplicaQueueDelete(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// A replica that we will use to operate against.
	r := replica{
		fidStr:  "test",
		log:     NewTestLogger(),
		storage: &Storage{},
	}
	s := Storage{
		replicas: map[string]*replica{
			"test": &r,
		},
		settings: Settings{
			BaseDirectory:          T.TempDir(),
			BaseLogger:             NewTestLogger(),
			CompressWorkQueue:      workqueue.New(0),
			DelayQueue:             &delayqueue.DelayQueue{},
			DeleteLocalWorkQueue:   workqueue.New(0),
			DeleteRemotesWorkQueue: workqueue.New(0),
			UploadWorkQueue:        workqueue.New(0),
		},
	}
	r.settings = &s.settings

	// If the replica does not exist then we expect no error
	// to be returned.
	T.ExpectSuccess(s.ReplicaQueueDelete("not_found"))
	T.Equal(s.metrics.ReplicaQueueDeletes.Total, int64(1))
	T.Equal(s.metrics.ReplicaQueueDeletes.Successes, int64(1))

	// If the If the replica is in the proper state then it can be
	// deleted properly and we should expect it to have its state
	// changed accordingly.
	r.state = replicaStateWaiting
	T.ExpectSuccess(s.ReplicaQueueDelete("test"))
	T.Equal(r.state, replicaStatePendingDelete)
	T.Equal(s.metrics.ReplicaQueueDeletes.Total, int64(2))
	T.Equal(s.metrics.ReplicaQueueDeletes.Successes, int64(2))

	// And if the replica is in a bad state then it can't be deleted.
	r.state = -1
	T.ExpectErrorMessage(
		s.ReplicaQueueDelete("test"),
		"Can not delete the replica, its in the wrong state:")
	T.Equal(r.state, int32(-1))
	T.Equal(s.metrics.ReplicaQueueDeletes.Total, int64(3))
	T.Equal(s.metrics.ReplicaQueueDeletes.Successes, int64(2))
	T.Equal(s.metrics.ReplicaQueueDeletes.Failures, int64(1))
}

func TestStorage_SetDebugging(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	log := NewTestLogger()
	plog := log.NewChild()
	rlog := log.NewChild()

	s := Storage{
		primaries: map[string]*primary{
			"test": &primary{log: plog},
		},
		replicas: map[string]*replica{
			"test": &replica{log: rlog},
		},
		settings: Settings{
			BaseLogger: log,
		},
	}
	s.SetDebugging(true)
	T.Equal(log.DebugEnabled(), true)
	T.Equal(plog.DebugEnabled(), true)
	T.Equal(rlog.DebugEnabled(), true)
	s.SetDebugging(false)
	T.Equal(log.DebugEnabled(), false)
	T.Equal(plog.DebugEnabled(), false)
	T.Equal(rlog.DebugEnabled(), false)
}

func TestStorage_Status(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	s := Storage{
		primaries: map[string]*primary{
			"a": &primary{},
			"b": &primary{},
			"c": &primary{},
			"d": &primary{},
		},
		replicas: map[string]*replica{
			"a": &replica{},
			"b": &replica{},
		},
	}
	b := bytes.Buffer{}
	s.Status(&b)
	T.Equal(b.String(), strings.Join([]string{
		"    Primaries:",
		"         state=new size=0B",
		"         state=new size=0B",
		"         state=new size=0B",
		"         state=new size=0B",
		"    Replicas:",
		"         state=new size=0B",
		"         state=new size=0B",
		""},
		"\n"))
}

func TestStorage_Start(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	runTest := func(compress bool) {
		// Keep track of the expected files.
		expected := make([]string, 0, 3)

		// Make a temporary directory with 3 files in it that can be used
		// for testing.
		dir := T.TempDir()
		for i := 0; i < 3; i++ {
			// Get a fid string to work with.
			f := fid.FID{}
			rand.Read([]byte(f[:]))
			fidStr := f.String()

			// Create a file (replica if i is even, primary if i is odd).
			prefix := ""
			if i%2 == 0 {
				prefix = "r-"
			}
			fd, err := os.Create(filepath.Join(dir, prefix+fidStr))
			T.ExpectSuccess(err)
			T.ExpectSuccess(fd.Close())

			// Add the name to the expected list.
			expected = append(expected, fidStr)
		}

		// Also create a non blobby data file.
		fd, err := os.Create(filepath.Join(dir, "nonblobby"))
		T.ExpectSuccess(err)
		T.ExpectSuccess(fd.Close())

		// And a directory.
		T.ExpectSuccess(os.Mkdir(filepath.Join(dir, "directory"), 0755))

		// Create a stub Storage object.
		s := Storage{
			settings: Settings{
				BaseDirectory:          dir,
				BaseLogger:             NewTestLogger(),
				Compress:               compress,
				CompressWorkQueue:      workqueue.New(1),
				DelayQueue:             &delayqueue.DelayQueue{},
				DeleteLocalWorkQueue:   workqueue.New(1),
				DeleteRemotesWorkQueue: workqueue.New(1),
				UploadWorkQueue:        workqueue.New(1),
			},
			replicas: make(map[string]*replica, 10),
		}

		// Monkey patch out the go routines that Start() fires so that this
		// wont accidentally start any background routines that we don't
		// expect.
		checkIdleFilesRun := false
		defer monkey.Patch(
			(*Storage).checkIdleFiles,
			func(*Storage) { checkIdleFilesRun = true },
		).Unpatch()

		// On startup the replica will have its state set to the right
		// value but this will kick start a bunch of work. We need to mock
		// out this function to prevent that.
		defer monkey.Patch(
			(*replica).setState,
			func(r *replica, s int32) {},
		).Unpatch()

		// Check that startup works.
		T.ExpectSuccess(s.Start())
		T.TryUntil(
			func() bool { return checkIdleFilesRun },
			time.Second,
			"*Storage.checkIdleFiles was not started.")

		// Make sure that each of the expected files was created.
		T.Equal(len(s.replicas), len(expected))
		for _, fidStr := range expected {
			T.NotEqual(s.replicas[fidStr], nil)
			T.Equal(s.replicas[fidStr].fidStr, fidStr)
			T.Equal(s.replicas[fidStr].fid.String(), fidStr)
			T.Equal(s.replicas[fidStr].settings, &s.settings)
			T.Equal(s.replicas[fidStr].storage, &s)
			T.NotEqual(s.replicas[fidStr].fd, nil)
			T.NotEqual(s.replicas[fidStr].log, nil)
		}
	}

	// Run the test with compression and without
	runTest(false)
	runTest(true)
}

func TestStorage_Start_BadDirectory(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	dir := filepath.Join(T.TempDir(), "notfound")
	s := Storage{
		settings: Settings{
			BaseDirectory: dir,
			BaseLogger:    NewTestLogger(),
		},
		replicas: make(map[string]*replica, 10),
	}
	T.ExpectErrorMessage(s.Start(), "no such file or directory")
}

func TestStorage_Start_OpenFailure(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	dir := T.TempDir()
	f := fid.FID{}
	rand.Read([]byte(f[:]))
	fidStr := f.String()

	// Make sure that the file does not have read permissions
	// so that opening it will fail.
	fd, err := os.OpenFile(
		filepath.Join(dir, fidStr),
		os.O_CREATE|os.O_WRONLY|os.O_APPEND,
		0x0)
	T.ExpectSuccess(err)
	T.ExpectSuccess(fd.Close())
	s := Storage{
		settings: Settings{
			BaseDirectory: dir,
			BaseLogger:    NewTestLogger(),
		},
		replicas: make(map[string]*replica, 10),
	}
	T.ExpectErrorMessage(s.Start(), "permission denied")
}

func TestStorage_PrimaryStateChange(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// A test runner that wraps the logic so the actual tests can be
	// written as a table rather than copy pasted code.
	runTest := func(o, n int32, c, r bool) {
		p := &primary{
			fidStr: "test",
			log:    NewTestLogger(),
		}
		s := &Storage{
			appendablePrimaries: 1,
			primaries:           map[string]*primary{"test": p},
		}
		checked := false
		defer monkey.Patch(
			(*Storage).checkIdleFiles,
			func(*Storage) { checked = true },
		).Unpatch()
		s.primaryStateChange(p, o, n)
		testName := fmt.Sprintf(
			"%s -> %s",
			primaryStateStrings[o],
			primaryStateStrings[n])
		T.Equal(checked, c, testName)
		if c {
			T.Equal(s.appendablePrimaries, int32(0), testName)
		} else {
			T.Equal(s.appendablePrimaries, int32(1), testName)
		}
		if r {
			T.Equal(len(s.primaries), 0, testName)
		} else {
			T.Equal(len(s.primaries), 1, testName)
		}
	}

	// States that should trigger the checkIdleFiles condition.
	states := []int32{
		primaryStateNew,
		primaryStateOpening,
		primaryStateInitializingRepls,
		primaryStateWaiting,
		primaryStateInserting,
		primaryStateReplicating,
	}
	for _, s := range states {
		runTest(s, primaryStateNew, false, false)
		runTest(s, primaryStateOpening, false, false)
		runTest(s, primaryStateInitializingRepls, false, false)
		runTest(s, primaryStateWaiting, false, false)
		runTest(s, primaryStateInserting, false, false)
		runTest(s, primaryStateReplicating, false, false)
		runTest(s, primaryStatePendingCompression, true, false)
		runTest(s, primaryStateCompressing, true, false)
		runTest(s, primaryStatePendingUpload, true, false)
		runTest(s, primaryStateUploading, true, false)
		runTest(s, primaryStatePendingDeleteCompressed, true, false)
		runTest(s, primaryStateDeletingCompressed, true, false)
		runTest(s, primaryStatePendingDeleteRemotes, true, false)
		runTest(s, primaryStateDeletingRemotes, true, false)
		runTest(s, primaryStateDelayLocalDelete, true, false)
		runTest(s, primaryStatePendingDeleteLocal, true, false)
		runTest(s, primaryStateDeletingLocal, true, false)
		runTest(s, primaryStateComplete, true, true)
	}

	// State transitions that should not trigger the check idle files
	// condition.
	states = []int32{
		primaryStatePendingCompression,
		primaryStateCompressing,
		primaryStatePendingUpload,
		primaryStateUploading,
		primaryStatePendingDeleteCompressed,
		primaryStateDeletingCompressed,
		primaryStatePendingDeleteRemotes,
		primaryStateDeletingRemotes,
		primaryStateDelayLocalDelete,
		primaryStatePendingDeleteLocal,
		primaryStateDeletingLocal,
		primaryStateComplete,
	}
	for _, s := range states {
		runTest(s, primaryStateNew, false, false)
		runTest(s, primaryStateOpening, false, false)
		runTest(s, primaryStateInitializingRepls, false, false)
		runTest(s, primaryStateWaiting, false, false)
		runTest(s, primaryStateInserting, false, false)
		runTest(s, primaryStateReplicating, false, false)
		runTest(s, primaryStatePendingCompression, false, false)
		runTest(s, primaryStateCompressing, false, false)
		runTest(s, primaryStatePendingUpload, false, false)
		runTest(s, primaryStateUploading, false, false)
		runTest(s, primaryStatePendingDeleteCompressed, false, false)
		runTest(s, primaryStateDeletingCompressed, false, false)
		runTest(s, primaryStatePendingDeleteRemotes, false, false)
		runTest(s, primaryStateDeletingRemotes, false, false)
		runTest(s, primaryStateDelayLocalDelete, false, false)
		runTest(s, primaryStatePendingDeleteLocal, false, false)
		runTest(s, primaryStateDeletingLocal, false, false)
		runTest(s, primaryStateComplete, false, true)
	}
}
