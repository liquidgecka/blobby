package storage

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"math/rand"
	"testing"
	"time"

	"bou.ke/monkey"
	"github.com/liquidgecka/testlib"

	"github.com/liquidgecka/blobby/internal/delayqueue"
	"github.com/liquidgecka/blobby/internal/workqueue"
)

func TestPrimary_Insert(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Mock out storage.primaryStateChange to do nothing.
	stateChange := 0
	defer monkey.Patch(
		(*Storage).primaryStateChange,
		func(s *Storage, p *primary, o, n int32) {
			switch stateChange {
			case 0:
				T.Equal(o, primaryStateWaiting)
				T.Equal(n, primaryStateInserting)
			case 1:
				T.Equal(o, primaryStateInserting)
				T.Equal(n, primaryStateReplicating)
			case 2:
				T.Equal(o, primaryStateReplicating)
				T.Equal(n, primaryStateWaiting)
			default:
				T.Fatalf("Unexpected state change.")
			}
			stateChange += 1
		},
	).Unpatch()

	// Mock out time.Now() so we can control its return,
	mockTime := time.Date(2020, time.February, 20, 2, 2, 2, 2, time.UTC)
	defer monkey.Patch(time.Now, func() time.Time {
		return mockTime
	}).Unpatch()

	// Mock out the DelayQueue.Alter function so no alteration is
	// actually attempted.
	defer monkey.Patch(
		(*delayqueue.DelayQueue).Alter,
		func(
			queue *delayqueue.DelayQueue,
			token *delayqueue.Token,
			t time.Time,
			f func(),
		) {
			return
		},
	).Unpatch()

	// Setup a "remote" that the data can be replicated too.
	remote := testRemote{
		name: "test_remote",
		replicate: func(rc RemoteReplicateConfig) (bool, error) {
			return false, nil
		},
	}

	// Setup an ephemeral primary to work with.
	p := primary{
		fd:      T.TempFile(),
		log:     NewTestLogger(),
		state:   primaryStateWaiting,
		offset:  1000,
		storage: &Storage{},
		remotes: []Remote{&remote},
		settings: &Settings{
			UploadLargerThan: 1024 * 1024 * 1024,
			Compress:         false,
		},
	}
	p.fd.Write(make([]byte, int(p.offset)))

	// Setup some data to insert.
	raw := make([]byte, 1024)
	rand.Read(raw)
	insertData := InsertData{
		Source: bytes.NewBuffer(raw),
		Length: int64(len(raw)),
		Tracer: nil,
	}

	// Perform the insert.
	id, err := p.Insert(&insertData)
	T.ExpectSuccess(err)
	T.NotEqual(id, "")

	// Check that all the appropriate things were modified.
	T.Equal(p.offset, uint64(1000+len(raw)))
	T.NotEqual(p.storage.metrics.PrimaryInsertWriteNanoseconds, int64(0))
	T.NotEqual(p.storage.metrics.PrimaryInsertReplicateNanoseconds, int64(0))
	T.Equal(p.firstInsert, mockTime)

	// Read the file and validate that everything expected was written to it.
	contents, err := ioutil.ReadFile(p.fd.Name())
	T.ExpectSuccess(err)
	expected := make([]byte, 1000+len(raw))
	copy(expected[1000:1000+len(raw)], raw)
	T.Equal(contents, expected)
}

func TestPrimary_Insert_ReadError(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Mock out storage.primaryStateChange to do nothing.
	stateChange := 0
	defer monkey.Patch(
		(*Storage).primaryStateChange,
		func(s *Storage, p *primary, o, n int32) {
			switch stateChange {
			case 0:
				T.Equal(o, primaryStateWaiting)
				T.Equal(n, primaryStateInserting)
			case 1:
				T.Equal(o, primaryStateInserting)
				T.Equal(n, primaryStateWaiting)
			default:
				T.Fatalf("Unexpected state change.")
			}
			stateChange += 1
		},
	).Unpatch()

	// Mock out time.Now() so we can control its return,
	mockTime := time.Date(2020, time.February, 20, 2, 2, 2, 2, time.UTC)
	defer monkey.Patch(time.Now, func() time.Time {
		return mockTime
	}).Unpatch()

	// Mock out the DelayQueue.Alter function so no alteration is
	// actually attempted.
	defer monkey.Patch(
		(*delayqueue.DelayQueue).Alter,
		func(
			queue *delayqueue.DelayQueue,
			token *delayqueue.Token,
			t time.Time,
			f func(),
		) {
			return
		},
	).Unpatch()

	// Setup a "remote" that the data can be replicated too.
	remote := testRemote{
		name: "test_remote",
		replicate: func(rc RemoteReplicateConfig) (bool, error) {
			return false, nil
		},
	}

	// Setup an ephemeral primary to work with.
	p := primary{
		fd:      T.TempFile(),
		log:     NewTestLogger(),
		state:   primaryStateWaiting,
		offset:  1000,
		storage: &Storage{},
		remotes: []Remote{&remote},
		settings: &Settings{
			UploadLargerThan: 1024 * 1024 * 1024,
			Compress:         false,
		},
	}
	p.fd.Write(make([]byte, int(p.offset)))

	// Setup some data to insert.
	insertData := InsertData{
		Source: &testReader{
			read: func([]byte) (int, error) {
				return 0, fmt.Errorf("EXPECTED")
			},
		},
		Length: int64(1024),
		Tracer: nil,
	}

	// Perform the insert.
	id, err := p.Insert(&insertData)
	T.ExpectErrorMessage(err, "EXPECTED")
	T.Equal(id, "")

	// Check that all the appropriate things were modified.
	T.Equal(p.offset, uint64(1000))
	T.Equal(p.storage.metrics.InternalInsertErrors, int64(0))

	// Read the file and validate that everything expected was written to it.
	contents, err := ioutil.ReadFile(p.fd.Name())
	T.ExpectSuccess(err)
	expected := make([]byte, 1000)
	T.Equal(contents, expected)
}

func TestPrimary_Insert_RemoteIsNil(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Mock out storage.primaryStateChange to do nothing.
	stateChange := 0
	defer monkey.Patch(
		(*Storage).primaryStateChange,
		func(s *Storage, p *primary, o, n int32) {
			switch stateChange {
			case 0:
				T.Equal(o, primaryStateWaiting)
				T.Equal(n, primaryStateInserting)
			case 1:
				T.Equal(o, primaryStateInserting)
				T.Equal(n, primaryStateReplicating)
			case 2:
				T.Equal(o, primaryStateReplicating)
				T.Equal(n, primaryStatePendingUpload)
			default:
				T.Fatalf("Unexpected state change.")
			}
			stateChange += 1
		},
	).Unpatch()

	// Mock out time.Now() so we can control its return,
	mockTime := time.Date(2020, time.February, 20, 2, 2, 2, 2, time.UTC)
	defer monkey.Patch(time.Now, func() time.Time {
		return mockTime
	}).Unpatch()

	// Mock out the DelayQueue.Alter function so no alteration is
	// actually attempted.
	defer monkey.Patch(
		(*delayqueue.DelayQueue).Alter,
		func(
			queue *delayqueue.DelayQueue,
			token *delayqueue.Token,
			t time.Time,
			f func(),
		) {
			return
		},
	).Unpatch()

	// Also mock out the insert function to prevent new work from being
	// scheduled.
	defer monkey.Patch(
		(*workqueue.WorkQueue).Insert,
		func(q *workqueue.WorkQueue, f func()) {
			return
		},
	).Unpatch()

	// Setup an ephemeral primary to work with.
	p := primary{
		fd:      T.TempFile(),
		log:     NewTestLogger(),
		state:   primaryStateWaiting,
		offset:  1000,
		storage: &Storage{},
		remotes: []Remote{nil},
		settings: &Settings{
			UploadLargerThan: 1024 * 1024 * 1024,
			Compress:         false,
		},
	}
	p.fd.Write(make([]byte, int(p.offset)))

	// Setup some data to insert.
	raw := make([]byte, 1024)
	rand.Read(raw)
	insertData := InsertData{
		Source: bytes.NewBuffer(raw),
		Length: int64(len(raw)),
		Tracer: nil,
	}

	// Perform the insert.
	id, err := p.Insert(&insertData)
	T.ExpectErrorMessage(
		err,
		"Replication failed: Remote failed on a previous step.")
	T.Equal(id, "")

	// Check that all the appropriate things were modified.
	T.Equal(p.offset, uint64(1000))
	T.Equal(p.storage.metrics.InternalInsertErrors, int64(1))

	// Read the file and validate that everything expected was written to it.
	contents, err := ioutil.ReadFile(p.fd.Name())
	T.ExpectSuccess(err)
	expected := make([]byte, 1000)
	T.Equal(contents, expected)
}

func TestPrimary_Insert_ReplicationFails(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Mock out storage.primaryStateChange to do nothing.
	stateChange := 0
	defer monkey.Patch(
		(*Storage).primaryStateChange,
		func(s *Storage, p *primary, o, n int32) {
			switch stateChange {
			case 0:
				T.Equal(o, primaryStateWaiting)
				T.Equal(n, primaryStateInserting)
			case 1:
				T.Equal(o, primaryStateInserting)
				T.Equal(n, primaryStateReplicating)
			case 2:
				T.Equal(o, primaryStateReplicating)
				T.Equal(n, primaryStatePendingUpload)
			default:
				T.Fatalf("Unexpected state change.")
			}
			stateChange += 1
		},
	).Unpatch()

	// Mock out time.Now() so we can control its return,
	mockTime := time.Date(2020, time.February, 20, 2, 2, 2, 2, time.UTC)
	defer monkey.Patch(time.Now, func() time.Time {
		return mockTime
	}).Unpatch()

	// Mock out the DelayQueue.Alter function so no alteration is
	// actually attempted.
	defer monkey.Patch(
		(*delayqueue.DelayQueue).Alter,
		func(
			queue *delayqueue.DelayQueue,
			token *delayqueue.Token,
			t time.Time,
			f func(),
		) {
			return
		},
	).Unpatch()

	// Also mock out the insert function to prevent new work from being
	// scheduled.
	defer monkey.Patch(
		(*workqueue.WorkQueue).Insert,
		func(q *workqueue.WorkQueue, f func()) {
			return
		},
	).Unpatch()

	// Setup a "remote" that the data can be replicated too.
	remote := testRemote{
		name: "test_remote",
		replicate: func(rc RemoteReplicateConfig) (bool, error) {
			return false, fmt.Errorf("EXPECTED")
		},
	}

	// Setup an ephemeral primary to work with.
	p := primary{
		fd:      T.TempFile(),
		log:     NewTestLogger(),
		state:   primaryStateWaiting,
		offset:  1000,
		storage: &Storage{},
		remotes: []Remote{&remote},
		settings: &Settings{
			UploadLargerThan: 1024 * 1024 * 1024,
			Compress:         false,
		},
	}
	p.fd.Write(make([]byte, int(p.offset)))

	// Setup some data to insert.
	raw := make([]byte, 1024)
	rand.Read(raw)
	insertData := InsertData{
		Source: bytes.NewBuffer(raw),
		Length: int64(len(raw)),
		Tracer: nil,
	}

	// Perform the insert.
	id, err := p.Insert(&insertData)
	T.ExpectErrorMessage(err, "Replication failed: test_remote: EXPECTED")
	T.Equal(id, "")

	// Check that all the appropriate things were modified.
	T.Equal(p.offset, uint64(1000))
	T.Equal(p.storage.metrics.InternalInsertErrors, int64(1))

	// Read the file and validate that everything expected was written to it.
	contents, err := ioutil.ReadFile(p.fd.Name())
	T.ExpectSuccess(err)
	expected := make([]byte, 1000)
	T.Equal(contents, expected)
}

func TestPrimary_Insert_ShortRead(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Mock out storage.primaryStateChange to do nothing.
	stateChange := 0
	defer monkey.Patch(
		(*Storage).primaryStateChange,
		func(s *Storage, p *primary, o, n int32) {
			switch stateChange {
			case 0:
				T.Equal(o, primaryStateWaiting)
				T.Equal(n, primaryStateInserting)
			case 1:
				T.Equal(o, primaryStateInserting)
				T.Equal(n, primaryStateWaiting)
			default:
				T.Fatalf("Unexpected state change.")
			}
			stateChange += 1
		},
	).Unpatch()

	// Mock out time.Now() so we can control its return,
	mockTime := time.Date(2020, time.February, 20, 2, 2, 2, 2, time.UTC)
	defer monkey.Patch(time.Now, func() time.Time {
		return mockTime
	}).Unpatch()

	// Mock out the DelayQueue.Alter function so no alteration is
	// actually attempted.
	defer monkey.Patch(
		(*delayqueue.DelayQueue).Alter,
		func(
			queue *delayqueue.DelayQueue,
			token *delayqueue.Token,
			t time.Time,
			f func(),
		) {
			return
		},
	).Unpatch()

	// Setup a "remote" that the data can be replicated too.
	remote := testRemote{
		name: "test_remote",
		replicate: func(rc RemoteReplicateConfig) (bool, error) {
			return false, nil
		},
	}

	// Setup an ephemeral primary to work with.
	p := primary{
		fd:      T.TempFile(),
		log:     NewTestLogger(),
		state:   primaryStateWaiting,
		offset:  1000,
		storage: &Storage{},
		remotes: []Remote{&remote},
		settings: &Settings{
			UploadLargerThan: 1024 * 1024 * 1024,
			Compress:         false,
		},
	}
	p.fd.Write(make([]byte, int(p.offset)))

	// Setup some data to insert.
	raw := make([]byte, 1024)
	rand.Read(raw)
	insertData := InsertData{
		Source: bytes.NewBuffer(raw),
		Length: int64(len(raw) * 2),
		Tracer: nil,
	}

	// Perform the insert.
	id, err := p.Insert(&insertData)
	T.ExpectErrorMessage(err, "Short read from client.")
	T.Equal(id, "")

	// Check that all the appropriate things were modified.
	T.Equal(p.offset, uint64(1000))
	T.Equal(p.storage.metrics.InternalInsertErrors, int64(0))

	// Read the file and validate that everything expected was written to it.
	contents, err := ioutil.ReadFile(p.fd.Name())
	T.ExpectSuccess(err)
	expected := make([]byte, 1000)
	T.Equal(contents, expected)
}

func TestPrimary_Insert_WriteError(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Mock out storage.primaryStateChange to do nothing.
	stateChange := 0
	defer monkey.Patch(
		(*Storage).primaryStateChange,
		func(s *Storage, p *primary, o, n int32) {
			switch stateChange {
			case 0:
				T.Equal(o, primaryStateWaiting)
				T.Equal(n, primaryStateInserting)
			case 1:
				T.Equal(o, primaryStateInserting)
				T.Equal(n, primaryStatePendingUpload)
			default:
				T.Fatalf("Unexpected state change.")
			}
			stateChange += 1
		},
	).Unpatch()

	// Mock out time.Now() so we can control its return,
	mockTime := time.Date(2020, time.February, 20, 2, 2, 2, 2, time.UTC)
	defer monkey.Patch(time.Now, func() time.Time {
		return mockTime
	}).Unpatch()

	// Mock out the DelayQueue.Alter function so no alteration is
	// actually attempted.
	defer monkey.Patch(
		(*delayqueue.DelayQueue).Alter,
		func(
			queue *delayqueue.DelayQueue,
			token *delayqueue.Token,
			t time.Time,
			f func(),
		) {
			return
		},
	).Unpatch()

	// Also mock out the insert function to prevent new work from being
	// scheduled.
	defer monkey.Patch(
		(*workqueue.WorkQueue).Insert,
		func(q *workqueue.WorkQueue, f func()) {
			return
		},
	).Unpatch()

	// Setup a "remote" that the data can be replicated too.
	remote := testRemote{
		name: "test_remote",
		replicate: func(rc RemoteReplicateConfig) (bool, error) {
			return false, nil
		},
	}

	// Setup an ephemeral primary to work with.
	p := primary{
		fd:      T.TempFile(),
		log:     NewTestLogger(),
		state:   primaryStateWaiting,
		offset:  1000,
		storage: &Storage{},
		remotes: []Remote{&remote},
		settings: &Settings{
			UploadLargerThan: 1024 * 1024 * 1024,
			Compress:         false,
		},
	}
	p.fd.Write(make([]byte, int(p.offset)))

	// Setup some data to insert.
	raw := make([]byte, 1024)
	rand.Read(raw)
	insertData := InsertData{
		Source: bytes.NewBuffer(raw),
		Length: int64(len(raw)),
		Tracer: nil,
	}

	// Clsoe the fd so writes to it fail.
	T.ExpectSuccess(p.fd.Close())

	// Perform the insert.
	id, err := p.Insert(&insertData)
	T.ExpectErrorMessage(err, "file already closed")
	T.Equal(id, "")

	// Check that all the appropriate things were modified.
	T.Equal(p.offset, uint64(1000))
	T.Equal(p.storage.metrics.InternalInsertErrors, int64(1))

	// Read the file and validate that everything expected was written to it.
	contents, err := ioutil.ReadFile(p.fd.Name())
	T.ExpectSuccess(err)
	expected := make([]byte, 1000)
	T.Equal(contents, expected)
}

func TestPrimary_State(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Mock out time.Now() so we can control its return,
	mockTime := time.Date(2020, time.February, 20, 2, 2, 2, 2, time.UTC)
	defer monkey.Patch(time.Now, func() time.Time {
		return mockTime
	}).Unpatch()
	p := primary{
		fidStr:      "fidTest",
		state:       primaryStateOpening,
		offset:      10000,
		firstInsert: time.Date(2020, time.February, 20, 2, 1, 1, 2, time.UTC),
		remotes: []Remote{
			&testRemote{name: "rem1"},
			&testRemote{name: "rem2"},
		},
	}
	T.Equal(
		p.Status(),
		"fidTest state=opening size=10kB oldest=1m1s remotes=rem1,rem2")
}
