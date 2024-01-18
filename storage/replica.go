package storage

import (
	"compress/gzip"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/liquidgecka/blobby/internal/compat"
	"github.com/liquidgecka/blobby/internal/delayqueue"
	"github.com/liquidgecka/blobby/internal/human"
	"github.com/liquidgecka/blobby/internal/logging"
	"github.com/liquidgecka/blobby/storage/fid"
	"github.com/liquidgecka/blobby/storage/hasher"
)

const (
	replicaStateNew = int32(iota)
	replicaStateOpening
	replicaStateWaiting
	replicaStateAppending
	replicaStateFailed
	replicaStatePendingCompression
	replicaStateCompressing
	replicaStatePendingUpload
	replicaStateUploading
	replicaStatePendingDelete
	replicaStateDeletingCompressed
	replicaStateClosingCompressed
	replicaStateDeleting
	replicaStateClosing
	replicaStateCompleted
)

var replicaStateStrings = map[int32]string{
	replicaStateNew:                "new",
	replicaStateOpening:            "opening",
	replicaStateWaiting:            "waiting",
	replicaStateAppending:          "appending",
	replicaStateFailed:             "failed",
	replicaStatePendingCompression: "pending-compression",
	replicaStateCompressing:        "compressing",
	replicaStatePendingUpload:      "pending-upload",
	replicaStateUploading:          "uploading",
	replicaStatePendingDelete:      "pending-delete",
	replicaStateDeletingCompressed: "deleting-compressed",
	replicaStateClosingCompressed:  "closing-compressed",
	replicaStateDeleting:           "deleting",
	replicaStateClosing:            "closing",
	replicaStateCompleted:          "completed",
}

type replica struct {
	// The file id. This is the data that will be used as the file name
	// portion of the file.
	fid    fid.FID
	fidStr string

	// The settings and storage object associated with this replica.
	settings *Settings
	storage  *Storage

	// Tracks where this fid will end up in S3.
	s3key string

	// Unlike primaries the Replicas can be talked with in parallel and
	// as such they need locking to project that condition.
	lock sync.Mutex

	// The state of this replica.
	state int32

	// The file descriptor to the open file on disk.
	fd *os.File

	// If the file needs to be compressed then this will be a pointer to the
	// compressed file descriptor.
	compressFd *os.File

	// The current write offset within the file.
	offset uint64

	// Tracks the amount of time that the replica has been in an uploadable
	// state for monitoring of upload failures.
	queuedForUpload time.Time

	// Each replica should receive a heart beat from the primary every so
	// often and if it doesn't then it will fail the replica and automatically
	// start the upload process. We keep a lock and a time stamp of the last
	// successful heart beat (or operation) that this replica has received.
	// We also keep a Token for the DelayQueue processor that we can reset
	// every time an operation is performed.
	heartBeatLock  sync.Mutex
	heartBeatLast  time.Time
	heartBeatToken delayqueue.Token

	// All logging will be routed through this logger.
	log *logging.Logger
}

// Called to initiate the compression state. This should compress the file
// locally so it can be uploaded to S3.
func (r *replica) Compress() {
	// Obtain the lock.
	r.lock.Lock()
	defer r.lock.Unlock()

	// Don't bother compressing a file with zero content.
	if r.offset == 0 {
		r.log.Debug("Skipping compression, the file has no content.")
		r.setState(replicaStatePendingDelete)
		return
	}

	// Set the state.
	r.setState(replicaStateCompressing)
	r.log.Debug("Compressing the file.")

	// Open the file that will store the compressed data long term.
	fpath := filepath.Join(r.settings.BaseDirectory, r.fidStr) + ".gz"
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND | os.O_TRUNC
	mode := os.FileMode(0644)
	var err error
	if r.compressFd, err = os.OpenFile(fpath, flags, mode); err != nil {
		// Log the error and then set the state to complete since the
		// file was not able to be opened on disk.
		r.log.Error(
			"Error opening compressed file",
			logging.NewField("file", fpath),
			logging.NewFieldIface("error", err))
		r.setState(replicaStatePendingCompression)
		return
	}

	// Seek back to the very start of the data file.
	if _, err = r.fd.Seek(0, io.SeekStart); err != nil {
		r.log.Error(
			"Error seeking in the data file.",
			logging.NewField("file", r.fd.Name()),
			logging.NewFieldIface("error", err))
		r.setState(replicaStatePendingCompression)
		return
	}

	// Create a gzip writer. Note that any error here is going to purely
	// be related to the compression level so its okay to just panic.
	zipper, err := gzip.NewWriterLevel(r.compressFd, r.settings.CompressLevel)
	if err != nil {
		panic(err)
	}

	// Copy data from the source file into the gzipper.
	buffer := [4096]byte{}
	if _, err = io.CopyBuffer(zipper, r.fd, buffer[:]); err != nil {
		r.log.Error(
			"Error generating the compressed data file.",
			logging.NewField("source-file", r.fd.Name()),
			logging.NewField("dest-file", r.compressFd.Name()),
			logging.NewFieldIface("error", err))
		r.setState(replicaStatePendingCompression)
		return
	}

	// Close out the gzip routine.
	if err = zipper.Close(); err != nil {
		r.log.Error(
			"Error closing the compressed file.",
			logging.NewField("file", r.compressFd.Name()),
			logging.NewFieldIface("error", err))
		r.setState(replicaStatePendingCompression)
		return
	}

	// Success.
	r.log.Info("Successfully compressed the data file.")
	r.setState(replicaStatePendingUpload)
}

// Called by the Deletable interface in order to perform the background
// deletion tasks.
func (r *replica) Delete() {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Metrics
	r.storage.metrics.ReplicaDeletes.IncTotal()
	r.setState(replicaStateClosing)

	// If defined then we need to remove and close the compressed file that
	// was generated prior to uploading.
	if r.compressFd != nil {
		// Remove the file.
		r.setState(replicaStateDeleting)
		r.storage.metrics.FilesDeleted.IncTotal()
		err := os.Remove(r.compressFd.Name())
		if err != nil && !os.IsNotExist(err) {
			// Error removing the file, not much we can do beside logging it
			// and retrying.
			r.log.Error(
				"There was an error removing the temporary compressed file.",
				logging.NewField("file", r.fd.Name()),
				logging.NewFieldIface("error", err))
			r.storage.metrics.FilesDeleted.IncFailures()
			r.setState(replicaStatePendingDelete)
		} else {
			// Success
			r.storage.metrics.FilesDeleted.IncSuccesses()
			r.log.Info("File removed from disk.")
		}

		// Close the file descriptor.
		r.setState(replicaStateClosing)
		if err := r.compressFd.Close(); err != nil {
			// Not much we can do here besides log.
			r.log.Warning(
				"Unable to close the temporary compressed file!",
				logging.NewField("file", r.compressFd.Name()),
				logging.NewFieldIface("error", err))
		}
		r.compressFd = nil
	}

	// Remove and close the primary data file.
	if r.fd != nil {
		// Remove the file.
		r.setState(replicaStateDeleting)
		r.storage.metrics.FilesDeleted.IncTotal()
		if err := os.Remove(r.fd.Name()); err != nil && !os.IsNotExist(err) {
			// Error removing the file, not much we can do beside logging it
			// and retrying.
			r.log.Error(
				"There was an error removing the file.",
				logging.NewField("file", r.fd.Name()),
				logging.NewFieldIface("error", err))
			r.storage.metrics.FilesDeleted.IncFailures()
			r.setState(replicaStatePendingDelete)
		} else {
			// Success
			r.storage.metrics.FilesDeleted.IncSuccesses()
			r.log.Info("File removed from disk.")
		}

		// Close the file descriptor.
		r.setState(replicaStateClosing)
		if err := r.fd.Close(); err != nil {
			// Not much we can do here besides log.
			r.log.Warning(
				"Unable to close the file!",
				logging.NewField("file", r.fd.Name()),
				logging.NewFieldIface("error", err))
		}
		r.fd = nil
	}

	// Everything is done.
	r.setState(replicaStateCompleted)
}

// Called to indicate that the primary has performed a heart beat
// call against the replica. If the replica is in the right state
// then this return nil, otherwise this will return an error
// explaining the problem.
func (r *replica) HeartBeat() error {
	r.heartBeatLock.Lock()
	defer r.heartBeatLock.Unlock()
	r.log.Debug("Heart beat received.")
	// When sending heart beats to the replica we accept them only if the
	// replica is actually in a state where it will be able to accept
	// updates otherwise it will return an error so that the primary
	// knows that the file is no longer available for writes.
	switch r.state {
	case replicaStateNew:
	case replicaStateOpening:
	case replicaStateWaiting:
	case replicaStateAppending:
	default:
		// Any other state is not valid and should return an error.
		return ErrWrongReplicaState{}
	}
	r.heartBeatLast = time.Now()
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		time.Now().Add(r.settings.HeartBeatTime),
		r.event)
	return nil
}

// Opens the file on disk for use as a Replica.
func (r *replica) Open() error {
	// Set the state before doing anything.
	r.setState(replicaStateOpening)

	// Open the actual file on disk.
	fpath := filepath.Join(r.settings.BaseDirectory, "r-"+r.fidStr)
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND
	mode := os.FileMode(0644)
	var err error
	r.log.Debug("Opening file")
	if r.fd, err = os.OpenFile(fpath, flags, mode); err != nil {
		r.log.Error(
			"Error opening file",
			logging.NewFieldIface("error", err))
		r.setState(replicaStateCompleted)
		return err
	}

	// TODO: Falloc support on linux?

	// Setup the DelayQueue Token that will be used to managing heart beat
	// timeouts and such.
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		time.Now().Add(r.settings.HeartBeatTime),
		r.event)

	// Set the replica state to Waiting which should automatically
	// initialize the heart beat timer as well.
	r.setState(replicaStateWaiting)
	r.log.Debug("New replica opened.")

	// Success!
	return nil
}

// Allows data to be replicated into a replica file.
func (r *replica) Replicate(rc RemoteReplicateConfig) error {
	// Lock to ensure that we only process one operation at a time
	// in this replica.
	r.lock.Lock()
	defer r.lock.Unlock()

	// If the replica is not in the waiting state then we can not
	// progress forward.
	if r.state != replicaStateWaiting {
		r.log.Warning(
			"Attempt to replicate to a replica not in a waiting state.",
			logging.NewField("state", replicaStateStrings[r.state]))
		return fmt.Errorf(""+
			"Attempt to replicate to a replica that is not waiting "+
			"for updates. The replica is in the %s state.",
			replicaStateStrings[r.state])
	}

	// Set the state to appending.
	r.setState(replicaStateAppending)

	// Check that the offset is actually correct so that we don't append
	// the data in the wrong place.
	if rcOffset := rc.Offset(); rcOffset != r.offset {
		// The requested offset does not match the current offset for
		// this file. This often means that the file has fallen behind
		// and needs to be marked as failed.
		if r.log.DebugEnabled() {
			r.log.Debug(
				"Attempt to replicate to the wrong offset in a replica.",
				logging.NewField(
					"current-offset",
					strconv.FormatUint(r.offset, 10)),
				logging.NewField(
					"request-offset",
					strconv.FormatUint(rcOffset, 10)))
		}
		r.setState(replicaStateFailed)
		return fmt.Errorf(""+
			"Attempt to replicate to a replica where the offsets do not "+
			"match. This is a synchronization error. (%d != %d)",
			r.offset,
			rcOffset)
	}

	// Setup a pass through hash calculator on the body of this
	// request so that we can verify the hash at the end of the
	// upload.
	hsum, err := hasher.Validator(rc.Hash(), r.fd)
	if err != nil {
		r.log.Error(
			"Error parsing client provided hash string.",
			logging.NewField("hash-str", rc.Hash()),
			logging.NewFieldIface("error", err))
		r.setState(replicaStateFailed)
		return err
	}

	// Copy the data into the file.
	buffer := [32 * 1024]byte{}
	if n, err := io.CopyBuffer(hsum, rc.GetBody(), buffer[:]); err != nil {
		r.log.Error(
			"Error copying data to the replica.",
			logging.NewFieldIface("error", err))
		r.setState(replicaStateFailed)
		return fmt.Errorf("Error writing to replica: %s", err.Error())
	} else if size := rc.Size(); size != uint64(n) {
		r.log.Error(
			"Short write while copying data to the replica.",
			logging.NewField(
				"expected-length",
				strconv.FormatUint(size, 10)),
			logging.NewField(
				"written-length",
				strconv.FormatInt(n, 10)))
		r.setState(replicaStateFailed)
		return fmt.Errorf(""+
			"Insufficient bytes received when writing data, "+
			"expected %d, but wrote %d",
			size,
			n)
	} else if !hsum.Check() {
		// Data was written to disk but the resulting hash was not correct.
		r.log.Error(
			"Received data did not match the expected checksum.",
			logging.NewField("expected-hash", rc.Hash()),
			logging.NewField("received-hash", hsum.Hash()))
		r.setState(replicaStateFailed)
		return fmt.Errorf(""+
			"The hash of the data written to disk (%s) does not match "+
			"the one we expected. (%s)",
			hsum.Hash(),
			rc.Hash())
	} else {
		r.offset += uint64(n)
	}

	// Reset the heart beat timer since inserts count as a heart beat.
	r.settings.DelayQueue.Alter(
		&r.heartBeatToken,
		time.Now().Add(r.settings.HeartBeatTime),
		r.event)

	// Put the file back in the Waiting state.
	r.setState(replicaStateWaiting)

	// Success!
	return nil
}

// Called to get the current status of this replica.
func (r *replica) Status() string {
	b := compat.Builder{}
	state := atomic.LoadInt32(&r.state)
	b.WriteString(r.fidStr)
	b.WriteString(" state=")
	b.WriteString(replicaStateStrings[state])
	b.WriteString(" size=")
	b.WriteString(human.Bytes(r.offset))
	return b.String()
}

// Called by the http interface in order to trigger the deletion process.
// This doesn't actually delete the file, it just gets the replica into
// the delete queue in the storage interface.
func (r *replica) QueueDelete() error {
	r.lock.Lock()
	defer r.lock.Unlock()

	// Check the current state, the file can only be moved to deleting
	// from a few key places in the flow.
	switch r.state {
	case replicaStateWaiting:
	case replicaStateFailed:

	// There are a few places where its actually okay to attempt to queue
	// a delete as well. In these states the file is already deleting so
	// there is no danger accepting the call.
	case replicaStatePendingDelete:
		fallthrough
	case replicaStateClosingCompressed:
		fallthrough
	case replicaStateDeletingCompressed:
		fallthrough
	case replicaStateClosing:
		fallthrough
	case replicaStateDeleting:
		fallthrough
	case replicaStateCompleted:
		return nil

	// All other states should return an error.
	default:
		r.log.Error(
			"Attempting to delete a file from the wrong state.",
			logging.NewField("current-state", replicaStateStrings[r.state]))
		return fmt.Errorf(
			"Can not delete the replica, its in the wrong state: %s",
			replicaStateStrings[r.state])
	}

	// Set the state to pending delete.
	r.setState(replicaStatePendingDelete)
	return nil
}

// Called to perform the upload of the underlying data in the replica.
func (r *replica) Upload() {
	// Obtain the lock.
	r.lock.Lock()
	defer r.lock.Unlock()

	// Don't bother uploading a file with zero content.
	if r.offset == 0 {
		r.log.Debug("Skipping compression, the file has no content.")
		r.setState(replicaStatePendingDelete)
		return
	}

	// Set the state
	r.storage.metrics.ReplicaUploads.IncTotal()
	r.setState(replicaStateUploading)
	r.log.Debug("Starting upload.")

	// Attempt the upload.
	fd := r.fd
	if r.settings.Compress {
		fd = r.compressFd
	}
	if !uploadToS3(fd, r.fid, r.s3key, r.settings, r.log) {
		r.log.Warning("Requeuing for upload.")
		r.setState(replicaStatePendingUpload)
		r.storage.metrics.ReplicaUploads.IncFailures()
		return
	} else {
		r.setState(replicaStatePendingDelete)
		r.storage.metrics.ReplicaUploads.IncSuccesses()
	}
}

// Called when the DelayQueue event is triggered. This can only really happen
// if the primary has not properly sent a heart beat in a reasonable amount of
// time. This gets called in a goroutine via the DelayQueue worker.
func (r *replica) event() {
	// Obtain the lock.
	r.lock.Lock()
	defer r.lock.Unlock()

	// If the current replica is not in a state where this matters then we
	// should ignore the operation as the state can not be changed. Note
	// that we hold the lock which means that we should not be in the appending
	// state.
	switch r.state {
	case replicaStateWaiting:
	case replicaStateFailed:
	default:
		return
	}

	// Metrics
	atomic.AddInt64(&r.storage.metrics.ReplicaOrphaned, 1)

	// Log something so the users can track what is happening.
	r.log.Warning("The replica has been orphaned.")

	// The replica has been orphaned. We need to either upload it if it has
	// data, or remote if it is empty.
	if r.offset == 0 {
		r.log.Info("The replica is empty, no need to upload it.")
		r.setState(replicaStatePendingDelete)
	} else if r.settings.Compress {
		r.setState(replicaStatePendingCompression)
	} else {
		r.setState(replicaStatePendingUpload)
	}
}

// Sets the state of the replica in an atomic way.
func (r *replica) setState(n int32) {
	// Change the state locally.
	oldN := atomic.SwapInt32(&r.state, n)
	if r.log.DebugEnabled() {
		r.log.Debug(
			"state changed.",
			logging.NewField("old-state", replicaStateStrings[oldN]),
			logging.NewField("new-state", replicaStateStrings[n]),
		)
	}

	// If the state has moved to one that no longer needs heart beat timers
	// then we need to cancel the replica token.
	switch n {
	case replicaStateWaiting:
	case replicaStateAppending:
	case replicaStateFailed:
	default:
		r.settings.DelayQueue.Cancel(&r.heartBeatToken)
	}

	// If the file has moved into an uploadable state then we need to track
	// it, and if its been uploaded then we need to remove that tracking.
	switch n {
	case replicaStatePendingUpload:
		if r.queuedForUpload == (time.Time{}) {
			r.queuedForUpload = time.Now()
		}
	case replicaStatePendingDelete:
		r.queuedForUpload = time.Time{}
	}

	// Depending on the current state we need to add work to a workqueue.
	switch n {
	case replicaStatePendingCompression:
		r.settings.CompressWorkQueue.Insert(r.Compress)
	case replicaStatePendingUpload:
		r.settings.UploadWorkQueue.Insert(r.Upload)
	case replicaStatePendingDelete:
		r.settings.DeleteLocalWorkQueue.Insert(r.Delete)
	case replicaStateCompleted:
		r.storage.replicaCompleted(r)
	}
}
