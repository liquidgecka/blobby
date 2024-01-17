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

	"github.com/iterable/blobby/internal/compat"
	"github.com/iterable/blobby/internal/delayqueue"
	"github.com/iterable/blobby/internal/errors"
	"github.com/iterable/blobby/internal/human"
	"github.com/iterable/blobby/internal/logging"
	"github.com/iterable/blobby/internal/tracing"
	"github.com/iterable/blobby/storage/fid"
	"github.com/iterable/blobby/storage/hasher"
	"github.com/iterable/blobby/storage/iohelp"
)

const (
	primaryStateNew = int32(iota)
	primaryStateOpening
	primaryStateInitializingRepls
	primaryStateWaiting
	primaryStateInserting
	primaryStateReplicating
	primaryStatePendingCompression
	primaryStateCompressing
	primaryStatePendingUpload
	primaryStateUploading
	primaryStatePendingDeleteCompressed
	primaryStateDeletingCompressed
	primaryStatePendingDeleteRemotes
	primaryStateDeletingRemotes
	primaryStateDelayLocalDelete
	primaryStatePendingDeleteLocal
	primaryStateDeletingLocal
	primaryStateComplete
)

var primaryStateStrings = map[int32]string{
	primaryStateNew:                     "new",
	primaryStateOpening:                 "opening",
	primaryStateInitializingRepls:       "initializing-replicas",
	primaryStateWaiting:                 "waiting",
	primaryStateInserting:               "inserting",
	primaryStateReplicating:             "replicating",
	primaryStatePendingCompression:      "pending-compression",
	primaryStateCompressing:             "compressing",
	primaryStatePendingUpload:           "pending-upload",
	primaryStateUploading:               "uploading",
	primaryStatePendingDeleteCompressed: "pending-delete-compressed",
	primaryStateDeletingCompressed:      "deleting-compressed",
	primaryStatePendingDeleteRemotes:    "pending-delete-remotes",
	primaryStateDeletingRemotes:         "deleting-remotes",
	primaryStateDelayLocalDelete:        "delete-delay",
	primaryStatePendingDeleteLocal:      "pending-delete-local",
	primaryStateDeletingLocal:           "deleting-local",
	primaryStateComplete:                "complete",
}

// Used when returning the "replica is shutting down error". This defined
// at the top level so we can prevent allocations for each replica every
// time the log line is generated.
var (
	replicaIsShuttingDownError = errors.New("Replica is shutting down.")
)

type primary struct {
	// The file id. This is the data that will be used as the file name
	// portion of the file.
	fid    fid.FID
	fidStr string

	// Tracks the eventual destination that this primary will be written
	// too. This is used in Blast Path reads as well as during the
	// upload phase.
	s3key string

	// Settings associated with this storage namespace and the storage
	// object that created this primary.
	settings *Settings
	storage  *Storage

	// The file descriptor to the open file on disk.
	fd *os.File

	// If the file was compressed for uploading then this will hold
	// on to the open file descriptor for that file.
	compressFd *os.File

	// Tracks the current state of this file.
	state int32

	// When delaying the file delete this is the Token used with the
	// DelayQueue.
	delayDeleteToken delayqueue.Token

	// Each primary file is allowed to exist for a limited amount of time
	// after which it is supposed to be expired and automatically uploaded.
	// This Token is used for keeping track of that process.
	expireToken delayqueue.Token

	// For tracking the last time a heart beat was sent to the
	// replicas. Note that we also need to keep sending heart beats
	// to replicas until the DELETE call is performed since they
	// won't know about uploading states.
	heartBeatTime  time.Time
	heartBeatTimer *time.Timer

	// We use a delayqueue.Token for tracking when a heart beat should be
	// issued to the replicas.
	heartBeatToken delayqueue.Token

	// The current write offset within the file.
	offset uint64

	// A list of all Blobby instances that also contain a copy of this
	// file. This is used during recovery to find the instance with the
	// most complete dataset. We also keep a list that is a 1:1 mapping
	// of the Remote implementation to a bool that defines if the given
	// remote has failed.
	remotes       []Remote
	failedRemotes []bool

	// If this primary has had an error of any sort then this will be set
	// to true to indicate that it is now unhealthy.
	unhealthy bool

	// Time stamp of first insert, not file creation. This is used for tracking
	// the oldest time of data that may exist on disk.
	firstInsert time.Time

	// When a file transitions into an "Uploading" state this time gets set.
	// Its used to track how long the oldest uploadable data is in Blobby.
	queuedForUpload time.Time

	// This logger will be used for logging events that happen specifically
	// when processing this primary.
	log *logging.Logger

	// The Storage object that manages this primary will use this
	// to keep a list of priority ordered idle primary files that
	// can be selected to perform an append when a client calls.
	//
	// These two values are used in that process, next being used
	// to turn this object into a node in a linked list, and
	// expires being used to set the priority of the node.
	next    *primary
	expires int64
}

// Inserts data into this primary. The data will be read from the given reader,
// and written to disk. The id returned can be used to look up this data
// later if needed.
func (p *primary) Insert(data *InsertData) (string, error) {
	// Setup a tracer that will track the time taken inside of the Insert()
	// call.
	trace := data.Tracer.NewChild("storage/(primary.Insert)")
	defer trace.End()

	// Update the primary file state.
	p.setState(primaryStateInserting)

	// Setup a hasher that we can use to get the Highway hash of the data
	// that was written to disk.
	hsum, err := hasher.Computer("hh", io.Writer(p.fd))
	if err != nil {
		// This shouldn't ever happen, as such its ALWAYS a panic.
		atomic.AddInt64(&p.storage.metrics.InternalInsertErrors, 1)
		panic(err)
	}

	// Keep the starting offset of the data that is being written to
	// disk. This is used to set the start maker and to roll back to
	// if needed.
	start := p.offset

	// This is a function wrapper that helps unwind the insert in case
	// anything goes wrong.
	truncate := func(canCont bool) {
		// Track truncation time in the tracer.
		defer trace.NewChild("storage/(primary.Insert):truncating").End()

		// Attempt to truncate the file back to where it was prior to the
		// start of this operation. If that is successful then we can
		// return this file to the Inserting state, otherwise we need
		// to mark it as failed.
		if err := p.fd.Truncate(int64(start)); err == nil {
			if canCont {
				p.setState(primaryStateWaiting)
				return
			}
		} else {
			// Since the file couldn't be returned to service we have to
			// move it to the uploading state.
			p.log.Warning(
				"Additional error truncating file.",
				logging.NewFieldIface("error", err))
		}

		// This file needs to be kicked into the next state depending on
		// its current condition. If its empty then it can skip uploading,
		// otherwise it needs to have the delete cycle started.
		p.shutdown()
	}

	// Copy the data from the reader into the file. Note that if a gzipper
	// is used then length will be incorrect as it will represent the number
	// of bytes read from the client.
	writeStart := time.Now()
	copyTrace := trace.NewChild("storage/(primary.Insert):copying")
	buffer := [1024 * 32]byte{}
	length, derr, rerr := iohelp.CopyBuffer(hsum, data.Source, buffer[:])
	copyTrace.End()
	atomic.AddUint64(
		&p.storage.metrics.PrimaryInsertWriteNanoseconds,
		uint64(time.Since(writeStart)))
	if rerr != nil {
		// Errors on read typically indicate that there was a problem with
		// the HTTP connection or request in some way. These are not internal
		// errors but we still need to roll back the write to disk.
		// We log these at a debug level since they can be common if a
		// client is not well behaved.
		if p.log.DebugEnabled() {
			p.log.Debug(
				"Error reading data from the client.",
				logging.NewFieldIface("error", rerr))
		}
		truncate(true)
		return "", rerr
	} else if derr != nil {
		// An error writing to disk is completely unexpected and an internal
		// error. We need to log this at the Error level so the user
		// can debug what went wrong.
		atomic.AddInt64(&p.storage.metrics.InternalInsertErrors, 1)
		p.log.Error(
			"Error copying data into the file.",
			logging.NewFieldIface("error", derr))
		truncate(true)
		return "", derr
	} else if data.Length != length && data.Length > 0 {
		// The data written to the local disk was not as long as the data
		// the client was expected to send us.
		p.log.Warning(
			"Insufficient data while reading from the client.",
			logging.NewFieldInt64("expected-bytes", data.Length),
			logging.NewFieldInt64("received-bytes", length))
		truncate(true)
		return "", errors.New("Short read from client.")
	} else if p.log.DebugEnabled() {
		p.log.Debug(
			"Copied data from source.",
			logging.NewFieldInt64("bytes", length))
	}

	// Update the primary file state.
	p.setState(primaryStateReplicating)

	// Since we won't know when the replication step completes and the
	// replicas mark themselves as having received a heart beat we need
	// to update our heart beat timer before we even make the call.
	// This ensures that we initiate a new call before the time runs
	// out with plenty of time to spare.
	p.resetHeartBeatTimer()

	// Now that the data is on disk we can attempt to replicate it to
	// the Remotes. By writing to disk locally first we allow multiple
	// reads from disk rather than having to buffer it in memory. If
	// there is free memory then we should still have the data in disk
	// cache anyway.
	rc := replicatorConfig{
		end:       start + uint64(length),
		fd:        p.fd,
		fid:       p.fidStr,
		hash:      hsum.Hash(),
		namespace: p.settings.NameSpace,
		start:     start,
	}
	wg := sync.WaitGroup{}
	fields := make([]logging.Field, len(p.remotes))
	errs := make([]error, len(p.remotes))
	errCount := int32(0)
	replicaTrace := trace.NewChild("storage/(primary.Insert):replicate")
	replicateStart := time.Now()
	shuttingDown := int32(0)
	for i, remote := range p.remotes {
		if remote == nil {
			// The remote was previously marked as failed and as such
			// we need to basically automatically mark its slot as failed
			// so the insert fails.
			ei := atomic.AddInt32(&errCount, 1) - 1
			errs[ei] = fmt.Errorf("Remote failed on a previous step.")
			fields[int(ei)] = logging.NewFieldIface(
				"replica-"+strconv.Itoa(i)+"-error",
				fmt.Errorf("Remote previously failed."))
			continue
		}
		is := strconv.Itoa(i)
		t := replicaTrace.NewChild(
			"storage/(primary.Insert):replicate(replia-" + is + ")",
		)
		wg.Add(1)
		go func(i int, is string, remote Remote, trace *tracing.Trace) {
			defer wg.Done()
			defer trace.End()
			if shutDown, err := remote.Replicate(&rc); err != nil {
				ei := atomic.AddInt32(&errCount, 1) - 1
				errs[ei] = fmt.Errorf("%s: %s", remote.String(), err.Error())
				fields[int(ei)] = logging.NewFieldIface(
					"replica-"+is+"-error",
					err)
			} else if shutDown {
				atomic.AddInt32(&shuttingDown, 1)
			}
		}(i, is, remote, t)
	}
	wg.Wait()
	replicaTrace.End()
	atomic.AddUint64(
		&p.storage.metrics.PrimaryInsertReplicateNanoseconds,
		uint64(time.Since(replicateStart)))

	// If replication failed to a host then we need to handle that.
	if errCount > 0 {
		// Log that the replication failed for tracking. This is not a normal
		// case so this can be logged at the Warning level. Since we have
		// committed to disk already we can not roll back here, as such we
		// need to truncate and immediately move to the Uploading state.
		atomic.AddInt64(&p.storage.metrics.InternalInsertErrors, 1)
		p.log.Warning("Replication failed.", fields[0:errCount]...)
		truncate(false)
		return "", errors.NewMultipleError(
			"Replication failed",
			errs[0:errCount])
	} else {
		p.log.Debug("Replicas accepted the update.")
	}

	// Set the new offset for the next write to the file.
	p.offset += uint64(length)
	p.log.Debug("Insertion successful.")

	// If there have been no inserts in the primary yet then set the first
	// insert time.
	if p.firstInsert == (time.Time{}) {
		p.firstInsert = time.Now()
	}

	// Return the id for the data generated.
	atomic.AddInt64(&p.storage.metrics.BytesInserted, length)
	fid := p.fid.ID(start, uint32(length))
	if p.log.DebugEnabled() {
		p.log.Debug(
			"Insertion completed.",
			logging.NewFieldUint64("start-offset", start),
			logging.NewFieldInt64("length", length),
			logging.NewField("fid", fid))
	}

	// If the file is larger than is allowed via our settings then
	// we need to transition into uploading here, otherwise we need
	// to transition back into the waiting state to signal that we
	// are able to accept more data.
	if shuttingDown > 0 {
		p.log.Debug(
			"At least one replica is shutting down. Queuing for upload.")
		if p.settings.Compress {
			p.setState(primaryStatePendingCompression)
		} else {
			p.setState(primaryStatePendingUpload)
		}
	} else if p.offset > p.settings.UploadLargerThan {
		p.log.Debug("File is too large, queuing for upload.")
		if p.settings.Compress {
			p.setState(primaryStatePendingCompression)
		} else {
			p.setState(primaryStatePendingUpload)
		}
	} else {
		p.log.Debug("File can still be grown.")
		p.setState(primaryStateWaiting)
	}

	return fid, nil
}

// Opens the file on disk, and finds remotes to start replicating too.
func (p *primary) Open() bool {
	// Setup the failedRemotes array. This is used for tracking which
	// remotes must receive the Delete() operation in order for a file
	// delete to be considered successful.
	p.failedRemotes = make([]bool, len(p.remotes))

	// Open the file on disk so we have a workable file descriptor.
	p.setState(primaryStateOpening)
	fpath := filepath.Join(p.settings.BaseDirectory, p.fidStr)
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND
	mode := os.FileMode(0644)
	var err error
	if p.fd, err = os.OpenFile(fpath, flags, mode); err != nil {
		// Log the error and then set the state to complete since the
		// file was not able to be opened on disk.
		p.log.Error(
			"Error opening file",
			logging.NewFieldIface("error", err))
		p.setState(primaryStateComplete)
		return false
	}

	// Initialize the heart beat timer before making the call so that we
	// ensure that the follow up happens before the timer expires on
	// the replica.
	p.resetHeartBeatTimer()

	// Notify each replica in parallel so they can initialize locally.
	p.setState(primaryStateInitializingRepls)
	fields := make([]logging.Field, len(p.remotes))
	errCount := int32(0)
	wg := sync.WaitGroup{}
	for i, r := range p.remotes {
		wg.Add(1)
		go func(i int, r Remote) {
			defer wg.Done()
			err := r.Initialize(p.settings.NameSpace, p.fidStr)
			if err != nil {
				i := int(atomic.AddInt32(&errCount, 1)) - 1
				fields[i] = logging.NewFieldIface(
					"replica-"+strconv.FormatInt(int64(i), 10)+"-error",
					err)
				p.failedRemotes[i] = true
				p.remotes[i] = nil
			}
		}(i, r)
	}
	wg.Wait()

	// Check for errors.
	if errCount != 0 {
		// Describe the error to the caller so that it can be handled. We
		// log the errors with details and then return a generic description
		// to the caller.
		p.log.Error("Error initializing the replicas.", fields[0:errCount]...)

		// Change the state to the Pending Delete state so its files can
		// be cleaned up by the deleter task.
		p.setState(primaryStatePendingDeleteRemotes)

		// Return the error generated above.
		return false
	}

	// Setup the expiration token so that the file is eventually uploaded
	// to S3 once it becomes too old to accept new inserts.
	p.settings.DelayQueue.Alter(
		&p.expireToken,
		time.Now().Add(p.settings.UploadOlder),
		p.expire)

	// TODO: Falloc support?

	// Success.
	p.log.Debug("Primary file successfully opened.")
	p.setState(primaryStateWaiting)
	return true
}

// Called to generate a string representation of this primary. This
// returns a single line representation that can be used in the
// Storage.Status call.
func (p *primary) Status() string {
	state := atomic.LoadInt32(&p.state)
	b := compat.Builder{}
	b.WriteString(p.fidStr)
	b.WriteString(" state=")
	b.WriteString(primaryStateStrings[state])
	b.WriteString(" size=")
	b.WriteString(human.Bytes(p.offset))

	if p.firstInsert != (time.Time{}) {
		b.WriteString(" oldest=")
		b.WriteString(time.Now().Sub(p.firstInsert).String())
	}

	if len(p.remotes) > 0 {
		b.WriteString(" remotes=")
		b.WriteString(p.remotes[0].String())
		for i := 1; i < len(p.remotes); i++ {
			b.WriteByte(',')
			b.WriteString(p.remotes[i].String())
		}
	}
	return b.String()
}

// Called by the CompressWorkQueue to initiate compression on the underlying
// file.
func (p *primary) compress() {
	// Set the state.
	p.setState(primaryStateCompressing)
	p.log.Debug("Compressing the file.")

	// Open the file that will store the compressed data long term.
	fpath := filepath.Join(p.settings.BaseDirectory, p.fidStr) + ".gz"
	flags := os.O_CREATE | os.O_RDWR | os.O_APPEND | os.O_TRUNC
	mode := os.FileMode(0644)
	var err error
	if p.compressFd, err = os.OpenFile(fpath, flags, mode); err != nil {
		// Log the error and then set the state to complete since the
		// file was not able to be opened on disk.
		p.log.Error(
			"Error opening compressed file",
			logging.NewField("file", fpath),
			logging.NewFieldIface("error", err))
		p.setState(primaryStatePendingCompression)
		return
	}

	// Seek back to the very start of the data file.
	if _, err = p.fd.Seek(0, io.SeekStart); err != nil {
		p.log.Error(
			"Error seeking in the data file.",
			logging.NewField("file", p.fd.Name()),
			logging.NewFieldIface("error", err))
		p.setState(primaryStatePendingCompression)
		return
	}

	// Create a gzip writer. Note that any error here is going to purely
	// be related to the compression level so its okay to just panic.
	zipper, err := gzip.NewWriterLevel(p.compressFd, p.settings.CompressLevel)
	if err != nil {
		panic(err)
	}

	// Copy data from the source file into the gzipper.
	buffer := [4096]byte{}
	if _, err = io.CopyBuffer(zipper, p.fd, buffer[:]); err != nil {
		p.log.Error(
			"Error generating the compressed data file.",
			logging.NewField("source-file", p.fd.Name()),
			logging.NewField("dest-file", p.compressFd.Name()),
			logging.NewFieldIface("error", err))
		p.setState(primaryStatePendingCompression)
		return
	}

	// Close out the gzip routine.
	if err = zipper.Close(); err != nil {
		p.log.Error(
			"Error closing the compressed file.",
			logging.NewField("file", p.compressFd.Name()),
			logging.NewFieldIface("error", err))
		p.setState(primaryStatePendingCompression)
		return
	}

	// Success.
	p.log.Info("Successfully compressed the data file.")
	p.setState(primaryStatePendingUpload)
}

// Called by the DelayQueue to indicate that the delete delay has expired and
// now the file can be moved into the deleting phase.
func (p *primary) delayDelete() {
	p.log.Info("Delete delay has passed.")
	p.setState(primaryStatePendingDeleteLocal)
}

// Called by the DeleteWorkQueue to initiate the deleting of a local file.
func (p *primary) deleteLocal() {
	// Metrics
	p.storage.metrics.PrimaryDeletes.IncTotal()

	// Now close the local file and remove it from the file system.
	p.setState(primaryStateDeletingLocal)
	p.storage.metrics.FilesDeleted.IncTotal()
	if p.fd != nil {
		err := os.Remove(p.fd.Name())
		if err != nil && !os.IsNotExist(err) {
			// There was an error deleting the file, queue it back up so it
			// can be deleted again.
			p.log.Error(
				"Error deleting local file.",
				logging.NewFieldIface("error", err))
			p.storage.metrics.FilesDeleted.IncFailures()
			p.setState(primaryStatePendingDeleteLocal)
			return
		} else {
			p.storage.metrics.FilesDeleted.IncSuccesses()
		}

		// Close the open file handle. If there is an error log it, but there
		// is not much more we can do so move on anyway.
		if err := p.fd.Close(); err != nil {
			p.log.Warning(
				"Error closing open file handle.",
				logging.NewFieldIface("error", err))
		}

		// Set the file descriptor to nil so it won't be used now that
		// its closed.
		p.fd = nil
	}

	// Success, change state and notify the storage implementation.
	p.setState(primaryStateComplete)
	p.log.Info("Processing complete and files are removed.")
	p.storage.metrics.PrimaryDeletes.IncSuccesses()
}

// Deletes the compressed file from disk (if configured).
func (p *primary) deleteCompressed() {
	if p.compressFd != nil {
		p.setState(primaryStateDeletingCompressed)
		p.storage.metrics.FilesDeleted.IncTotal()
		err := os.Remove(p.compressFd.Name())
		if err != nil && !os.IsNotExist(err) {
			// There was an error deleting the file, queue it back up so it
			// can be deleted again.
			p.log.Error(
				"Error deleting local compressed file.",
				logging.NewFieldIface("error", err))
			p.storage.metrics.FilesDeleted.IncFailures()
			p.setState(primaryStatePendingDeleteCompressed)
			return
		} else {
			p.storage.metrics.FilesDeleted.IncSuccesses()
			p.log.Debug("Successfully deleted the compressed file.")
		}
		p.compressFd = nil
	}

	// There are three possible branches after this state, if there are
	// remotes defined then we need to move into
	// primaryStatePendingDeleteRemotes, if there are no remotes and there
	// is a DelayDelete set in settings then we need to move into
	// primaryStateDelayLocalDelete, and if neither of the above conditions
	// are true we move into primaryStatePendingDeleteLocal.
	if len(p.remotes) > 0 {
		p.setState(primaryStatePendingDeleteRemotes)
	} else if p.settings.DelayDelete > 0 {
		p.setState(primaryStateDelayLocalDelete)
	} else {
		p.setState(primaryStatePendingDeleteLocal)
	}
}

// Deletes the files from the remote replicas.
func (p *primary) deleteRemotes() {
	// Set the state on the primary.
	p.setState(primaryStateDeletingRemotes)

	// Contact each remote in parallel and request its deletion.
	p.log.Debug("Triggering a delete on all remotes.")
	wg := sync.WaitGroup{}
	fields := make([]logging.Field, len(p.remotes))
	errCount := int32(0)
	for i, remote := range p.remotes {
		if remote == nil {
			// The remote was already marked as failed so there is no reason
			// to attempt to call Delete on it.
			continue
		}
		wg.Add(1)
		go func(i int, remote Remote) {
			defer wg.Done()
			err := remote.Delete(p.settings.NameSpace, p.fidStr)
			if err != nil && !p.failedRemotes[i] {
				ei := atomic.AddInt32(&errCount, 1) - 1
				fields[int(ei)] = logging.NewFieldIface(
					"replica-"+strconv.FormatInt(int64(i), 10)+"-error",
					err)
			}
			// Mark the node as failed/done so that it won't receive
			// further processing.
			p.remotes[i] = nil
		}(i, remote)
	}
	wg.Wait()

	// Check for errors.
	if errCount != 0 {
		// If there was an error queuing the delete then we don't want to
		// block deletion forever. As such we simply log it and move forward.
		// If the remote misses the Delete call from the primary then it will
		// eventually trigger an upload of the same data which should not
		// be an issue other than duplicating work.
		p.log.Warning("Error deleting from remotes.", fields...)
	} else {
		p.log.Debug("Successfully deleted replicas.")
	}

	// FIXME
	// We can branch into two destinations depending on configuration. If
	// there is a DelayDelete set in settings then we need to go into that
	// state, otherwise we need to go into PendingDeleteLocal.
	if p.settings.DelayDelete > 0 {
		p.setState(primaryStateDelayLocalDelete)
	} else {
		p.setState(primaryStatePendingDeleteLocal)
	}
}

// Called to indicate that the primary has expired. Expiration means that
// the file is old enough to be force uploaded at this point.
func (p *primary) expire() {
	// Only expire the file if its in the waiting list otherwise some other
	// process will be altering the state which will cause consistency
	// errors if we attempt to change things.
	if p.storage.waiting.Remove(p) {
		p.shutdown()
	}
}

// Triggered by the DelayQueue to signal that a heart beat needs to be
// executed against the replicas.
func (p *primary) heartBeatEvent() {
	p.log.Debug("Performing a heart beat to the replicas.")

	// Schedule the next heart beat.
	p.resetHeartBeatTimer()

	// We need to initiate a new heart beat against each replica.
	wg := sync.WaitGroup{}
	fields := make([]logging.Field, len(p.remotes))
	errCount := int32(0)
	for i, remote := range p.remotes {
		if remote == nil {
			// Skip remotes already marked as failed.
			continue
		}
		wg.Add(1)
		go func(i int, remote Remote) {
			defer wg.Done()
			ns := p.settings.NameSpace
			if shutDown, err := remote.HeartBeat(ns, p.fidStr); err != nil {
				ei := atomic.AddInt32(&errCount, 1) - 1
				fields[int(ei)] = logging.NewFieldIface(
					"replica-"+strconv.FormatInt(int64(i), 10)+"-error",
					err)
			} else if shutDown {
				ei := atomic.AddInt32(&errCount, 1) - 1
				fields[int(ei)] = logging.NewFieldIface(
					"replica-"+strconv.FormatInt(int64(i), 10)+"-error",
					replicaIsShuttingDownError)
			}
		}(i, remote)
	}
	wg.Wait()

	// If all the heart beats where successful then we can move on.
	if errCount == 0 {
		p.log.Debug("Heart beat successful.")
		return
	}

	// The primary is still in the Waiting state, which means that it is in
	// the waiting list as well.
	p.log.Warning(
		"Error sending health check, failing the file.",
		fields[0:errCount]...)

	// If the primary is currently in the waiting queue then we can take
	// control directly and shut it down which will clean up the file
	// based on its current size and configuration.
	p.unhealthy = true
	if p.storage.waiting.Remove(p) {
		p.shutdown()
	}
}

// Resets the heart beat token to the next expected heart beat time.
func (p *primary) resetHeartBeatTimer() {
	hbTime := p.settings.HeartBeatTime / 2
	p.log.Debug(
		"Setting heart beat timer",
		logging.NewField("timer", hbTime.String()))
	p.settings.DelayQueue.Alter(
		&p.heartBeatToken,
		time.Now().Add(hbTime),
		p.heartBeatEvent)
}

// Sets the current state of this primary.
func (p *primary) setState(n int32) {
	// When changing states we need to check to see if a backend has failed
	// so that we do not transition into the waiting state again.
	if n == primaryStateWaiting && p.unhealthy {
		p.shutdown()
		return
	}

	// Swap the old and new states so that we can log them both.
	oldN := atomic.SwapInt32(&p.state, n)
	if p.log.DebugEnabled() {
		p.log.Debug(
			"State changed.",
			logging.NewField("old-state", primaryStateStrings[oldN]),
			logging.NewField("new-state", primaryStateStrings[n]),
		)
	}
	p.storage.primaryStateChange(p, oldN, n)

	// We need to cancel the heart beat timer in any state that is not
	// one where we expect it to be running in the background. From
	// the point of initializing the replicas, to deleting the replicas
	// it needs to be active.
	switch n {
	case primaryStateInitializingRepls:
	case primaryStateWaiting:
	case primaryStateInserting:
	case primaryStateReplicating:
	case primaryStatePendingCompression:
	case primaryStateCompressing:
	case primaryStatePendingUpload:
	case primaryStateUploading:
	case primaryStatePendingDeleteCompressed:
	case primaryStateDeletingCompressed:
	case primaryStatePendingDeleteRemotes:
	default:
		p.log.Debug("Canceling heart beat timers.")
		p.settings.DelayQueue.Cancel(&p.heartBeatToken)
	}

	// In order to track the queuedForUpload value we check the state. Some
	// states cancel that value while others set it.
	switch n {
	case primaryStatePendingUpload:
		fallthrough
	case primaryStateUploading:
		if p.queuedForUpload == (time.Time{}) {
			p.queuedForUpload = time.Now()
		}
	default:
		p.queuedForUpload = time.Time{}
	}

	// Depending on the new state we need to add the primary to one of several
	// queues for post processing.
	switch n {
	case primaryStatePendingCompression:
		p.log.Info("Queuing for compression.")
		p.settings.CompressWorkQueue.Insert(p.compress)
	case primaryStatePendingUpload:
		p.log.Info("Queuing for upload.")
		p.settings.UploadWorkQueue.Insert(p.upload)
	case primaryStatePendingDeleteCompressed:
		p.log.Info("Queuing for local compressed file delete.")
		p.settings.DeleteLocalWorkQueue.Insert(p.deleteCompressed)
	case primaryStatePendingDeleteRemotes:
		p.log.Info("Queuing for remote deletes.")
		p.settings.DeleteRemotesWorkQueue.Insert(p.deleteRemotes)
	case primaryStateDelayLocalDelete:
		p.log.Info(
			"Delaying local file delete.",
			logging.NewField("time", p.settings.DelayDelete.String()))
		p.settings.DelayQueue.Alter(
			&p.delayDeleteToken,
			time.Now().Add(p.settings.DelayDelete),
			p.delayDelete)
	case primaryStatePendingDeleteLocal:
		p.log.Info("Queuing for local delete.")
		p.settings.DeleteLocalWorkQueue.Insert(p.deleteLocal)
	}
}

// Shuts down the file. This is used to move the file from the inserting
// phase of the files life cycle into the uploading and deleting phase.
func (p *primary) shutdown() {
	// Depending on the state of the file we can decide where to send it.
	// If the file is empty then we don't need to bother with uploading
	// it. We can just skip right to the delete stages.
	if p.offset == 0 {
		p.log.Info("The primary expired but is empty, skipping upload.")
		if len(p.remotes) > 0 {
			p.setState(primaryStatePendingDeleteRemotes)
		} else {
			p.setState(primaryStatePendingDeleteLocal)
		}
		return
	}

	// Since there is data in the file we need to send it off for either
	// uploading or compressing.
	if p.settings.Compress {
		p.log.Info("This primary expired, starting compression process.")
		p.setState(primaryStatePendingCompression)
	} else {
		p.log.Info("This primary expired, starting the upload process.")
		p.setState(primaryStatePendingUpload)
	}
}

// Called by the uploader to trigger a Upload of the underlying file.
func (p *primary) upload() {
	// Set the state
	p.storage.metrics.PrimaryUploads.IncTotal()
	p.setState(primaryStateUploading)
	p.log.Debug("Starting upload.")

	// Attempt the upload.
	fd := p.fd
	if p.settings.Compress {
		fd = p.compressFd
	}
	if !uploadToS3(fd, p.fid, p.s3key, p.settings, p.log) {
		p.log.Warning("Requeuing for upload.")
		p.setState(primaryStatePendingUpload)
		p.storage.metrics.PrimaryUploads.IncFailures()
		return
	} else {
		p.storage.metrics.PrimaryUploads.IncSuccesses()
	}

	// Once the upload is successful we can branch in several directions
	// depending on configuration.
	if p.settings.Compress {
		p.setState(primaryStatePendingDeleteCompressed)
	} else if len(p.remotes) > 0 {
		p.setState(primaryStatePendingDeleteRemotes)
	} else if p.settings.DelayDelete > 0 {
		p.setState(primaryStateDelayLocalDelete)
	} else {
		p.setState(primaryStatePendingDeleteLocal)
	}
}
