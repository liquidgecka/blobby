package storage

import (
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/liquidgecka/blobby/internal/backoff"
	"github.com/liquidgecka/blobby/internal/compat"
	"github.com/liquidgecka/blobby/internal/logging"
	"github.com/liquidgecka/blobby/storage/blastpath"
	"github.com/liquidgecka/blobby/storage/fid"
	"github.com/liquidgecka/blobby/storage/metrics"
)

// Handles on disk storage for a Blobby instance.
type Storage struct {
	// A counter for the total number of primary files that are either open,
	// or being opened. This is needed for calculating when a new primary file
	// needs to be opened.
	appendablePrimaries int32

	// We track metrics via the metrics object. This specifically
	// allows us to keep the code for generating and aggregating those
	// metrics all in a single place.
	metrics metrics.Metrics

	// When attempting to open new primaries we need to slowly back off
	// if there have been errors. This prevents us from spamming new
	// file creation on the file system and in the logs.
	newFileBackOff backoff.BackOff

	// A mapping of each primaries "FID" string to the primary object
	// associated with it.
	primaries     map[string]*primary
	primariesLock sync.Mutex

	// A mapping of each replicas "FID" string to the replica object
	// associated with it.
	replicas     map[string]*replica
	replicasLock sync.Mutex

	// Settings associated with this Storage object.
	settings Settings

	// A list of primary objects that are waiting to be appended into.
	waiting list
}

// Creates a new Storage object from the given settings.
func New(settings *Settings) *Storage {
	// Validate that settings has all the required fields.
	switch {
	case settings.AssignRemotes == nil:
		panic("settings.AssignRemotes is required.")
	case settings.AWSUploader == nil:
		panic("settings.AWSUploader is required.")
	case settings.BaseDirectory == "":
		panic("settings.BaseDirectory is required.")
	case settings.Compress && settings.CompressLevel < -1:
		panic("settings.CompressLevel can not be less than -1.")
	case settings.Compress && settings.CompressLevel > gzip.BestCompression:
		panic(fmt.Sprintf(
			"settings.CompressLevel can not be greater than %d.",
			gzip.BestCompression))
	case settings.DelayQueue == nil:
		panic("settings.DelayQueue is required.")
	case settings.Read == nil:
		panic("settings.Read is required.")
	case settings.S3Client == nil:
		panic("settings.S3Client is required.")
	case settings.S3Bucket == "":
		panic("settings.S3Bucket is required.")
	}

	// Make a copy of the settings object so that it can't be modified after
	// being passed to New(). Also set defaults for any value that didn't
	// get set.
	s := &Storage{
		newFileBackOff: backoff.BackOff{
			Max:    time.Second * 1,
			Period: time.Second * 30,
			X:      time.Millisecond * 100,
		},
		primaries: make(map[string]*primary, 10),
		replicas:  make(map[string]*replica, 10),
		settings:  *settings,
	}
	if s.settings.HeartBeatTime == 0 {
		s.settings.HeartBeatTime = defaultHeartBeatTime
	}
	if s.settings.NameSpace == "" {
		s.settings.NameSpace = "default"
	}
	if s.settings.OpenFilesMaximum == 0 {
		s.settings.OpenFilesMaximum = defaultOpenFilesMaximum
	}
	if s.settings.OpenFilesMinimum == 0 {
		s.settings.OpenFilesMinimum = defaultOpenFilesMinimum
	}
	if s.settings.UploadLargerThan == 0 {
		s.settings.UploadLargerThan = defaultUploadLargerThan
	}
	if s.settings.UploadOlder == 0 {
		s.settings.UploadOlder = defaultUploadOlder
	}
	if s.settings.Compress {
		switch s.settings.CompressLevel {
		case 0:
			s.settings.CompressLevel = gzip.DefaultCompression
		case -1:
			s.settings.CompressLevel = gzip.NoCompression
		}
	}

	return s
}

// "Blast Path" read function that can fetch a ranged portion of a primary
// file. This works just like Read except that it will only read from
// local files and works on byte ranges rather than Blobby IDs.
func (s *Storage) BlastPathRead(
	fid string,
	start uint64,
	end uint64,
) (
	io.ReadCloser,
	error,
) {
	// Start by getting the primary associated with this fid. If it doesn't
	// exist then bail out quickly.
	primary := func() *primary {
		s.primariesLock.Lock()
		defer s.primariesLock.Unlock()
		return s.primaries[fid]
	}()
	if primary == nil {
		return nil, ErrNotFound(fid)
	}

	// We are working with the primary out of its normal controlling method
	// so we need to be careful to not step on any internal values. Lets
	// quickly check to see if the end is large enough to accommodate
	// the request.
	if primary.offset < end {
		return nil, &ErrNotPossible{}
	}

	// If the fd value is nil then the file has been deleted and we need
	// to pretend like it wasn't found.
	pFd := primary.fd
	if pFd == nil {
		return nil, ErrNotFound(fid)
	}

	// Next we attempt to open the file on disk so we have a second
	// file descriptor to work with.
	fd, err := os.Open(pFd.Name())
	if err != nil {
		if _, ok := err.(ErrNotFound); ok {
			return nil, ErrNotFound(fid)
		} else {
			return nil, err
		}
	}

	// Seek to the start position in the file, then do a relative seek to the
	// same position in order to get an accurate offset of where the next
	// read will come from. This protects us from the seek managing to walk
	// of the end of the file since that is not technically an error.
	if _, err := fd.Seek(int64(start), io.SeekCurrent); err != nil {
		fd.Close()
		return nil, err
	} else if n, err := fd.Seek(0, io.SeekCurrent); err != nil {
		fd.Close()
		return nil, err
	} else if uint64(n) != start {
		fd.Close()
		return nil, fmt.Errorf("Short seek.")
	}

	// Success. We can return the resulting data to the caller
	// wrapped up in a data limiter.
	return &limitReadCloser{
		RC: fd,
		N:  int64(end - start),
	}, nil
}

// "Blast Path" status output for this Storage Name Space. This will output
// the current status of the primaries hosted by this instance.
func (s *Storage) BlastPathStatus(out io.Writer) {
	// Generate the array in a sub function so that we can limit the amount
	// of time that we are holding the primariesLock.
	array := func() (array []blastpath.Record) {
		s.primariesLock.Lock()
		defer s.primariesLock.Unlock()
		array = make([]blastpath.Record, len(s.primaries))
		i := 0
		for _, p := range s.primaries {
			array[i].FID = p.fidStr
			array[i].Size = p.offset
			array[i].S3Bucket = s.settings.S3Bucket
			array[i].S3Key = p.s3key
			i++
		}
		return
	}()

	// Encode the array into JSON data and write it out to the io.Writer
	// that the caller gave us.
	json.NewEncoder(out).Encode(array)
}

// Writes debugging information about the given ID to the writer given.
func (s *Storage) DebugID(out io.Writer, id string) {
	// Start off by parsing the ID into a fid, start and length.
	fid, start, length, err := fid.ParseID(id)
	if err != nil {
		fmt.Fprintf(out, "Invalid ID: %s\n", err.Error())
		return
	}
	fidStr := fid.String()
	fmt.Fprintf(out, "Name space: %s\n", s.settings.NameSpace)
	fmt.Fprintf(out, "File ID: %s\n", fidStr)
	fmt.Fprintf(out, "Machine ID: %d\n", fid.Machine())
	fmt.Fprintf(out, "Start offset: %d\n", start)
	fmt.Fprintf(out, "Length: %d\n", length)
	fmt.Fprintf(
		out,
		"S3 destination: s3://%s/%s",
		s.settings.S3BasePath,
		s.settings.S3KeyFormat.Format(fid))

	// Lastly we check the status of this fid to see if its currently doing
	// anything locally.
	ps, ok := func() (int32, bool) {
		s.primariesLock.Lock()
		defer s.primariesLock.Unlock()
		if p, ok := s.primaries[fidStr]; ok {
			return p.state, true
		} else {
			return -1, false
		}
	}()
	if ok {
		fmt.Fprintf(out, "\nThis ID is served locally by a primary that is\n")
		fmt.Fprintf(out, "in the %s state.\n", primaryStateStrings[ps])
	}
	rs, ok := func() (int32, bool) {
		s.replicasLock.Lock()
		defer s.replicasLock.Unlock()
		if r, ok := s.replicas[fidStr]; ok {
			return r.state, true
		} else {
			return -1, false
		}
	}()
	if ok {
		fmt.Fprintf(out, "\nThis ID is server locally by a replica that is\n")
		fmt.Fprintf(out, "in the %s state.\n", replicaStateStrings[rs])
	}
}

// Returns a copy of the metrics associated with this Storage object.
func (s *Storage) GetMetrics() (m metrics.Metrics) {
	m.CopyFrom(&s.metrics)
	oldestPrimary := time.Now()
	queuedForUpload := time.Now()
	m.QueuedInserts = int64(s.waiting.Waiting())
	func() {
		s.primariesLock.Lock()
		defer s.primariesLock.Unlock()
		for _, p := range s.primaries {
			switch {
			case p.firstInsert == (time.Time{}):
			case p.firstInsert.Before(oldestPrimary):
				oldestPrimary = p.firstInsert
			}
			switch {
			case p.queuedForUpload == (time.Time{}):
			case p.queuedForUpload.Before(queuedForUpload):
				queuedForUpload = p.queuedForUpload
			}
		}
	}()
	func() {
		s.replicasLock.Lock()
		defer s.replicasLock.Unlock()
		for _, r := range s.replicas {
			switch {
			case r.queuedForUpload == (time.Time{}):
			case r.queuedForUpload.Before(queuedForUpload):
				queuedForUpload = r.queuedForUpload
			}
		}
	}()
	m.OldestUnUploadedData = time.Since(oldestPrimary).Seconds()
	m.OldestQueuedUpload = time.Since(queuedForUpload).Seconds()

	return
}

// Returns true if this Storage is healthy and a string representing the
// reason why this Storage implementation is healthy.
func (s *Storage) Health() (bool, string) {
	healthy := true
	output := compat.Builder{}
	output.Grow(4096)

	// Check that new file creation is successful.
	if !s.newFileBackOff.Healthy() {
		output.WriteString("New file creation: FAILED\n")
		healthy = false
	} else {
		output.WriteString("New file creation: SUCCEEDED\n")
	}

	// And finally return the results.
	return healthy, output.String()
}

// Inserts new data into one of the open primary files. This will return the
// ID of the newly created object or an error if something went wrong.
// If an error is returned then it is not safe to assume that the
// data was successfully written.
func (s *Storage) Insert(data *InsertData) (id string, err error) {
	// Metrics
	s.metrics.PrimaryInserts.IncTotal()

	// Get the next available primary, blocking until one becomes
	// available. The given call will call the check function before
	// sleeping each time in order to ensure that new primaries will
	// be opened if there are not currently enough given the waiting
	// callers.
	start := time.Now()
	prim := s.waiting.Get(s.checkIdleFiles)
	atomic.AddUint64(
		&s.metrics.PrimaryInsertQueueNanoseconds,
		uint64(time.Since(start)))

	// With the primary in hand we can now call Insert to add the data that
	// was passed into us. Errors encountered during the insertion
	// process will be logged and handled internally. If the file is
	// considered done for uploads then this call returns true, otherwise
	// the file is allowed to be put back on the waiting queue for accepting
	// more data.
	if id, err = prim.Insert(data); err != nil {
		// The primary file returned an error which means that it will no
		// longer accept data so we need to remove it from our primaries
		// pool so a new file will be opened when needed. Its not necessary
		// to put the primary on a new list because that is done via the
		// state change mechanism already.
		s.waiting.signal()
		s.metrics.PrimaryInserts.IncFailures()
	} else {
		s.metrics.PrimaryInserts.IncSuccesses()
	}

	// If the primary is no longer waiting then we need to signal an existing
	// waiter to check and see if more files need to be opened.
	if prim.state != primaryStateWaiting {
		s.waiting.signal()
	}

	// Success or error we can return here.
	return
}

// Reads an individual ID (provided via rc). This may involve directly talking
// to S3, or talking to the remote machine that is serving the given ID.
func (s *Storage) Read(rc ReadConfig) (io.ReadCloser, error) {
	// Debug logging so we know where to start.
	log := rc.Logger()
	if log == nil {
		log = s.settings.BaseLogger.NewChild().
			AddField("id", rc.ID()).
			AddField("fid", rc.FIDString()).
			AddField("start", rc.Start()).
			AddField("length", rc.Length())
	}
	log.Debug("starting request processing.")

	// First of all we can check to see if we have a copy of this fid
	// stored locally. If we do then hurray we can serve this request
	// directly.
	fn, ok := func() (string, bool) {
		fn, ok := func() (string, bool) {
			s.primariesLock.Lock()
			defer s.primariesLock.Unlock()
			if p, ok := s.primaries[rc.FIDString()]; ok {
				return p.fd.Name(), true
			} else {
				return "", false
			}
		}()
		if ok {
			return fn, ok
		}
		fn, ok = func() (string, bool) {
			s.replicasLock.Lock()
			defer s.replicasLock.Unlock()
			if r, ok := s.replicas[rc.FIDString()]; ok {
				return r.fd.Name(), true
			} else {
				return "", false
			}
		}()
		if ok {
			return fn, ok
		}
		return "", false
	}()
	if ok {
		// We have a primary with this fid! Try opening the file and seeking
		// to the starting position of the data we need.
		fd, err := os.Open(fn)
		if err != nil {
			// The file must have been removed before we were able to open
			// it, in this case we need to just continue on.
			log.Debug(""+
				"Attempt at a file open failed, falling back to "+
				"alternate options.",
				logging.NewField("file", fn),
				logging.NewFieldIface("error", err))
		} else if _, err := fd.Seek(int64(rc.Start()), io.SeekStart); err != nil {
			// There was an error seeking in the file. This is not expected
			// but we can continue on pretending that the file was not
			// able to be processed at all.
			log.Debug(""+
				"Attempt at a file seek failed, falling back to "+
				"alternate options.",
				logging.NewField("file", fn),
				logging.NewFieldIface("error", err))
		} else if n, err := fd.Seek(0, io.SeekCurrent); err != nil {
			// After the above seek executes we want to find out where we
			// are in the file, Seeking to 1000 in a 10 byte file will work
			// and return the offset of 10000. We seek to 0 with a relative
			// offset to get the real location that the file pointer landed.
			// Getting here means that the first seek worked, but the second
			// didn't which is very odd and shouldn't ever happen.
			log.Debug(""+
				"Could not obtain the current offset of the file pointer, "+
				"falling back to alternate options.",
				logging.NewField("file", fn),
				logging.NewFieldIface("error", err))
		} else if uint64(n) != rc.Start() {
			// The file was not large enough to get us to "start" as an offset
			// and thus we need to return an error.
			log.Debug(""+
				"Short seek when attempting to find id, falling back to"+
				"alternate options.",
				logging.NewField("file", fn),
				logging.NewFieldIface("seeked-offset", n))
		} else {
			// We have a file with the position at the right place,
			// now we need to create a limited reader that will
			// only read the number of bytes necessary for the operation.
			log.Debug(
				"Serving read request locally.",
				logging.NewField("file", fn))
			return &limitReadCloser{
				RC: fd,
				N:  int64(rc.Length()),
			}, nil
		}
	}

	// If this ReadConfig specifies that this should be a LocalOnly read then
	// we need to stop here. We only want to process local files which we have
	// done.
	if rc.LocalOnly() {
		if log.DebugEnabled() {
			log.Debug("" +
				"Request was not found locally and Local Only was true, " +
				"returning not found.")
		}
		return nil, ErrNotFound(rc.ID())
	}

	// From the file id we can get the machine id, and from the machine
	// id we can get the Remote that created and served this file. That
	// will let us fetch the data raw off disk from a remote machine
	// rather than using the AWS API.
	if s.settings.MachineID == rc.Machine() {
		// This file was created on this machine, no need to try reading it
		// from a remote.
		log.Debug("" +
			"Data was created locally, but its not present. " +
			"Falling back to S3.")
	} else if rcloser, err := s.settings.Read(rc); err == nil {
		// There was no error which means that rc is fit for us to
		// return to the called.
		s.settings.BaseLogger.Debug(
			"Serving data from a remote Blobby server.",
			logging.NewFieldUint32("machine-id", rc.Machine()))
		return rcloser, nil
	} else if _, ok := err.(ErrNotFound); ok {
		// Getting a 404 back from the caller means that the object was not
		// found locally on the remote side so now we can proceed
		// to fetch from S3.
		if log.DebugEnabled() {
			log.Debug(""+
				"The remote did not have the data locally. "+
				"Falling back to S3.",
				logging.NewFieldUint32("machine-id", rc.Machine()))
		}
	} else {
		// There was an error fetching the file that was NOT a ErrNotFound
		// error which could be a network issue, or machine issue on the
		// other end. As such we need to log it and then fall back to an
		// S3 fetch.
		log.Warning(""+
			"Error communicating with remote Blobby instance, "+
			"Falling back to S3.",
			logging.NewFieldUint32("machine-id", rc.Machine()),
			logging.NewFieldIface("error", err))
	}

	// S3 only works if the file was not compressed when being uploaded
	// otherwise the offsets will be all wrong. For now we just return
	// a sentinel error to indicate that it is not possible to fetch
	// the record.
	if s.settings.Compress {
		log.Debug("" +
			"Local data is not available and the files are compressed " +
			"in S3 which makes them impossible to seek in. Rejecting " +
			"request")
		return nil, ErrNotPossible{}
	}

	// Lastly we check S3 to see if it has the object.
	key := filepath.Join(
		s.settings.S3BasePath,
		s.settings.S3KeyFormat.Format(rc.FID()))
	rng := fmt.Sprintf("bytes=%d-%d", rc.Start(), rc.Start()+uint64(rc.Length())-1)
	goi := s3.GetObjectInput{
		Bucket: &s.settings.S3Bucket,
		Key:    &key,
		Range:  &rng,
	}
	log.AddField("bucket", s.settings.S3Bucket)
	log.AddField("key", *goi.Key)
	goo, err := s.settings.S3Client.GetObject(&goi)
	if err != nil {
		if awsErr, ok := err.(awserr.Error); ok {
			switch awsErr.Code() {
			case s3.ErrCodeNoSuchBucket:
				log.Error("AWS S3 Bucket does not exist.")
				return nil, fmt.Errorf("S3 bucket does not exist.")
			case s3.ErrCodeNoSuchKey:
				log.Debug("Object was not found in S3.")
				return nil, ErrNotFound(rc.ID())
			}
		}
		log.Error(
			"Error calling the S3 API.",
			logging.NewFieldIface("error", err))
		return nil, err
	} else if goo.ContentLength == nil {
		// There was no length returned which means we can not be sure
		// that this is the right data.
		log.Warning("S3 did not return a Content-Length header.")
		return nil, fmt.Errorf("Missing content-length")
	} else if *goo.ContentLength != int64(rc.Length()) {
		// The wrong length was returned.
		log.Warning(
			"S3 returned an invalid Content-Length header.",
			logging.NewFieldUint32("expected", rc.Length()),
			logging.NewFieldInt64("got", *goo.ContentLength))
		return nil, fmt.Errorf("Invalid content-length.")
	} else if log.DebugEnabled() {
		// The request can be satisfied via S3 directly.
		log.Debug("Serving read request from S3.")
	}

	// As an added security precaution we make sure that we do not serve
	// more content than would be expected via the request ID that we were
	// given.
	return &limitReadCloser{RC: goo.Body, N: int64(rc.Length())}, nil
}

// Performs a Heart Beat on a replica. The only error condition here is that
// the replica does not exist.
func (s *Storage) ReplicaHeartBeat(fn string) error {
	s.metrics.ReplicaHeartBeats.IncTotal()
	repl := func(fn string) *replica {
		s.replicasLock.Lock()
		defer s.replicasLock.Unlock()
		return s.replicas[fn]
	}(fn)
	if repl == nil {
		s.metrics.ReplicaHeartBeats.IncFailures()
		return ErrReplicaNotFound(fn)
	}
	if err := repl.HeartBeat(); err != nil {
		s.metrics.ReplicaHeartBeats.IncFailures()
		return err
	}
	s.metrics.ReplicaHeartBeats.IncSuccesses()
	return nil
}

// Initializes a new replica in this Storage instance.
func (s *Storage) ReplicaInitialize(fn string) error {
	// Metrics
	s.metrics.ReplicaInitializes.IncTotal()

	// Start by adding a new replica object into our internal cache.
	// If it already exists then we need to return an error.
	repl := &replica{
		settings: &s.settings,
		storage:  s,
		log: s.settings.BaseLogger.NewChild().
			AddField("fid", fn).
			AddField("type", "replica"),
	}
	if err := repl.fid.Parse(fn); err != nil {
		s.metrics.ReplicaInitializes.IncFailures()
		return fmt.Errorf("Invalid file name: %s", err.Error())
	}
	repl.fidStr = fn
	repl.s3key = filepath.Join(
		s.settings.S3BasePath,
		s.settings.S3KeyFormat.Format(repl.fid))
	ok := func() bool {
		s.replicasLock.Lock()
		defer s.replicasLock.Unlock()
		if _, ok := s.replicas[fn]; ok {
			return false
		} else {
			s.replicas[fn] = repl
			return true
		}
	}()
	if !ok {
		s.metrics.ReplicaInitializes.IncFailures()
		return fmt.Errorf(
			"Can not initialize '%s' in namespace '%s', it already exists.",
			fn,
			s.settings.NameSpace)
	}

	// Open the replica file.
	if err := repl.Open(); err != nil {
		s.metrics.ReplicaInitializes.IncFailures()
		return err
	}

	// Success.
	s.metrics.ReplicaInitializes.IncSuccesses()
	return nil
}

// Performs a replica replication call.
func (s *Storage) ReplicaReplicate(fn string, rc RemoteReplicateConfig) error {
	s.metrics.ReplicaReplicates.IncTotal()
	repl := func(fn string) *replica {
		s.replicasLock.Lock()
		defer s.replicasLock.Unlock()
		return s.replicas[fn]
	}(fn)
	if repl == nil {
		s.metrics.ReplicaReplicates.IncFailures()
		return ErrReplicaNotFound(fn)
	}
	if err := repl.Replicate(rc); err != nil {
		s.metrics.ReplicaReplicates.IncFailures()
		return err
	} else {
		s.metrics.ReplicaReplicates.IncSuccesses()
		return nil
	}
}

// Queues a replica file for deletion.
func (s *Storage) ReplicaQueueDelete(fn string) error {
	s.metrics.ReplicaQueueDeletes.IncTotal()
	repl := func(fn string) *replica {
		s.replicasLock.Lock()
		defer s.replicasLock.Unlock()
		return s.replicas[fn]
	}(fn)
	if repl == nil {
		// If the replica does not exist then we actually accept and approve
		// the call. Since the call is to delete, and the replica does not
		// exist, then we are good right? =)
		s.metrics.ReplicaQueueDeletes.IncSuccesses()
		return nil
	}
	if err := repl.QueueDelete(); err != nil {
		s.metrics.ReplicaQueueDeletes.IncFailures()
		return err
	} else {
		s.metrics.ReplicaQueueDeletes.IncSuccesses()
		return nil
	}
}

// Sets debug logging to either true or false.
func (s *Storage) SetDebugging(enable bool) {
	if enable {
		s.settings.BaseLogger.EnableDebug()
		func() {
			s.replicasLock.Lock()
			defer s.replicasLock.Unlock()
			for _, r := range s.replicas {
				r.log.EnableDebug()
			}
		}()
		func() {
			s.replicasLock.Lock()
			defer s.replicasLock.Unlock()
			for _, m := range s.primaries {
				m.log.EnableDebug()
			}
		}()
	} else {
		s.settings.BaseLogger.DisableDebug()
		func() {
			s.replicasLock.Lock()
			defer s.replicasLock.Unlock()
			for _, r := range s.replicas {
				r.log.DisableDebug()
			}
		}()
		func() {
			s.replicasLock.Lock()
			defer s.replicasLock.Unlock()
			for _, m := range s.primaries {
				m.log.DisableDebug()
			}
		}()
	}
}

// Starts all of the supporting routines for this Storage implementation.
// This will also scan the storage directory looking for files created
// by a previous run of blobby. These will be automatically configured
// as a replica that is in the Uploading state in order to ensure that
// the data is written to S3 as quickly as possible since it may be from
// a failed instance.
func (s *Storage) Start() error {
	// Check the directory for pre-existing blobby files and for each
	// add them as a replica.
	files, err := ioutil.ReadDir(s.settings.BaseDirectory)
	if err != nil {
		s.settings.BaseLogger.Error(
			"Error scanning for existing files.",
			logging.NewFieldIface("error", err))
		return err
	}
	for _, file := range files {
		if !file.Mode().IsRegular() {
			// Ignore anything that is not a regular file.
			s.settings.BaseLogger.Debug(
				"Ignoring irregular file",
				logging.NewField("file", file.Name()))
			continue
		}
		fidStr := strings.TrimPrefix(file.Name(), "r-")
		repl := &replica{
			fidStr:   fidStr,
			settings: &s.settings,
			storage:  s,
			log: s.settings.BaseLogger.NewChild().
				AddField("fid", fidStr).
				AddField("type", "replica"),
			offset: uint64(file.Size()),
		}
		if err := repl.fid.Parse(repl.fidStr); err != nil {
			// The file is not a valid blobby file, as such it needs to
			// be ignored as well.
			s.settings.BaseLogger.Debug(
				"Ignoring non blobby data file, its no a valid fid.",
				logging.NewField("file", file.Name()))
			continue
		}
		repl.s3key = filepath.Join(
			s.settings.S3BasePath,
			s.settings.S3KeyFormat.Format(repl.fid))
		var err error
		repl.fd, err = os.Open(
			filepath.Join(s.settings.BaseDirectory, file.Name()))
		if err != nil {
			// There was an error opening the file. This is actually
			// a critical error as it means that we can not recover
			// data that was left behind by a previous instance of
			// blobby.
			repl.log.Error(
				"Error opening existing data file.",
				logging.NewField("file", file.Name()),
				logging.NewFieldIface("error", err))
			return err
		}
		func() {
			s.replicasLock.Lock()
			defer s.replicasLock.Unlock()
			s.replicas[fidStr] = repl
		}()
		repl.log.Info("Found pre-existing replica on disk at startup.")
		if s.settings.Compress {
			repl.setState(replicaStatePendingCompression)
		} else {
			repl.setState(replicaStatePendingUpload)
		}
	}

	// Make sure we open the minimum number of primary files which will
	// happen if we call this function.
	s.checkIdleFiles()

	// Log so its clear that the namespace is initialized.
	s.settings.BaseLogger.Info("Namespace started.")
	return nil
}

// Gets the status for this Storage implementation and writes it to the
// given io.Writer. This is a human readable status intended for administration
// so the format is undefined.
func (s *Storage) Status(out io.Writer) {
	// Get a list of primaries. Since this requires a lock we need to do
	// this in a sub function.
	primaries := func() primarySlice {
		s.primariesLock.Lock()
		defer s.primariesLock.Unlock()
		primaries := make(primarySlice, len(s.primaries))
		i := 0
		for _, p := range s.primaries {
			primaries[i] = p
			i++
		}
		return primaries
	}()

	// Next we need to sort our primaries by the fidStr value so that they
	// appear in a consistent order.
	sort.Sort(primaries)

	// Output the state of each of the primaries.
	if len(primaries) > 0 {
		fmt.Fprintf(out, "    Primaries:\n")
		for _, p := range primaries {
			fmt.Fprintf(out, "        %s\n", p.Status())
		}
	}

	// Get a list of replicas. Since this requires a lock we need to do
	// this in a sub function.
	replicas := func() replicaSlice {
		s.replicasLock.Lock()
		defer s.replicasLock.Unlock()
		replicas := make(replicaSlice, len(s.replicas))
		i := 0
		for _, p := range s.replicas {
			replicas[i] = p
			i++
		}
		return replicas
	}()

	// Next we need to sort our replicas by the fidStr value so that they
	// appear in a consistent order.
	sort.Sort(replicas)

	// Output the state of each of the replicas.
	if len(replicas) > 0 {
		fmt.Fprintf(out, "    Replicas:\n")
		for _, p := range replicas {
			fmt.Fprintf(out, "        %s\n", p.Status())
		}
	}
}

// Checks to see if the number of open primary files matches what the
// configuration expects. This will initiate the opening process if there
// are not currently enough files. This will be called within the
// get() loop for the waiting lists so it can not block on any
// operation. All work must be done in a goroutine.
func (s *Storage) checkIdleFiles() {
	// To start we initialize replicas until the replica count number is
	// at least equal to the minimum replica count numbers. Normally this
	// won't do anything but its cheap to check up front.
	var of int32
	for {
		of = atomic.LoadInt32(&s.appendablePrimaries)
		if of < s.settings.OpenFilesMinimum {
			atomic.AddInt32(&s.appendablePrimaries, 1)
			go s.openNewPrimaryFile()
		} else {
			break
		}
	}

	// If the open file count is greater or equal to the maximum allowed
	// open files then we can stop now.
	if of >= s.settings.OpenFilesMaximum {
		return
	}

	// Next we check to see if the number of open files is appropriate
	// for the given number of routines waiting on files being open.
	// We use a very simple double power model here to ensure that we
	// don't just open files quickly when its completely not necessary
	// in most cases.
	//
	// FIXME: Ideally we add a multiplier here to make this not just a
	// simple power of 2, but instead a less aggressive curve.
	if s.waiting.waiting > 1<<uint32(of) {
		atomic.AddInt32(&s.appendablePrimaries, 1)
		go s.openNewPrimaryFile()
		return
	}
}

// Opens a new primary file and places it in the idle pool. This is expected
// to be run as a goroutine so it does not return errors.
func (s *Storage) openNewPrimaryFile() {
	// Metrics
	s.metrics.PrimaryOpens.IncTotal()

	// Create a logger that we can use for logging information about this
	// primary file creation.
	plog := s.settings.BaseLogger.NewChild().
		AddField("type", "primary")

	// See if we need to delay due to previous failures, and if so log that
	// then delay.
	if delay := s.newFileBackOff.Wait(); delay > 0 {
		plog.Warning(
			"Delaying primary file creation due to previous errors.",
			logging.NewField("delay", delay.String()))
		time.Sleep(delay)
	}

	// Get a list of replicas that should be used for this new primary and
	// then add them to the logger so that they will appear in the list
	// for each log line.
	plog.Debug("Assigning replicas")
	var err error
	remotes, err := s.settings.AssignRemotes(s.settings.Replicas)
	if err != nil {
		// Log the error for debugging.
		plog.Error(
			"Error assigning replicas.",
			logging.NewFieldIface("error", err))

		// Since opening the primary file failed we need to start the
		// process of opening a new file. We do not need to worry about
		// adjusting the appendablePrimaries function though since nothing
		// will have decremented it.
		go s.openNewPrimaryFile()
		s.newFileBackOff.Failure()
		return
	}
	for i := range remotes {
		plog.AddField(
			"replica-"+strconv.FormatInt(int64(i), 10),
			remotes[i].String())
	}
	plog.Debug("Replicas assigned.")

	// Start generating the primary structure that we will use for this
	// primary. We do not want to add this to the map of primaries until
	// we have replicas assigned so that we do not run the risk of having
	// to revert.
	p := &primary{
		expires:  time.Now().UnixNano() + int64(s.settings.UploadOlder),
		log:      plog,
		remotes:  remotes,
		settings: &s.settings,
		state:    primaryStateNew,
		storage:  s,
	}
	p.fid.Generate(s.settings.MachineID)
	p.fidStr = p.fid.String()
	p.s3key = filepath.Join(
		s.settings.S3BasePath,
		s.settings.S3KeyFormat.Format(p.fid))
	plog.AddField("primary-fid", p.fidStr)
	plog.Debug("Assigning fid to the new primary.")

	// Generate the base structure for the new primary file and add it to the
	// list of all primary files so that it can be processed.
	func() {
		s.primariesLock.Lock()
		defer s.primariesLock.Unlock()
		s.primaries[p.fidStr] = p
	}()

	// Open the file on disk.
	if ok := p.Open(); !ok {
		// If the Open call fails it will automatically have moved the file
		// into the appropriate state to be cleaned up and removed from
		// the above map. This in turn will also decrement our
		// appendablePrimaries value which will in turn allow a new primary
		// to be created via a call to this function. This means that we can
		// not just restart this function like we did with the AssignRemotes
		// failure above. We will simply terminate and allow the
		// checkPrimaries operation do the heavy lifting later.
		s.newFileBackOff.Failure()
		plog.Error("Failed to open new master file.")
		return
	}

	// Log a message at the Info level so file creation can be tracked.
	plog.Info("New primary file initialized.")
}

// Called when a primary state changes so that the storage layer
// can move it into the right list.
func (s *Storage) primaryStateChange(p *primary, old, current int32) {
	// We want to keep track of the count of primaries in a state that means
	// that they are capable of being appended too. If the state was one
	// of the early states, (New/Open/InitializingReplicas/etc) then
	// the error will be followed by another attempt at opening a new file,
	// but if the state was Inserting or Waiting then we need to decrement
	// the opening files counter.
	switch old {
	case primaryStateNew:
		fallthrough
	case primaryStateOpening:
		fallthrough
	case primaryStateInitializingRepls:
		fallthrough
	case primaryStateWaiting:
		fallthrough
	case primaryStateInserting:
		fallthrough
	case primaryStateReplicating:
		switch current {
		case primaryStatePendingCompression:
			fallthrough
		case primaryStateCompressing:
			fallthrough
		case primaryStatePendingUpload:
			fallthrough
		case primaryStateUploading:
			fallthrough
		case primaryStatePendingDeleteCompressed:
			fallthrough
		case primaryStateDeletingCompressed:
			fallthrough
		case primaryStatePendingDeleteRemotes:
			fallthrough
		case primaryStateDeletingRemotes:
			fallthrough
		case primaryStateDelayLocalDelete:
			fallthrough
		case primaryStatePendingDeleteLocal:
			fallthrough
		case primaryStateDeletingLocal:
			fallthrough
		case primaryStateComplete:
			atomic.AddInt32(&s.appendablePrimaries, -1)
			s.checkIdleFiles()
		}
	}

	// Take action based on the new state.
	switch current {
	case primaryStateWaiting:
		s.waiting.Put(p)
	case primaryStateComplete:
		s.primariesLock.Lock()
		defer s.primariesLock.Unlock()
		delete(s.primaries, p.fidStr)
	}
}

// Marks a replica as being deleted.
func (s *Storage) replicaCompleted(r *replica) {
	s.replicasLock.Lock()
	defer s.replicasLock.Unlock()
	delete(s.replicas, r.fidStr)
}
