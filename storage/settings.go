package storage

import (
	"io"
	"log/slog"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/liquidgecka/blobby/internal/delayqueue"
	"github.com/liquidgecka/blobby/internal/workqueue"
	"github.com/liquidgecka/blobby/storage/fid"
)

const (
	// The default heart beat interval.
	defaultHeartBeatTime = time.Minute

	// Default OpenFilesMaximum is 32
	defaultOpenFilesMaximum = int32(32)

	// Default OpenFilesMinimum is 1
	defaultOpenFilesMinimum = int32(1)

	// Default UploadLargerThan is 100MB.
	defaultUploadLargerThan = uint64(1024 * 1024 * 100)

	// Default UploadOlder is 30 minutes.
	defaultUploadOlder = time.Minute * 30
)

type Settings struct {
	// User to perform uploads from this namespace.
	AWSUploader *s3manager.Uploader

	// A function that will return a pool of Blobby remotes that
	// should be used for a new Replica.
	AssignRemotes func(int) ([]Remote, error)

	// The base directory that files will be stored in for this namespace.
	BaseDirectory string

	// If this is set to something other than nil then logging will be
	// written to this output.
	BaseLogger *slog.Logger

	// When set to true then the file will be compressed before its uploaded
	// to S3. This will break the ability to fetch identifiers not found in
	// the local file system cache to use accordingly.
	Compress      bool
	CompressLevel int

	// A work queue for Compression related activities.
	CompressWorkQueue *workqueue.WorkQueue

	// If configured to do so then blobby will keep the primary file around
	// after it has been uploaded. This allows Read() operations to use the
	// local file rather than fetching from S3.
	DelayDelete time.Duration

	// The DelayQueue that will be used to schedule events like heart beat
	// timers, replica timeouts, etc.
	DelayQueue *delayqueue.DelayQueue

	// A WorkQueue for processing local file delete requests.
	DeleteLocalWorkQueue *workqueue.WorkQueue

	// A WorkQueue for processing remote replica delete requests.
	DeleteRemotesWorkQueue *workqueue.WorkQueue

	// After this amount of time a replica will be considered "orphaned" and
	// will trigger an upload of the data. This ensures that a primary being
	// lost won't cause data loss.
	HeartBeatTime time.Duration

	// The machine ID that is serving this name space. This must be unique
	// within all of the instances in the list of remotes.
	MachineID uint32

	// The name of the napespace that this Storage implementation will
	// be serving.
	NameSpace string

	// The prefix for the namespace= tag; a value of blobby_ for this field
	// would give blobby_namespace as the tag key in the rendered Prometheus
	// metrics.
	NamespaceTagKeyPrefix string

	// The minimum and maximum number of open master files that are allowed
	// to be open.
	OpenFilesMaximum int32
	OpenFilesMinimum int32

	// A function that fetches data from a remote.
	Read func(ReadConfig) (io.ReadCloser, error)

	// The number of replicas that each master file should be assigned.
	Replicas int

	// S3 client used for downloading objects from S3.
	S3Client *s3.S3

	// The S3 bucket and base path used for uploads as well as an optional
	// formatter for the file name as it will be written to S3. If not
	// provided it will default to the FID string.
	S3Bucket    string
	S3BasePath  string
	S3KeyFormat *fid.Formatter

	// If a file grows beyond this size then it will be moved into an
	// uploading state.
	UploadLargerThan uint64

	// Upload files after this much time regardless of size.
	UploadOlder time.Duration

	// A WorkQueue for processing Upload requests.
	UploadWorkQueue *workqueue.WorkQueue
}
