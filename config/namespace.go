package config

import (
	"compress/gzip"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"

	"github.com/liquidgecka/blobby/httpserver"
	"github.com/liquidgecka/blobby/storage"
	"github.com/liquidgecka/blobby/storage/fid"
)

var (
	defaultCompress         = false
	defaultCompressLevel    = 0
	defaultDelayDelete      = time.Duration(0)
	defaultOpenFilesMinimum = int32(1)
	defaultReplicas         = int(1)
	defaultS3BasePath       = ""
	defaultUploadFileSize   = uint64(1024 * 1024 * 1024) // 1 GB
	defaultUploadOlder      = time.Hour
)

type nameSpace struct {
	// The name of AWS profile that should be used for uploading.
	AWSProfile *string `toml:"aws_profile"`

	// Blast Path Access Control List which establishes protections around
	// the BLASTSTATUS and BLASTREAD API calls.
	BlastPathACL *acl `toml:"blast_path_acl"`

	// If set to true then uploads will be gzipped as they are sent to
	// AWS. This ensures that the actual stored contents will be small
	// but also breaks the ability to perform a GET on uploaded data when
	// it is forced to fall back to S3.
	Compress      *bool `toml:"compress"`
	CompressLevel *int  `toml:"compress_level"`

	// If greater than zero then the local file will have a delay between
	// its overall shutdown and when the file gets removed from disk. This
	// can be used to ensure that local caching is available for callers
	// for some time after the file has stopped being processed.
	DelayDelete *time.Duration `toml:"delay_delete"`

	// The Directory that files should be written to for this namespace.
	Directory *string `toml:"directory"`

	// Insert Access Control List which establishes protections around
	// who is allowed to insert data into the name space.
	InsertACL *acl `toml:"insert_acl"`

	// The minimum and maximum number of open primary files.
	OpenFilesMaximum *int32 `toml:"max_open_files"`
	OpenFilesMinimum *int32 `toml:"min_open_files"`

	// This ACL controls which servers are allowed to request this name space
	// act as a replica.
	PrimaryACL *acl `roml:"primary_acl"`

	// Read Access Control List which establishes who is allowed to read
	// data from this namespace.
	ReadACL *acl `toml:"read_acl"`

	// The number of replicas that each primary file should be assigned.
	Replicas *int `toml:"replicas"`

	// The S3 bucket and base path that define where data from this name
	// space will be uploaded. There is also an optional formatter that can
	// format the eventual Key in S3 using properties like time stamps and
	// such.
	S3Bucket    *string `toml:"s3_bucket"`
	S3BasePath  *string `toml:"s3_base_path"`
	S3KeyFormat *string `toml:"s3_key_format"`

	// Any primary file that grows beyond this size will be automatically
	// uploaded.
	UploadFileSize value `toml:"upload_file_size"`
	uploadFileSize uint64

	// Upload files that are at least this old.
	UploadOlder *time.Duration `toml:"upload_older"`

	// A quick reference to the top configuration element.
	top *top

	// The name given to this name space.
	name string

	// The formatter created for S3KeyFormat.
	formatter *fid.Formatter

	// A reference to the storage object.
	storage *storage.Storage

	// A reference to the NameSpaceSettings object created for the httpserver.
	nameSpaceSettings *httpserver.NameSpaceSettings
}

// Returns a httpserver.NameSpaceSettings object for this name space. This
// must be called after validation and logging is initialized.
func (n *nameSpace) getNameSpaceSettings() *httpserver.NameSpaceSettings {
	return &httpserver.NameSpaceSettings{
		BlastPathACL: n.BlastPathACL.access(),
		InsertACL:    n.InsertACL.access(),
		PrimaryACL:   n.PrimaryACL.access(),
		ReadACL:      n.ReadACL.access(),
		Storage:      n.Storage(),
	}
}

// Returns the storage object associated with this name space. This must be
// called after logging is initialized!
func (n *nameSpace) Storage() *storage.Storage {
	if n.storage == nil {
		l := n.top.Log.logger.
			NewChild().
			AddField("component", "storage").
			AddField("namespace", n.name)
		awsSession, _ := n.top.getAWSSession(*n.AWSProfile)
		uploader := s3manager.NewUploader(awsSession)
		s3client := s3.New(awsSession)
		n.storage = storage.New(&storage.Settings{
			AssignRemotes:          n.top.remotePool.AssignRemotes,
			AWSUploader:            uploader,
			BaseDirectory:          *n.Directory,
			BaseLogger:             l,
			CompressLevel:          *n.CompressLevel,
			Compress:               *n.Compress,
			CompressWorkQueue:      n.top.getCompressWorkQueue(),
			DelayDelete:            *n.DelayDelete,
			DelayQueue:             n.top.getDelayQueue(),
			DeleteLocalWorkQueue:   n.top.getDeleteLocalWorkQueue(),
			DeleteRemotesWorkQueue: n.top.getDeleteRemotesWorkQueue(),
			MachineID:              *n.top.MachineID,
			NameSpace:              n.name,
			OpenFilesMaximum:       *n.OpenFilesMaximum,
			OpenFilesMinimum:       *n.OpenFilesMinimum,
			Read:                   n.top.remotePool.Read,
			Replicas:               *n.Replicas,
			S3BasePath:             *n.S3BasePath,
			S3Bucket:               *n.S3Bucket,
			S3Client:               s3client,
			S3KeyFormat:            n.formatter,
			UploadLargerThan:       n.uploadFileSize,
			UploadOlder:            *n.UploadOlder,
			UploadWorkQueue:        n.top.getUploadWorkQueue(),
		})
	}

	return n.storage
}

func (n *nameSpace) validate(top *top, name string) []string {
	var errors []string

	// Copy the name given into the structure.
	n.name = name
	n.top = top

	// AWSProfile
	if n.AWSProfile == nil {
		errors = append(
			errors,
			"namespace."+name+".aws_profile is a required field.")
	} else if _, ok := n.top.AWSProfiles[*n.AWSProfile]; !ok {
		errors = append(
			errors,
			"namespace."+name+".aws_profile is not a defined profile.")
	}

	// BlastPathACL
	if n.BlastPathACL != nil {
		errors = append(
			errors,
			n.BlastPathACL.validate(top, name+".blast_path_acl")...)
	}

	// Compress
	if n.Compress == nil {
		n.Compress = &defaultCompress
	}

	// CompressLevel
	if n.CompressLevel != nil && !*n.Compress {
		errors = append(
			errors,
			"namespace."+name+".compress_level requires compress be true.")
	} else if n.CompressLevel == nil {
		n.CompressLevel = &defaultCompressLevel
	} else if *n.CompressLevel < -1 || *n.CompressLevel > gzip.BestCompression {
		errors = append(
			errors,
			"namespace."+name+".compress_level must be between -1 and 9.")
	}

	// DelayDelete
	if n.DelayDelete == nil {
		n.DelayDelete = &defaultDelayDelete
	} else if *n.DelayDelete < 0 {
		errors = append(
			errors,
			"namespace."+name+".delay_delete can not be negative.")
	}

	// Directory
	if n.Directory == nil {
		errors = append(errors, "namespace."+name+".directory is required.")
	}

	// InsertACL
	if n.InsertACL != nil {
		errors = append(
			errors,
			n.InsertACL.validate(top, name+".insert_acl")...)
	}

	// OpenFilesMinimum
	if n.OpenFilesMinimum == nil {
		n.OpenFilesMinimum = &defaultOpenFilesMinimum
	} else if *n.OpenFilesMinimum < 1 {
		errors = append(
			errors,
			"namespace."+name+".min_open_files must be greater than 0.")
	}

	// OpenFilesMaximum
	if n.OpenFilesMaximum == nil {
		n.OpenFilesMaximum = n.OpenFilesMinimum
	} else if *n.OpenFilesMaximum < *n.OpenFilesMinimum {
		errors = append(errors, ""+
			"namespace."+name+".max_open_files must be "+
			"greater than min_open_files.")
	}

	// PrimaryACL
	if n.PrimaryACL != nil {
		errors = append(
			errors,
			n.PrimaryACL.validate(top, name+".primary_acl")...)
	}

	// ReadACL
	if n.ReadACL != nil {
		errors = append(
			errors,
			n.ReadACL.validate(top, name+".read_acl")...)
	}

	// Replicas
	if n.Replicas == nil {
		n.Replicas = &defaultReplicas
	} else if *n.Replicas < 0 {
		errors = append(
			errors,
			"namespace."+name+".replicas can not be negative.")
	}

	// S3Bucket
	if n.S3Bucket == nil {
		errors = append(errors, "namespace."+name+".s3_bucket is required.")
	}

	// S3BasePath
	if n.S3BasePath == nil {
		n.S3BasePath = &defaultS3BasePath
	}

	// S3KeyFormat
	if n.S3KeyFormat != nil {
		f, err := fid.NewFormatter(*n.S3KeyFormat)
		if err != nil {
			errors = append(
				errors,
				"namespace."+name+".s3_key_format is not valid ("+
					err.Error()+")")
		} else {
			n.formatter = f
		}
	}

	// UploadFileSize
	if !n.UploadFileSize.set {
		n.uploadFileSize = defaultUploadFileSize
	} else if u, err := n.UploadFileSize.Bytes(); err != nil {
		errors = append(
			errors,
			"namespace."+name+".upload_file_size "+err.Error())
	} else if u < 1 {
		errors = append(
			errors,
			"namespace."+name+".upload_file_size must be greater than 0.")
	} else {
		n.uploadFileSize = uint64(u)
	}

	// UploadOlder
	if n.UploadOlder == nil {
		n.UploadOlder = &defaultUploadOlder
	} else if *n.UploadOlder < time.Second {
		errors = append(
			errors,
			"namespace."+name+".upload_older must be at least 1 second.")
	}

	// Return any errors encountered.
	return errors
}
