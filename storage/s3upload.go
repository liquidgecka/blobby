package storage

import (
	"crypto/md5"
	"encoding/base64"
	"encoding/hex"
	"io"
	"os"
	"strings"

	"github.com/aws/aws-sdk-go/service/s3"

	"github.com/liquidgecka/blobby/internal/logging"
	"github.com/liquidgecka/blobby/storage/fid"
)

// Uploads a file to S3, performing all necessary operations to get it into
// the right place and right encoding.
func uploadToS3(
	fd *os.File,
	f fid.FID,
	s3key string,
	s *Settings,
	l *logging.Logger,
) bool {
	// Seek to the start of the file.
	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		l.Error(
			"Error seeking to the start of the file.",
			logging.NewField("file", fd.Name()),
			logging.NewFieldIface("error", err))
		return false
	}

	// Perform the actual file upload to S3 using a single PutObject
	// call. Ideally later we can make this multi part for better
	// performance but for now this is specifically a singular
	// write. Note that this does NOT use the upload manager provided
	// by AWS because it was found to cause data loss on uploads in
	// rare cases.
	poi := s3.PutObjectInput{
		Bucket: &s.S3Bucket,
		Body:   fd,
		Key:    &s3key,
	}

	// Set the Content-Type of the object to binary since we
	// do not know the type of data being stored in the file.
	ct := "application/octet-stream"
	poi.ContentType = &ct

	// Stat the file to get its size for use with the PutObject request.
	stat, err := fd.Stat()
	if err != nil {
		l.Error(
			"Error stating the file.",
			logging.NewField("file", fd.Name()),
			logging.NewFieldIface("error", err))
		return false
	}
	size := stat.Size()

	// We also can get the MD5 of the content which helps validate
	// the upload to ensure the file is only accepted if the data
	// is correct. We can also get the file length here which helps
	// with validation as well.
	m := md5.New()
	buffer := [1024]byte{}
	if n, err := io.CopyBuffer(m, fd, buffer[:]); err != nil {
		l.Error(
			"Error reading from the file.",
			logging.NewField("file", fd.Name()),
			logging.NewFieldIface("error", err))
		return false
	} else if n != size {
		l.Error(
			"Short copy when calculating MD5 hash.",
			logging.NewFieldInt64("expected-bytes", size),
			logging.NewFieldInt64("copied-bytes", n))
		return false
	}
	poi.ContentLength = &size
	hash := m.Sum(nil)
	base64Hash := base64.StdEncoding.EncodeToString(hash)
	hexHash := hex.EncodeToString(hash)
	poi.ContentMD5 = &base64Hash

	// And lastly we need to seek back to the start again.
	if _, err := fd.Seek(0, io.SeekStart); err != nil {
		l.Error(
			"Error seeking to the start of the file.",
			logging.NewField("file", fd.Name()),
			logging.NewFieldIface("error", err))
		return false
	}

	// Next we need to actually initiate the transfer.
	poo, err := s.S3Client.PutObject(&poi)
	if err != nil {
		l.Warning(
			"Error calling s3:PutObject. The request will be retried.",
			logging.NewField("bucket", *poi.Bucket),
			logging.NewField("key", *poi.Key),
			logging.NewFieldIface("error", err))
		return false
	}

	// Validate that the uploaded content matches the expected validation
	// sums.
	if strings.Trim(*poo.ETag, `"`) != hexHash {
		l.Warning(
			"Uploaded data has a different MD5 hash.",
			logging.NewField("bucket", *poi.Bucket),
			logging.NewField("key", *poi.Key),
			logging.NewField("local-file", fd.Name()),
			logging.NewField("expected-md5", *poi.ContentMD5),
			logging.NewField("returned-md5", *poo.ETag))
		return false
	}

	// Log something so its clear that something got uploaded.
	l.Info(
		"Successfully uploaded to S3.",
		logging.NewField("bucket", *poi.Bucket),
		logging.NewField("key", *poi.Key))

	// Success!
	return true
}
