package blastpath

// The output of the Blast Path API call will return an array of these objects.
// Each describes a primary file in the given namespace and some data about it.
type Record struct {
	FID      string `json:"fid,omitempty"`
	Size     uint64 `json:"size,omitempty"`
	S3Bucket string `json:"s3_bucket,omitempty"`
	S3Key    string `json:"s3_key,omitempty"`
}
