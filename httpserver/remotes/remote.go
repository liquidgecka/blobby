package remotes

import (
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strconv"

	"github.com/pkg/errors"

	"github.com/iterable/blobby/storage"
)

type Remote struct {
	// Tracks the name of this Remote. This is used in logging to identify
	// a given remote against others. This may be a nice hostname where the
	// actual configuration points to an IP address directly.
	Name string

	// The URL (Including protocol, port) used to access the blobby
	// server. This is the base so it shouldn't contain any paths at all.
	URL string

	// The numeric ID given to this specific server. This needs to be unique
	// in the cluster.
	ID uint32

	// The http.Client that will be used when making http requests to this
	// specific client. This is useful if you need to support sending TLS
	// requests to a specific IP but still want to perform hostname validation.
	Client *http.Client
}

func (r *Remote) Delete(namespace, fn string) error {
	// Generate the request.
	request, err := http.NewRequest(
		"DELETE",
		fmt.Sprintf("%s/%s/%s",
			r.URL,
			namespace,
			fn),
		nilReader{})
	if err != nil {
		return errors.WithMessage(
			err,
			"Error generating DELETE request.",
		)
	}

	// Perform the request.
	resp, err := r.Client.Do(request)
	if err != nil {
		return errors.Wrap(
			err,
			"Error sending a request to remote: ")
	}

	// Ensure that the Body of the request is read so the connection
	// can get reused.
	defer ioutil.ReadAll(resp.Body)

	// Check the status code.
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf(
			"Invalid response code: %d",
			resp.StatusCode)
	}

	// Success!
	return nil
}

func (r *Remote) HeartBeat(namespace, fn string) (bool, error) {
	// Generate the request.
	request, err := http.NewRequest(
		"HEARTBEAT",
		fmt.Sprintf("%s/%s/%s",
			r.URL,
			namespace,
			fn),
		nilReader{})
	if err != nil {
		return true, errors.Wrap(
			err,
			"Error generating HEARTBEAT request: ",
		)
	}

	// Perform the request.
	resp, err := r.Client.Do(request)
	if err != nil {
		return true, errors.Wrap(
			err,
			"Error sending a request to remote: ")
	}

	// Ensure that the Body of the request is read so the connection
	// can get reused.
	defer ioutil.ReadAll(resp.Body)

	// Check the status code.
	if resp.StatusCode != http.StatusNoContent {
		return true, fmt.Errorf(
			"Invalid response code: %d",
			resp.StatusCode)
	}

	// Success!
	return resp.Header.Get("Shutting-Down") == "true", nil
}

func (r *Remote) Initialize(namespace, fn string) error {
	// Generate the request.
	request, err := http.NewRequest(
		"INITIALIZE",
		fmt.Sprintf("%s/%s/%s",
			r.URL,
			namespace,
			fn),
		nilReader{})
	if err != nil {
		return errors.WithMessage(
			err,
			"Error generating INITIALIZE request.",
		)
	}

	// Perform the request.
	resp, err := r.Client.Do(request)
	if err != nil {
		return errors.Wrap(
			err,
			"Error sending a request to remote: ")
	}

	// Ensure that the Body of the request is read so the connection
	// can get reused.
	defer ioutil.ReadAll(resp.Body)

	// Check the status code.
	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf(
			"Invalid response code: %d",
			resp.StatusCode)
	}

	// Success!
	return nil
}

// When the storage.Storage object gets a Read() request for a file
// id that was generated on another machine it will attempt to forward the
// request to that machine so it can be processed locally on that machine
// which is cheaper than going to S3.
func (r *Remote) Read(rc storage.ReadConfig) (io.ReadCloser, error) {
	// Generate the request.
	request, err := http.NewRequest(
		"GET",
		fmt.Sprintf("%s/%s/%s",
			r.URL,
			rc.NameSpace(),
			rc.ID()),
		nilReader{})
	if err != nil {
		return nil, errors.Wrap(
			err,
			"Error generating READ request: ",
		)
	}

	f, ok := rc.Context().(func(r *http.Request))
	if ok {
		f(request)
	}

	// Since this is a Read() request against a replica it should be
	// configured to be local only.
	request.Header.Set("Blobby-Local-Only", "true")

	// Perform the request.
	resp, err := r.Client.Do(request)
	if err != nil {
		return nil, errors.Wrap(
			err,
			"Error sending a request to remote: ")
	}

	// Check the status code.
	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusNotFound:
		ioutil.ReadAll(resp.Body)
		return nil, storage.ErrNotFound("")
	default:
		// Consume the body so the connection is reusable.
		ioutil.ReadAll(resp.Body)
		return nil, fmt.Errorf(
			"Invalid response code: %d",
			resp.StatusCode)
	}

	// Success, return the body.
	return resp.Body, nil
}

// Replicates data that was written to the primary into the replica.
// This takes a RemoteReplicateConfig object that contains a bunch of
// parameters to establish what should be passed to the replica.
// This returns an error if the replication failed for any reason and
// a bool that indicates that the replica is going to be shut down
// soon so further replication should cease.
func (r *Remote) Replicate(
	rc storage.RemoteReplicateConfig,
) (
	shuttingDown bool,
	err error,
) {
	// Generate the request.
	request, err := http.NewRequest(
		"REPLICATE",
		fmt.Sprintf("%s/%s/%s",
			r.URL,
			rc.NameSpace(),
			rc.FileName()),
		rc.GetBody())
	if err != nil {
		return false, errors.WithMessage(
			err,
			"Error generating REPLICATE request.",
		)
	}

	// Add the request headers.
	start := rc.Offset()
	length := rc.Size()
	request.Header.Add("Start", strconv.FormatUint(start, 10))
	request.Header.Add("End", strconv.FormatUint(start+length, 10))
	request.Header.Add("Hash", rc.Hash())

	// Perform the request.
	resp, err := r.Client.Do(request)
	if err != nil {
		return false, errors.Wrap(
			err,
			"Error sending a request to remote: ")
	}

	// Ensure that the body of the request is fully read.
	defer ioutil.ReadAll(resp.Body)

	// Check the status code.
	if resp.StatusCode != http.StatusNoContent {
		return false, fmt.Errorf(
			"Invalid response code: %d",
			resp.StatusCode)
	}

	// Success!
	return resp.Header.Get("Shutting-Down") == "true", nil
}

// Returns the name of the remote as a string.
func (r *Remote) String() string {
	return r.Name
}
