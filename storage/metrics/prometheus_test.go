package metrics

import (
	"bytes"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/liquidgecka/testlib"
)

func setValue(T *testlib.T, v reflect.Value, s int64) {
	switch v.Kind() {
	case reflect.Float64:
		v.Set(reflect.ValueOf(float64(s)))
	case reflect.Int:
		v.Set(reflect.ValueOf(int(s)))
	case reflect.Int8:
		v.Set(reflect.ValueOf(int8(s)))
	case reflect.Int16:
		v.Set(reflect.ValueOf(int16(s)))
	case reflect.Int32:
		v.Set(reflect.ValueOf(int32(s)))
	case reflect.Int64:
		v.Set(reflect.ValueOf(int64(s)))
	case reflect.Uint:
		v.Set(reflect.ValueOf(uint(s)))
	case reflect.Uint8:
		v.Set(reflect.ValueOf(uint8(s)))
	case reflect.Uint16:
		v.Set(reflect.ValueOf(uint16(s)))
	case reflect.Uint32:
		v.Set(reflect.ValueOf(uint32(s)))
	case reflect.Uint64:
		v.Set(reflect.ValueOf(uint64(s)))
	case reflect.String:
		v.Set(reflect.ValueOf(""))
	case reflect.Struct:
		for i := 0; i < v.NumField(); i++ {
			setValue(T, v.Field(i), s)
		}
	default:
		T.Fatalf("The Metrics{} object has a field with an unknown type.")
	}
}

func TestPrometheus(t *testing.T) {
	T := testlib.NewT(t)
	defer T.Finish()

	// Create a set of metrics objects (3) that we can use for metric
	// generation. Each will have every single field populated with the
	// number of its iteration.
	m1 := Metrics{}
	setValue(T, reflect.Indirect(reflect.ValueOf(&m1)), 1)
	m2 := Metrics{}
	setValue(T, reflect.Indirect(reflect.ValueOf(&m2)), 2)
	m3 := Metrics{}
	setValue(T, reflect.Indirect(reflect.ValueOf(&m3)), 3)
	metrics := map[string]Metrics{
		"test1": m1,
		"test2": m2,
		"test3": m3,
	}

	// Create a buffer for saving the results.
	buffer := bytes.NewBuffer(nil)

	// The output rendered via prometheus can be _unordered_ meaning that
	// the individual metrics within a "section" can actually be rendered
	// in any order that the map returns. As such we need to adjust the
	// output to be strictly ordered so we can do a simple string
	// comparison.
	have := func() string {
		lines := strings.Split(buffer.String(), "\n")
		f := -1
		for i, l := range lines {
			if strings.Contains(l, "{") {
				if f == -1 {
					f = i
				}
			} else if f != -1 {
				sort.Strings(lines[f:i])
				f = -1
			}
		}
		return strings.Join(lines, "\n")
	}

	want := `# TYPE bytes_inserted counter
# HELP bytes_inserted Bytes successfully inserted to this namespace.
bytes_inserted{namespace="test1"} 1
bytes_inserted{namespace="test2"} 2
bytes_inserted{namespace="test3"} 3

# TYPE file_deletion_failures counter
# HELP file_deletion_failures Number of failed file deletes
file_deletion_failures{namespace="test1"} 1
file_deletion_failures{namespace="test2"} 2
file_deletion_failures{namespace="test3"} 3

# TYPE file_deletion_successes counter
# HELP file_deletion_successes Number of successful file deletes
file_deletion_successes{namespace="test1"} 1
file_deletion_successes{namespace="test2"} 2
file_deletion_successes{namespace="test3"} 3

# TYPE file_deletion_total counter
# HELP file_deletion_total Total number of file deletes
file_deletion_total{namespace="test1"} 1
file_deletion_total{namespace="test2"} 2
file_deletion_total{namespace="test3"} 3

# TYPE internal_errors counter
# HELP internal_errors The number of internally generated errors encountered.
internal_errors{namespace="test1",type="insert"} 1
internal_errors{namespace="test2",type="insert"} 2
internal_errors{namespace="test3",type="insert"} 3

# TYPE oldest_queued_upload_seconds gauge
# HELP oldest_queued_upload_seconds The amount of time the oldest file has been queued for upload.
oldest_queued_upload_seconds{namespace="test1"} 1.000000
oldest_queued_upload_seconds{namespace="test2"} 2.000000
oldest_queued_upload_seconds{namespace="test3"} 3.000000

# TYPE primary_delete_failures counter
# HELP primary_delete_failures Number of failed primary deletes
primary_delete_failures{namespace="test1"} 1
primary_delete_failures{namespace="test2"} 2
primary_delete_failures{namespace="test3"} 3

# TYPE primary_delete_successes counter
# HELP primary_delete_successes Number of successful primary deletes
primary_delete_successes{namespace="test1"} 1
primary_delete_successes{namespace="test2"} 2
primary_delete_successes{namespace="test3"} 3

# TYPE primary_delete_total counter
# HELP primary_delete_total Total number of primary deletes
primary_delete_total{namespace="test1"} 1
primary_delete_total{namespace="test2"} 2
primary_delete_total{namespace="test3"} 3

# TYPE primary_insert_failures counter
# HELP primary_insert_failures Number of failed primary inserts
primary_insert_failures{namespace="test1"} 1
primary_insert_failures{namespace="test2"} 2
primary_insert_failures{namespace="test3"} 3

# TYPE primary_insert_successes counter
# HELP primary_insert_successes Number of successful primary inserts
primary_insert_successes{namespace="test1"} 1
primary_insert_successes{namespace="test2"} 2
primary_insert_successes{namespace="test3"} 3

# TYPE primary_insert_total counter
# HELP primary_insert_total Total number of primary inserts
primary_insert_total{namespace="test1"} 1
primary_insert_total{namespace="test2"} 2
primary_insert_total{namespace="test3"} 3

# TYPE primary_oldest_unuploaded_file_age_seconds gauge
# HELP primary_oldest_unuploaded_file_age_seconds Age (in seconds) of oldest file that has not been uploaded to S3
primary_oldest_unuploaded_file_age_seconds{namespace="test1"} 1.000000
primary_oldest_unuploaded_file_age_seconds{namespace="test2"} 2.000000
primary_oldest_unuploaded_file_age_seconds{namespace="test3"} 3.000000

# TYPE primary_open_failures counter
# HELP primary_open_failures Number of failed primary opens
primary_open_failures{namespace="test1"} 1
primary_open_failures{namespace="test2"} 2
primary_open_failures{namespace="test3"} 3

# TYPE primary_open_successes counter
# HELP primary_open_successes Number of successful primary opens
primary_open_successes{namespace="test1"} 1
primary_open_successes{namespace="test2"} 2
primary_open_successes{namespace="test3"} 3

# TYPE primary_open_total counter
# HELP primary_open_total Total number of primary opens
primary_open_total{namespace="test1"} 1
primary_open_total{namespace="test2"} 2
primary_open_total{namespace="test3"} 3

# TYPE primary_upload_failures counter
# HELP primary_upload_failures Number of failed primary uploads
primary_upload_failures{namespace="test1"} 1
primary_upload_failures{namespace="test2"} 2
primary_upload_failures{namespace="test3"} 3

# TYPE primary_upload_successes counter
# HELP primary_upload_successes Number of successful primary uploads
primary_upload_successes{namespace="test1"} 1
primary_upload_successes{namespace="test2"} 2
primary_upload_successes{namespace="test3"} 3

# TYPE primary_upload_total counter
# HELP primary_upload_total Total number of primary uploads
primary_upload_total{namespace="test1"} 1
primary_upload_total{namespace="test2"} 2
primary_upload_total{namespace="test3"} 3

# TYPE queued_inserts gauge
# HELP queued_inserts The number of callers waiting in q eue for a file to write too.
queued_inserts{namespace="test1"} 1
queued_inserts{namespace="test2"} 2
queued_inserts{namespace="test3"} 3

# TYPE replica_delete_failures counter
# HELP replica_delete_failures Number of failed replica deletes
replica_delete_failures{namespace="test1"} 1
replica_delete_failures{namespace="test2"} 2
replica_delete_failures{namespace="test3"} 3

# TYPE replica_delete_successes counter
# HELP replica_delete_successes Number of successful replica deletes
replica_delete_successes{namespace="test1"} 1
replica_delete_successes{namespace="test2"} 2
replica_delete_successes{namespace="test3"} 3

# TYPE replica_delete_total counter
# HELP replica_delete_total Total number of replica deletes
replica_delete_total{namespace="test1"} 1
replica_delete_total{namespace="test2"} 2
replica_delete_total{namespace="test3"} 3

# TYPE replica_heartbeat_failures counter
# HELP replica_heartbeat_failures Number of failed replica heartbeats
replica_heartbeat_failures{namespace="test1"} 1
replica_heartbeat_failures{namespace="test2"} 2
replica_heartbeat_failures{namespace="test3"} 3

# TYPE replica_heartbeat_successes counter
# HELP replica_heartbeat_successes Number of successful replica heartbeats
replica_heartbeat_successes{namespace="test1"} 1
replica_heartbeat_successes{namespace="test2"} 2
replica_heartbeat_successes{namespace="test3"} 3

# TYPE replica_heartbeat_total counter
# HELP replica_heartbeat_total Total number of replica heartbeats
replica_heartbeat_total{namespace="test1"} 1
replica_heartbeat_total{namespace="test2"} 2
replica_heartbeat_total{namespace="test3"} 3

# TYPE replica_initialize_failures counter
# HELP replica_initialize_failures Number of failed replica initializationss
replica_initialize_failures{namespace="test1"} 1
replica_initialize_failures{namespace="test2"} 2
replica_initialize_failures{namespace="test3"} 3

# TYPE replica_initialize_successes counter
# HELP replica_initialize_successes Number of successful replica initializations
replica_initialize_successes{namespace="test1"} 1
replica_initialize_successes{namespace="test2"} 2
replica_initialize_successes{namespace="test3"} 3

# TYPE replica_initialize_total counter
# HELP replica_initialize_total Total number of replica initializations
replica_initialize_total{namespace="test1"} 1
replica_initialize_total{namespace="test2"} 2
replica_initialize_total{namespace="test3"} 3

# TYPE replica_queuedelete_failures counter
# HELP replica_queuedelete_failures Number of failed replica queue deletes
replica_queuedelete_failures{namespace="test1"} 1
replica_queuedelete_failures{namespace="test2"} 2
replica_queuedelete_failures{namespace="test3"} 3

# TYPE replica_queuedelete_successes counter
# HELP replica_queuedelete_successes Number of successful replica queue deletes
replica_queuedelete_successes{namespace="test1"} 1
replica_queuedelete_successes{namespace="test2"} 2
replica_queuedelete_successes{namespace="test3"} 3

# TYPE replica_queuedelete_total counter
# HELP replica_queuedelete_total Total number of replica queue deletes
replica_queuedelete_total{namespace="test1"} 1
replica_queuedelete_total{namespace="test2"} 2
replica_queuedelete_total{namespace="test3"} 3

# TYPE replica_replicate_failures counter
# HELP replica_replicate_failures Number of failed replica replications
replica_replicate_failures{namespace="test1"} 1
replica_replicate_failures{namespace="test2"} 2
replica_replicate_failures{namespace="test3"} 3

# TYPE replica_replicate_successes counter
# HELP replica_replicate_successes Number of successful replica replications
replica_replicate_successes{namespace="test1"} 1
replica_replicate_successes{namespace="test2"} 2
replica_replicate_successes{namespace="test3"} 3

# TYPE replica_replicate_total counter
# HELP replica_replicate_total Total number of replica replications
replica_replicate_total{namespace="test1"} 1
replica_replicate_total{namespace="test2"} 2
replica_replicate_total{namespace="test3"} 3

# TYPE replica_upload_failures counter
# HELP replica_upload_failures Number of failed replica uploads
replica_upload_failures{namespace="test1"} 1
replica_upload_failures{namespace="test2"} 2
replica_upload_failures{namespace="test3"} 3

# TYPE replica_upload_successes counter
# HELP replica_upload_successes Number of successful replica uploads
replica_upload_successes{namespace="test1"} 1
replica_upload_successes{namespace="test2"} 2
replica_upload_successes{namespace="test3"} 3

# TYPE replica_upload_total counter
# HELP replica_upload_total Total number of replica uploads
replica_upload_total{namespace="test1"} 1
replica_upload_total{namespace="test2"} 2
replica_upload_total{namespace="test3"} 3

# TYPE replicas_orphaned counter
# HELP replicas_orphaned Count of the number of replicas that have been marked as orphaned and have moved into an uploading state.
replicas_orphaned{namespace="test1"} 1
replicas_orphaned{namespace="test2"} 2
replicas_orphaned{namespace="test3"} 3

# TYPE timing_data_nanoseconds counter
# HELP timing_data_nanoseconds The amount of time various operations have taken in aggregate since server startup.
timing_data_nanoseconds{namespace="test1",type="primary_insert_queue"} 1
timing_data_nanoseconds{namespace="test1",type="primary_insert_replicate"} 1
timing_data_nanoseconds{namespace="test1",type="primary_insert_write"} 1
timing_data_nanoseconds{namespace="test2",type="primary_insert_queue"} 2
timing_data_nanoseconds{namespace="test2",type="primary_insert_replicate"} 2
timing_data_nanoseconds{namespace="test2",type="primary_insert_write"} 2
timing_data_nanoseconds{namespace="test3",type="primary_insert_queue"} 3
timing_data_nanoseconds{namespace="test3",type="primary_insert_replicate"} 3
timing_data_nanoseconds{namespace="test3",type="primary_insert_write"} 3
`

	// We run this test with both an empty prefix (default) and with a
	// defined prefix to make sure that the output is what we expect.
	RenderPrometheus(buffer, "", metrics)
	T.Equal(strings.Split(have(), "\n"), strings.Split(want, "\n"))

	// Truncate the buffer and modify the `want` value to have prefixes.
	buffer.Truncate(0)
	want = strings.ReplaceAll(want, "namespace=", "prefix_namespace=")
	want = strings.ReplaceAll(want, "type=", "prefix_type=")
	RenderPrometheus(buffer, "prefix_", metrics)
	T.Equal(strings.Split(have(), "\n"), strings.Split(want, "\n"))
}
