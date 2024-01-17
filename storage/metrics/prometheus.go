package metrics

import (
	"fmt"
	"io"
)

// Renders the various metrics into a prometheus 0.0.4 compatible output.
func RenderPrometheus(w io.Writer, prefix string, metrics map[string]Metrics) {
	fmt.Fprintf(w, "# TYPE bytes_inserted counter\n")
	fmt.Fprintf(w, "# HELP bytes_inserted Bytes successfully inserted to this namespace.\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `bytes_inserted{%snamespace="%s"} %d`, prefix, namespace, m.BytesInserted)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE file_deletion_failures counter\n")
	fmt.Fprintf(w, "# HELP file_deletion_failures Number of failed file deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `file_deletion_failures{%snamespace="%s"} %d`, prefix, namespace, m.FilesDeleted.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE file_deletion_successes counter\n")
	fmt.Fprintf(w, "# HELP file_deletion_successes Number of successful file deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `file_deletion_successes{%snamespace="%s"} %d`, prefix, namespace, m.FilesDeleted.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE file_deletion_total counter\n")
	fmt.Fprintf(w, "# HELP file_deletion_total Total number of file deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `file_deletion_total{%snamespace="%s"} %d`, prefix, namespace, m.FilesDeleted.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE internal_errors counter\n")
	fmt.Fprintf(w, "# HELP internal_errors The number of internally generated errors encountered.\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `internal_errors{%snamespace="%s",%stype="insert"} %d`, prefix, namespace, prefix, m.InternalInsertErrors)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE oldest_queued_upload_seconds gauge\n")
	fmt.Fprintf(w, "# HELP oldest_queued_upload_seconds The amount of time the oldest file has been queued for upload.\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `oldest_queued_upload_seconds{%snamespace="%s"} %f`, prefix, namespace, m.OldestQueuedUpload)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_delete_failures counter\n")
	fmt.Fprintf(w, "# HELP primary_delete_failures Number of failed primary deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_delete_failures{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryDeletes.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_delete_successes counter\n")
	fmt.Fprintf(w, "# HELP primary_delete_successes Number of successful primary deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_delete_successes{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryDeletes.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_delete_total counter\n")
	fmt.Fprintf(w, "# HELP primary_delete_total Total number of primary deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_delete_total{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryDeletes.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_insert_failures counter\n")
	fmt.Fprintf(w, "# HELP primary_insert_failures Number of failed primary inserts\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_insert_failures{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryInserts.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_insert_successes counter\n")
	fmt.Fprintf(w, "# HELP primary_insert_successes Number of successful primary inserts\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_insert_successes{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryInserts.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_insert_total counter\n")
	fmt.Fprintf(w, "# HELP primary_insert_total Total number of primary inserts\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_insert_total{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryInserts.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_oldest_unuploaded_file_age_seconds gauge\n")
	fmt.Fprintf(w, "# HELP primary_oldest_unuploaded_file_age_seconds Age (in seconds) of oldest file that has not been uploaded to S3\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_oldest_unuploaded_file_age_seconds{%snamespace="%s"} %f`, prefix, namespace, m.OldestUnUploadedData)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_open_failures counter\n")
	fmt.Fprintf(w, "# HELP primary_open_failures Number of failed primary opens\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_open_failures{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryOpens.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_open_successes counter\n")
	fmt.Fprintf(w, "# HELP primary_open_successes Number of successful primary opens\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_open_successes{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryOpens.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_open_total counter\n")
	fmt.Fprintf(w, "# HELP primary_open_total Total number of primary opens\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_open_total{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryOpens.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_upload_failures counter\n")
	fmt.Fprintf(w, "# HELP primary_upload_failures Number of failed primary uploads\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_upload_failures{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryUploads.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_upload_successes counter\n")
	fmt.Fprintf(w, "# HELP primary_upload_successes Number of successful primary uploads\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_upload_successes{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryUploads.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE primary_upload_total counter\n")
	fmt.Fprintf(w, "# HELP primary_upload_total Total number of primary uploads\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `primary_upload_total{%snamespace="%s"} %d`, prefix, namespace, m.PrimaryUploads.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE queued_inserts gauge\n")
	fmt.Fprintf(w, "# HELP queued_inserts The number of callers waiting in q eue for a file to write too.\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `queued_inserts{%snamespace="%s"} %d`, prefix, namespace, m.QueuedInserts)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_delete_failures counter\n")
	fmt.Fprintf(w, "# HELP replica_delete_failures Number of failed replica deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_delete_failures{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaDeletes.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_delete_successes counter\n")
	fmt.Fprintf(w, "# HELP replica_delete_successes Number of successful replica deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_delete_successes{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaDeletes.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_delete_total counter\n")
	fmt.Fprintf(w, "# HELP replica_delete_total Total number of replica deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_delete_total{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaDeletes.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_heartbeat_failures counter\n")
	fmt.Fprintf(w, "# HELP replica_heartbeat_failures Number of failed replica heartbeats\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_heartbeat_failures{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaHeartBeats.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_heartbeat_successes counter\n")
	fmt.Fprintf(w, "# HELP replica_heartbeat_successes Number of successful replica heartbeats\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_heartbeat_successes{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaHeartBeats.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_heartbeat_total counter\n")
	fmt.Fprintf(w, "# HELP replica_heartbeat_total Total number of replica heartbeats\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_heartbeat_total{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaHeartBeats.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_initialize_failures counter\n")
	fmt.Fprintf(w, "# HELP replica_initialize_failures Number of failed replica initializationss\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_initialize_failures{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaInitializes.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_initialize_successes counter\n")
	fmt.Fprintf(w, "# HELP replica_initialize_successes Number of successful replica initializations\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_initialize_successes{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaInitializes.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_initialize_total counter\n")
	fmt.Fprintf(w, "# HELP replica_initialize_total Total number of replica initializations\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_initialize_total{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaInitializes.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_queuedelete_failures counter\n")
	fmt.Fprintf(w, "# HELP replica_queuedelete_failures Number of failed replica queue deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_queuedelete_failures{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaQueueDeletes.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_queuedelete_successes counter\n")
	fmt.Fprintf(w, "# HELP replica_queuedelete_successes Number of successful replica queue deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_queuedelete_successes{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaQueueDeletes.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_queuedelete_total counter\n")
	fmt.Fprintf(w, "# HELP replica_queuedelete_total Total number of replica queue deletes\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_queuedelete_total{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaQueueDeletes.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_replicate_failures counter\n")
	fmt.Fprintf(w, "# HELP replica_replicate_failures Number of failed replica replications\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_replicate_failures{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaReplicates.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_replicate_successes counter\n")
	fmt.Fprintf(w, "# HELP replica_replicate_successes Number of successful replica replications\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_replicate_successes{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaReplicates.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_replicate_total counter\n")
	fmt.Fprintf(w, "# HELP replica_replicate_total Total number of replica replications\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_replicate_total{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaReplicates.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_upload_failures counter\n")
	fmt.Fprintf(w, "# HELP replica_upload_failures Number of failed replica uploads\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_upload_failures{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaUploads.Failures)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_upload_successes counter\n")
	fmt.Fprintf(w, "# HELP replica_upload_successes Number of successful replica uploads\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_upload_successes{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaUploads.Successes)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replica_upload_total counter\n")
	fmt.Fprintf(w, "# HELP replica_upload_total Total number of replica uploads\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replica_upload_total{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaUploads.Total)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE replicas_orphaned counter\n")
	fmt.Fprintf(w, "# HELP replicas_orphaned Count of the number of replicas that have been marked as orphaned and have moved into an uploading state.\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `replicas_orphaned{%snamespace="%s"} %d`, prefix, namespace, m.ReplicaOrphaned)
		w.Write([]byte{'\n'})
	}
	w.Write([]byte{'\n'})

	fmt.Fprintf(w, "# TYPE timing_data_nanoseconds counter\n")
	fmt.Fprintf(w, "# HELP timing_data_nanoseconds The amount of time various operations have taken in aggregate since server startup.\n")
	for namespace, m := range metrics {
		fmt.Fprintf(w, `timing_data_nanoseconds{%snamespace="%s",%stype="primary_insert_queue"} %d`, prefix, namespace, prefix, m.PrimaryInsertQueueNanoseconds)
		w.Write([]byte{'\n'})
		fmt.Fprintf(w, `timing_data_nanoseconds{%snamespace="%s",%stype="primary_insert_replicate"} %d`, prefix, namespace, prefix, m.PrimaryInsertReplicateNanoseconds)
		w.Write([]byte{'\n'})
		fmt.Fprintf(w, `timing_data_nanoseconds{%snamespace="%s",%stype="primary_insert_write"} %d`, prefix, namespace, prefix, m.PrimaryInsertWriteNanoseconds)
		w.Write([]byte{'\n'})
	}
}
