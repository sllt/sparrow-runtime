package main

import (
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"syscall"
)

// runStatefulDAG is the state-bearing profile-11 process oracle.  The older
// DAG scenario only exercised stateless Lookup/Branch replay; this one keeps
// both a Count window and a Hysteresis latch in the same required File DAG.
func runStatefulDAG(root, serverBin string) bool {
	must(os.Mkdir(root, 0700))
	first := newCapture()
	defer first.close()
	second := newCapture()
	defer second.close()

	file := filepath.Join(root, "sensors.ndjson")
	checkpoint := filepath.Join(root, "checkpoint-v11")
	const pipeline = "stateful"
	port := freePort()
	a := newAPI(port)
	var process *child
	stop := func(signal os.Signal) {
		if process != nil {
			process.stop(signal)
			process = nil
		}
	}
	defer func() { stop(syscall.SIGKILL) }()
	start := func() {
		process = spawnServer(serverBin, root, filepath.Join(root, "server.log"), port)
		waitHealth(a, "stateful DAG server health")
	}

	start()
	configure(a, first, 0)
	allowCaptures(a, second)
	r1 := publishTable(a, 10, 0)
	save(filepath.Join(root, "table-r1.json"), r1)

	appendRows(file, []struct {
		key   string
		value int
	}{{"a", 6}, {"a", 7}, {"a", 1}})
	spec := statefulDAGSpec(file, first.url(), second.url(), checkpoint, r1)
	save(filepath.Join(root, "spec-r1.json"), spec)
	startPipeline(a, pipeline, spec)

	threshold := 10
	baselineExpected := []wantedRow{{key: "a", value: 13, field: "s", threshold: &threshold}}
	assertRows(first, 0, baselineExpected)
	assertRows(second, 0, baselineExpected)
	require(first.rowCount() == 1 && second.rowCount() == 1,
		"stateful DAG baseline emitted more than the Active transition")

	// The periodic cut is made while the File contains exactly three records.
	// State count 2 proves that both Count's pending row and Hysteresis's latch
	// participate in the profile-11 snapshot.
	baselineStatus := waitPeriodicStatus(a, pipeline)
	stopPipeline(a, pipeline)
	save(filepath.Join(root, "baseline-stopped-status.json"), statusOf(a, pipeline))
	baselineInventory := inventory(a, pipeline)
	requireCurrentSnapshotVersion(baselineInventory, 11)
	baselineInfo := parseSnapshotInfo(checkpoint, baselineInventory, r1, 11, 2)
	require(baselineInfo.SourceRecord == 3,
		"stateful DAG baseline checkpoint did not cut exactly three File records")
	require(baselineInfo.StateCount == 2,
		"stateful DAG baseline omitted Count or Hysteresis state")
	baselineID := storageCurrent(baselineInventory)
	require(baselineID != 0, "stateful DAG baseline has no checkpoint identity")
	save(filepath.Join(root, "baseline-status.json"), baselineStatus)
	save(filepath.Join(root, "baseline-checkpoints.json"), baselineInventory)
	save(filepath.Join(root, "baseline-snapshot-info.json"), infoToMap(baselineInfo))

	// Publishing newer revisions must not move the pipeline's explicit r1
	// binding.  The eventual rows continue to assert threshold=10, not r2/r3.
	r2 := publishTable(a, 90, 1)
	r3 := publishTable(a, 30, 2)
	save(filepath.Join(root, "table-r2.json"), r2)
	save(filepath.Join(root, "table-r3.json"), r3)
	manual := clone(spec).(map[string]any)
	manual["checkpoint"].(map[string]any)["interval_ms"] = nil
	save(filepath.Join(root, "spec-r1-manual.json"), manual)
	startPipeline(a, pipeline, manual)

	// The first suffix pair combines with the baseline's pending Count row and
	// produces s=8, which remains Active because the exact high hysteresis exit
	// threshold is 5.  The next pair produces s=2 and exits; the final pair
	// produces s=13 and re-enters.  Thus only 2 and 13 are emitted.
	suffix := []struct {
		key   string
		value int
	}{{"a", 7}, {"a", 1}, {"a", 1}, {"a", 6}, {"a", 7}}
	preSuffixFirst, preSuffixSecond := first.rowCount(), second.rowCount()
	appendRows(file, suffix)
	suffixExpected := []wantedRow{
		{key: "a", value: 2, field: "s", threshold: &threshold},
		{key: "a", value: 13, field: "s", threshold: &threshold},
	}
	assertRows(first, preSuffixFirst, suffixExpected)
	assertRows(second, preSuffixSecond, suffixExpected)
	require(first.rowCount() == preSuffixFirst+2 && second.rowCount() == preSuffixSecond+2,
		"stateful DAG did not suppress the in-band Hysteresis transition")

	// No suffix checkpoint has been accepted.  Kill the server and restart
	// from the cut-3 snapshot; both required sinks must replay exactly the two
	// transition rows, proving the restored Count pending row and Active latch.
	stop(syscall.SIGKILL)
	replayFirst, replaySecond := first.rowCount(), second.rowCount()
	start()
	a.ok(http.MethodPost, "/v1/pipelines/"+pipeline+"/start", map[string]any{})
	waitRunning(a, pipeline)
	assertRows(first, replayFirst, suffixExpected)
	assertRows(second, replaySecond, suffixExpected)
	require(first.rowCount() == replayFirst+2 && second.rowCount() == replaySecond+2,
		"stateful DAG restart replayed an unexpected number of rows")
	restored := statusOf(a, pipeline)
	require(number(nested(restored, "checkpoint", "restored_from_checkpoint")) == baselineID,
		"stateful DAG restart did not restore the cut-3 checkpoint")
	save(filepath.Join(root, "restore-status.json"), restored)
	save(filepath.Join(root, "outputs.json"), map[string]any{
		"first": first.snapshot(), "second": second.snapshot(),
	})

	// Commit the replayed suffix and inspect the actual v11/CPL3 state-bearing
	// snapshot, including its source cut and immutable dependency identity.
	_, previousSucceeded, _, _, _ := checkpointFields(restored)
	_, code := manualCheckpoint(a, pipeline)
	require(code >= 200 && code < 300, "stateful DAG replay checkpoint request failed")
	actualStatus := waitCheckpointSuccess(a, pipeline, previousSucceeded)
	actualInventory := inventory(a, pipeline)
	requireCurrentSnapshotVersion(actualInventory, 11)
	actualInfo := parseSnapshotInfo(checkpoint, actualInventory, r1, 11, 2)
	require(actualInfo.SourceRecord == 8,
		"stateful DAG replay checkpoint did not cut all eight File records")
	require(actualInfo.StateCount == 2,
		"stateful DAG replay checkpoint omitted Count or Hysteresis state")
	require(actualInfo.ReferenceRev == number(r1["revision"]) && actualInfo.ReferenceSHA == r1["sha256"],
		"stateful DAG replay checkpoint changed the explicit r1 dependency")
	save(filepath.Join(root, "actual-status.json"), actualStatus)
	save(filepath.Join(root, "actual-checkpoints.json"), actualInventory)
	save(filepath.Join(root, "actual-snapshot-info.json"), infoToMap(actualInfo))

	deps := a.ok(http.MethodGet, "/v1/tables/limits/dependencies", nil)
	save(filepath.Join(root, "dependencies.json"), deps)
	a.ok(http.MethodPost, "/v1/tables/limits/gc", map[string]any{})
	r1AfterGC := a.ok(http.MethodGet, "/v1/tables/limits/revisions/1", nil)
	require(r1AfterGC["sha256"] == r1["sha256"],
		"stateful DAG GC deleted the checkpoint-required r1 table")
	save(filepath.Join(root, "r1-after-gc.json"), r1AfterGC)

	stopPipeline(a, pipeline)
	stop(syscall.SIGTERM)
	saveScenarioSummary(root, "reference_stateful_dag_file_v11", 11, map[string]any{
		"source_kind":             "file-dag-v1",
		"state_count":             actualInfo.StateCount,
		"baseline_checkpoint_id":  baselineID,
		"actual_checkpoint_id":    storageCurrent(actualInventory),
		"baseline_source_record":  baselineInfo.SourceRecord,
		"actual_source_record":    actualInfo.SourceRecord,
		"baseline_active_latch":   true,
		"baseline_pending_count":  true,
		"suffix_live_outputs":     []int{2, 13},
		"suffix_replay_outputs":   []int{2, 13},
		"both_required_sinks":     true,
		"fixed_r1_after_r2_gc":    true,
		"reference_revision":      actualInfo.ReferenceRev,
		"reference_sha256":        actualInfo.ReferenceSHA,
		"reference_runtime_crc32": actualInfo.ReferenceCRC32,
		"tested_cases": []string{
			"count_pending_plus_hysteresis_active_at_cut3",
			"exact_hysteresis_exit5_enter10",
			"required_two_sink_replay",
			"r1_fixed_after_r2_r3_and_gc",
		},
	})
	fmt.Println("REFERENCE_STATEFUL_DAG_OK")
	return true
}

func statefulDAGSpec(file, sinkA, sinkB, checkpoint string, binding map[string]any) map[string]any {
	source := fileSource(file)
	return map[string]any{
		"version": 1, "stream": "sensors",
		"reference_tables": map[string]any{
			"limits": map[string]any{"revision": binding["revision"], "sha256": binding["sha256"]},
		},
		"source": source,
		"sink": map[string]any{
			"kind": "http", "url": sinkA, "batch_rows": 2, "linger_ms": 1,
			"max_inflight": 1, "outbox_capacity": 8,
		},
		"delivery": "live_best_effort", "recovery": "aligned", "checkpoint_dir": checkpoint,
		"checkpoint": checkpointPolicy(float64(1000), true),
		"graph_io": map[string]any{
			"sources": map[string]any{"1": source},
			"sinks": map[string]any{
				"6": map[string]any{"kind": "http", "url": sinkA, "batch_rows": 2, "linger_ms": 1, "max_inflight": 1, "outbox_capacity": 8},
				"7": map[string]any{"kind": "http", "url": sinkB, "batch_rows": 2, "linger_ms": 1, "max_inflight": 1, "outbox_capacity": 8},
			},
		},
		"graph": map[string]any{"version": 1, "pipeline_id": 14001, "revision_id": 1, "nodes": []any{
			map[string]any{"id": 1, "kind": "memory_source", "table": "sensors", "out": []uint32{2}},
			lookupNode(2, 3),
			countNode(3, 4, []string{"device_id", "threshold"}),
			map[string]any{"id": 4, "kind": "hysteresis", "iot": statefulHysteresisConfig(), "out": []uint32{5}},
			map[string]any{"id": 5, "kind": "branch", "out": []uint32{6, 7}},
			map[string]any{"id": 6, "kind": "capture_sink", "name": "stateful_a"},
			map[string]any{"id": 7, "kind": "capture_sink", "name": "stateful_b"},
		}},
	}
}

func statefulHysteresisConfig() map[string]any {
	return map[string]any{
		"keys": []string{"device_id"}, "fields": []string{"s"},
		"emit_first": true, "ttl_micros": 0, "max_keys": 64, "invalid": "ignore",
		"hysteresis": map[string]any{"direction": "high", "enter": 10.0, "exit": 5.0},
	}
}
