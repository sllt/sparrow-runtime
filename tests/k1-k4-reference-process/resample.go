package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

const resamplePeriod = int64(2000000)

func resampleSpec(file, sink, checkpoint, mode string, brokerPort int) map[string]any {
	spec := timedSpec(file, sink, checkpoint, "hold_for", brokerPort)
	node := spec["graph"].(map[string]any)["nodes"].([]any)[1].(map[string]any)
	node["kind"] = "resample"
	iot := node["iot"].(map[string]any)
	iot["fields"] = []string{"value"}
	wait, gap := int64(0), int64(0)
	if mode == "interpolate" {
		wait, gap = 1000000, 4000000
	}
	iot["timing"] = map[string]any{"kind": "resample", "mode": mode, "clock": "paused",
		"period_micros": resamplePeriod, "max_wait_micros": wait, "max_gap_micros": gap,
		"max_emissions_per_decision": 256}
	if mode == "mean" {
		// Exercise the declared optional pure-transform boundary on both
		// sides, including the independently typed/nullable sample output.
		graph := spec["graph"].(map[string]any)
		nodes := graph["nodes"].([]any)
		nodes[0].(map[string]any)["out"] = []int{4}
		node["out"] = []int{5}
		project := func(id, next int, names []string) map[string]any {
			exprs := make([]any, 0, len(names))
			for _, name := range names {
				exprs = append(exprs, map[string]any{"alias": name, "expr": map[string]any{"k": "col", "name": name}})
			}
			return map[string]any{"id": id, "kind": "project", "out": []int{next}, "exprs": exprs}
		}
		graph["nodes"] = append(nodes, project(4, 2, []string{"device_id", "value"}),
			project(5, 3, []string{"device_id", "value", "sparrow_resample_mode", "sparrow_resample_time",
				"sparrow_resample_emitted_at", "sparrow_resample_missing", "sparrow_resample_samples",
				"sparrow_resample_generation", "sparrow_resample_operator"}))
	}
	// One HTTP row makes the held decision/output boundary unambiguous.
	spec["sink"].(map[string]any)["batch_rows"] = 1
	spec["sink"].(map[string]any)["linger_ms"] = 0
	return spec
}

func waitResampleMicros(checkpoint string, version uint16, micros int64) timeCut {
	var result timeCut
	wait("resample committed logical time", func() bool {
		var ready bool
		result, ready = sampleTimeCut(checkpoint, version)
		return ready && result.Micros >= micros
	})
	return result
}

func runResampleProcess(root, serverBin, natsBin, mode string, jetstream bool) int {
	must(os.Mkdir(root, 0700))
	c := newCaptureWithNumbers(true)
	defer c.close()
	file := filepath.Join(root, "input.ndjson")
	must(os.WriteFile(file, nil, 0600))
	checkpoint := filepath.Join(root, "checkpoint")
	port := freePort()
	a := newAPI(port)
	var process *child
	crashes := 0
	stop := func(signal os.Signal) {
		if process != nil {
			silenceStop(&process, signal)
			if signal == syscall.SIGKILL {
				crashes++
			}
		}
	}
	defer func() { stop(syscall.SIGKILL) }()
	start := func() {
		process = spawnServer(serverBin, root, filepath.Join(root, "server.log"), port)
		waitHealth(a, "resample process")
	}
	brokerPort := 0
	var producer *nats
	version := uint16(25)
	if jetstream {
		var broker *child
		broker, producer, brokerPort = startBroker(root, natsBin)
		defer broker.stop(syscall.SIGTERM)
		defer producer.close()
		version = 26
	}
	start()
	allowCaptures(a, c)
	if jetstream {
		a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": brokerPort})
	}
	a.ok(http.MethodPut, "/v1/streams/telemetry", map[string]any{"fields": []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "value", "type": "uint64", "nullable": true},
	}})
	spec := resampleSpec(file, c.url(), checkpoint, mode, brokerPort)
	save(filepath.Join(root, "spec.json"), spec)
	startPipeline(a, "resample", spec)
	waitTimeCut(checkpoint, 0, 1, version)
	// Non-zero bootstrap avoids making the interpolation's first point an
	// accidental exact-zero output instead of the pending-grid fixture.
	waitResampleMicros(checkpoint, version, 100000)
	c.setHold()
	value := json.Number("2")
	if mode == "last" {
		value = json.Number("18446744073709551615")
	}
	body := data(map[string]any{"device_id": "a", "value": value})
	if jetstream {
		producer.request("input.rows", body)
	} else {
		f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
		must(err)
		_, err = f.Write(append(body, '\n'))
		must(err)
		must(f.Sync())
		must(f.Close())
	}
	left := waitTimeCut(checkpoint, 1, 1, version)
	grid := (left.Micros/resamplePeriod + 1) * resamplePeriod
	require(left.Micros%resamplePeriod != 0 && grid-left.Micros > 500000, "fixture did not establish an interior sample with remaining time")
	require(c.receivedRows() == 0, "unexpected output before interval closed")
	stop(syscall.SIGKILL)
	save(filepath.Join(root, "sample-cut.json"), currentTimeCut(checkpoint, version))
	time.Sleep(2200 * time.Millisecond)
	start()
	a.ok(http.MethodPost, "/v1/pipelines/resample/start", map[string]any{})
	waitRunning(a, "resample")
	time.Sleep(150 * time.Millisecond)
	require(c.receivedRows() == 0, "outage advanced sampling clock")
	if mode == "interpolate" {
		pending := waitResampleMicros(checkpoint, version, grid)
		require(pending.Micros < grid+700000 && c.receivedRows() == 0, "fixture missed interpolation wait")
		stop(syscall.SIGKILL)
		save(filepath.Join(root, "waiting-cut.json"), currentTimeCut(checkpoint, version))
		time.Sleep(1200 * time.Millisecond)
		start()
		a.ok(http.MethodPost, "/v1/pipelines/resample/start", map[string]any{})
		waitRunning(a, "resample")
		time.Sleep(150 * time.Millisecond)
		require(c.receivedRows() == 0, "outage expired pending interpolation")
	}
	c.waitHeld()
	received := c.snapshot().Received
	require(len(received) == 1, "unexpected held output count")
	first := received[0]
	verifyOutputIDs([]map[string]any{first}, "", 1)
	row := rowData(first)
	require(row["sparrow_resample_mode"] == mode && number(row["sparrow_resample_time"]) == uint64(grid), "wrong sampling mode/grid")
	require(number(row["sparrow_resample_emitted_at"]) >= uint64(grid), "output predates grid")
	require(row["device_id"] == "a" && generationText(row["sparrow_resample_generation"]) != "", "missing sampling identity")
	if mode == "interpolate" {
		require(row["value"] == nil && row["sparrow_resample_missing"] == true && number(row["sparrow_resample_samples"]) == 0, "interpolation extrapolated without right point")
	} else {
		require(row["sparrow_resample_missing"] == false && number(row["sparrow_resample_samples"]) == 1, "wrong sampled interval")
		if mode == "last" {
			require(fmt.Sprint(row["value"]) == "18446744073709551615", "Last lost UInt64 precision")
		} else {
			mean, err := strconv.ParseFloat(fmt.Sprint(row["value"]), 64)
			require(err == nil && mean == 2, "wrong mean")
		}
	}
	before := currentTimeCut(checkpoint, version)
	decision := pendingTime(checkpoint)
	require(number(decision["sequence"]) == before.Sequence+1, "pending resample decision is not immediate successor")
	currentHash, pendingHash := hash(filepath.Join(checkpoint, "CURRENT")), hash(filepath.Join(checkpoint, "TIME_PENDING"))
	save(filepath.Join(root, "held-cut.json"), before)
	save(filepath.Join(root, "held-decision.json"), decision)
	save(filepath.Join(root, "first-output.json"), first)
	stop(syscall.SIGKILL)
	c.releaseHold(true)
	require(hash(filepath.Join(checkpoint, "CURRENT")) == currentHash && hash(filepath.Join(checkpoint, "TIME_PENDING")) == pendingHash, "SIGKILL changed durable sampling decision")
	start()
	a.ok(http.MethodPost, "/v1/pipelines/resample/start", map[string]any{})
	waitRunning(a, "resample")
	wait("resample pending replay", func() bool { return c.rowCount() == 1 })
	require(bytes.Equal(data(first), data(c.snapshot().Rows[0])), "sampling replay changed ID, grid, time or payload")
	committed := waitTimeCut(checkpoint, 1, 2, version)
	if jetstream {
		wait("resample source ACK", func() bool {
			info := consumerInfo(producer)
			return number(nested(info, "ack_floor", "stream_seq")) == 1 && number(info["num_ack_pending"]) == 0
		})
	}
	stop(syscall.SIGKILL)
	start()
	a.ok(http.MethodPost, "/v1/pipelines/resample/start", map[string]any{})
	waitRunning(a, "resample")
	time.Sleep(250 * time.Millisecond)
	require(c.rowCount() == 1 && c.receivedRows() == 2, "committed sampling output repeated after restart")
	stop(syscall.SIGTERM)
	save(filepath.Join(root, "capture.json"), c.snapshot())
	save(filepath.Join(root, "committed-cut.json"), committed)
	expected := 3
	if mode == "interpolate" {
		expected = 4
	}
	require(crashes == expected, "wrong actual SIGKILL count")
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "mode": mode, "jetstream": jetstream, "snapshot_version": version,
		"actual_sigkill": true, "crash_scenarios": crashes, "pending_replay_identical": true, "committed_restart_no_repeat": true,
		"downtime_paused": true, "interpolation_wait_restore_checked": mode == "interpolate", "certified": false})
	return crashes
}

func runResampleMatrix(root, serverBin, oldServerBin, natsBin string, fileOnly bool) {
	cases, crashes := 0, 0
	transports := []bool{false}
	if !fileOnly {
		transports = append(transports, true)
	}
	guards := map[string]any{}
	for _, js := range transports {
		transport := "file"
		if js {
			transport = "jetstream"
		}
		for _, mode := range []string{"last", "mean", "interpolate"} {
			crashes += runResampleProcess(filepath.Join(root, transport+"-"+mode), serverBin, natsBin, mode, js)
			cases++
		}
		prior := filepath.Join(root, transport+"-last")
		guards[transport] = runProfileGuard(filepath.Join(root, "old-"+transport+"-guard"), oldServerBin,
			filepath.Join(prior, "checkpoint"), filepath.Join(prior, "input.ndjson"), "checkpoint source profile mismatch", "pre-resample profile guard")
	}
	require(crashes == len(transports)*10, "resample matrix crash coverage incomplete")
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "file_only": fileOnly, "transport_cases": cases,
		"crash_scenarios": crashes, "actual_sigkill": true, "modes": []string{"last", "mean", "interpolate"},
		"snapshot_versions": func() []int {
			if fileOnly {
				return []int{25}
			}
			return []int{25, 26}
		}(),
		"old_profile_guards": guards, "exactly_once_claimed": false, "certified": false})
	fmt.Println("RESAMPLE_PROCESS_OK")
}
