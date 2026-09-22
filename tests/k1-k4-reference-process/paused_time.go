package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// This oracle reads the durable outer cut independently of Rust and compares
// actual HTTP bytes/IDs across real SIGKILL. It does not reimplement timers.
type timeCut struct {
	ID         uint64 `json:"checkpoint_id"`
	Version    uint16 `json:"version"`
	Ingested   uint64 `json:"ingested"`
	Sequence   uint64 `json:"sequence"`
	Micros     int64  `json:"micros"`
	NextOutput uint64 `json:"next_output"`
}

func currentTimeCut(checkpoint string) timeCut {
	current, err := os.ReadFile(filepath.Join(checkpoint, "CURRENT"))
	must(err)
	id, err := strconv.ParseUint(strings.TrimPrefix(strings.TrimSpace(string(current)), "chk-"), 10, 64)
	must(err)
	payload := snapshotPayload(checkpoint, id)
	require(len(payload) >= 38, "truncated timed snapshot")
	version := binary.LittleEndian.Uint16(payload[4:6])
	require(version == 14 || version == 15, "wrong timed snapshot version")
	offset := 38
	readString := func() string {
		n, ok := readU32(payload, &offset)
		require(ok && n <= 65536, "timed string bound")
		b, ok := readBytes(payload, &offset, int(n))
		require(ok, "timed string truncation")
		return string(b)
	}
	kind, path := readString(), readString()
	require((version == 14 && kind == "paused-file-v1") || (version == 15 && kind == "paused-jetstream-v1"), "timed source/profile mismatch")
	decoded, err := hex.DecodeString(path)
	must(err)
	require(len(decoded) >= 20 && string(decoded[:4]) == "PTC1", "time cut encoding")
	_, ok := readBytes(payload, &offset, 16+8+8+16)
	require(ok, "timed provenance truncated")
	_, ok = readBytes(payload, &offset, 16)
	require(ok, "timed output epoch truncated")
	next, ok := readU64(payload, &offset)
	require(ok && next > 0, "timed output cursor absent")
	n, ok := readU32(payload, &offset)
	require(ok && n <= 256*1024, "timed plan bound")
	plan, ok := readBytes(payload, &offset, int(n))
	require(ok && len(plan) >= 4 && string(plan[:4]) == "CPL1", "timed manifest version")
	states, ok := readU16(payload, &offset)
	require(ok && states == 1, "timed profile must have one state")
	return timeCut{ID: id, Version: version, Ingested: binary.LittleEndian.Uint64(payload[14:22]),
		Sequence: binary.LittleEndian.Uint64(decoded[4:12]), Micros: int64(binary.LittleEndian.Uint64(decoded[12:20])), NextOutput: next}
}
func pendingTime(checkpoint string) map[string]any {
	raw, err := os.ReadFile(filepath.Join(checkpoint, "TIME_PENDING"))
	must(err)
	require(len(raw) >= 36 && len(raw) <= 128*1024 && string(raw[:4]) == "TPD1", "decision log envelope")
	sum := sha256.Sum256(raw[36:])
	require(bytes.Equal(sum[:], raw[4:36]), "decision log SHA mismatch")
	var result map[string]any
	must(json.Unmarshal(raw[36:], &result))
	return result
}
func waitTimeCut(checkpoint string, rows, ordinal uint64) timeCut {
	var result timeCut
	wait("committed time cut", func() bool {
		if _, err := os.Stat(filepath.Join(checkpoint, "CURRENT")); err != nil {
			return false
		}
		var ready bool
		result, ready = sampleTimeCut(checkpoint)
		return ready && result.Ingested >= rows && result.NextOutput >= ordinal
	})
	return result
}

// CURRENT is durably switched before PUBLISHED is added for history/GC.
// An unlocked observer can also lose an older generation to GC while reading.
// Retry ONLY ENOENT within wait's fixed deadline, never CRC/format mismatches;
// persistent missing/corrupt publication still cannot pass the oracle.
func sampleTimeCut(checkpoint string) (cut timeCut, ready bool) {
	defer func() {
		if problem := recover(); problem != nil {
			if err, ok := problem.(error); ok && os.IsNotExist(err) {
				ready = false
				return
			}
			panic(problem)
		}
	}()
	return currentTimeCut(checkpoint), true
}
func timedSpec(file, sink, checkpoint, kind string, brokerPort int) map[string]any {
	spec := hysteresisFileSpec(file, sink, checkpoint)
	if brokerPort != 0 {
		spec = hysteresisJSSpec(brokerPort, sink, checkpoint, "paused")
	}
	spec["fail_on_decode"] = true
	spec["checkpoint"] = checkpointPolicy(float64(100), true)
	timing := map[string]any{"kind": "hold_for", "clock": "paused", "duration_micros": 1000000}
	nodeKind := kind
	if kind != "hold_for" {
		nodeKind = "debounce"
		timing = map[string]any{"kind": "debounce", "clock": "paused", "quiet_micros": 200000, "max_wait_micros": 1000000,
			"leading": kind == "debounce_leading", "trailing": kind != "debounce_leading", "reset_on_repeat": true}
	}
	nodes := spec["graph"].(map[string]any)["nodes"].([]any)
	nodes[1] = map[string]any{"id": 2, "kind": nodeKind, "out": []uint32{3}, "iot": map[string]any{
		"keys": []string{"device_id"}, "fields": []string{"active"}, "emit_first": false, "ttl_micros": 0,
		"max_keys": 16, "invalid": "ignore", "timing": timing}}
	return spec
}
func configureTimed(a api, c *capture, brokerPort int) {
	allowCaptures(a, c)
	if brokerPort != 0 {
		a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": brokerPort})
	}
	a.ok(http.MethodPut, "/v1/streams/telemetry", map[string]any{"fields": []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "active", "type": "bool", "nullable": true}}})
}
func runTimedProcess(root, serverBin, natsBin, kind string, jetstream bool) string {
	must(os.Mkdir(root, 0700))
	c := newCapture()
	defer c.close()
	file := filepath.Join(root, "signals.ndjson")
	must(os.WriteFile(file, nil, 0600))
	checkpoint := filepath.Join(root, "checkpoint")
	port := freePort()
	a := newAPI(port)
	var process *child
	stop := func(signal os.Signal) {
		if process != nil {
			command := process.cmd
			process.stop(signal)
			if signal == syscall.SIGKILL {
				require(command.ProcessState != nil, "timed process lacks wait status")
				status, ok := command.ProcessState.Sys().(syscall.WaitStatus)
				require(ok && status.Signaled() && status.Signal() == syscall.SIGKILL, "timed process did not actually die from SIGKILL")
			}
			process = nil
		}
	}
	defer func() { stop(syscall.SIGKILL) }()
	start := func() {
		process = spawnServer(serverBin, root, filepath.Join(root, "server.log"), port)
		waitHealth(a, "timed process")
	}
	brokerPort := 0
	var producer *nats
	if jetstream {
		var broker *child
		broker, producer, brokerPort = startBroker(root, natsBin)
		defer broker.stop(syscall.SIGTERM)
		defer producer.close()
	}
	start()
	configureTimed(a, c, brokerPort)
	spec := timedSpec(file, c.url(), checkpoint, kind, brokerPort)
	save(filepath.Join(root, "spec.json"), spec)
	startPipeline(a, "timed", spec)
	bootstrap := waitTimeCut(checkpoint, 0, 1)
	require(bootstrap.Ingested == 0, "bootstrap consumed input")
	body := data(map[string]any{"device_id": "a", "active": true})
	c.setHold()
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
	// A committed HoldFor pending latch must survive a process outage longer
	// than its duration without spending its remaining logical time.
	if kind == "hold_for" {
		waitTimeCut(checkpoint, 1, 1)
		stop(syscall.SIGKILL)
		stopped := currentTimeCut(checkpoint)
		require(c.receivedRows() == 0, "HoldFor fired before fixture stopped")
		save(filepath.Join(root, "stopped-cut.json"), stopped)
		time.Sleep(1100 * time.Millisecond)
		start()
		a.ok(http.MethodPost, "/v1/pipelines/timed/start", map[string]any{})
		waitRunning(a, "timed")
		time.Sleep(150 * time.Millisecond)
		require(c.receivedRows() == 0, "downtime advanced HoldFor")
	}
	c.waitHeld()
	received := c.snapshot().Received
	require(len(received) == 1, "unexpected pre-crash output count")
	first := received[0]
	verifyOutputIDs([]map[string]any{first}, "", 1)
	require(rowData(first)["active"] == true, "wrong timer payload")
	require(first["id"] != nil, "timed File/JetStream output lacks ID")
	before := currentTimeCut(checkpoint)
	currentHash := hash(filepath.Join(checkpoint, "CURRENT"))
	logHash := hash(filepath.Join(checkpoint, "TIME_PENDING"))
	pending := pendingTime(checkpoint)
	require(number(pending["sequence"]) == before.Sequence+1, "uncommitted decision is not immediate successor")
	save(filepath.Join(root, "held-cut.json"), before)
	save(filepath.Join(root, "held-decision.json"), pending)
	save(filepath.Join(root, "first-output.json"), first)
	if jetstream {
		info := consumerInfo(producer)
		save(filepath.Join(root, "held-broker.json"), info)
		if kind == "debounce_leading" {
			require(number(nested(info, "ack_floor", "stream_seq")) == 0 && number(info["num_ack_pending"]) == 1, "uncommitted input was ACKed")
		}
	}
	time.Sleep(100 * time.Millisecond)
	require(hash(filepath.Join(checkpoint, "CURRENT")) == currentHash, "held HTTP advanced CURRENT")
	stop(syscall.SIGKILL)
	c.releaseHold(true)
	require(hash(filepath.Join(checkpoint, "CURRENT")) == currentHash && hash(filepath.Join(checkpoint, "TIME_PENDING")) == logHash, "SIGKILL changed durable cut")
	start()
	a.ok(http.MethodPost, "/v1/pipelines/timed/start", map[string]any{})
	waitRunning(a, "timed")
	wait("time suffix replay", func() bool { return c.rowCount() == 1 })
	replay := c.snapshot().Rows[0]
	require(bytes.Equal(data(first), data(replay)), "uncommitted output ID/payload changed after SIGKILL")
	committed := waitTimeCut(checkpoint, 1, 2)
	if jetstream {
		wait("timed input ACK", func() bool {
			info := consumerInfo(producer)
			return number(nested(info, "ack_floor", "stream_seq")) == 1 && number(info["num_ack_pending"]) == 0
		})
		save(filepath.Join(root, "committed-broker.json"), consumerInfo(producer))
	}
	stop(syscall.SIGKILL)
	start()
	a.ok(http.MethodPost, "/v1/pipelines/timed/start", map[string]any{})
	waitRunning(a, "timed")
	time.Sleep(400 * time.Millisecond)
	require(c.rowCount() == 1 && c.receivedRows() == 2, "committed timer output was repeated")
	stop(syscall.SIGTERM)
	save(filepath.Join(root, "capture.json"), c.snapshot())
	save(filepath.Join(root, "committed-cut.json"), committed)
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "kind": kind, "jetstream": jetstream,
		"real_sigkill": true, "pending_replay_identical": true, "committed_restart_no_repeat": true,
		"downtime_checked": kind == "hold_for", "broker_uncommitted_input_checked": jetstream && kind == "debounce_leading",
		"profile": committed.Version, "certified": false})
	return checkpoint
}
func runPausedTimeMatrix(root, serverBin, oldServerBin, natsBin string) {
	checkpoints := map[string]string{}
	for _, transport := range []string{"file", "jetstream"} {
		for _, kind := range []string{"hold_for", "debounce", "debounce_leading"} {
			name := transport + "-" + kind
			checkpoints[name] = runTimedProcess(filepath.Join(root, name), serverBin, natsBin, kind, transport == "jetstream")
		}
	}
	guards := map[string]any{}
	for _, transport := range []string{"file", "jetstream"} {
		source := filepath.Join(root, transport+"-hold_for")
		guards[transport] = runProfileGuard(filepath.Join(root, "old-"+transport+"-guard"), oldServerBin,
			filepath.Join(source, "checkpoint"), filepath.Join(source, "signals.ndjson"), "checkpoint source profile mismatch", "old timed profile guard")
	}
	cost := []map[string]any{}
	for _, delay := range []int{0, 20} {
		cost = append(cost, runTimedCost(filepath.Join(root, fmt.Sprintf("serialized-cost-%dms", delay)), serverBin, delay))
	}
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "process_scenarios": 6, "snapshot_versions": []int{14, 15},
		"old_profile_guards": guards, "serialized_cost": cost, "exactly_once_claimed": false, "certified": false})
	fmt.Println("PAUSED_TIME_PROCESS_OK")
}

// A cost observation, NOT a high-throughput capacity or cross-product gate.
// Debounce leading-only with a 1us burst produces exactly one required POST
// per durable source decision.  All 100 rows must reach committed CURRENT.
func runTimedCost(root, serverBin string, delayMS int) map[string]any {
	must(os.Mkdir(root, 0700))
	c := newCapture()
	defer c.close()
	c.setResponseDelay(time.Duration(delayMS) * time.Millisecond)
	file := filepath.Join(root, "signals.ndjson")
	must(os.WriteFile(file, nil, 0600))
	checkpoint := filepath.Join(root, "checkpoint")
	port := freePort()
	a := newAPI(port)
	process := spawnServer(serverBin, root, filepath.Join(root, "server.log"), port)
	defer process.stop(syscall.SIGTERM)
	waitHealth(a, "timed cost server")
	configureTimed(a, c, 0)
	spec := timedSpec(file, c.url(), checkpoint, "debounce_leading", 0)
	timing := spec["graph"].(map[string]any)["nodes"].([]any)[1].(map[string]any)["iot"].(map[string]any)["timing"].(map[string]any)
	timing["quiet_micros"] = 1
	timing["max_wait_micros"] = 1
	save(filepath.Join(root, "spec.json"), spec)
	startPipeline(a, "timed", spec)
	waitTimeCut(checkpoint, 0, 1)
	const rows = 100
	var input bytes.Buffer
	for i := 0; i < rows; i++ {
		input.WriteString("{\"device_id\":\"a\",\"active\":true}\n")
	}
	started := time.Now()
	f, err := os.OpenFile(file, os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	_, err = f.Write(input.Bytes())
	must(err)
	must(f.Sync())
	must(f.Close())
	cut := waitTimeCut(checkpoint, rows, rows+1)
	elapsed := time.Since(started)
	stopPipeline(a, "timed")
	captured := c.snapshot()
	require(len(captured.Rows) == rows && captured.ReceivedRows == rows, "timed cost lost or repeated output")
	verifyOutputIDs(captured.Rows, "", 1)
	for _, row := range captured.Rows {
		require(rowData(row)["active"] == true, "timed cost payload mismatch")
	}
	result := map[string]any{"valid": true, "rows": rows, "http_response_delay_ms": delayMS, "elapsed_seconds": elapsed.Seconds(),
		"committed_rows_per_second": float64(rows) / elapsed.Seconds(), "cut": cut, "output_count": len(captured.Rows),
		"scope": "one_row_per_decision_and_POST; local_filesystem; exploratory_cost_only", "capacity_certified": false}
	save(filepath.Join(root, "summary.json"), result)
	save(filepath.Join(root, "capture.json"), captured)
	return result
}
