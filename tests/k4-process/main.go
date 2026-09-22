// Private loopback File -> IoT -> HTTP process-fault oracle. No broker,
// service, firewall, or existing deployment is modified. Standard library only.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"
)

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func require(ok bool, message string) {
	if !ok {
		panic(message)
	}
}

func data(value any) []byte {
	raw, err := json.Marshal(value)
	must(err)
	return raw
}

func save(path string, value any) {
	must(os.WriteFile(path, append(data(value), '\n'), 0600))
}

func hash(path string) string {
	file, err := os.Open(path)
	must(err)
	defer file.Close()
	digest := sha256.New()
	_, err = io.Copy(digest, file)
	must(err)
	return hex.EncodeToString(digest.Sum(nil))
}

// checkpointTreeHash covers committed history and generation markers, while
// excluding the process ownership lock and a deliberately-created CURRENT.tmp.
// It is an evidence digest, not a restore authorization mechanism.
func checkpointTreeHash(root string) string {
	digest := sha256.New()
	must(filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		name := filepath.Base(path)
		if name == "WRITER_LOCK" || name == "CURRENT.tmp" {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if info.IsDir() && strings.HasPrefix(name, "chk-") {
			// A failed publication may leave a complete but unpublished
			// generation. It is not committed history and must not make the
			// CURRENT-preservation oracle fail.
			if _, statErr := os.Stat(filepath.Join(path, "PUBLISHED")); os.IsNotExist(statErr) {
				return filepath.SkipDir
			}
		}
		if info.IsDir() {
			return nil
		}
		relative, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		_, _ = digest.Write([]byte(relative))
		_, _ = digest.Write([]byte{0})
		_, _ = digest.Write(raw)
		_, _ = digest.Write([]byte{0})
		return nil
	}))
	return hex.EncodeToString(digest.Sum(nil))
}

func copyTree(source, destination string) {
	must(filepath.Walk(source, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		name := filepath.Base(path)
		if name == "WRITER_LOCK" || name == "CURRENT.tmp" {
			if info.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		relative, err := filepath.Rel(source, path)
		if err != nil {
			return err
		}
		target := filepath.Join(destination, relative)
		if info.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("checkpoint copy contains non-regular file %s", path)
		}
		input, err := os.Open(path)
		if err != nil {
			return err
		}
		defer input.Close()
		output, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			return err
		}
		_, copyErr := io.Copy(output, input)
		closeErr := output.Close()
		if copyErr != nil {
			return copyErr
		}
		return closeErr
	}))
}

func wait(label string, condition func() bool) {
	deadline := time.Now().Add(15 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("deadline: " + label)
}

func freePort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	port := listener.Addr().(*net.TCPAddr).Port
	must(listener.Close())
	return port
}

type capture struct {
	mu       sync.Mutex
	rows     []map[string]any
	at       []time.Time
	server   *http.Server
	listener net.Listener

	holdMu      sync.Mutex
	hold        bool
	holdStarted chan struct{}
	holdRelease chan struct{}
}

func newCapture() *capture {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	capture := &capture{listener: listener}
	capture.holdRelease = make(chan struct{})
	close(capture.holdRelease)
	capture.server = &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			defer request.Body.Close()
			body, err := io.ReadAll(io.LimitReader(request.Body, 1024*1024+1))
			if err != nil || len(body) > 1024*1024 {
				writer.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			var rows []map[string]any
			if json.Unmarshal(body, &rows) != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}

			capture.holdMu.Lock()
			holding := capture.hold
			started := capture.holdStarted
			release := capture.holdRelease
			capture.holdMu.Unlock()
			if holding {
				select {
				case started <- struct{}{}:
				default:
				}
				<-release
			}

			capture.mu.Lock()
			capture.rows = append(capture.rows, rows...)
			for range rows {
				capture.at = append(capture.at, time.Now())
			}
			capture.mu.Unlock()
			writer.WriteHeader(http.StatusOK)
		}),
	}
	go func() { _ = capture.server.Serve(listener) }()
	return capture
}

func (c *capture) port() int {
	return c.listener.Addr().(*net.TCPAddr).Port
}

func (c *capture) snapshot() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.rows...)
}

func (c *capture) setHold() {
	c.holdMu.Lock()
	defer c.holdMu.Unlock()
	require(!c.hold, "capture already held")
	c.hold = true
	c.holdStarted = make(chan struct{}, 1)
	c.holdRelease = make(chan struct{})
}

func (c *capture) waitHeld() {
	c.holdMu.Lock()
	started := c.holdStarted
	c.holdMu.Unlock()
	select {
	case <-started:
	case <-time.After(15 * time.Second):
		panic("deadline: HTTP request did not enter hold")
	}
}

func (c *capture) releaseHold() {
	c.holdMu.Lock()
	defer c.holdMu.Unlock()
	if c.hold {
		c.hold = false
		close(c.holdRelease)
	}
}

type api struct {
	base   string
	client http.Client
}

func (a api) call(method, path string, value any) (map[string]any, int) {
	var body io.Reader
	if value != nil {
		body = bytes.NewReader(data(value))
	}
	request, err := http.NewRequest(method, a.base+path, body)
	must(err)
	request.Header.Set("Authorization", "Bearer k4-private-process-fixture")
	request.Header.Set("Content-Type", "application/json")
	if method == "PUT" && strings.HasPrefix(path, "/v1/pipelines/") && strings.Count(path, "/") == 3 {
		current, code := a.call("GET", path, nil)
		if code == http.StatusOK {
			etag, ok := current["etag"].(string)
			require(ok && etag != "", "pipeline update requires current ETag")
			request.Header.Set("If-Match", etag)
		}
	}
	response, err := a.client.Do(request)
	if err != nil {
		return nil, 0
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(io.LimitReader(response.Body, 2*1024*1024))
	if err != nil {
		return nil, response.StatusCode
	}
	var result map[string]any
	_ = json.Unmarshal(raw, &result)
	return result, response.StatusCode
}

func (a api) ok(method, path string, value any) map[string]any {
	result, code := a.call(method, path, value)
	require(code >= 200 && code < 300,
		fmt.Sprintf("%s %s returned %d: %v", method, path, code, result))
	return result
}

func nested(value map[string]any, keys ...string) any {
	var current any = value
	for _, key := range keys {
		next, ok := current.(map[string]any)
		if !ok {
			return nil
		}
		current = next[key]
	}
	return current
}

func number(value any) uint64 {
	switch value := value.(type) {
	case float64:
		if value < 0 {
			return 0
		}
		return uint64(value)
	case json.Number:
		n, err := value.Int64()
		if err == nil && n >= 0 {
			return uint64(n)
		}
	}
	return 0
}

func checkpointFields(status map[string]any) (started, succeeded uint64, active bool, last uint64, generation string) {
	checkpoint, ok := status["checkpoint"].(map[string]any)
	if !ok {
		return 0, 0, false, 0, ""
	}
	return number(checkpoint["started_total"]),
		number(checkpoint["succeeded_total"]),
		checkpoint["active"] == true,
		number(checkpoint["last_success_id"]),
		fmt.Sprint(checkpoint["state_generation"])
}

func requireCurrentSnapshotVersion(inventory map[string]any, want uint64) {
	storage, ok := inventory["storage"].(map[string]any)
	require(ok, "checkpoint inventory storage is unavailable")
	current := number(storage["current"])
	generations, ok := storage["generations"].([]any)
	require(ok, "checkpoint inventory generations are unavailable")
	for _, raw := range generations {
		generation, ok := raw.(map[string]any)
		if !ok || number(generation["id"]) != current {
			continue
		}
		require(number(generation["snapshot_version"]) == want,
			fmt.Sprintf("CURRENT snapshot version=%v, want %d", generation["snapshot_version"], want))
		return
	}
	panic(fmt.Sprintf("checkpoint inventory has no CURRENT generation %d", current))
}

func appendTemps(path, device string, values []float64) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	for _, value := range values {
		_, err = file.Write(append(data(map[string]any{
			"device_id":   device,
			"temperature": value,
		}), '\n'))
		must(err)
	}
	must(file.Sync())
	must(file.Close())
}

func temperatures(rows []map[string]any) []float64 {
	values := make([]float64, 0, len(rows))
	for _, row := range rows {
		value, ok := row["temperature"].(float64)
		require(ok, "HTTP output schema/value mismatch")
		values = append(values, value)
	}
	return values
}

func equalTemperatures(left, right []float64) bool {
	return bytes.Equal(data(left), data(right))
}

func segment(c *capture, from int) []float64 {
	rows := c.snapshot()
	require(from >= 0 && from <= len(rows), "invalid capture segment")
	return temperatures(rows[from:])
}

func clone(value any) any {
	raw := data(value)
	var result any
	must(json.Unmarshal(raw, &result))
	return result
}

func makeSpec(path, sinkURL, checkpoint, kind string, resumeLatest bool) map[string]any {
	iot := map[string]any{
		"keys":       []string{"device_id"},
		"fields":     []string{"temperature"},
		"emit_first": true,
		"ttl_micros": 0,
		"max_keys":   1024,
		"invalid":    "ignore",
	}
	if kind == "deadband" {
		iot["invalid"] = "error"
		iot["deadband"] = map[string]any{
			"mode":      "absolute",
			"baseline":  "last_output",
			"threshold": 1.0,
		}
	}
	return map[string]any{
		"version":        1,
		"stream":         "telemetry",
		"source":         map[string]any{"kind": "file", "path": path, "file_contract": "append_only", "inbox_capacity": 8},
		"sink":           map[string]any{"kind": "http", "url": sinkURL, "outbox_capacity": 8, "batch_rows": 1, "linger_ms": 0},
		"delivery":       "live_best_effort",
		"recovery":       "aligned",
		"checkpoint_dir": checkpoint,
		"checkpoint": map[string]any{
			"interval_ms":        1000,
			"timeout_ms":         5000,
			"retain_generations": 3,
			"max_store_bytes":    33554432,
			"resume_latest":      resumeLatest,
		},
		"graph": map[string]any{
			"version":     1,
			"pipeline_id": 401,
			"revision_id": 1,
			"nodes": []any{
				map[string]any{"id": 1, "kind": "memory_source", "table": "telemetry", "out": []int{2}},
				map[string]any{"id": 2, "kind": kind, "iot": iot, "out": []int{3}},
				map[string]any{"id": 3, "kind": "capture_sink", "name": kind},
			},
		},
	}
}

func makeLegacySpec(path, sinkURL, checkpoint string) map[string]any {
	return map[string]any{
		"version":        1,
		"stream":         "telemetry",
		"sql":            "SELECT SUM(temperature) AS s FROM telemetry GROUP BY COUNT_WINDOW(3)",
		"source":         map[string]any{"kind": "file", "path": path, "file_contract": "append_only", "inbox_capacity": 8},
		"sink":           map[string]any{"kind": "http", "url": sinkURL, "outbox_capacity": 8},
		"delivery":       "live_best_effort",
		"recovery":       "aligned",
		"checkpoint_dir": checkpoint,
		"checkpoint": map[string]any{
			"timeout_ms":         5000,
			"retain_generations": 3,
			"max_store_bytes":    33554432,
			"resume_latest":      true,
		},
	}
}

func launch(binary, root string, port int, logPath string) (*exec.Cmd, *os.File) {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	command := exec.Command(binary,
		"--bind", fmt.Sprintf("127.0.0.1:%d", port),
		"--catalog", filepath.Join(root, "catalog.db"),
		"--max-jobs", "1",
		"--safe-mode",
	)
	command.Env = append(os.Environ(),
		"SPARROW_TOKEN=k4-private-process-fixture",
		"SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef",
		"SPARROW_REQUIRE_SECRETS_KEY=1",
		"SPARROW_DATA_ROOTS="+root,
	)
	command.Stdout = logFile
	command.Stderr = logFile
	must(command.Start())
	return command, logFile
}

func waitHealth(a api, label string) {
	wait(label, func() bool {
		_, code := a.call("GET", "/v1/health", nil)
		return code == http.StatusOK
	})
}

func stopProcess(command *exec.Cmd, logFile *os.File, signal os.Signal) {
	if command == nil {
		return
	}
	_ = command.Process.Signal(signal)
	done := make(chan struct{})
	go func() {
		_ = command.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		_ = command.Process.Kill()
		<-done
	}
	_ = logFile.Close()
}

func manualCheckpoint(a api, name string) (map[string]any, int) {
	for attempt := 0; attempt < 80; attempt++ {
		result, code := a.call("POST", "/v1/pipelines/"+name+"/checkpoint", nil)
		if code >= 200 && code < 300 {
			return result, code
		}
		if code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable || code == 0 {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		return result, code
	}
	return nil, 0
}

func waitSuccess(a api, name string, previous uint64) map[string]any {
	var last map[string]any
	wait("checkpoint success", func() bool {
		last = a.ok("GET", "/v1/pipelines/"+name+"/status", nil)
		_, succeeded, active, _, _ := checkpointFields(last)
		return !active && succeeded > previous
	})
	return last
}

func waitActual(a api, name, expected string) map[string]any {
	var last map[string]any
	wait("actual="+expected, func() bool {
		last = a.ok("GET", "/v1/pipelines/"+name+"/status", nil)
		return nested(last, "actual", "status") == expected
	})
	return last
}

func mutateSemantic(spec map[string]any, kind string) map[string]any {
	changed := clone(spec).(map[string]any)
	graph := changed["graph"].(map[string]any)
	nodes := graph["nodes"].([]any)
	node := nodes[1].(map[string]any)
	iot := node["iot"].(map[string]any)
	if kind == "deadband" {
		iot["deadband"].(map[string]any)["threshold"] = 2.0
	} else {
		iot["emit_first"] = false
	}
	return changed
}

func fullExpected(kind string) []float64 {
	if kind == "deadband" {
		return []float64{10, 12.1, 13.2, 20, 18, 14.3, 16}
	}
	return []float64{10, 11, 12.1, 13.2, 20, 18, 14.3, 16}
}

func scenario(root, binary, oldBinary, kind string) {
	must(os.Mkdir(root, 0700))
	first := newCapture()
	defer first.server.Close()
	defer first.releaseHold()

	port := freePort()
	a := api{base: fmt.Sprintf("http://127.0.0.1:%d", port), client: http.Client{Timeout: 15 * time.Second}}
	var command *exec.Cmd
	var logFile *os.File
	stopCurrent := func(signal os.Signal) {
		if command != nil {
			stopProcess(command, logFile, signal)
			command = nil
			logFile = nil
		}
	}
	defer func() { stopCurrent(syscall.SIGKILL) }()

	input := filepath.Join(root, "telemetry.ndjson")
	checkpoint := filepath.Join(root, "checkpoint-v6")
	appendTemps(input, "a", []float64{10, 10, 11, 12.1})
	spec := makeSpec(input, fmt.Sprintf("http://127.0.0.1:%d/telemetry", first.port()), checkpoint, kind, true)
	save(filepath.Join(root, "spec.json"), spec)

	command, logFile = launch(binary, root, port, filepath.Join(root, "server.log"))
	waitHealth(a, "K4 server health")
	a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": first.port()})
	a.ok("PUT", "/v1/streams/telemetry", map[string]any{"fields": []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "temperature", "type": "float64", "nullable": true},
	}})
	save(filepath.Join(root, "explain.json"), a.ok("POST", "/v1/explain", spec))
	a.ok("PUT", "/v1/pipelines/k4", spec)
	a.ok("POST", "/v1/pipelines/k4/start", map[string]any{})

	status := func() map[string]any {
		return a.ok("GET", "/v1/pipelines/k4/status", nil)
	}
	initialExpected := []float64{10, 11, 12.1}
	if kind == "deadband" {
		initialExpected = []float64{10, 12.1}
	}
	wait("initial IoT outputs", func() bool { return len(first.snapshot()) == len(initialExpected) })
	require(equalTemperatures(segment(first, 0), initialExpected), "initial IoT oracle")

	// The production template is periodic: require a real periodic commit
	// before adding the uncommitted suffix used by the replay oracle.
	var before map[string]any
	wait("periodic checkpoint", func() bool {
		before = status()
		_, succeeded, active, _, _ := checkpointFields(before)
		return !active && succeeded > 0
	})
	_, _, _, baselineID, baselineGeneration := checkpointFields(before)
	require(baselineID != 0 && baselineGeneration != "", "periodic checkpoint lacks durable status identity")
	save(filepath.Join(root, "periodic-checkpoint-status.json"), before)
	periodicInventory := a.ok("GET", "/v1/pipelines/k4/checkpoints", nil)
	requireCurrentSnapshotVersion(periodicInventory, 6)
	save(filepath.Join(root, "periodic-checkpoints.json"), periodicInventory)
	save(filepath.Join(root, "periodic-current.sha256"), map[string]any{"sha256": hash(filepath.Join(checkpoint, "CURRENT")), "checkpoint_id": baselineID})
	// Prove the 1 s periodic template above, then turn only its scheduling
	// policy off before fault injection. Otherwise a scheduler tick can commit
	// the allegedly-uncommitted suffix and invalidate the crash oracle.
	a.ok("POST", "/v1/pipelines/k4/stop", map[string]any{})
	waitActual(a, "k4", "stopped")
	stoppedInventory := a.ok("GET", "/v1/pipelines/k4/checkpoints", nil)
	baselineID = number(nested(stoppedInventory, "storage", "current"))
	require(baselineID > 0, "stopped periodic profile lost CURRENT")
	save(filepath.Join(root, "stopped-periodic-checkpoints.json"), stoppedInventory)
	spec["checkpoint"].(map[string]any)["interval_ms"] = nil
	a.ok("PUT", "/v1/pipelines/k4", spec)
	a.ok("POST", "/v1/pipelines/k4/start", map[string]any{})
	waitActual(a, "k4", "running")
	save(filepath.Join(root, "manual-fault-spec.json"), spec)

	// This suffix is intentionally added after the periodic cut. The same
	// value must not be emitted after restore; the next value must be.
	beforeSuffix := len(first.snapshot())
	appendTemps(input, "a", []float64{12.1, 13.2})
	appendTemps(input, "b", []float64{20})
	wait("post-cut IoT outputs", func() bool { return len(first.snapshot()) == beforeSuffix+2 })
	require(equalTemperatures(segment(first, beforeSuffix), []float64{13.2, 20}), "post-cut IoT oracle")

	stopCurrent(syscall.SIGKILL)
	restoreStart := len(first.snapshot())
	command, logFile = launch(binary, root, port, filepath.Join(root, "server.log"))
	waitHealth(a, "K4 restore server health")
	a.ok("POST", "/v1/pipelines/k4/start", map[string]any{})
	wait("restored suffix", func() bool { return len(first.snapshot()) == restoreStart+2 })
	require(equalTemperatures(segment(first, restoreStart), []float64{13.2, 20}), "same-value restore must suppress and next value must emit")
	restoredStatus := status()
	restoredFrom := number(nested(restoredStatus, "checkpoint", "restored_from_checkpoint"))
	require(restoredFrom == baselineID, fmt.Sprintf("restore status points at %d, want baseline %d", restoredFrom, baselineID))
	save(filepath.Join(root, "restore-status.json"), restoredStatus)
	restoreInventory := a.ok("GET", "/v1/pipelines/k4/checkpoints", nil)
	requireCurrentSnapshotVersion(restoreInventory, 6)
	save(filepath.Join(root, "restore-checkpoints.json"), restoreInventory)

	// Establish a committed post-restore point before testing the HTTP hold.
	_, currentSuccess, _, currentID, _ := checkpointFields(restoredStatus)
	result, code := manualCheckpoint(a, "k4")
	require(code >= 200 && code < 300, fmt.Sprintf("post-restore checkpoint failed: %d %v", code, result))
	committedStatus := waitSuccess(a, "k4", currentSuccess)
	_, currentSuccess, _, currentID, _ = checkpointFields(committedStatus)
	require(currentID > baselineID, "post-restore checkpoint did not advance")
	save(filepath.Join(root, "post-restore-checkpoint-status.json"), committedStatus)

	// Hold a real HTTP response open. The checkpoint must become active but
	// cannot report success or publish a new CURRENT until this response is
	// released.
	wait("checkpoint idle before HTTP hold", func() bool {
		_, _, active, _, _ := checkpointFields(status())
		return !active
	})
	holdCurrentHash := hash(filepath.Join(checkpoint, "CURRENT"))
	first.setHold()
	appendTemps(input, "a", []float64{18})
	first.waitHeld()
	beforeHold := status()
	beforeHoldStarted, beforeHoldSuccess, _, _, _ := checkpointFields(beforeHold)
	resultChannel := make(chan callResult, 1)
	go func() {
		result, code := a.call("POST", "/v1/pipelines/k4/checkpoint", nil)
		resultChannel <- callResult{result: result, code: code}
	}()
	var blocked map[string]any
	wait("HTTP-flush checkpoint active", func() bool {
		blocked = status()
		started, succeeded, active, _, _ := checkpointFields(blocked)
		require(succeeded == beforeHoldSuccess, "checkpoint succeeded while HTTP body was held")
		require(hash(filepath.Join(checkpoint, "CURRENT")) == holdCurrentHash,
			"checkpoint published CURRENT while HTTP body was held")
		return active && started > beforeHoldStarted
	})
	save(filepath.Join(root, "http-hold-checkpoint-status.json"), blocked)
	first.releaseHold()
	manualResult := <-resultChannel
	require(manualResult.code >= 200 && manualResult.code < 300,
		fmt.Sprintf("held HTTP checkpoint did not complete after release: %d %v", manualResult.code, manualResult.result))
	heldDone := waitSuccess(a, "k4", beforeHoldSuccess)
	save(filepath.Join(root, "http-hold-checkpoint-result.json"), map[string]any{"request": manualResult.result, "status": heldDone})

	// Force a commit publication failure with a real checkpoint request after
	// output has already been accepted. CURRENT must remain unchanged; a failed
	// generation may be left unpublished (or pruned) and the suffix is
	// replayable on restart.
	_, _, _, failureBaselineID, _ := checkpointFields(heldDone)
	failureCurrentHash := hash(filepath.Join(checkpoint, "CURRENT"))
	failureOutputStart := len(first.snapshot())
	appendTemps(input, "a", []float64{14.3, 16})
	wait("failure suffix outputs", func() bool { return len(first.snapshot()) == failureOutputStart+2 })
	must(os.Mkdir(filepath.Join(checkpoint, "CURRENT.tmp"), 0700))
	failedResult, failedCode := manualCheckpoint(a, "k4")
	require(failedCode >= 400, fmt.Sprintf("CURRENT.tmp checkpoint unexpectedly succeeded: %d %v", failedCode, failedResult))
	save(filepath.Join(root, "failed-current-tmp-response.json"), failedResult)
	failedStatus := status()
	save(filepath.Join(root, "failed-current-tmp-status.json"), failedStatus)
	require(hash(filepath.Join(checkpoint, "CURRENT")) == failureCurrentHash, "failed CURRENT.tmp commit changed CURRENT")
	must(os.Remove(filepath.Join(checkpoint, "CURRENT.tmp")))

	stopCurrent(syscall.SIGKILL)
	replayStart := len(first.snapshot())
	command, logFile = launch(binary, root, port, filepath.Join(root, "server.log"))
	waitHealth(a, "K4 replay server health")
	a.ok("POST", "/v1/pipelines/k4/start", map[string]any{})
	wait("failed suffix replay", func() bool { return len(first.snapshot()) == replayStart+2 })
	require(equalTemperatures(segment(first, replayStart), []float64{14.3, 16}), "failed publication suffix must replay")
	replayedStatus := status()
	save(filepath.Join(root, "failed-suffix-replay-status.json"), replayedStatus)
	save(filepath.Join(root, "failed-suffix-replay-outputs.json"), first.snapshot())

	// A changed state contract must reject v6 restore before it touches
	// CURRENT. Use a changed semantic parameter, not an invalid configuration.
	semanticCurrentHash := hash(filepath.Join(checkpoint, "CURRENT"))
	semantic := mutateSemantic(spec, kind)
	save(filepath.Join(root, "semantic-change-spec.json"), semantic)
	a.ok("PUT", "/v1/pipelines/k4", semantic)
	a.ok("POST", "/v1/pipelines/k4/start", map[string]any{})
	semanticStatus := waitActual(a, "k4", "failed")
	require(hash(filepath.Join(checkpoint, "CURRENT")) == semanticCurrentHash,
		"semantic refusal must preserve CURRENT")
	save(filepath.Join(root, "semantic-change-status.json"), semanticStatus)

	// Explicit fresh/reset uses the same aligned profile with resume_latest
	// disabled. It must replay from the beginning, emit the first value, and
	// persist a new state generation instead of silently reusing v6 state.
	fresh := clone(spec).(map[string]any)
	fresh["checkpoint"].(map[string]any)["resume_latest"] = false
	save(filepath.Join(root, "fresh-reset-spec.json"), fresh)
	freshStart := len(first.snapshot())
	a.ok("PUT", "/v1/pipelines/k4", fresh)
	a.ok("POST", "/v1/pipelines/k4/start", map[string]any{})
	expectedFresh := fullExpected(kind)
	wait("fresh/reset outputs", func() bool { return len(first.snapshot()) == freshStart+len(expectedFresh) })
	require(equalTemperatures(segment(first, freshStart), expectedFresh), "fresh/reset first-value and full replay oracle")
	freshStatus := status()
	_, _, _, _, freshGeneration := checkpointFields(freshStatus)
	require(freshGeneration != "" && freshGeneration != baselineGeneration, "fresh/reset did not create a new state generation")
	save(filepath.Join(root, "fresh-reset-status.json"), freshStatus)

	a.ok("POST", "/v1/pipelines/k4/stop", map[string]any{})
	waitActual(a, "k4", "stopped")
	stopCurrent(syscall.SIGTERM)

	oldChecked := false
	oldEvidence := map[string]any{"checked": false, "reason": "old server binary not supplied"}
	if oldBinary != "" {
		oldChecked = true
		// Use a separate catalog containing only a legacy linear Count spec.
		// The old binary therefore reaches its v3 profile guard instead of
		// rejecting the newer GraphSpec/Iot fields during deserialization.
		legacyRoot := filepath.Join(root, "legacy-v6-root")
		legacyCheckpoint := filepath.Join(legacyRoot, "checkpoint-v6")
		legacyInput := filepath.Join(legacyRoot, "telemetry.ndjson")
		must(os.MkdirAll(legacyRoot, 0700))
		copyTree(checkpoint, legacyCheckpoint)
		copyTree(input, legacyInput)
		legacySetupPort := freePort()
		legacySetupAPI := api{base: fmt.Sprintf("http://127.0.0.1:%d", legacySetupPort), client: http.Client{Timeout: 15 * time.Second}}
		// Build the reader catalog with the old binary itself.  The new binary
		// may have a newer catalog schema, which would make the old process fail
		// at catalog open instead of reaching the intended v6 checkpoint guard.
		setupCommand, setupLog := launch(oldBinary, legacyRoot, legacySetupPort, filepath.Join(root, "legacy-setup-server.log"))
		waitHealth(legacySetupAPI, "legacy catalog setup server health")
		legacySetupAPI.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": first.port()})
		legacySetupAPI.ok("PUT", "/v1/streams/telemetry", map[string]any{"fields": []any{
			map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
			map[string]any{"name": "temperature", "type": "float64", "nullable": true},
		}})
		legacy := makeLegacySpec(legacyInput, fmt.Sprintf("http://127.0.0.1:%d/telemetry", first.port()), legacyCheckpoint)
		save(filepath.Join(root, "legacy-v6-spec.json"), legacy)
		legacySetupAPI.ok("PUT", "/v1/pipelines/legacy-v6", legacy)
		stopProcess(setupCommand, setupLog, syscall.SIGTERM)
		beforeOldTree := checkpointTreeHash(legacyCheckpoint)
		beforeOldCurrent := hash(filepath.Join(legacyCheckpoint, "CURRENT"))
		beforeOldOutputs := len(first.snapshot())
		oldPort := freePort()
		oldAPI := api{base: fmt.Sprintf("http://127.0.0.1:%d", oldPort), client: http.Client{Timeout: 500 * time.Millisecond}}
		oldCommand, oldLog := launch(oldBinary, legacyRoot, oldPort, filepath.Join(root, "old-server.log"))
		healthy := false
		deadline := time.Now().Add(6 * time.Second)
		for time.Now().Before(deadline) {
			_, oldCode := oldAPI.call("GET", "/v1/health", nil)
			if oldCode == http.StatusOK {
				healthy = true
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		var startResult map[string]any
		var oldStatus map[string]any
		startCode := 0
		if healthy {
			startResult, startCode = oldAPI.call("POST", "/v1/pipelines/legacy-v6/start", map[string]any{})
			if startCode >= 200 && startCode < 300 {
				// The old supervisor reports asynchronous start failure.
				wait("old v6 profile rejection", func() bool {
					oldStatus, _ = oldAPI.call("GET", "/v1/pipelines/legacy-v6/status", nil)
					return nested(oldStatus, "actual", "status") == "failed"
				})
				save(filepath.Join(root, "old-v6-status.json"), oldStatus)
			}
		}
		stopProcess(oldCommand, oldLog, syscall.SIGTERM)
		logBytes, err := os.ReadFile(filepath.Join(root, "old-server.log"))
		must(err)
		logText := strings.ToLower(string(logBytes))
		// Safe-mode lifecycle failures are persisted in status; they need not
		// also be printed to stderr. Verify the actual guard error, not a
		// particular logging configuration of the old binary.
		guardError := strings.ToLower(fmt.Sprint(nested(oldStatus, "actual", "last_error")))
		guardError += " " + strings.ToLower(fmt.Sprint(nested(startResult, "error", "message")))
		require(healthy && (strings.Contains(guardError, "checkpoint source profile mismatch") ||
			strings.Contains(logText, "checkpoint source profile mismatch")),
			"old binary did not reach the v6 checkpoint profile guard")
		require(checkpointTreeHash(legacyCheckpoint) == beforeOldTree, "old binary changed v6 checkpoint history")
		require(hash(filepath.Join(legacyCheckpoint, "CURRENT")) == beforeOldCurrent, "old binary changed v6 CURRENT")
		require(len(first.snapshot()) == beforeOldOutputs, "old binary emitted output before v6 profile rejection")
		oldEvidence = map[string]any{
			"checked":           true,
			"healthy":           healthy,
			"catalog":           "isolated linear Count pipeline",
			"start_code":        startCode,
			"start_response":    startResult,
			"error_oracle":      "checkpoint source profile mismatch",
			"error_response":    guardError,
			"history_preserved": true,
			"current_preserved": true,
			"output_preserved":  true,
		}
		save(filepath.Join(root, "old-v6-evidence.json"), oldEvidence)
	}

	summary := map[string]any{
		"valid":                          true,
		"kind":                           kind,
		"operator":                       kind,
		"aligned_ttl_micros":             0,
		"checkpoint_interval_ms":         1000,
		"periodic_checkpoint":            true,
		"checkpoint_snapshot_version":    6,
		"checkpoint_id":                  failureBaselineID,
		"baseline_checkpoint_id":         baselineID,
		"post_restore_checkpoint_id":     currentID,
		"failed_current_tmp_preserved":   true,
		"failed_suffix_replayed":         true,
		"http_flush_blocked_checkpoint":  true,
		"semantic_change_refused":        true,
		"fresh_reset_first_emitted":      true,
		"fresh_state_generation_changed": true,
		"cursor_oracle":                  "checkpoint_id/status restored_from/storage CURRENT only; received counters are not treated as committed source cursors",
		"old_v14_profile_guard":          oldEvidence,
		"certified":                      false,
	}
	save(filepath.Join(root, "summary.json"), summary)
	fmt.Println("K4_PROCESS_OK", kind, "old_checked=", oldChecked)
}

type callResult struct {
	result map[string]any
	code   int
}

func main() {
	binary := flag.String("server-bin", "", "K4 production server")
	oldBinary := flag.String("old-server-bin", "", "optional pre-K4 server for v6 profile rejection")
	out := flag.String("out", "", "new artifact directory")
	flag.Parse()
	require(*binary != "" && *out != "", "server-bin and out are required")

	root, err := filepath.Abs(*out)
	must(err)
	must(os.Mkdir(root, 0700))
	self, err := os.Executable()
	must(err)
	save(filepath.Join(root, "binaries.json"), map[string]any{
		"server_sha256": hash(*binary),
		"driver_sha256": hash(self),
	})
	for _, kind := range []string{"change_detect", "deadband"} {
		scenario(filepath.Join(root, kind), *binary, *oldBinary, kind)
	}
}
