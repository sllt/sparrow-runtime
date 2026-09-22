// Real production-process oracle for the B2-A static immutable Lookup profile.
//
// This driver deliberately uses only the Go standard library.  It exercises
// the running server through its HTTP control API and a real File -> Lookup ->
// required HTTP pipeline; it does not reimplement the Rust snapshot codec.
package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
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
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	token        = "core-b-isolated-process-fixture"
	maxHTTPBody  = 1 << 20
	waitDuration = 20 * time.Second
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
	raw := data(value)
	must(os.WriteFile(path, append(raw, '\n'), 0600))
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

func wait(label string, condition func() bool) {
	deadline := time.Now().Add(waitDuration)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(15 * time.Millisecond)
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

// checkpointTreeHash is an evidence digest only. It covers committed history
// and markers, but excludes the ownership lock, the deliberately injected
// CURRENT.tmp obstruction, and unpublished crash-cut generations.
func checkpointTreeHash(root string) string {
	return checkpointHash(root, true)
}

// checkpointFullTreeHash is used only by the profile-guard processes. A guard
// must not be able to mutate or hide an unpublished historical generation.
func checkpointFullTreeHash(root string) string {
	return checkpointHash(root, false)
}

func checkpointHash(root string, skipUnpublished bool) string {
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
		if skipUnpublished && info.IsDir() && strings.HasPrefix(name, "chk-") {
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
	must(os.MkdirAll(destination, 0700))
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
		if relative == "." {
			return nil
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
		output, err := os.OpenFile(target, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0600)
		if err != nil {
			_ = input.Close()
			return err
		}
		_, copyErr := io.Copy(output, input)
		closeOutErr := output.Close()
		closeInErr := input.Close()
		if copyErr != nil {
			return copyErr
		}
		if closeOutErr != nil {
			return closeOutErr
		}
		return closeInErr
	}))
}

type child struct {
	cmd     *exec.Cmd
	log     *os.File
	logPath string
	done    chan error
	exitErr error
}

func launch(binary, root, catalog, logfile string, port int) *child {
	logFile, err := os.OpenFile(logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	command := exec.Command(binary,
		"--bind", fmt.Sprintf("127.0.0.1:%d", port),
		"--catalog", catalog,
		"--max-jobs", "1",
		"--safe-mode",
	)
	command.Env = append(os.Environ(),
		"SPARROW_TOKEN="+token,
		"SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef",
		"SPARROW_REQUIRE_SECRETS_KEY=1",
		"SPARROW_DATA_ROOTS="+root,
	)
	command.Stdout = logFile
	command.Stderr = logFile
	must(command.Start())
	result := &child{cmd: command, log: logFile, logPath: logfile, done: make(chan error, 1)}
	go func() { result.done <- command.Wait() }()
	return result
}

func killedBySIGKILL(err error) bool {
	if err == nil {
		return false
	}
	exit, ok := err.(*exec.ExitError)
	if !ok || exit.ProcessState == nil {
		return false
	}
	status, ok := exit.ProcessState.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && status.Signal() == syscall.SIGKILL
}

func checkRustPanic(logPath string) {
	raw, err := os.ReadFile(logPath)
	must(err)
	lower := strings.ToLower(string(raw))
	for _, marker := range []string{"panicked at", "panic: runtime error", "fatal runtime error"} {
		require(!strings.Contains(lower, marker), fmt.Sprintf("Rust/process panic marker %q in %s", marker, logPath))
	}
}

func (c *child) stop(signal os.Signal) {
	if c == nil || c.cmd == nil {
		return
	}
	signalErr := c.cmd.Process.Signal(signal)
	timedOut := false
	var waitErr error
	select {
	case waitErr = <-c.done:
	case <-time.After(8 * time.Second):
		timedOut = true
		_ = c.cmd.Process.Kill()
		waitErr = <-c.done
	}
	must(c.log.Close())
	c.exitErr = waitErr
	c.cmd = nil
	checkRustPanic(c.logPath)
	if signal == syscall.SIGKILL {
		// A process which already exited cleanly is also fine.  A non-zero
		// exit, including a Rust panic, must not be hidden by the intended
		// crash signal.
		require(waitErr == nil || killedBySIGKILL(waitErr),
			fmt.Sprintf("SIGKILL child exited unexpectedly: signal=%v wait=%v", signalErr, waitErr))
		return
	}
	require(!timedOut, fmt.Sprintf("child did not exit after %v: %v", signal, signalErr))
	require(waitErr == nil,
		fmt.Sprintf("child did not exit cleanly after %v: signal=%v wait=%v", signal, signalErr, waitErr))
}

type api struct {
	base   string
	client *http.Client
}

func (a api) call(method, path string, value any, auth bool) (map[string]any, int) {
	var body io.Reader
	if value != nil {
		body = bytes.NewReader(data(value))
	}
	request, err := http.NewRequest(method, a.base+path, body)
	must(err)
	if auth {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	request.Header.Set("Content-Type", "application/json")
	if method == http.MethodPut && strings.HasPrefix(path, "/v1/pipelines/") && strings.Count(path, "/") == 3 {
		current, code := a.call(http.MethodGet, path, nil, true)
		if code == http.StatusOK {
			etag, ok := current["etag"].(string)
			require(ok && etag != "", "pipeline update requires the current ETag")
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
	if len(raw) == 0 {
		return map[string]any{}, response.StatusCode
	}
	var result map[string]any
	if json.Unmarshal(raw, &result) != nil {
		return map[string]any{"raw": string(raw)}, response.StatusCode
	}
	return result, response.StatusCode
}

func (a api) ok(method, path string, value any) map[string]any {
	result, code := a.call(method, path, value, true)
	require(code >= 200 && code < 300,
		fmt.Sprintf("%s %s returned %d: %v", method, path, code, result))
	return result
}

func (a api) rejected(method, path string, value any) map[string]any {
	result, code := a.call(method, path, value, true)
	require(code >= 400 && code < 500,
		fmt.Sprintf("expected client rejection: %s %s returned %d: %v", method, path, code, result))
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
		if value >= 0 {
			return uint64(value)
		}
	case json.Number:
		parsed, err := strconv.ParseUint(string(value), 10, 64)
		if err == nil {
			return parsed
		}
	case int:
		if value >= 0 {
			return uint64(value)
		}
	case uint64:
		return value
	}
	return 0
}

func stateGeneration(value any) string {
	switch value := value.(type) {
	case string:
		if len(value) == 32 {
			if _, err := hex.DecodeString(value); err == nil && value != strings.Repeat("0", 32) {
				return strings.ToLower(value)
			}
		}
	case []any:
		if len(value) != 16 {
			return ""
		}
		decoded := make([]byte, 16)
		for i, raw := range value {
			if number(raw) > 255 {
				return ""
			}
			decoded[i] = byte(number(raw))
		}
		if bytes.Equal(decoded, make([]byte, 16)) {
			return ""
		}
		return hex.EncodeToString(decoded)
	case []byte:
		if len(value) == 16 && !bytes.Equal(value, make([]byte, 16)) {
			return hex.EncodeToString(value)
		}
	}
	return ""
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
		stateGeneration(checkpoint["state_generation"])
}

func status(a api, name string) map[string]any {
	return a.ok(http.MethodGet, "/v1/pipelines/"+name+"/status", nil)
}

func waitHealth(a api, label string) {
	wait(label, func() bool {
		_, code := a.call(http.MethodGet, "/v1/health", nil, false)
		return code == http.StatusOK
	})
}

func waitActual(a api, name, expected string) map[string]any {
	var result map[string]any
	wait("actual status="+expected, func() bool {
		result = status(a, name)
		return nested(result, "actual", "status") == expected
	})
	return result
}

func waitGuardFailure(a api, name string) map[string]any {
	var result map[string]any
	wait("profile guard failure", func() bool {
		result = status(a, name)
		actual := fmt.Sprint(nested(result, "actual", "last_error"))
		return nested(result, "actual", "status") == "failed" &&
			strings.Contains(strings.ToLower(actual), "checkpoint source profile mismatch")
	})
	return result
}

func waitRestoreFailure(a api, name string) map[string]any {
	var result map[string]any
	wait("checkpoint restore refusal", func() bool {
		result = status(a, name)
		actual := strings.ToLower(fmt.Sprint(nested(result, "actual", "last_error")))
		return nested(result, "actual", "status") == "failed" && strings.Contains(actual,
			"checkpoint participant set, schema, codec or pipeline semantics changed")
	})
	return result
}

func manualCheckpoint(a api, name string) (map[string]any, int) {
	for attempt := 0; attempt < 80; attempt++ {
		result, code := a.call(http.MethodPost, "/v1/pipelines/"+name+"/checkpoint", nil, true)
		if code >= 200 && code < 300 {
			return result, code
		}
		if code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable {
			time.Sleep(30 * time.Millisecond)
			continue
		}
		return result, code
	}
	return nil, 0
}

func waitSuccess(a api, name string, previous uint64) map[string]any {
	var result map[string]any
	wait("checkpoint success", func() bool {
		result = status(a, name)
		_, succeeded, active, _, _ := checkpointFields(result)
		return !active && succeeded > previous
	})
	return result
}

func inventory(a api, name string) map[string]any {
	return a.ok(http.MethodGet, "/v1/pipelines/"+name+"/checkpoints", nil)
}

func storageCurrent(value map[string]any) uint64 {
	return number(nested(value, "storage", "current"))
}

func requireCurrentSnapshotVersion(value map[string]any, want uint64) {
	current := storageCurrent(value)
	require(current != 0, "checkpoint inventory has no CURRENT")
	generations, ok := nested(value, "storage", "generations").([]any)
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

// snapshotPayload reads only the chunk framing needed for a process oracle.
// It is intentionally not a second implementation of PipelineSnapshot.decode.
func snapshotPayload(checkpoint string, id uint64) []byte {
	generation := filepath.Join(checkpoint, fmt.Sprintf("chk-%08d", id))
	manifest, err := os.ReadFile(filepath.Join(generation, "MANIFEST"))
	must(err)
	require(len(manifest) >= 18 && bytes.Equal(manifest[:4], []byte("MAN2")), "CURRENT MANIFEST is not MAN2")
	chunks := binary.LittleEndian.Uint32(manifest[14:18])
	require(chunks > 0 && chunks <= 4096, "invalid MANIFEST chunk count")
	payload := make([]byte, 0)
	for i := uint32(0); i < chunks; i++ {
		part, err := os.ReadFile(filepath.Join(generation, fmt.Sprintf("%04d.bin", i)))
		must(err)
		payload = append(payload, part...)
	}
	return payload
}

func readU16(raw []byte, offset *int) (uint16, bool) {
	if *offset < 0 || *offset+2 > len(raw) {
		return 0, false
	}
	value := binary.LittleEndian.Uint16(raw[*offset : *offset+2])
	*offset += 2
	return value, true
}

func readU32(raw []byte, offset *int) (uint32, bool) {
	if *offset < 0 || *offset+4 > len(raw) {
		return 0, false
	}
	value := binary.LittleEndian.Uint32(raw[*offset : *offset+4])
	*offset += 4
	return value, true
}

func readU64(raw []byte, offset *int) (uint64, bool) {
	if *offset < 0 || *offset+8 > len(raw) {
		return 0, false
	}
	value := binary.LittleEndian.Uint64(raw[*offset : *offset+8])
	*offset += 8
	return value, true
}

func readBytes(raw []byte, offset *int, size int) ([]byte, bool) {
	if size < 0 || *offset < 0 || size > len(raw)-*offset {
		return nil, false
	}
	value := raw[*offset : *offset+size]
	*offset += size
	return value, true
}

// referenceManifestEvidence parses only the bounded v8 outer-plan section.
// It intentionally does not decode semantics or reimplement the typed table
// CRC; the Rust runtime remains the CRC authority.
func referenceManifestEvidence(payload []byte, expected map[string]any) map[string]any {
	offset := 38
	for i := 0; i < 2; i++ {
		length, ok := readU32(payload, &offset)
		require(ok && length <= 64*1024, "truncated v8 source identity")
		_, ok = readBytes(payload, &offset, int(length))
		require(ok, "truncated v8 source identity value")
	}
	_, ok := readBytes(payload, &offset, 16) // size, fingerprint
	require(ok, "truncated v8 source identity metadata")
	_, ok = readBytes(payload, &offset, 8+8+16) // attempt, revision, generation
	require(ok, "truncated v8 provenance")
	planLength, ok := readU32(payload, &offset)
	require(ok && planLength <= 256*1024, "invalid v8 checkpoint plan length")
	plan, ok := readBytes(payload, &offset, int(planLength))
	require(ok, "truncated v8 checkpoint plan")
	require(len(plan) >= 4+4+4+2 && bytes.Equal(plan[:4], []byte("CPL3")),
		"v8 checkpoint does not carry a CPL3 plan")
	planOffset := 4 + 4 + 4
	stateCount, ok := readU16(plan, &planOffset)
	require(ok && stateCount == 0, "reference v8 plan unexpectedly carries state participants")
	refCount, ok := readU16(plan, &planOffset)
	require(ok && refCount == 1, "reference v8 plan must carry exactly one dependency")
	nameLength, ok := readU16(plan, &planOffset)
	require(ok && nameLength > 0 && nameLength <= 64, "invalid CPL3 reference name length")
	nameBytes, ok := readBytes(plan, &planOffset, int(nameLength))
	require(ok, "truncated CPL3 reference name")
	name := string(nameBytes)
	revision, ok := readU64(plan, &planOffset)
	require(ok, "truncated CPL3 reference revision")
	digest, ok := readBytes(plan, &planOffset, 32)
	require(ok, "truncated CPL3 reference SHA-256")
	runtimeCRC, ok := readU32(plan, &planOffset)
	require(ok, "truncated CPL3 reference runtime CRC")
	expectedDigest, err := hex.DecodeString(fmt.Sprint(expected["sha256"]))
	must(err)
	require(name == "limits" && revision == number(expected["revision"]) && bytes.Equal(digest, expectedDigest),
		"CPL3 reference identity does not match the bound r1 table")
	return map[string]any{
		"reference_count":          uint64(refCount),
		"reference_name":           name,
		"reference_revision":       revision,
		"reference_sha256":         hex.EncodeToString(digest),
		"reference_runtime_crc32":  uint64(runtimeCRC),
		"dependency_bytes_present": true,
	}
}

func snapshotEvidence(checkpoint string, value map[string]any, expected map[string]any) map[string]any {
	id := storageCurrent(value)
	payload := snapshotPayload(checkpoint, id)
	require(len(payload) >= 38 && bytes.Equal(payload[:4], []byte("SPV1")), "CURRENT payload header is not SPV1")
	version := binary.LittleEndian.Uint16(payload[4:6])
	require(version == 8, fmt.Sprintf("CURRENT pipeline snapshot version=%d, want 8", version))
	recordIndex := binary.LittleEndian.Uint64(payload[30:38])
	manifestEvidence := referenceManifestEvidence(payload, expected)
	payloadHash := sha256.Sum256(payload)
	evidence := map[string]any{
		"checkpoint_id":               id,
		"snapshot_version":            version,
		"source_record_index":         recordIndex,
		"cpl3_manifest_present":       true,
		"metadata_is_header_only":     true,
		"runtime_crc_authority":       "Rust ReferenceTable verified_dependency admission; driver does not duplicate typed CRC codec",
		"current_payload_sha256":      hex.EncodeToString(payloadHash[:]),
		"current_payload_byte_length": len(payload),
	}
	for key, value := range manifestEvidence {
		evidence[key] = value
	}
	return evidence
}

type capturedBody struct {
	Rows      int       `json:"rows"`
	At        time.Time `json:"at"`
	Responded bool      `json:"responded"`
	Discarded bool      `json:"discarded"`
}

type captureSnapshot struct {
	Requests     int              `json:"requests"`
	ReceivedRows int              `json:"received_rows"`
	Responses    int              `json:"responses"`
	ResponseRows int              `json:"response_rows"`
	Bodies       []capturedBody   `json:"bodies"`
	Rows         []map[string]any `json:"rows"`
}

type capture struct {
	mu           sync.Mutex
	rows         []map[string]any
	requests     int
	received     int
	responses    int
	responseRows int
	bodies       []capturedBody

	holdMu      sync.Mutex
	hold        bool
	holdStarted chan struct{}
	holdRelease chan struct{}
	discardHeld bool

	server    *http.Server
	listener  net.Listener
	serveDone chan struct{}
	closeOnce sync.Once
}

func newCapture() *capture {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	result := &capture{listener: listener, holdRelease: make(chan struct{}), serveDone: make(chan struct{})}
	close(result.holdRelease)
	result.server = &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			defer request.Body.Close()
			raw, err := io.ReadAll(io.LimitReader(request.Body, maxHTTPBody+1))
			if err != nil || len(raw) > maxHTTPBody {
				writer.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			var rows []map[string]any
			if json.Unmarshal(raw, &rows) != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}

			body := capturedBody{Rows: len(rows), At: time.Now()}
			result.mu.Lock()
			result.requests++
			result.received += len(rows)
			result.bodies = append(result.bodies, body)
			bodyIndex := len(result.bodies) - 1
			result.mu.Unlock()

			result.holdMu.Lock()
			holding := result.hold
			started := result.holdStarted
			release := result.holdRelease
			// A released crash-cut applies only to handlers that entered
			// that hold, never to later requests from the resumed process.
			discard := holding && result.discardHeld
			result.holdMu.Unlock()
			if holding {
				select {
				case started <- struct{}{}:
				default:
				}
				<-release
				// The kill path flips discardHeld while this handler is
				// blocked.  Re-read it after the explicit release so a
				// disconnected client cannot be recorded as a successful 2xx.
				result.holdMu.Lock()
				discard = discard || result.discardHeld
				result.holdMu.Unlock()
			}
			if discard || request.Context().Err() != nil {
				result.mu.Lock()
				result.bodies[bodyIndex].Discarded = true
				result.mu.Unlock()
				return
			}

			result.mu.Lock()
			result.rows = append(result.rows, rows...)
			result.responses++
			result.responseRows += len(rows)
			result.bodies[bodyIndex].Responded = true
			result.mu.Unlock()
			writer.WriteHeader(http.StatusOK)
		}),
	}
	go func() {
		_ = result.server.Serve(listener)
		close(result.serveDone)
	}()
	return result
}

func (c *capture) url() string {
	return fmt.Sprintf("http://127.0.0.1:%d/telemetry", c.listener.Addr().(*net.TCPAddr).Port)
}

func (c *capture) close() {
	c.closeOnce.Do(func() {
		c.releaseHold(true)
		_ = c.server.Close()
		select {
		case <-c.serveDone:
		case <-time.After(5 * time.Second):
			panic("capture HTTP server did not stop")
		}
	})
}

func (c *capture) setHold() {
	c.holdMu.Lock()
	defer c.holdMu.Unlock()
	require(!c.hold, "capture already held")
	c.hold = true
	c.discardHeld = false
	c.holdStarted = make(chan struct{}, 1)
	c.holdRelease = make(chan struct{})
}

func (c *capture) waitHeld() {
	c.holdMu.Lock()
	started := c.holdStarted
	c.holdMu.Unlock()
	select {
	case <-started:
	case <-time.After(waitDuration):
		panic("deadline: HTTP request did not enter explicit hold")
	}
}

func (c *capture) releaseHold(discard bool) {
	c.holdMu.Lock()
	defer c.holdMu.Unlock()
	if c.hold {
		c.hold = false
		c.discardHeld = discard
		close(c.holdRelease)
	}
}

func (c *capture) snapshot() captureSnapshot {
	c.mu.Lock()
	defer c.mu.Unlock()
	rows := append([]map[string]any(nil), c.rows...)
	bodies := append([]capturedBody(nil), c.bodies...)
	return captureSnapshot{
		Requests: c.requests, ReceivedRows: c.received, Responses: c.responses,
		ResponseRows: c.responseRows, Bodies: bodies, Rows: rows,
	}
}

func (c *capture) rowCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.rows)
}

func (c *capture) receivedRows() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.received
}

type expectedRow struct {
	key       string
	value     int
	threshold *int
}

func expectRows(c *capture, from int, expected []expectedRow) {
	wait("HTTP response rows", func() bool {
		return c.rowCount() >= from+len(expected)
	})
	snapshot := c.snapshot()
	require(len(snapshot.Rows) == from+len(expected),
		fmt.Sprintf("unexpected HTTP output rows: got %d want %d", len(snapshot.Rows), from+len(expected)))
	for i, want := range expected {
		row := snapshot.Rows[from+i]
		require(fmt.Sprint(row["device_id"]) == want.key,
			fmt.Sprintf("row %d key=%v want=%s", i, row["device_id"], want.key))
		require(number(row["v"]) == uint64(want.value),
			fmt.Sprintf("row %d v=%v want=%d", i, row["v"], want.value))
		if want.threshold == nil {
			require(row["threshold"] == nil,
				fmt.Sprintf("row %d expected lookup miss, got threshold=%v", i, row["threshold"]))
		} else {
			require(number(row["threshold"]) == uint64(*want.threshold),
				fmt.Sprintf("row %d threshold=%v want=%d", i, row["threshold"], *want.threshold))
		}
	}
}

func clone(value any) any {
	raw := data(value)
	var result any
	must(json.Unmarshal(raw, &result))
	return result
}

func sourceFields() []any {
	return []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "v", "type": "int64", "nullable": false},
	}
}

func referenceTable(threshold int) map[string]any {
	return map[string]any{
		"fields": []any{
			map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
			map[string]any{"name": "threshold", "type": "int64", "nullable": false},
		},
		"keys": []string{"device_id"},
		"rows": []any{[]any{"a", threshold}, []any{"b", threshold + 10}},
	}
}

func publishTable(a api, threshold int, expectedRevision uint64) map[string]any {
	return a.ok(http.MethodPut, "/v1/tables/limits", map[string]any{
		"expected_revision": expectedRevision,
		"table":             referenceTable(threshold),
	})
}

func lookupSpec(file, sink, checkpoint string, binding map[string]any, interval any, resume bool) map[string]any {
	return map[string]any{
		"version": 1,
		"stream":  "sensors",
		"reference_tables": map[string]any{
			"limits": map[string]any{
				"revision": binding["revision"],
				"sha256":   binding["sha256"],
			},
		},
		"source": map[string]any{
			"kind":           "file",
			"path":           file,
			"file_contract":  "append_only",
			"inbox_capacity": 8,
		},
		"sink": map[string]any{
			"kind":            "http",
			"url":             sink,
			"batch_rows":      2,
			"linger_ms":       1,
			"max_inflight":    1,
			"outbox_capacity": 8,
		},
		"delivery":       "live_best_effort",
		"recovery":       "aligned",
		"checkpoint_dir": checkpoint,
		"checkpoint": map[string]any{
			"interval_ms":        interval,
			"timeout_ms":         5000,
			"retain_generations": 3,
			"max_store_bytes":    33554432,
			"resume_latest":      resume,
		},
		"graph": map[string]any{
			"version":     1,
			"pipeline_id": 8201,
			"revision_id": 1,
			"nodes": []any{
				map[string]any{"id": 1, "kind": "memory_source", "table": "sensors", "out": []int{2}},
				map[string]any{
					"id": 2, "kind": "lookup", "table": "limits",
					"on":   []any{map[string]any{"stream": "device_id", "table": "device_id"}},
					"keep": []string{"threshold"}, "out": []int{3},
				},
				map[string]any{"id": 3, "kind": "capture_sink", "name": "out"},
			},
		},
	}
}

func legacyCountSpec(file, sink, checkpoint string) map[string]any {
	return map[string]any{
		"version": 1,
		"stream":  "sensors",
		"sql":     "SELECT SUM(v) AS s FROM sensors GROUP BY COUNT_WINDOW(3)",
		"source": map[string]any{
			"kind":           "file",
			"path":           file,
			"file_contract":  "append_only",
			"inbox_capacity": 8,
		},
		"sink": map[string]any{
			"kind":            "http",
			"url":             sink,
			"outbox_capacity": 8,
		},
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

func appendRow(path, key string, value int) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	_, err = file.Write(append(data(map[string]any{"device_id": key, "v": value}), '\n'))
	must(err)
	must(file.Sync())
	must(file.Close())
}

func configureServer(a api, sink *capture, streamName string) {
	portText := strconv.Itoa(sink.listener.Addr().(*net.TCPAddr).Port)
	port, err := strconv.Atoi(portText)
	must(err)
	a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": port})
	a.ok(http.MethodPut, "/v1/streams/"+streamName, map[string]any{"fields": sourceFields()})
}

func startPipeline(a api, name string, spec map[string]any) {
	a.ok(http.MethodPost, "/v1/validate", spec)
	a.ok(http.MethodPut, "/v1/pipelines/"+name, spec)
	result, code := a.call(http.MethodPost, "/v1/pipelines/"+name+"/start", map[string]any{}, true)
	require(code >= 200 && code < 300,
		fmt.Sprintf("start %s returned %d: %v", name, code, result))
	waitActual(a, name, "running")
}

func runProfile(root, binary string, capture *capture) {
	file := filepath.Join(root, "sensors.ndjson")
	checkpoint := filepath.Join(root, "checkpoint-v8")
	catalog := filepath.Join(root, "catalog.db")
	port := freePort()
	a := api{base: fmt.Sprintf("http://127.0.0.1:%d", port), client: &http.Client{Timeout: waitDuration}}
	var process *child
	stopProcess := func(signal os.Signal) {
		if process != nil {
			process.stop(signal)
			process = nil
		}
	}
	defer capture.releaseHold(true)
	defer func() { stopProcess(syscall.SIGKILL) }()

	process = launch(binary, root, catalog, filepath.Join(root, "server.log"), port)
	waitHealth(a, "B2-A server health")
	configureServer(a, capture, "sensors")

	// The unauthenticated mutation is a separate negative oracle and must not
	// create a table or a pipeline side effect.
	_, code := a.call(http.MethodPut, "/v1/tables/limits", map[string]any{
		"expected_revision": 0, "table": referenceTable(10),
	}, false)
	require(code == http.StatusUnauthorized || code == http.StatusForbidden,
		"unauthenticated reference-table mutation was accepted")

	r1 := publishTable(a, 10, 0)
	save(filepath.Join(root, "table-r1.json"), r1)
	badCAS := map[string]any{"expected_revision": 0, "table": referenceTable(90)}
	a.rejected(http.MethodPut, "/v1/tables/limits", badCAS)
	require(a.ok(http.MethodGet, "/v1/tables/limits", nil)["sha256"] == r1["sha256"],
		"failed table CAS changed latest revision")

	appendRow(file, "a", 1)
	appendRow(file, "unknown", 2)
	initial := lookupSpec(file, capture.url(), checkpoint, r1, float64(1000), true)
	save(filepath.Join(root, "lookup-r1-spec.json"), initial)
	startPipeline(a, "lookup", initial)
	threshold10, threshold20 := 10, 20
	expectRows(capture, 0, []expectedRow{
		{key: "a", value: 1, threshold: &threshold10},
		{key: "unknown", value: 2, threshold: nil},
	})
	initialOutput := capture.snapshot()
	require(initialOutput.ResponseRows == 2,
		"HTTP oracle must count response rows, not request count")

	// A real periodic checkpoint is the immutable r1 cut.  The payload header
	// and CPL3 marker are inspected, while typed CRC validation remains Rust's
	// runtime authority rather than a duplicate Go codec.
	var periodicStatus map[string]any
	wait("periodic v8 checkpoint", func() bool {
		periodicStatus = status(a, "lookup")
		_, succeeded, active, _, generation := checkpointFields(periodicStatus)
		return succeeded > 0 && !active && generation != "" && storageCurrent(nestedMap(periodicStatus, "checkpoint")) != 0
	})
	// Save the observed periodic status, then stop/join immediately.  Reading
	// the durable baseline only after the scheduler is stopped avoids a second
	// 1 s tick racing the inventory/hash oracle.
	save(filepath.Join(root, "periodic-running-status.json"), periodicStatus)
	a.ok(http.MethodPost, "/v1/pipelines/lookup/stop", map[string]any{})
	waitActual(a, "lookup", "stopped")
	periodicInventory := inventory(a, "lookup")
	requireCurrentSnapshotVersion(periodicInventory, 8)
	periodicEvidence := snapshotEvidence(checkpoint, periodicInventory, r1)
	require(number(periodicEvidence["source_record_index"]) >= 2,
		"periodic checkpoint cut did not include the initial File rows")
	save(filepath.Join(root, "periodic-checkpoints.json"), periodicInventory)
	save(filepath.Join(root, "periodic-snapshot-evidence.json"), periodicEvidence)
	baselineID := storageCurrent(periodicInventory)
	baselineCurrentHash := hash(filepath.Join(checkpoint, "CURRENT"))
	baselineTreeHash := checkpointTreeHash(checkpoint)
	_, _, _, _, baselineGeneration := checkpointFields(periodicStatus)
	require(baselineID != 0 && baselineGeneration != "", "periodic checkpoint identity is incomplete")

	// Publishing r2/r3 never changes the already-attached r1 snapshot.  No
	// input is appended until the periodic scheduler is disabled, so the
	// restart cut remains the r1 checkpoint above.
	r2 := publishTable(a, 90, 1)
	r3 := publishTable(a, 30, 2)
	save(filepath.Join(root, "table-r2.json"), r2)
	save(filepath.Join(root, "table-r3.json"), r3)
	latest := a.ok(http.MethodGet, "/v1/tables/limits", nil)
	require(latest["revision"] == r3["revision"] && latest["sha256"] == r3["sha256"],
		"latest table publication did not reach r3")

	// Change only the scheduler.  The new spec is a
	// revision, but its computation/reference identity remains r1; this is the
	// same safe policy transition used by the production K4 oracle.
	manual := clone(initial).(map[string]any)
	manual["checkpoint"].(map[string]any)["interval_ms"] = nil
	save(filepath.Join(root, "lookup-r1-manual-spec.json"), manual)
	startPipeline(a, "lookup", manual)
	require(hash(filepath.Join(checkpoint, "CURRENT")) == baselineCurrentHash,
		"scheduler-only restart changed the r1 CURRENT")

	// Uncommitted rows exercise both the normal replay suffix and the
	// response-body hold.  All output must still carry r1's threshold.
	preSuffix := capture.rowCount()
	appendRow(file, "a", 3)
	appendRow(file, "b", 4)
	expectRows(capture, preSuffix, []expectedRow{
		{key: "a", value: 3, threshold: &threshold10},
		{key: "b", value: 4, threshold: &threshold20},
	})

	capture.setHold()
	preHoldOutput := capture.rowCount()
	preHoldReceived := capture.receivedRows()
	appendRow(file, "a", 5)
	wait("held HTTP body received", func() bool { return capture.receivedRows() >= preHoldReceived+1 })
	capture.waitHeld()
	beforeHold := status(a, "lookup")
	holdStarted, holdSucceeded, _, _, _ := checkpointFields(beforeHold)
	holdCurrentHash := hash(filepath.Join(checkpoint, "CURRENT"))
	holdResult := make(chan struct {
		value map[string]any
		code  int
	}, 1)
	go func() {
		value, code := a.call(http.MethodPost, "/v1/pipelines/lookup/checkpoint", nil, true)
		holdResult <- struct {
			value map[string]any
			code  int
		}{value: value, code: code}
	}()
	var blocked map[string]any
	wait("checkpoint blocked on HTTP response", func() bool {
		blocked = status(a, "lookup")
		started, succeeded, active, _, _ := checkpointFields(blocked)
		require(succeeded == holdSucceeded, "checkpoint succeeded while HTTP response was held")
		require(hash(filepath.Join(checkpoint, "CURRENT")) == holdCurrentHash,
			"checkpoint advanced CURRENT before the held HTTP response completed")
		return active && started > holdStarted
	})
	save(filepath.Join(root, "http-hold-blocked-status.json"), blocked)
	// Kill while the HTTP response is unknown.  The capture is then released
	// in discard mode so its handler cannot fabricate a successful 2xx after
	// the client process has gone away.
	stopProcess(syscall.SIGKILL)
	capture.releaseHold(true)
	select {
	case result := <-holdResult:
		save(filepath.Join(root, "http-hold-killed-result.json"), map[string]any{"http_status": result.code, "response": result.value})
	case <-time.After(5 * time.Second):
		panic("checkpoint request remained stuck after process kill")
	}
	require(capture.rowCount() == preHoldOutput,
		"held HTTP body was counted as a successful output after SIGKILL")

	replayStart := capture.rowCount()
	process = launch(binary, root, catalog, filepath.Join(root, "server.log"), port)
	waitHealth(a, "B2-A HTTP-hold restore health")
	a.ok(http.MethodPost, "/v1/pipelines/lookup/start", map[string]any{})
	waitActual(a, "lookup", "running")
	expectRows(capture, replayStart, []expectedRow{
		{key: "a", value: 3, threshold: &threshold10},
		{key: "b", value: 4, threshold: &threshold20},
		{key: "a", value: 5, threshold: &threshold10},
	})
	restored := status(a, "lookup")
	restoredFrom := number(nested(restored, "checkpoint", "restored_from_checkpoint"))
	require(restoredFrom == baselineID,
		fmt.Sprintf("restore used checkpoint %d, want r1 cut %d", restoredFrom, baselineID))
	require(hash(filepath.Join(checkpoint, "CURRENT")) == baselineCurrentHash,
		"HTTP-hold restore changed CURRENT before a new commit")
	save(filepath.Join(root, "http-hold-restore-status.json"), restored)
	save(filepath.Join(root, "http-hold-restore-outputs.json"), capture.snapshot())

	// Establish a new committed point, then inject a real CURRENT.tmp
	// obstruction.  The failed commit may leave an unpublished generation, but
	// the durable CURRENT and its source cut must remain the old point.
	beforeCheckpoint := status(a, "lookup")
	_, beforeSucceeded, _, _, _ := checkpointFields(beforeCheckpoint)
	beforeCheckpointError := strings.ToLower(fmt.Sprint(nested(beforeCheckpoint, "checkpoint", "last_error_code")))
	require(!strings.Contains(beforeCheckpointError, "internal"),
		"CURRENT.tmp fault started with a stale internal checkpoint error")
	checkpointResult, checkpointCode := manualCheckpoint(a, "lookup")
	require(checkpointCode >= 200 && checkpointCode < 300,
		fmt.Sprintf("post-restore checkpoint failed: %d %v", checkpointCode, checkpointResult))
	committed := waitSuccess(a, "lookup", beforeSucceeded)
	committedInventory := inventory(a, "lookup")
	requireCurrentSnapshotVersion(committedInventory, 8)
	committedEvidence := snapshotEvidence(checkpoint, committedInventory, r1)
	require(number(committedEvidence["reference_runtime_crc32"]) == number(periodicEvidence["reference_runtime_crc32"]),
		"the same r1 table produced different runtime CRC identities across checkpoints")
	save(filepath.Join(root, "post-restore-checkpoint-status.json"), committed)
	save(filepath.Join(root, "post-restore-checkpoints.json"), committedInventory)
	save(filepath.Join(root, "post-restore-snapshot-evidence.json"), committedEvidence)

	failureBaselineID := storageCurrent(committedInventory)
	failureCurrentHash := hash(filepath.Join(checkpoint, "CURRENT"))
	failureTreeHash := checkpointTreeHash(checkpoint)
	failureOutputStart := capture.rowCount()
	appendRow(file, "a", 6)
	appendRow(file, "b", 7)
	expectRows(capture, failureOutputStart, []expectedRow{
		{key: "a", value: 6, threshold: &threshold10},
		{key: "b", value: 7, threshold: &threshold20},
	})
	must(os.Mkdir(filepath.Join(checkpoint, "CURRENT.tmp"), 0700))
	tmpInfo, tmpErr := os.Stat(filepath.Join(checkpoint, "CURRENT.tmp"))
	require(tmpErr == nil && tmpInfo.IsDir(), "CURRENT.tmp fault fixture is not a directory")
	failedResult, failedCode := manualCheckpoint(a, "lookup")
	require(failedCode >= 400,
		fmt.Sprintf("CURRENT.tmp checkpoint unexpectedly succeeded: %d %v", failedCode, failedResult))
	failedMessage := strings.ToLower(fmt.Sprint(nested(failedResult, "error", "message")))
	require(strings.Contains(failedMessage, "checkpoint io:") && strings.Contains(failedMessage, "is a directory"),
		fmt.Sprintf("CURRENT.tmp failure did not reach checkpoint I/O: %v", failedResult))
	var failedStatus map[string]any
	wait("CURRENT.tmp checkpoint failure status", func() bool {
		failedStatus = status(a, "lookup")
		actualStatus := nested(failedStatus, "actual", "status")
		actualError := strings.ToLower(fmt.Sprint(nested(failedStatus, "actual", "last_error")))
		checkpointError := strings.ToLower(fmt.Sprint(nested(failedStatus, "checkpoint", "last_error_code")))
		checkpointActive := nested(failedStatus, "checkpoint", "active") == true
		// The File worker currently keeps the attempt alive after a commit
		// error. Accept that nonfatal contract only when the checkpoint
		// control state records the exact internal I/O failure; if the
		// product makes it fatal, require the same exact error on actual.
		return !checkpointActive && strings.Contains(checkpointError, "internal") &&
			((actualStatus == "failed" && strings.Contains(actualError, "checkpoint io:") && strings.Contains(actualError, "is a directory")) ||
				strings.Contains(failedMessage, "checkpoint io:") && strings.Contains(failedMessage, "is a directory"))
	})
	require(hash(filepath.Join(checkpoint, "CURRENT")) == failureCurrentHash,
		"CURRENT.tmp failure changed CURRENT")
	failedTreeAfter := checkpointTreeHash(checkpoint)
	require(failedTreeAfter == failureTreeHash,
		"CURRENT.tmp failure changed committed checkpoint history")
	save(filepath.Join(root, "current-tmp-failure-response.json"), failedResult)
	save(filepath.Join(root, "current-tmp-failure-status.json"), failedStatus)
	removeErr := os.Remove(filepath.Join(checkpoint, "CURRENT.tmp"))
	must(removeErr)

	replayFailureStart := capture.rowCount()
	stopProcess(syscall.SIGKILL)
	process = launch(binary, root, catalog, filepath.Join(root, "server.log"), port)
	waitHealth(a, "B2-A CURRENT.tmp replay health")
	a.ok(http.MethodPost, "/v1/pipelines/lookup/start", map[string]any{})
	waitActual(a, "lookup", "running")
	expectRows(capture, replayFailureStart, []expectedRow{
		{key: "a", value: 6, threshold: &threshold10},
		{key: "b", value: 7, threshold: &threshold20},
	})
	save(filepath.Join(root, "current-tmp-replay-outputs.json"), capture.snapshot())
	postFailureStatus := status(a, "lookup")
	postFailureInventory := inventory(a, "lookup")
	require(storageCurrent(postFailureInventory) == failureBaselineID,
		"replay after CURRENT.tmp failure advanced beyond the previous durable point")
	require(number(nested(postFailureStatus, "checkpoint", "restored_from_checkpoint")) == failureBaselineID,
		"CURRENT.tmp replay did not restore exactly the previous durable point")
	save(filepath.Join(root, "current-tmp-replay-status.json"), postFailureStatus)
	save(filepath.Join(root, "current-tmp-replay-checkpoints.json"), postFailureInventory)

	// A missing table and a wrong digest are API negatives; neither is allowed
	// to touch the live job's output or checkpoint history.
	negativeCurrentHash := hash(filepath.Join(checkpoint, "CURRENT"))
	negativeOutputRows := capture.rowCount()
	missingTable := clone(manual).(map[string]any)
	delete(missingTable["reference_tables"].(map[string]any), "limits")
	missingResult, missingCode := a.call(http.MethodPost, "/v1/validate", missingTable, true)
	require(missingCode >= 400 && missingCode < 500, "missing reference-table binding was accepted")
	wrongDigest := clone(manual).(map[string]any)
	wrongDigest["reference_tables"].(map[string]any)["limits"].(map[string]any)["sha256"] = strings.Repeat("0", 64)
	digestResult, digestCode := a.call(http.MethodPost, "/v1/validate", wrongDigest, true)
	require(digestCode >= 400 && digestCode < 500, "wrong reference-table digest was accepted")
	require(hash(filepath.Join(checkpoint, "CURRENT")) == negativeCurrentHash && capture.rowCount() == negativeOutputRows,
		"invalid reference binding changed live output or CURRENT")
	save(filepath.Join(root, "missing-table-rejection.json"), map[string]any{"status": missingCode, "response": missingResult})
	save(filepath.Join(root, "wrong-digest-rejection.json"), map[string]any{"status": digestCode, "response": digestResult})

	// Replace the binding with r2.  It is a syntactically valid new pipeline
	// revision, but restore must reject the v8 snapshot before source activation
	// and leave both the output and CURRENT untouched.
	semanticCurrentHash := hash(filepath.Join(checkpoint, "CURRENT"))
	semanticOutputRows := capture.rowCount()
	changedBinding := clone(manual).(map[string]any)
	changedBinding["reference_tables"].(map[string]any)["limits"] = map[string]any{
		"revision": r2["revision"], "sha256": r2["sha256"],
	}
	save(filepath.Join(root, "changed-binding-spec.json"), changedBinding)
	a.ok(http.MethodPut, "/v1/pipelines/lookup", changedBinding)
	startResult, startCode := a.call(http.MethodPost, "/v1/pipelines/lookup/start", map[string]any{}, true)
	require(startCode >= 200 && startCode < 300,
		fmt.Sprintf("changed-binding start request returned %d: %v", startCode, startResult))
	changedStatus := waitRestoreFailure(a, "lookup")
	guardText := strings.ToLower(fmt.Sprint(nested(changedStatus, "actual", "last_error")))
	require(strings.Contains(guardText, "checkpoint participant set, schema, codec or pipeline semantics changed"),
		"changed binding failed without the precise checkpoint compatibility error")
	require(hash(filepath.Join(checkpoint, "CURRENT")) == semanticCurrentHash && capture.rowCount() == semanticOutputRows,
		"changed binding touched CURRENT or emitted output before restore rejection")
	save(filepath.Join(root, "changed-binding-status.json"), changedStatus)

	// Store GC must retain r1: it is used by the historical pipeline revision
	// and by the checkpoint dependency.  The dependency preview is metadata
	// only; exact runtime CRC remains in the v8 payload/runtime admission.
	dependencies := a.ok(http.MethodGet, "/v1/tables/limits/dependencies", nil)
	save(filepath.Join(root, "dependencies-final.json"), dependencies)
	pins, ok := nested(dependencies, "pins", "items").([]any)
	require(ok, "dependency preview did not return bounded pin items")
	foundR1Pin := false
	for _, raw := range pins {
		pin, ok := raw.(map[string]any)
		if ok && number(pin["table_revision"]) == number(r1["revision"]) && pin["sha256"] == r1["sha256"] {
			foundR1Pin = true
		}
	}
	require(foundR1Pin, "dependency preview omitted the historical r1 pin")
	gc := a.ok(http.MethodPost, "/v1/tables/limits/gc", map[string]any{})
	save(filepath.Join(root, "gc-final.json"), gc)
	retainedR1 := a.ok(http.MethodGet, "/v1/tables/limits/revisions/1", nil)
	require(retainedR1["sha256"] == r1["sha256"], "GC deleted checkpoint-required r1")

	// Leave the old pipeline revision and its v8 store untouched for the two
	// profile-guard process checks below.
	require(hash(filepath.Join(checkpoint, "CURRENT")) == semanticCurrentHash,
		"post-GC maintenance changed the failed binding's CURRENT")
	save(filepath.Join(root, "r1-retained.json"), retainedR1)
	save(filepath.Join(root, "summary-partial.json"), map[string]any{
		"baseline_checkpoint_id":     baselineID,
		"post_restore_checkpoint_id": failureBaselineID,
		"baseline_tree_sha256":       baselineTreeHash,
		"r1_binding_sha256":          r1["sha256"],
		"r2_binding_sha256":          r2["sha256"],
		"r3_binding_sha256":          r3["sha256"],
	})
}

func nestedMap(value map[string]any, keys ...string) map[string]any {
	result, _ := nested(value, keys...).(map[string]any)
	return result
}

func runNoReferenceGuard(root, binary string, capture *capture) map[string]any {
	checkpoint := filepath.Join(root, "checkpoint-v8")
	file := filepath.Join(root, "sensors.ndjson")
	must(os.WriteFile(file, []byte("{\"device_id\":\"a\",\"v\":99}\n"), 0600))
	port := freePort()
	a := api{base: fmt.Sprintf("http://127.0.0.1:%d", port), client: &http.Client{Timeout: waitDuration}}
	catalog := filepath.Join(root, "catalog.db")
	var process *child
	defer func() {
		if process != nil {
			process.stop(syscall.SIGKILL)
		}
	}()
	process = launch(binary, root, catalog, filepath.Join(root, "server.log"), port)
	waitHealth(a, "new no-reference guard health")
	configureServer(a, capture, "sensors")
	spec := legacyCountSpec(file, capture.url(), checkpoint)
	save(filepath.Join(root, "legacy-count-no-reference-spec.json"), spec)
	a.ok(http.MethodPut, "/v1/pipelines/no-reference", spec)
	beforeTree := checkpointFullTreeHash(checkpoint)
	beforeCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	beforeOutput := capture.rowCount()
	result, code := a.call(http.MethodPost, "/v1/pipelines/no-reference/start", map[string]any{}, true)
	require(code >= 200 && code < 300,
		fmt.Sprintf("no-reference guard start returned %d: %v", code, result))
	failed := waitGuardFailure(a, "no-reference")
	text := strings.ToLower(fmt.Sprint(nested(failed, "actual", "last_error")))
	require(strings.Contains(text, "checkpoint source profile mismatch"),
		"new no-reference profile did not reach the v8 checkpoint guard")
	process.stop(syscall.SIGTERM)
	process = nil
	require(checkpointFullTreeHash(checkpoint) == beforeTree && hash(filepath.Join(checkpoint, "CURRENT")) == beforeCurrent,
		"new no-reference guard changed v8 history")
	require(capture.rowCount() == beforeOutput, "new no-reference guard emitted output")
	evidence := map[string]any{
		"checked":           true,
		"start_code":        code,
		"error":             text,
		"history_preserved": true,
		"current_preserved": true,
		"output_preserved":  true,
	}
	save(filepath.Join(root, "evidence.json"), evidence)
	return evidence
}

func runOldGuard(root, oldBinary string, capture *capture) map[string]any {
	checkpoint := filepath.Join(root, "checkpoint-v8")
	file := filepath.Join(root, "sensors.ndjson")
	must(os.WriteFile(file, []byte("{\"device_id\":\"a\",\"v\":99}\n"), 0600))
	catalog := filepath.Join(root, "catalog.db")
	setupPort := freePort()
	setupAPI := api{base: fmt.Sprintf("http://127.0.0.1:%d", setupPort), client: &http.Client{Timeout: waitDuration}}
	var setup *child
	defer func() {
		if setup != nil {
			setup.stop(syscall.SIGKILL)
		}
	}()
	setup = launch(oldBinary, root, catalog, filepath.Join(root, "setup-server.log"), setupPort)
	waitHealth(setupAPI, "old B1 catalog setup health")
	configureServer(setupAPI, capture, "sensors")
	spec := legacyCountSpec(file, capture.url(), checkpoint)
	save(filepath.Join(root, "legacy-count-spec.json"), spec)
	setupAPI.ok(http.MethodPut, "/v1/pipelines/legacy-count", spec)
	setup.stop(syscall.SIGTERM)
	setup = nil

	beforeTree := checkpointFullTreeHash(checkpoint)
	beforeCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	beforeOutput := capture.rowCount()
	runPort := freePort()
	runAPI := api{base: fmt.Sprintf("http://127.0.0.1:%d", runPort), client: &http.Client{Timeout: waitDuration}}
	var run *child
	defer func() {
		if run != nil {
			run.stop(syscall.SIGKILL)
		}
	}()
	run = launch(oldBinary, root, catalog, filepath.Join(root, "server.log"), runPort)
	waitHealth(runAPI, "old B1 v8 guard health")
	startResult, code := runAPI.call(http.MethodPost, "/v1/pipelines/legacy-count/start", map[string]any{}, true)
	require(code >= 200 && code < 300,
		fmt.Sprintf("old B1 guard start returned %d: %v", code, startResult))
	failed := waitGuardFailure(runAPI, "legacy-count")
	text := strings.ToLower(fmt.Sprint(nested(failed, "actual", "last_error")))
	logBytes, err := os.ReadFile(filepath.Join(root, "server.log"))
	must(err)
	text += " " + strings.ToLower(string(logBytes))
	require(strings.Contains(text, "checkpoint source profile mismatch"),
		"old B1 binary did not reach the v8 checkpoint profile guard")
	run.stop(syscall.SIGTERM)
	run = nil
	require(checkpointFullTreeHash(checkpoint) == beforeTree && hash(filepath.Join(checkpoint, "CURRENT")) == beforeCurrent,
		"old B1 profile guard changed v8 history")
	require(capture.rowCount() == beforeOutput, "old B1 profile guard emitted output")
	evidence := map[string]any{
		"checked":           true,
		"start_code":        code,
		"error":             text,
		"catalog":           "old B1 binary-created legacy Count catalog",
		"history_preserved": true,
		"current_preserved": true,
		"output_preserved":  true,
	}
	save(filepath.Join(root, "evidence.json"), evidence)
	return evidence
}

func main() {
	binaryPath := flag.String("server-bin", "", "B2-A production server binary")
	oldBinaryPath := flag.String("old-server-bin", "", "required old B1 server binary for the v8 guard")
	out := flag.String("out", "", "new isolated artifact directory (must not exist)")
	flag.Parse()
	require(*binaryPath != "" && *oldBinaryPath != "" && *out != "",
		"server-bin, old-server-bin and out are required")

	root, err := filepath.Abs(*out)
	must(err)
	if _, err := os.Stat(root); err == nil {
		panic("out directory already exists; refusing to reuse artifacts")
	} else if !os.IsNotExist(err) {
		panic(err)
	}
	must(os.Mkdir(root, 0700))
	self, err := os.Executable()
	must(err)
	save(filepath.Join(root, "binaries.json"), map[string]any{
		"server_sha256":     hash(*binaryPath),
		"old_server_sha256": hash(*oldBinaryPath),
		"driver_sha256":     hash(self),
	})

	lookupRoot := filepath.Join(root, "static-lookup")
	must(os.Mkdir(lookupRoot, 0700))
	lookupCapture := newCapture()
	defer lookupCapture.close()
	runProfile(lookupRoot, *binaryPath, lookupCapture)
	save(filepath.Join(lookupRoot, "outputs.json"), lookupCapture.snapshot())

	// Both guard checks get independent catalogs and checkpoint copies.  The
	// original v8 directory is never opened by a legacy writer in-place.
	noReferenceRoot := filepath.Join(root, "new-no-reference-guard")
	must(os.Mkdir(noReferenceRoot, 0700))
	copyTree(filepath.Join(lookupRoot, "checkpoint-v8"), filepath.Join(noReferenceRoot, "checkpoint-v8"))
	noReferenceCapture := newCapture()
	defer noReferenceCapture.close()
	noReferenceEvidence := runNoReferenceGuard(noReferenceRoot, *binaryPath, noReferenceCapture)
	save(filepath.Join(noReferenceRoot, "outputs.json"), noReferenceCapture.snapshot())

	oldRoot := filepath.Join(root, "old-b1-guard")
	must(os.Mkdir(oldRoot, 0700))
	copyTree(filepath.Join(lookupRoot, "checkpoint-v8"), filepath.Join(oldRoot, "checkpoint-v8"))
	oldCapture := newCapture()
	defer oldCapture.close()
	oldEvidence := runOldGuard(oldRoot, *oldBinaryPath, oldCapture)
	save(filepath.Join(oldRoot, "outputs.json"), oldCapture.snapshot())

	summary := map[string]any{
		"valid":                          true,
		"profile":                        "static_lookup_file_aligned_v8",
		"checkpoint_snapshot_version":    8,
		"checkpoint_manifest":            "CPL3",
		"file_source_contract":           "append_only",
		"r1_binding_fixed_at_checkpoint": true,
		"r2_r3_latest_publish_no_drift":  true,
		// These aliases are the stable shell-validator contract.  Keep the
		// more descriptive fields above as human-readable evidence as well.
		"fixed_binding":                          true,
		"suffix_recovery":                        true,
		"http_unknown_no_early_current":          true,
		"current_failure_preserved":              true,
		"changed_binding_refused":                true,
		"gc_preserved_dependency":                true,
		"old_profile_guard":                      true,
		"new_profile_guard":                      true,
		"missing_dependency_api_refused":         true,
		"periodic_checkpoint_v8":                 true,
		"http_response_rows_counted":             true,
		"http_received_body_not_2xx_not_commit":  true,
		"http_hold_checkpoint_blocked":           true,
		"sigkill_replayed_file_suffix":           true,
		"file_stable_business_id_claimed":        false,
		"current_tmp_failure_preserved":          true,
		"binding_change_restore_rejected":        true,
		"gc_retained_r1_dependency":              true,
		"missing_table_rejected":                 true,
		"wrong_digest_rejected":                  true,
		"new_no_reference_profile_guard":         noReferenceEvidence["checked"] == true,
		"old_b1_profile_guard":                   oldEvidence["checked"] == true,
		"old_b1_catalog_isolated":                true,
		"history_and_output_preserved_on_guards": true,
		"exactly_once_claimed":                   false,
		"certified":                              false,
	}
	save(filepath.Join(root, "summary.json"), summary)
	fmt.Println("CORE_B2_PROCESS_OK static Lookup v8/CPL3, source-cut replay, HTTP hold, CAS/GC and profile guards")
}
