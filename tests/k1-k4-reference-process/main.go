// Real process oracle for the reference-table/state combinations introduced
// after B2-A.  The driver deliberately uses only the Go standard library:
// control operations go through Sparrow's HTTP API, JetStream operations use
// a bounded Core NATS fixture client, and the output checks are independent
// golden values rather than a second implementation of the Rust evaluator.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"hash/crc32"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	token        = "k1-k4-reference-process-fixture"
	maxHTTPBody  = 1 << 20
	waitDuration = 20 * time.Second
)

type callResult struct {
	result map[string]any
	code   int
}

// Keep asynchronous API responses in evidence rather than serializing the
// private transport fields as an empty JSON object.
func (r callResult) MarshalJSON() ([]byte, error) {
	return json.Marshal(map[string]any{"status": r.code, "response": r.result})
}

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

// committedTreeHash intentionally ignores only transient writer artifacts and
// unpublished generations.  fullTreeHash is used by compatibility guards so
// an old reader cannot hide a crash-cut generation while failing closed.
func committedTreeHash(root string) string {
	return checkpointHash(root, true)
}

func fullTreeHash(root string) string {
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
}

func launch(binary, root, logfile string, args ...string) *child {
	logFile, err := os.OpenFile(logfile, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	command := exec.Command(binary, args...)
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

func checkPanicLog(path string) {
	raw, err := os.ReadFile(path)
	must(err)
	lower := strings.ToLower(string(raw))
	for _, marker := range []string{"panicked at", "panic: runtime error", "fatal runtime error"} {
		require(!strings.Contains(lower, marker), fmt.Sprintf("panic marker %q in %s", marker, path))
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
	_ = c.log.Close()
	c.cmd = nil
	checkPanicLog(c.logPath)
	if signal == syscall.SIGKILL {
		require(waitErr == nil || killedBySIGKILL(waitErr),
			fmt.Sprintf("SIGKILL child exit was not a kill: signal=%v wait=%v", signalErr, waitErr))
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

func (a api) call(method, path string, value any) (map[string]any, int) {
	var body io.Reader
	if value != nil {
		body = bytes.NewReader(data(value))
	}
	request, err := http.NewRequest(method, a.base+path, body)
	must(err)
	request.Header.Set("Authorization", "Bearer "+token)
	request.Header.Set("Content-Type", "application/json")
	if method == http.MethodPut && strings.HasPrefix(path, "/v1/pipelines/") && strings.Count(path, "/") == 3 {
		current, code := a.call(http.MethodGet, path, nil)
		if code == http.StatusOK {
			etag, ok := current["etag"].(string)
			require(ok && etag != "", "pipeline update did not return an ETag")
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
	result, code := a.call(method, path, value)
	require(code >= 200 && code < 300,
		fmt.Sprintf("%s %s returned %d: %v", method, path, code, result))
	return result
}

func (a api) rejected(method, path string, value any) map[string]any {
	result, code := a.call(method, path, value)
	require(code >= 400 && code < 500,
		fmt.Sprintf("expected API rejection: %s %s returned %d: %v", method, path, code, result))
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
	case uint32:
		return uint64(value)
	}
	return 0
}

func generationText(value any) string {
	switch value := value.(type) {
	case string:
		if len(value) == 32 {
			if raw, err := hex.DecodeString(value); err == nil && len(raw) == 16 && !bytes.Equal(raw, make([]byte, 16)) {
				return strings.ToLower(value)
			}
		}
	case []any:
		if len(value) != 16 {
			return ""
		}
		raw := make([]byte, 16)
		for i, item := range value {
			n := number(item)
			if n > 255 {
				return ""
			}
			raw[i] = byte(n)
		}
		if !bytes.Equal(raw, make([]byte, 16)) {
			return hex.EncodeToString(raw)
		}
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
	return number(checkpoint["started_total"]), number(checkpoint["succeeded_total"]),
		checkpoint["active"] == true, number(checkpoint["last_success_id"]),
		generationText(checkpoint["state_generation"])
}

func statusReliable(status map[string]any) (restored, published, committed, pending uint64) {
	return number(nested(status, "checkpoint", "reliable_source", "restored_cut")),
		number(nested(status, "checkpoint", "reliable_source", "published_cut")),
		number(nested(status, "checkpoint", "reliable_source", "committed_cut")),
		number(nested(status, "checkpoint", "reliable_source", "pending_messages"))
}

func statusOf(a api, name string) map[string]any {
	return a.ok(http.MethodGet, "/v1/pipelines/"+name+"/status", nil)
}

func waitHealth(a api, label string) {
	wait(label, func() bool {
		_, code := a.call(http.MethodGet, "/v1/health", nil)
		return code == http.StatusOK
	})
}

func waitActual(a api, name, expected string) map[string]any {
	var result map[string]any
	wait("actual status="+expected, func() bool {
		result = statusOf(a, name)
		return nested(result, "actual", "status") == expected
	})
	return result
}

func waitExactError(a api, name, needle string) map[string]any {
	var result map[string]any
	wait("expected error="+needle, func() bool {
		result = statusOf(a, name)
		return nested(result, "actual", "status") == "failed" &&
			strings.Contains(strings.ToLower(fmt.Sprint(nested(result, "actual", "last_error"))), strings.ToLower(needle))
	})
	return result
}

func manualCheckpoint(a api, name string) (map[string]any, int) {
	for attempt := 0; attempt < 100; attempt++ {
		result, code := a.call(http.MethodPost, "/v1/pipelines/"+name+"/checkpoint", nil)
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

func waitCheckpointSuccess(a api, name string, previous uint64) map[string]any {
	var result map[string]any
	wait("checkpoint success", func() bool {
		result = statusOf(a, name)
		_, succeeded, active, _, _ := checkpointFields(result)
		return !active && succeeded > previous
	})
	return result
}

func waitReliableCommitted(a api, name string, cut uint64) map[string]any {
	var result map[string]any
	wait("reliable committed cut", func() bool {
		result = statusOf(a, name)
		_, _, committed, pending := statusReliable(result)
		return committed == cut && pending == 0
	})
	return result
}

func inventory(a api, name string) map[string]any {
	return a.ok(http.MethodGet, "/v1/pipelines/"+name+"/checkpoints", nil)
}

func storageCurrent(value map[string]any) uint64 {
	return number(nested(value, "storage", "current"))
}

func checkpointGeneration(value map[string]any) string {
	current := storageCurrent(value)
	generations, ok := nested(value, "storage", "generations").([]any)
	require(ok && current != 0, "checkpoint generation inventory missing")
	for _, raw := range generations {
		generation, ok := raw.(map[string]any)
		if ok && number(generation["id"]) == current {
			identity := generationText(generation["state_generation"])
			require(identity != "", "CURRENT state generation missing")
			return identity
		}
	}
	panic("CURRENT generation not listed")
}

func requireCurrentSnapshotVersion(value map[string]any, want uint64) {
	current := storageCurrent(value)
	require(current != 0, "checkpoint inventory has no CURRENT")
	gens, ok := nested(value, "storage", "generations").([]any)
	require(ok, "checkpoint inventory generations missing")
	for _, raw := range gens {
		generation, ok := raw.(map[string]any)
		if ok && number(generation["id"]) == current {
			require(number(generation["snapshot_version"]) == want,
				fmt.Sprintf("CURRENT snapshot version=%v want=%d", generation["snapshot_version"], want))
			return
		}
	}
	panic(fmt.Sprintf("CURRENT generation %d was not listed", current))
}

func snapshotPayload(checkpoint string, id uint64) []byte {
	generation := filepath.Join(checkpoint, fmt.Sprintf("chk-%08d", id))
	manifest, err := os.ReadFile(filepath.Join(generation, "MANIFEST"))
	must(err)
	require(len(manifest) >= 30 && bytes.Equal(manifest[:4], []byte("MAN2")) && binary.LittleEndian.Uint16(manifest[4:6]) == 1, "checkpoint MANIFEST is not MAN2/v1")
	require(binary.LittleEndian.Uint64(manifest[6:14]) == id, "checkpoint MANIFEST identity differs from CURRENT")
	chunks := binary.LittleEndian.Uint32(manifest[14:18])
	require(chunks > 0 && chunks <= 4096, "invalid checkpoint chunk count")
	require(binary.LittleEndian.Uint32(manifest[26:30]) == chunks && len(manifest) == 30+4*int(chunks), "MANIFEST checksum table is invalid")
	size := binary.LittleEndian.Uint64(manifest[18:26])
	require(size > 0 && size <= 8*1024*1024, "snapshot payload size exceeds bound")
	marker, err := os.ReadFile(filepath.Join(generation, "PUBLISHED"))
	must(err)
	require(string(marker) == fmt.Sprintf("PUB1 %d %d\n", id, crc32.ChecksumIEEE(manifest)), "checkpoint publication proof mismatch")
	payload := make([]byte, 0)
	for i := uint32(0); i < chunks; i++ {
		part, err := os.ReadFile(filepath.Join(generation, fmt.Sprintf("%04d.bin", i)))
		must(err)
		require(crc32.ChecksumIEEE(part) == binary.LittleEndian.Uint32(manifest[30+4*i:34+4*i]), "checkpoint chunk CRC mismatch")
		payload = append(payload, part...)
	}
	require(uint64(len(payload)) == size, "checkpoint payload length mismatch")
	return payload
}

func snapshotHash(checkpoint string, id uint64) string {
	value := sha256.Sum256(snapshotPayload(checkpoint, id))
	return hex.EncodeToString(value[:])
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

func readBytes(raw []byte, offset *int, length int) ([]byte, bool) {
	if length < 0 || *offset < 0 || length > len(raw)-*offset {
		return nil, false
	}
	value := raw[*offset : *offset+length]
	*offset += length
	return value, true
}

type snapshotInfo struct {
	Version        uint16
	SourceKind     string
	SourceRecord   uint64
	Manifest       string
	StateCount     uint16
	ReferenceName  string
	ReferenceRev   uint64
	ReferenceSHA   string
	ReferenceCRC32 uint32
}

// snapshotInfo parses only the bounded outer envelope and CPL3 dependency
// section.  It intentionally does not duplicate the state/row codec.
func parseSnapshotInfo(checkpoint string, inventoryValue map[string]any, expected map[string]any, version uint16, stateCount uint16) snapshotInfo {
	id := storageCurrent(inventoryValue)
	payload := snapshotPayload(checkpoint, id)
	require(len(payload) >= 38 && bytes.Equal(payload[:4], []byte("SPV1")), "snapshot payload header is not SPV1")
	gotVersion := binary.LittleEndian.Uint16(payload[4:6])
	require(gotVersion == version, fmt.Sprintf("snapshot version=%d want=%d", gotVersion, version))
	offset := 38
	readString := func() string {
		length, ok := readU32(payload, &offset)
		require(ok && length <= 64*1024, "snapshot source string exceeds bound")
		value, ok := readBytes(payload, &offset, int(length))
		require(ok, "snapshot source string truncated")
		return string(value)
	}
	sourceKind := readString()
	_ = readString()
	_, ok := readBytes(payload, &offset, 16) // source size/fingerprint
	require(ok, "snapshot source identity truncated")
	_, ok = readBytes(payload, &offset, 8) // attempt
	require(ok, "snapshot attempt truncated")
	_, ok = readBytes(payload, &offset, 8) // pipeline revision
	require(ok, "snapshot revision truncated")
	_, ok = readBytes(payload, &offset, 16) // state generation
	require(ok, "snapshot generation truncated")
	if version == 10 {
		// Reliable reference snapshots carry the JetStream output sequence
		// (epoch + ordinal) immediately before the plan length.  The driver
		// checks the output IDs independently below; only skip this bounded
		// envelope here so the shared CPL3 parser stays profile-neutral.
		_, ok = readBytes(payload, &offset, 24)
		require(ok, "snapshot reliable output cursor truncated")
	}
	planLength, ok := readU32(payload, &offset)
	require(ok && planLength <= 256*1024, "snapshot plan exceeds bound")
	plan, ok := readBytes(payload, &offset, int(planLength))
	require(ok && len(plan) >= 14 && bytes.Equal(plan[:4], []byte("CPL3")), "snapshot plan is not CPL3")
	planOffset := 12
	states, ok := readU16(plan, &planOffset)
	require(ok && states == stateCount, fmt.Sprintf("snapshot state count=%d want=%d", states, stateCount))
	if states > 0 {
		_, ok = readBytes(plan, &planOffset, int(states)*11)
		require(ok, "snapshot state participant section truncated")
	}
	refs, ok := readU16(plan, &planOffset)
	require(ok && refs == 1, fmt.Sprintf("snapshot reference count=%d want=1", refs))
	nameLength, ok := readU16(plan, &planOffset)
	require(ok && nameLength > 0 && nameLength <= 64, "snapshot reference name length invalid")
	nameBytes, ok := readBytes(plan, &planOffset, int(nameLength))
	require(ok, "snapshot reference name truncated")
	revision, ok := readU64(plan, &planOffset)
	require(ok, "snapshot reference revision truncated")
	digest, ok := readBytes(plan, &planOffset, 32)
	require(ok, "snapshot reference digest truncated")
	crc, ok := readU32(plan, &planOffset)
	require(ok, "snapshot reference CRC truncated")
	expectedDigest, err := hex.DecodeString(fmt.Sprint(expected["sha256"]))
	must(err)
	require(string(nameBytes) == "limits" && revision == number(expected["revision"]) && bytes.Equal(digest, expectedDigest),
		"snapshot CPL3 dependency does not match r1")
	return snapshotInfo{
		Version: gotVersion, SourceKind: sourceKind,
		SourceRecord: binary.LittleEndian.Uint64(payload[30:38]),
		Manifest:     "CPL3", StateCount: states,
		ReferenceName: string(nameBytes), ReferenceRev: revision,
		ReferenceSHA: hex.EncodeToString(digest), ReferenceCRC32: crc,
	}
}

type captureBody struct {
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
	Bodies       []captureBody    `json:"bodies"`
	Rows         []map[string]any `json:"rows"`
	Received     []map[string]any `json:"received_payloads"`
}

type capture struct {
	mu           sync.Mutex
	rows         []map[string]any
	rawRows      []map[string]any
	requests     int
	received     int
	responses    int
	responseRows int
	bodies       []captureBody

	holdMu        sync.Mutex
	hold          bool
	holdStarted   chan struct{}
	holdRelease   chan struct{}
	discardHeld   bool
	status        int
	responseDelay time.Duration

	server    *http.Server
	listener  net.Listener
	serveDone chan struct{}
	closeOnce sync.Once
}

func newCapture() *capture {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	result := &capture{listener: listener, status: http.StatusOK, serveDone: make(chan struct{})}
	result.holdRelease = make(chan struct{})
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
			body := captureBody{Rows: len(rows), At: time.Now()}
			result.mu.Lock()
			result.requests++
			result.received += len(rows)
			result.rawRows = append(result.rawRows, rows...)
			result.bodies = append(result.bodies, body)
			index := len(result.bodies) - 1
			result.mu.Unlock()

			result.holdMu.Lock()
			holding, started, release := result.hold, result.holdStarted, result.holdRelease
			discard := holding && result.discardHeld
			result.holdMu.Unlock()
			if holding {
				select {
				case started <- struct{}{}:
				default:
				}
				<-release
				result.holdMu.Lock()
				discard = discard || result.discardHeld
				result.holdMu.Unlock()
			}
			if discard || request.Context().Err() != nil {
				result.mu.Lock()
				result.bodies[index].Discarded = true
				result.mu.Unlock()
				return
			}
			result.mu.Lock()
			status := result.status
			delay := result.responseDelay
			result.mu.Unlock()
			if delay > 0 {
				select {
				case <-time.After(delay):
				case <-request.Context().Done():
					return
				}
			}
			result.mu.Lock()
			if status >= 200 && status < 300 {
				result.rows = append(result.rows, rows...)
				result.responseRows += len(rows)
			}
			result.responses++
			result.bodies[index].Responded = status >= 200 && status < 300
			result.mu.Unlock()
			writer.WriteHeader(status)
		}),
	}
	go func() {
		err := result.server.Serve(listener)
		if err != nil && err != http.ErrServerClosed {
			panic(fmt.Sprintf("capture Serve failed: %v", err))
		}
		close(result.serveDone)
	}()
	return result
}

func (c *capture) url() string {
	return fmt.Sprintf("http://127.0.0.1:%d/telemetry", c.listener.Addr().(*net.TCPAddr).Port)
}

func (c *capture) setStatus(status int) {
	c.mu.Lock()
	c.status = status
	c.mu.Unlock()
}

func (c *capture) setResponseDelay(delay time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.responseDelay = delay
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
		panic("deadline: HTTP body did not enter hold")
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
	return captureSnapshot{
		Requests: c.requests, ReceivedRows: c.received, Responses: c.responses,
		ResponseRows: c.responseRows, Bodies: append([]captureBody(nil), c.bodies...),
		Rows:     append([]map[string]any(nil), c.rows...),
		Received: append([]map[string]any(nil), c.rawRows...),
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

func (c *capture) close() {
	c.closeOnce.Do(func() {
		c.releaseHold(true)
		_ = c.server.Close()
		select {
		case <-c.serveDone:
		case <-time.After(5 * time.Second):
			panic("capture server did not stop")
		}
	})
}

type wantedRow struct {
	key       string
	value     int
	field     string
	threshold *int
}

func rowData(row map[string]any) map[string]any {
	if value, ok := row["data"].(map[string]any); ok {
		return value
	}
	return row
}

func assertRows(c *capture, from int, expected []wantedRow) {
	wait("expected response rows", func() bool { return c.rowCount() >= from+len(expected) })
	snapshot := c.snapshot().Rows
	require(len(snapshot) == from+len(expected), fmt.Sprintf("output rows=%d want=%d", len(snapshot), from+len(expected)))
	for i, want := range expected {
		row := rowData(snapshot[from+i])
		require(fmt.Sprint(row["device_id"]) == want.key,
			fmt.Sprintf("row %d key=%v want=%s", i, row["device_id"], want.key))
		require(number(row[want.field]) == uint64(want.value),
			fmt.Sprintf("row %d %s=%v want=%d", i, want.field, row[want.field], want.value))
		if want.threshold == nil {
			continue
		}
		require(number(row["threshold"]) == uint64(*want.threshold),
			fmt.Sprintf("row %d threshold=%v want=%d", i, row["threshold"], *want.threshold))
	}
}

func rowSignature(row map[string]any) string {
	value := rowData(row)
	return fmt.Sprintf("%s|%s|%s|%s", fmt.Sprint(value["device_id"]), fmt.Sprint(value["v"]), fmt.Sprint(value["s"]), fmt.Sprint(value["threshold"]))
}

func assertRowSet(c *capture, from int, expected []wantedRow) {
	wait("expected response row set", func() bool { return c.rowCount() >= from+len(expected) })
	rows := c.snapshot().Rows
	require(len(rows) == from+len(expected), fmt.Sprintf("row set size=%d want=%d", len(rows), from+len(expected)))
	actual := make([]string, 0, len(expected))
	actualBySource := map[string][]string{}
	for _, row := range rows[from:] {
		actual = append(actual, rowSignature(row))
		key := fmt.Sprint(rowData(row)["device_id"])
		actualBySource[key] = append(actualBySource[key], rowSignature(row))
	}
	want := make([]string, 0, len(expected))
	wantBySource := map[string][]string{}
	for _, row := range expected {
		threshold := "<nil>"
		if row.threshold != nil {
			threshold = strconv.Itoa(*row.threshold)
		}
		if row.field == "s" {
			want = append(want, fmt.Sprintf("%s|<nil>|%d|%s", row.key, row.value, threshold))
		} else {
			want = append(want, fmt.Sprintf("%s|%d|<nil>|%s", row.key, row.value, threshold))
		}
		wantBySource[row.key] = append(wantBySource[row.key], want[len(want)-1])
	}
	// Union fixtures use distinct device keys for each File source. Global
	// interleaving may vary, but each source's subsequence must remain ordered.
	require(bytes.Equal(data(actualBySource), data(wantBySource)), "per-source Union order changed")
	sort.Strings(actual)
	sort.Strings(want)
	require(bytes.Equal(data(actual), data(want)), fmt.Sprintf("row set got=%v want=%v", actual, want))
}

func verifyOutputIDs(rows []map[string]any, generation string, first uint64) {
	for offset, row := range rows {
		id, ok := row["id"].(string)
		require(ok && len(id) == 48 && strings.ToLower(id) == id, "output ID is not lowercase 24-byte hex")
		decoded, err := hex.DecodeString(id)
		must(err)
		require(len(decoded) == 24 && (generation == "" || id[:32] == generation), "output ID epoch mismatch")
		require(binary.BigEndian.Uint64(decoded[16:]) == first+uint64(offset),
			fmt.Sprintf("output ordinal=%d want=%d", binary.BigEndian.Uint64(decoded[16:]), first+uint64(offset)))
	}
}

func rowsEqual(left, right []map[string]any) bool {
	return bytes.Equal(data(left), data(right))
}

func clone(value any) any {
	var result any
	must(json.Unmarshal(data(value), &result))
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

func publishTable(a api, threshold int, expected uint64) map[string]any {
	return a.ok(http.MethodPut, "/v1/tables/limits", map[string]any{
		"expected_revision": expected,
		"table":             referenceTable(threshold),
	})
}

func appendRow(path, key string, value int) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	_, err = file.Write(append(data(map[string]any{"device_id": key, "v": value}), '\n'))
	must(err)
	must(file.Sync())
	must(file.Close())
}

func appendRows(path string, rows []struct {
	key   string
	value int
}) {
	for _, row := range rows {
		appendRow(path, row.key, row.value)
	}
}

func checkpointPolicy(interval any, resume bool) map[string]any {
	return map[string]any{
		"interval_ms": interval, "timeout_ms": 5000,
		"retain_generations": 3, "max_store_bytes": 33554432,
		"resume_latest": resume,
	}
}

func sinkSpec(c *capture) map[string]any {
	return map[string]any{
		"kind": "http", "url": c.url(), "batch_rows": 2,
		"linger_ms": 1, "max_inflight": 1, "outbox_capacity": 8,
	}
}

func fileSource(path string) map[string]any {
	return map[string]any{"kind": "file", "path": path, "file_contract": "append_only", "inbox_capacity": 8}
}

func iotConfig(field string) map[string]any {
	return map[string]any{
		"keys": []string{"device_id"}, "fields": []string{field},
		"emit_first": true, "ttl_micros": 0, "max_keys": 64, "invalid": "ignore",
	}
}

func countNode(id, output uint32, keys []string) map[string]any {
	return map[string]any{
		"id": id, "kind": "window_agg", "keys": keys,
		"window": map[string]any{"kind": "count", "size": 2},
		"aggs":   []any{map[string]any{"fn": "sum", "expr": map[string]any{"k": "col", "name": "v"}, "alias": "s"}},
		"out":    []uint32{output},
	}
}

func lookupNode(id, output uint32) map[string]any {
	return map[string]any{
		"id": id, "kind": "lookup", "table": "limits",
		"on":   []any{map[string]any{"stream": "device_id", "table": "device_id"}},
		"keep": []string{"threshold"}, "out": []uint32{output},
	}
}

func fileLinearSpec(file, sink, checkpoint string, binding map[string]any, order string, interval any) map[string]any {
	nodes := []any{map[string]any{"id": uint32(1), "kind": "memory_source", "table": "sensors", "out": []uint32{2}}}
	if order == "lookup-count-iot" {
		nodes = append(nodes, lookupNode(2, 3), countNode(3, 4, []string{"device_id", "threshold"}),
			map[string]any{"id": uint32(4), "kind": "change_detect", "iot": iotConfig("s"), "out": []uint32{5}},
			map[string]any{"id": uint32(5), "kind": "capture_sink", "name": order})
	} else {
		nodes = append(nodes, countNode(2, 3, []string{"device_id"}), lookupNode(3, 4),
			map[string]any{"id": uint32(4), "kind": "capture_sink", "name": order})
	}
	return map[string]any{
		"version": 1, "stream": "sensors",
		"reference_tables": map[string]any{"limits": map[string]any{"revision": binding["revision"], "sha256": binding["sha256"]}},
		"source":           fileSource(file), "sink": map[string]any{"kind": "http", "url": sink, "batch_rows": 2, "linger_ms": 1, "max_inflight": 1, "outbox_capacity": 8},
		"delivery": "live_best_effort", "recovery": "aligned", "checkpoint_dir": checkpoint,
		"checkpoint": checkpointPolicy(interval, true),
		"graph":      map[string]any{"version": 1, "pipeline_id": 9001, "revision_id": 1, "nodes": nodes},
	}
}

func jsSource(brokerPort int, consumer string) map[string]any {
	return map[string]any{
		"kind": "jetstream", "inbox_capacity": 8,
		"jetstream": map[string]any{
			"servers":   []string{fmt.Sprintf("nats://127.0.0.1:%d", brokerPort)},
			"namespace": "reference_v10", "stream": "INPUT", "consumer": consumer,
			"ownership_bucket": "OWNERS", "max_pending": 32, "pending_bytes": 262144,
			"pull_messages": 8, "pull_bytes": 73728,
		},
	}
}

func jsLinearSpec(brokerPort int, sink, checkpoint, consumer string, binding map[string]any, order string) map[string]any {
	nodes := []any{map[string]any{"id": uint32(1), "kind": "memory_source", "table": "sensors", "out": []uint32{2}}}
	if order == "lookup-count" {
		nodes = append(nodes, lookupNode(2, 3), countNode(3, 4, []string{"device_id", "threshold"}),
			map[string]any{"id": uint32(4), "kind": "capture_sink", "name": order})
	} else {
		nodes = append(nodes, lookupNode(2, 3),
			map[string]any{"id": uint32(3), "kind": "change_detect", "iot": iotConfig("v"), "out": []uint32{4}},
			map[string]any{"id": uint32(4), "kind": "capture_sink", "name": order})
	}
	return map[string]any{
		"version": 1, "stream": "sensors",
		"reference_tables": map[string]any{"limits": map[string]any{"revision": binding["revision"], "sha256": binding["sha256"]}},
		"source":           jsSource(brokerPort, consumer),
		"sink":             map[string]any{"kind": "http", "url": sink, "batch_rows": 1, "linger_ms": 0, "max_inflight": 1, "outbox_capacity": 8},
		"delivery":         "checkpointed_at_least_once", "recovery": "aligned", "checkpoint_dir": checkpoint,
		"checkpoint": checkpointPolicy(float64(86400000), true),
		"graph":      map[string]any{"version": 1, "pipeline_id": 10001, "revision_id": 1, "nodes": nodes},
	}
}

func legacyFileSpec(file, sink, checkpoint string) map[string]any {
	return map[string]any{
		"version": 1, "stream": "sensors",
		"sql":      "SELECT SUM(v) AS s FROM sensors GROUP BY COUNT_WINDOW(3)",
		"source":   fileSource(file),
		"sink":     map[string]any{"kind": "http", "url": sink, "outbox_capacity": 8},
		"delivery": "live_best_effort", "recovery": "aligned", "checkpoint_dir": checkpoint,
		"checkpoint": checkpointPolicy(nil, true),
	}
}

func configure(a api, c *capture, brokerPort int) {
	a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": c.listener.Addr().(*net.TCPAddr).Port})
	if brokerPort != 0 {
		a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": brokerPort})
	}
	a.ok(http.MethodPut, "/v1/streams/sensors", map[string]any{"fields": sourceFields()})
}

// nats is intentionally a tiny bounded Core NATS client.  It is only used to
// create the isolated JetStream streams, publish fixture rows, and inspect
// the consumer ACK cut.  The server under test still uses its production
// JetStream client and therefore this is not a second source implementation.
type nats struct {
	conn   net.Conn
	in     *bufio.Reader
	serial uint64
}

func dialNATS(port int) *nats {
	var conn net.Conn
	wait("isolated NATS ready", func() bool {
		var err error
		conn, err = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		return err == nil
	})
	n := &nats{conn: conn, in: bufio.NewReader(conn)}
	_, err := fmt.Fprint(conn, "CONNECT {\"verbose\":false,\"pedantic\":true,\"lang\":\"go-reference-fixture\",\"version\":\"1\"}\r\n")
	must(err)
	return n
}

func (n *nats) close() {
	if n != nil && n.conn != nil {
		_ = n.conn.Close()
	}
}

func (n *nats) request(subject string, payload []byte) map[string]any {
	n.serial++
	inbox := fmt.Sprintf("_INBOX.referencefixture.%d.%d", os.Getpid(), n.serial)
	must(n.conn.SetDeadline(time.Now().Add(5 * time.Second)))
	_, err := fmt.Fprintf(n.conn, "SUB %s 1\r\nUNSUB 1 1\r\nPUB %s %s %d\r\n", inbox, subject, inbox, len(payload))
	must(err)
	_, err = n.conn.Write(append(payload, '\r', '\n'))
	must(err)
	for {
		line, readErr := n.in.ReadString('\n')
		must(readErr)
		require(len(line) <= 65536, "NATS fixture response header bound")
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "INFO":
			continue
		case "PING":
			_, writeErr := fmt.Fprint(n.conn, "PONG\r\n")
			must(writeErr)
		case "-ERR":
			panic("isolated NATS error: " + line)
		case "MSG":
			require(len(fields) >= 4, "NATS MSG framing")
			size, parseErr := strconv.Atoi(fields[len(fields)-1])
			must(parseErr)
			require(size >= 0 && size <= maxHTTPBody, "NATS response body bound")
			body := make([]byte, size+2)
			_, readErr = io.ReadFull(n.in, body)
			must(readErr)
			require(fields[1] == inbox, "NATS fixture reply mismatch")
			var value map[string]any
			must(json.Unmarshal(body[:size], &value))
			if problem, present := value["error"]; present && problem != nil {
				panic(fmt.Sprintf("NATS API error: %v", problem))
			}
			_ = n.conn.SetDeadline(time.Time{})
			return value
		}
	}
}

func startBroker(root, binary string) (*child, *nats, int) {
	port := freePort()
	config := fmt.Sprintf("host: 127.0.0.1\nport: %d\nmax_payload: 65536\njetstream {\nstore_dir: %q\nmax_file_store: 256MB\nmax_memory_store: 16MB\nsync_interval: always\n}\n", port, filepath.Join(root, "broker-data"))
	must(os.WriteFile(filepath.Join(root, "nats.conf"), []byte(config), 0600))
	broker := launch(binary, root, filepath.Join(root, "nats.log"), "-c", filepath.Join(root, "nats.conf"))
	producer := dialNATS(port)
	producer.request("$JS.API.STREAM.CREATE.INPUT", data(map[string]any{
		"name": "INPUT", "subjects": []string{"input.rows"}, "storage": "file", "retention": "limits",
		"max_bytes": 16 * 1024 * 1024, "max_msg_size": 65536, "max_consumers": 32,
		"num_replicas": 1, "deny_delete": true, "deny_purge": true,
	}))
	producer.request("$JS.API.STREAM.CREATE.KV_OWNERS", data(map[string]any{
		"name": "KV_OWNERS", "subjects": []string{"$KV.OWNERS.>"}, "storage": "file", "retention": "limits",
		"max_bytes": 1024 * 1024, "max_msg_size": 1024, "max_msgs_per_subject": 1,
		"num_replicas": 1, "discard": "new", "allow_direct": true, "allow_rollup_hdrs": true,
	}))
	return broker, producer, port
}

func publishRows(producer *nats, values []int, total int) {
	for _, value := range values {
		body := data(map[string]any{"device_id": "a", "v": value})
		_, err := fmt.Fprintf(producer.conn, "PUB input.rows %d\r\n%s\r\n", len(body), body)
		must(err)
	}
	wait("JetStream persistence fence", func() bool {
		info := producer.request("$JS.API.STREAM.INFO.INPUT", []byte("{}"))
		return number(nested(info, "state", "messages")) >= uint64(total)
	})
}

func publishRowsWithKeys(producer *nats, rows []struct {
	key   string
	value int
}, total int) {
	for _, row := range rows {
		body := data(map[string]any{"device_id": row.key, "v": row.value})
		_, err := fmt.Fprintf(producer.conn, "PUB input.rows %d\r\n%s\r\n", len(body), body)
		must(err)
	}
	wait("JetStream persistence fence", func() bool {
		info := producer.request("$JS.API.STREAM.INFO.INPUT", []byte("{}"))
		return number(nested(info, "state", "messages")) >= uint64(total)
	})
}

func readerName(producer *nats) string {
	var result map[string]any
	wait("single active JetStream reader", func() bool {
		result = producer.request("$JS.API.CONSUMER.NAMES.INPUT", []byte("{}"))
		consumers, ok := result["consumers"].([]any)
		return ok && len(consumers) == 1
	})
	consumers := result["consumers"].([]any)
	name, ok := consumers[0].(string)
	require(ok && name != "", "JetStream reader name is not a string")
	return name
}

func consumerInfo(producer *nats) map[string]any {
	return producer.request("$JS.API.CONSUMER.INFO.INPUT."+readerName(producer), []byte("{}"))
}

func nestedMap(value map[string]any, keys ...string) map[string]any {
	result, _ := nested(value, keys...).(map[string]any)
	return result
}

func waitRunning(a api, name string) map[string]any {
	return waitActual(a, name, "running")
}

func waitPublished(a api, name string, cut uint64) map[string]any {
	var result map[string]any
	wait("published source cut", func() bool {
		result = statusOf(a, name)
		_, published, _, _ := statusReliable(result)
		return published >= cut
	})
	return result
}

func startPipeline(a api, name string, spec map[string]any) {
	a.ok(http.MethodPost, "/v1/validate", spec)
	a.ok(http.MethodPut, "/v1/pipelines/"+name, spec)
	result, code := a.call(http.MethodPost, "/v1/pipelines/"+name+"/start", map[string]any{})
	require(code >= 200 && code < 300,
		fmt.Sprintf("start %s returned %d: %v", name, code, result))
	waitRunning(a, name)
}

func stopPipeline(a api, name string) {
	result, code := a.call(http.MethodPost, "/v1/pipelines/"+name+"/stop", map[string]any{})
	require(code >= 200 && code < 300,
		fmt.Sprintf("stop %s returned %d: %v", name, code, result))
	waitActual(a, name, "stopped")
}

func spawnServer(binary, root, logPath string, port int) *child {
	return launch(binary, root, logPath,
		"--bind", fmt.Sprintf("127.0.0.1:%d", port),
		"--catalog", filepath.Join(root, "catalog.db"),
		"--max-jobs", "1", "--safe-mode")
}

func newAPI(port int) api {
	return api{base: fmt.Sprintf("http://127.0.0.1:%d", port), client: &http.Client{Timeout: waitDuration}}
}

func allowCaptures(a api, captures ...*capture) {
	seen := map[int]bool{}
	for _, capture := range captures {
		port := capture.listener.Addr().(*net.TCPAddr).Port
		if seen[port] {
			continue
		}
		seen[port] = true
		a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": port})
	}
}

func waitPeriodicFile(a api, name, checkpoint string) (map[string]any, map[string]any) {
	status := waitPeriodicStatus(a, name)
	return status, inventory(a, name)
}

func waitPeriodicStatus(a api, name string) map[string]any {
	var status map[string]any
	wait("periodic reference checkpoint", func() bool {
		status = statusOf(a, name)
		_, succeeded, active, _, generation := checkpointFields(status)
		return succeeded > 0 && !active && generation != "" &&
			storageCurrent(nestedMap(status, "checkpoint")) != 0
	})
	return status
}

func waitActiveCheckpoint(a api, name, checkpoint, currentHash string, previousSucceeded uint64) map[string]any {
	var status map[string]any
	wait("checkpoint remains active until sink result", func() bool {
		status = statusOf(a, name)
		_, succeeded, active, _, _ := checkpointFields(status)
		require(succeeded == previousSucceeded,
			"checkpoint succeeded while required HTTP result was unknown")
		require(hash(filepath.Join(checkpoint, "CURRENT")) == currentHash,
			"checkpoint advanced CURRENT while required HTTP result was unknown")
		return active
	})
	return status
}

func waitCheckpointFailure(a api, name string) map[string]any {
	var status map[string]any
	wait("checkpoint I/O failure", func() bool {
		status = statusOf(a, name)
		text := strings.ToLower(fmt.Sprint(nested(status, "checkpoint", "last_error_code")))
		return nested(status, "checkpoint", "active") != true &&
			strings.Contains(text, "internal")
	})
	return status
}

func waitActualError(a api, name, needle string) map[string]any {
	var status map[string]any
	wait("actual error="+needle, func() bool {
		status = statusOf(a, name)
		return nested(status, "actual", "status") == "failed" &&
			strings.Contains(strings.ToLower(fmt.Sprint(nested(status, "actual", "last_error"))), strings.ToLower(needle))
	})
	return status
}

func expectedValueRows(values []int, threshold *int, field string, key string) []wantedRow {
	rows := make([]wantedRow, 0, len(values))
	for _, value := range values {
		rows = append(rows, wantedRow{key: key, value: value, field: field, threshold: threshold})
	}
	return rows
}

func saveScenarioSummary(root, profile string, version uint16, extra map[string]any) {
	result := map[string]any{
		"valid": true, "profile": profile, "snapshot_version": version,
		"manifest": "CPL3", "ttl_micros": 0, "certified": false,
	}
	for key, value := range extra {
		result[key] = value
	}
	save(filepath.Join(root, "summary.json"), result)
}

func replaceBinding(spec map[string]any, binding map[string]any) map[string]any {
	result := clone(spec).(map[string]any)
	refs, ok := result["reference_tables"].(map[string]any)
	require(ok, "reference binding missing from spec")
	refs["limits"] = map[string]any{"revision": binding["revision"], "sha256": binding["sha256"]}
	return result
}

func periodicBaseline(a api, name, checkpoint string, version uint16, states uint16, binding map[string]any, root string, stop func()) (uint64, string, string, map[string]any) {
	status := waitPeriodicStatus(a, name)
	// Stop/join before reading inventory so a second scheduler tick cannot
	// race the supposedly immutable baseline evidence.
	stop()
	save(filepath.Join(root, "periodic-stopped-status.json"), statusOf(a, name))
	inv := inventory(a, name)
	requireCurrentSnapshotVersion(inv, uint64(version))
	info := parseSnapshotInfo(checkpoint, inv, binding, version, states)
	current := hash(filepath.Join(checkpoint, "CURRENT"))
	tree := committedTreeHash(checkpoint)
	_, _, _, observedID, generation := checkpointFields(status)
	id := storageCurrent(inv)
	require(observedID != 0 && id >= observedID && generation == checkpointGeneration(inv), "periodic reference checkpoint identity is incomplete")
	save(filepath.Join(root, "periodic-status.json"), status)
	save(filepath.Join(root, "periodic-checkpoints.json"), inv)
	save(filepath.Join(root, "periodic-snapshot-info.json"), info)
	return id, current, tree, infoToMap(info)
}

func infoToMap(info snapshotInfo) map[string]any {
	return map[string]any{
		"version": info.Version, "source_kind": info.SourceKind,
		"source_record_index": info.SourceRecord, "manifest": info.Manifest,
		"state_count": info.StateCount, "reference_name": info.ReferenceName,
		"reference_revision": info.ReferenceRev, "reference_sha256": info.ReferenceSHA,
		"reference_runtime_crc32": info.ReferenceCRC32,
	}
}

// runFileLinear covers both legal v9 linear shapes.  It deliberately keeps
// the File source's business output ID out of the oracle: only values and the
// attached immutable r1 lookup row are compared.
func runFileLinear(root, serverBin, order string) bool {
	must(os.Mkdir(root, 0700))
	capture := newCapture()
	defer capture.close()
	file := filepath.Join(root, "sensors.ndjson")
	checkpoint := filepath.Join(root, "checkpoint-v9")
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
		waitHealth(a, "reference File server health")
	}

	start()
	configure(a, capture, 0)
	r1 := publishTable(a, 10, 0)
	r2 := publishTable(a, 90, 1)
	r3 := publishTable(a, 30, 2)
	save(filepath.Join(root, "table-r1.json"), r1)
	save(filepath.Join(root, "table-r2.json"), r2)
	save(filepath.Join(root, "table-r3.json"), r3)
	appendRows(file, []struct {
		key   string
		value int
	}{{"a", 1}, {"a", 2}, {"b", 3}, {"b", 4}})
	spec := fileLinearSpec(file, capture.url(), checkpoint, r1, order, float64(1000))
	save(filepath.Join(root, "spec-r1.json"), spec)
	startPipeline(a, "reference", spec)
	threshold10, threshold20 := 10, 20
	if order == "lookup-count-iot" {
		assertRows(capture, 0, []wantedRow{{"a", 3, "s", &threshold10}, {"b", 7, "s", &threshold20}})
	} else {
		assertRows(capture, 0, []wantedRow{{"a", 3, "s", &threshold10}, {"b", 7, "s", &threshold20}})
	}
	initialRows := capture.snapshot().Rows
	initialResponses := capture.snapshot().ResponseRows
	require(initialResponses == len(initialRows), "File HTTP response oracle counted requests instead of rows")
	stateCount := uint16(2)
	if order != "lookup-count-iot" {
		stateCount = 1
	}
	baselineID, baselineCurrent, baselineTree, baselineInfo := periodicBaseline(a, "reference", checkpoint, 9, stateCount, r1, root, func() { stopPipeline(a, "reference") })
	// The latest table moves to r3 while the stopped pipeline remains bound to
	// r1.  The following restart only changes the scheduler interval.
	manual := clone(spec).(map[string]any)
	manual["checkpoint"].(map[string]any)["interval_ms"] = nil
	save(filepath.Join(root, "spec-r1-manual.json"), manual)
	startPipeline(a, "reference", manual)
	require(hash(filepath.Join(checkpoint, "CURRENT")) == baselineCurrent, "scheduler-only restart changed r1 CURRENT")
	require(committedTreeHash(checkpoint) == baselineTree, "scheduler-only restart changed File committed history")
	preTail := capture.rowCount()
	appendRows(file, []struct {
		key   string
		value int
	}{{"a", 5}, {"a", 6}})
	assertRows(capture, preTail, []wantedRow{{"a", 11, "s", &threshold10}})

	// A held required sink response must keep the checkpoint active and the
	// File source suffix uncommitted.  SIGKILL is the deterministic cut.
	preHold := capture.rowCount()
	preReceived := capture.receivedRows()
	capture.setHold()
	appendRows(file, []struct {
		key   string
		value int
	}{{"b", 8}, {"b", 9}})
	wait("File held output body received", func() bool { return capture.receivedRows() >= preReceived+1 })
	capture.waitHeld()
	beforeHold := statusOf(a, "reference")
	_, holdSucceeded, _, holdLast, _ := checkpointFields(beforeHold)
	holdCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	holdResult := make(chan callResult, 1)
	go func() {
		result, code := a.call(http.MethodPost, "/v1/pipelines/reference/checkpoint", nil)
		holdResult <- callResult{result: result, code: code}
	}()
	var blocked map[string]any
	wait("File HTTP-held checkpoint", func() bool {
		blocked = statusOf(a, "reference")
		_, succeeded, active, last, _ := checkpointFields(blocked)
		require(succeeded == holdSucceeded && last == holdLast, "held File suffix crossed checkpoint success")
		require(hash(filepath.Join(checkpoint, "CURRENT")) == holdCurrent, "held File suffix changed CURRENT")
		return active
	})
	save(filepath.Join(root, "http-held-status.json"), blocked)
	stop(syscall.SIGKILL)
	capture.releaseHold(true)
	select {
	case result := <-holdResult:
		require(result.code == 0 || result.code >= 400, "held File checkpoint unexpectedly succeeded before SIGKILL")
		save(filepath.Join(root, "http-held-checkpoint.json"), result)
	case <-time.After(4 * time.Second):
		panic("held File checkpoint request did not terminate after SIGKILL")
	}
	require(capture.rowCount() == preHold, "held File body was counted as a successful output")
	replayStart := capture.rowCount()
	start()
	a.ok(http.MethodPost, "/v1/pipelines/reference/start", map[string]any{})
	waitRunning(a, "reference")
	wait("File suffix replay", func() bool { return capture.rowCount() >= replayStart+2 })
	replayed := capture.snapshot().Rows[replayStart:]
	assertRows(capture, replayStart, []wantedRow{{"a", 11, "s", &threshold10}, {"b", 17, "s", &threshold20}})
	require(len(replayed) == 2, "File suffix replay emitted an unexpected number of rows")
	restored := statusOf(a, "reference")
	require(number(nested(restored, "checkpoint", "restored_from_checkpoint")) == baselineID,
		"File suffix restore did not use the r1 baseline")
	_, replayPreviousSucceeded, _, _, _ := checkpointFields(restored)
	_, replayCode := manualCheckpoint(a, "reference")
	require(replayCode >= 200 && replayCode < 300, "File replay checkpoint failed")
	replayStatus := waitCheckpointSuccess(a, "reference", replayPreviousSucceeded)
	save(filepath.Join(root, "replay-status.json"), replayStatus)
	replayInventory := inventory(a, "reference")
	requireCurrentSnapshotVersion(replayInventory, 9)
	replayInfo := parseSnapshotInfo(checkpoint, replayInventory, r1, 9, stateCount)
	require(replayInfo.ReferenceCRC32 == parseCRC(baselineInfo), "r1 runtime CRC changed across File checkpoints")

	// Inject CURRENT.tmp while another required body is held.  The failure
	// must report checkpoint I/O, leave CURRENT and the committed tree intact,
	// and allow the old suffix to replay after the obstruction is removed.
	failureBaseline := storageCurrent(replayInventory)
	failureCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	failureTree := committedTreeHash(checkpoint)
	preFailureReceived := capture.receivedRows()
	capture.setHold()
	appendRows(file, []struct {
		key   string
		value int
	}{{"a", 7}, {"a", 8}})
	wait("File CURRENT.tmp body received", func() bool { return capture.receivedRows() >= preFailureReceived+1 })
	capture.waitHeld()
	must(os.Mkdir(filepath.Join(checkpoint, "CURRENT.tmp"), 0700))
	_, failureBeforeSucceeded, _, _, _ := checkpointFields(statusOf(a, "reference"))
	failureCall := make(chan callResult, 1)
	go func() {
		result, code := a.call(http.MethodPost, "/v1/pipelines/reference/checkpoint", nil)
		failureCall <- callResult{result: result, code: code}
	}()
	waitActiveCheckpoint(a, "reference", checkpoint, failureCurrent, failureBeforeSucceeded)
	capture.releaseHold(false)
	var failureResult callResult
	select {
	case failureResult = <-failureCall:
	case <-time.After(4 * time.Second):
		panic("CURRENT.tmp File checkpoint did not return after releasing HTTP hold")
	}
	failureCode := failureResult.code
	require(failureCode >= 400, fmt.Sprintf("CURRENT.tmp File checkpoint returned %d", failureCode))
	failureText := strings.ToLower(fmt.Sprint(nested(failureResult.result, "error", "message")))
	require(strings.Contains(failureText, "checkpoint io:") && strings.Contains(failureText, "is a directory"),
		fmt.Sprintf("CURRENT.tmp File error was not checkpoint I/O: %v", failureResult.result))
	var failureStatus map[string]any
	wait("File CURRENT.tmp checkpoint failure status", func() bool {
		failureStatus = statusOf(a, "reference")
		active := nested(failureStatus, "checkpoint", "active") == true
		lastError := strings.ToLower(fmt.Sprint(nested(failureStatus, "checkpoint", "last_error_code")))
		return !active && strings.Contains(lastError, "internal")
	})
	require(hash(filepath.Join(checkpoint, "CURRENT")) == failureCurrent, "CURRENT.tmp changed File CURRENT")
	require(committedTreeHash(checkpoint) == failureTree, "CURRENT.tmp changed committed File history")
	save(filepath.Join(root, "current-tmp-response.json"), failureResult)
	save(filepath.Join(root, "current-tmp-status.json"), failureStatus)
	must(os.Remove(filepath.Join(checkpoint, "CURRENT.tmp")))
	stop(syscall.SIGKILL)
	failureReplayStart := capture.rowCount()
	start()
	a.ok(http.MethodPost, "/v1/pipelines/reference/start", map[string]any{})
	waitRunning(a, "reference")
	assertRows(capture, failureReplayStart, []wantedRow{{"a", 15, "s", &threshold10}})
	require(number(nested(statusOf(a, "reference"), "checkpoint", "restored_from_checkpoint")) == failureBaseline,
		"CURRENT.tmp replay did not restore the previous File point")
	// A valid r2 binding is semantically incompatible with the r1 snapshot;
	// it must fail before either CURRENT or the already accepted output moves.
	semanticCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	semanticRows := capture.rowCount()
	stopPipeline(a, "reference")
	changed := replaceBinding(manual, r2)
	save(filepath.Join(root, "changed-binding-spec.json"), changed)
	a.ok(http.MethodPut, "/v1/pipelines/reference", changed)
	startResult, startCode := a.call(http.MethodPost, "/v1/pipelines/reference/start", map[string]any{})
	require(startCode >= 200 && startCode < 300, fmt.Sprintf("changed File binding start returned %d", startCode))
	save(filepath.Join(root, "changed-binding-start.json"), map[string]any{"code": startCode, "response": startResult})
	changedStatus := waitActualError(a, "reference", "checkpoint participant set, schema, codec or pipeline semantics changed")
	require(hash(filepath.Join(checkpoint, "CURRENT")) == semanticCurrent && capture.rowCount() == semanticRows,
		"changed File binding touched CURRENT or output")
	save(filepath.Join(root, "changed-binding-status.json"), changedStatus)

	deps := a.ok(http.MethodGet, "/v1/tables/limits/dependencies", nil)
	save(filepath.Join(root, "dependencies.json"), deps)
	a.ok(http.MethodPost, "/v1/tables/limits/gc", map[string]any{})
	r1AfterGC := a.ok(http.MethodGet, "/v1/tables/limits/revisions/1", nil)
	require(r1AfterGC["sha256"] == r1["sha256"], "GC deleted a checkpoint-required r1 table")
	save(filepath.Join(root, "r1-after-gc.json"), r1AfterGC)
	stop(syscall.SIGTERM)
	saveScenarioSummary(root, "static_lookup_state_file_v9", 9, map[string]any{
		"order": order, "fixed_binding": true, "suffix_recovery": true,
		"http_unknown_no_early_current": true, "current_failure_preserved": true,
		"changed_binding_refused": true, "gc_preserved_dependency": true,
		"reference_crc_stable": true, "tested_cases": []string{"r1-fixed", "r2-r3-latest-drift", "http-held-kill9", "CURRENT.tmp", "semantic-binding-refusal"},
	})
	fmt.Println("REFERENCE_FILE_LINEAR_OK", order)
	return true
}

func parseCRC(value map[string]any) uint32 {
	return uint32(number(value["reference_runtime_crc32"]))
}

func runJSLinear(root, serverBin, natsBin, order string) bool {
	must(os.Mkdir(root, 0700))
	capture := newCapture()
	defer capture.close()
	broker, producer, brokerPort := startBroker(root, natsBin)
	defer producer.close()
	defer broker.stop(syscall.SIGKILL)
	serverPort := freePort()
	a := newAPI(serverPort)
	var process *child
	stop := func(signal os.Signal) {
		if process != nil {
			process.stop(signal)
			process = nil
		}
	}
	defer func() { stop(syscall.SIGKILL) }()
	start := func() {
		process = spawnServer(serverBin, root, filepath.Join(root, "server.log"), serverPort)
		waitHealth(a, "reference JetStream server health")
	}

	start()
	configure(a, capture, brokerPort)
	r1 := publishTable(a, 10, 0)
	r2 := publishTable(a, 90, 1)
	r3 := publishTable(a, 30, 2)
	save(filepath.Join(root, "table-r1.json"), r1)
	save(filepath.Join(root, "table-r2.json"), r2)
	save(filepath.Join(root, "table-r3.json"), r3)
	consumer := "reference_" + strings.ReplaceAll(order, "-", "_")
	spec := jsLinearSpec(brokerPort, capture.url(), filepath.Join(root, "checkpoint-v10"), consumer, r1, order)
	save(filepath.Join(root, "spec-r1.json"), spec)
	startPipeline(a, "reference", spec)
	var initial []int
	var initialExpected []wantedRow
	if order == "lookup-count" {
		initial = []int{1, 2, 3, 4}
		initialExpected = []wantedRow{{"a", 3, "s", ptrInt(10)}, {"a", 7, "s", ptrInt(10)}}
	} else {
		initial = []int{10, 10, 11, 12}
		initialExpected = []wantedRow{{"a", 10, "v", ptrInt(10)}, {"a", 11, "v", ptrInt(10)}, {"a", 12, "v", ptrInt(10)}}
	}
	publishRows(producer, initial, len(initial))
	assertRows(capture, 0, initialExpected)
	waitPublished(a, "reference", uint64(len(initial)))
	beforeBaseline := statusOf(a, "reference")
	_, beforeSucceeded, _, _, _ := checkpointFields(beforeBaseline)
	_, baselineCode := manualCheckpoint(a, "reference")
	require(baselineCode >= 200 && baselineCode < 300, "JetStream baseline checkpoint request failed")
	baselineStatus := waitCheckpointSuccess(a, "reference", beforeSucceeded)
	baselineStatus = waitReliableCommitted(a, "reference", uint64(len(initial)))
	checkpoint := filepath.Join(root, "checkpoint-v10")
	baselineInv := inventory(a, "reference")
	requireCurrentSnapshotVersion(baselineInv, 10)
	baselineInfo := parseSnapshotInfo(checkpoint, baselineInv, r1, 10, 1)
	_, _, _, _, baselineGeneration := checkpointFields(baselineStatus)
	initialRows := capture.snapshot().Rows
	verifyOutputIDs(initialRows, baselineGeneration, 1)
	save(filepath.Join(root, "baseline-status.json"), baselineStatus)
	save(filepath.Join(root, "baseline-checkpoints.json"), baselineInv)
	save(filepath.Join(root, "baseline-snapshot-info.json"), baselineInfo)
	baselineCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	baselineBroker := consumerInfo(producer)
	require(number(nested(baselineBroker, "ack_floor", "stream_seq")) == uint64(len(initial)) && number(baselineBroker["num_ack_pending"]) == 0,
		"JetStream baseline did not durably ACK the complete input cut")

	// Latest r2/r3 publication cannot alter the immutable r1 binding.
	latest := a.ok(http.MethodGet, "/v1/tables/limits", nil)
	require(number(latest["revision"]) == number(r3["revision"]) && latest["sha256"] == r3["sha256"], "latest table did not reach r3")
	manual := clone(spec).(map[string]any)
	manual["checkpoint"].(map[string]any)["interval_ms"] = float64(86400000)
	stopPipeline(a, "reference")
	startPipeline(a, "reference", manual)
	require(hash(filepath.Join(checkpoint, "CURRENT")) == baselineCurrent, "JetStream scheduler-only restart changed CURRENT")

	var tail []int
	var tailExpected []wantedRow
	if order == "lookup-count" {
		tail = []int{5, 6}
		tailExpected = []wantedRow{{"a", 11, "s", ptrInt(10)}}
	} else {
		tail = []int{13}
		tailExpected = []wantedRow{{"a", 13, "v", ptrInt(10)}}
	}
	tailStart := capture.rowCount()
	publishRows(producer, tail, len(initial)+len(tail))
	assertRows(capture, tailStart, tailExpected)
	waitPublished(a, "reference", uint64(len(initial)+len(tail)))
	_, tailSucceeded, _, _, _ := checkpointFields(statusOf(a, "reference"))
	_, tailCheckpointCode := manualCheckpoint(a, "reference")
	require(tailCheckpointCode >= 200 && tailCheckpointCode < 300, "JetStream tail checkpoint request failed")
	tailStatus := waitCheckpointSuccess(a, "reference", tailSucceeded)
	tailStatus = waitReliableCommitted(a, "reference", uint64(len(initial)+len(tail)))
	tailInv := inventory(a, "reference")
	requireCurrentSnapshotVersion(tailInv, 10)
	tailInfo := parseSnapshotInfo(checkpoint, tailInv, r1, 10, 1)
	require(tailInfo.ReferenceCRC32 == baselineInfo.ReferenceCRC32, "JetStream r1 CRC identity changed")
	tailID := storageCurrent(tailInv)
	tailCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	tailTree := committedTreeHash(checkpoint)
	_, _, _, _, outputGeneration := checkpointFields(tailStatus)
	require(outputGeneration == baselineGeneration, "JetStream output epoch changed on compatible restore")
	brokerTail := consumerInfo(producer)
	require(number(nested(brokerTail, "ack_floor", "stream_seq")) == uint64(len(initial)+len(tail)) && number(brokerTail["num_ack_pending"]) == 0,
		"JetStream tail checkpoint did not ACK its source cut")
	save(filepath.Join(root, "tail-status.json"), tailStatus)
	verifyOutputIDs(capture.snapshot().Rows, baselineGeneration, 1)
	save(filepath.Join(root, "tail-checkpoints.json"), tailInv)
	save(filepath.Join(root, "tail-broker.json"), brokerTail)
	save(filepath.Join(root, "tail-current-evidence.json"), map[string]any{"checkpoint_id": tailID, "current_sha256": tailCurrent, "tree_sha256": tailTree})

	// A held body is an actual unknown sink result.  The checkpoint must stay
	// active, while the reliable source keeps its ACK floor at the last cut.
	var suffix []int
	var suffixExpected []wantedRow
	if order == "lookup-count" {
		suffix = []int{7, 8}
		suffixExpected = []wantedRow{{"a", 15, "s", ptrInt(10)}}
	} else {
		suffix = []int{14}
		suffixExpected = []wantedRow{{"a", 14, "v", ptrInt(10)}}
	}
	suffixStart := capture.rowCount()
	suffixReceived := capture.receivedRows()
	capture.setHold()
	publishRows(producer, suffix, len(initial)+len(tail)+len(suffix))
	wait("JetStream held body received", func() bool { return capture.receivedRows() >= suffixReceived+1 })
	capture.waitHeld()
	beforeHold := statusOf(a, "reference")
	_, holdSucceeded, _, holdLast, _ := checkpointFields(beforeHold)
	holdCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	holdCall := make(chan callResult, 1)
	go func() {
		result, code := a.call(http.MethodPost, "/v1/pipelines/reference/checkpoint", nil)
		holdCall <- callResult{result: result, code: code}
	}()
	blocked := waitActiveCheckpoint(a, "reference", checkpoint, holdCurrent, holdSucceeded)
	save(filepath.Join(root, "http-held-status.json"), blocked)
	_, blockedPublished, blockedCommitted, blockedPending := statusReliable(blocked)
	_, _, _, blockedLast, _ := checkpointFields(blocked)
	require(blockedLast == holdLast, "held JetStream suffix advanced checkpoint identity")
	// These status fields explicitly sample every 5 s. Waiting for a fresh
	// sample under a 5 s sink/checkpoint hold would test a timeout race, not
	// ACK correctness. The actual received body plus broker state below are
	// the live published/pending oracle; sampled counters must not get ahead.
	require(blockedPublished >= uint64(len(initial)+len(tail)) && blockedPublished <= uint64(len(initial)+len(tail)+len(suffix)) && blockedCommitted == uint64(len(initial)+len(tail)) && blockedPending <= uint64(len(suffix)),
		fmt.Sprintf("held JetStream sampled cuts invalid: published=%d committed=%d pending=%d", blockedPublished, blockedCommitted, blockedPending))
	holdBroker := consumerInfo(producer)
	require(number(nested(holdBroker, "ack_floor", "stream_seq")) == uint64(len(initial)+len(tail)) && number(holdBroker["num_ack_pending"]) == uint64(len(suffix)),
		"held JetStream suffix crossed broker ACK floor")
	save(filepath.Join(root, "http-held-status.json"), blocked)
	save(filepath.Join(root, "http-held-broker.json"), holdBroker)
	stop(syscall.SIGKILL)
	capture.releaseHold(true)
	select {
	case result := <-holdCall:
		require(result.code == 0 || result.code >= 400, "held checkpoint unexpectedly succeeded before SIGKILL")
		save(filepath.Join(root, "http-held-checkpoint.json"), result)
	case <-time.After(4 * time.Second):
		panic("JetStream held checkpoint did not terminate after SIGKILL")
	}
	require(capture.rowCount() == suffixStart, "held JetStream output was counted as a successful response")
	replayStart := capture.rowCount()
	start()
	a.ok(http.MethodPost, "/v1/pipelines/reference/start", map[string]any{})
	waitRunning(a, "reference")
	assertRows(capture, replayStart, suffixExpected)
	replayRows := capture.snapshot().Rows[replayStart:]
	verifyOutputIDs(replayRows, baselineGeneration, uint64(suffixStart+1))
	restoredStatus := statusOf(a, "reference")
	require(number(nested(restoredStatus, "checkpoint", "restored_from_checkpoint")) == tailID,
		"JetStream held suffix restored from the wrong checkpoint")
	waitPublished(a, "reference", uint64(len(initial)+len(tail)+len(suffix)))
	_, replaySucceeded, _, _, _ := checkpointFields(restoredStatus)
	_, replayCode := manualCheckpoint(a, "reference")
	require(replayCode >= 200 && replayCode < 300, "JetStream replay checkpoint request failed")
	replayStatus := waitCheckpointSuccess(a, "reference", replaySucceeded)
	replayStatus = waitReliableCommitted(a, "reference", uint64(len(initial)+len(tail)+len(suffix)))
	save(filepath.Join(root, "replay-status.json"), replayStatus)
	replayInv := inventory(a, "reference")
	requireCurrentSnapshotVersion(replayInv, 10)
	replayInfo := parseSnapshotInfo(checkpoint, replayInv, r1, 10, 1)
	require(replayInfo.ReferenceCRC32 == baselineInfo.ReferenceCRC32, "JetStream replay changed r1 CRC identity")
	replayBroker := consumerInfo(producer)
	require(number(nested(replayBroker, "ack_floor", "stream_seq")) == uint64(len(initial)+len(tail)+len(suffix)) && number(replayBroker["num_ack_pending"]) == 0,
		"JetStream replay did not ACK the complete source cut")

	// The second failure accepts a body before publication but leaves the
	// source cut uncommitted.  After restart the same output ID is expected to
	// be delivered again; this is at-least-once, not an exactly-once claim.
	var fault []int
	var faultExpected []wantedRow
	if order == "lookup-count" {
		fault = []int{9, 10}
		faultExpected = []wantedRow{{"a", 19, "s", ptrInt(10)}}
	} else {
		fault = []int{15}
		faultExpected = []wantedRow{{"a", 15, "v", ptrInt(10)}}
	}
	faultBaseline := storageCurrent(replayInv)
	faultCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	faultSnapshot := snapshotHash(checkpoint, faultBaseline)
	faultStart := capture.rowCount()
	faultReceived := capture.receivedRows()
	capture.setHold()
	publishRows(producer, fault, len(initial)+len(tail)+len(suffix)+len(fault))
	wait("JetStream CURRENT.tmp body received", func() bool { return capture.receivedRows() >= faultReceived+1 })
	capture.waitHeld()
	must(os.Mkdir(filepath.Join(checkpoint, "CURRENT.tmp"), 0700))
	_, faultSucceeded, _, _, _ := checkpointFields(statusOf(a, "reference"))
	faultCall := make(chan callResult, 1)
	go func() {
		result, code := a.call(http.MethodPost, "/v1/pipelines/reference/checkpoint", nil)
		faultCall <- callResult{result: result, code: code}
	}()
	waitActiveCheckpoint(a, "reference", checkpoint, faultCurrent, faultSucceeded)
	faultBlockedBroker := consumerInfo(producer)
	require(number(nested(faultBlockedBroker, "ack_floor", "stream_seq")) == uint64(len(initial)+len(tail)+len(suffix)),
		"CURRENT.tmp JetStream checkpoint advanced ACK floor before commit")
	require(number(faultBlockedBroker["num_ack_pending"]) == uint64(len(fault)), "CURRENT.tmp held JetStream pending set differs from fault input")
	capture.releaseHold(false)
	var faultResult callResult
	select {
	case faultResult = <-faultCall:
	case <-time.After(4 * time.Second):
		panic("CURRENT.tmp JetStream checkpoint did not return")
	}
	require(faultResult.code >= 400, "CURRENT.tmp JetStream checkpoint unexpectedly succeeded")
	save(filepath.Join(root, "current-tmp-response.json"), faultResult)
	acceptedFault := append([]map[string]any(nil), capture.snapshot().Rows[faultStart:]...)
	assertRows(capture, faultStart, faultExpected)
	require(len(acceptedFault) == len(faultExpected), "fault output row count is not deterministic")
	var faultStatus map[string]any
	wait("JetStream CURRENT.tmp status", func() bool {
		faultStatus = statusOf(a, "reference")
		text := strings.ToLower(fmt.Sprint(nested(faultStatus, "actual", "last_error")))
		// A reliable commit I/O error fails the attempt. The API waiter may
		// observe cancellation while Supervisor preserves the decisive error.
		return nested(faultStatus, "actual", "status") == "failed" &&
			strings.Contains(text, "checkpoint io:") && strings.Contains(text, "is a directory")
	})
	// Store may prune older, unprotected generations to reserve bounded
	// space before trying a new commit. It must preserve CURRENT and its
	// published payload, not every otherwise-retirable historical generation.
	require(hash(filepath.Join(checkpoint, "CURRENT")) == faultCurrent && snapshotHash(checkpoint, faultBaseline) == faultSnapshot,
		"CURRENT.tmp changed the JetStream durable checkpoint")
	faultBroker := consumerInfo(producer)
	require(number(nested(faultBroker, "ack_floor", "stream_seq")) == uint64(len(initial)+len(tail)+len(suffix)),
		"CURRENT.tmp ACKed the uncommitted JetStream suffix")
	require(number(faultBroker["num_ack_pending"]) == uint64(len(fault)), "CURRENT.tmp lost uncommitted JetStream pending ACKs")
	save(filepath.Join(root, "current-tmp-response.json"), faultResult)
	save(filepath.Join(root, "current-tmp-status.json"), faultStatus)
	save(filepath.Join(root, "current-tmp-broker.json"), faultBroker)
	must(os.Remove(filepath.Join(checkpoint, "CURRENT.tmp")))
	stop(syscall.SIGKILL)
	faultReplayStart := capture.rowCount()
	start()
	a.ok(http.MethodPost, "/v1/pipelines/reference/start", map[string]any{})
	waitRunning(a, "reference")
	assertRows(capture, faultReplayStart, faultExpected)
	faultReplay := capture.snapshot().Rows[faultReplayStart:]
	require(rowsEqual(acceptedFault, faultReplay), "CURRENT.tmp replay changed the accepted output ID or data")
	verifyOutputIDs(faultReplay, baselineGeneration, uint64(len(initialRows)+len(tailExpected)+len(suffixExpected)+1))
	require(number(nested(statusOf(a, "reference"), "checkpoint", "restored_from_checkpoint")) == faultBaseline,
		"CURRENT.tmp replay restored the wrong JetStream checkpoint")
	semanticCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	semanticRows := capture.rowCount()
	semanticBroker := consumerInfo(producer)
	stopPipeline(a, "reference")
	changed := replaceBinding(manual, r2)
	save(filepath.Join(root, "changed-binding-spec.json"), changed)
	a.ok(http.MethodPut, "/v1/pipelines/reference", changed)
	startResult, startCode := a.call(http.MethodPost, "/v1/pipelines/reference/start", map[string]any{})
	require(startCode >= 200 && startCode < 300, fmt.Sprintf("changed JetStream binding start returned %d", startCode))
	save(filepath.Join(root, "changed-binding-start.json"), map[string]any{"code": startCode, "response": startResult})
	changedStatus := waitActualError(a, "reference", "checkpoint participant set, schema, codec or pipeline semantics changed")
	require(hash(filepath.Join(checkpoint, "CURRENT")) == semanticCurrent && capture.rowCount() == semanticRows,
		"changed JetStream binding touched CURRENT or output")
	changedBroker := consumerInfo(producer)
	save(filepath.Join(root, "changed-binding-status.json"), changedStatus)
	require(number(nested(changedBroker, "ack_floor", "stream_seq")) == number(nested(semanticBroker, "ack_floor", "stream_seq")),
		"changed JetStream binding advanced ACK floor")
	deps := a.ok(http.MethodGet, "/v1/tables/limits/dependencies", nil)
	save(filepath.Join(root, "dependencies.json"), deps)
	a.ok(http.MethodPost, "/v1/tables/limits/gc", map[string]any{})
	rev1 := a.ok(http.MethodGet, "/v1/tables/limits/revisions/1", nil)
	require(rev1["sha256"] == r1["sha256"], "GC deleted the JetStream checkpoint dependency")
	save(filepath.Join(root, "r1-after-gc.json"), rev1)
	stop(syscall.SIGTERM)
	saveScenarioSummary(root, "reliable_reference_state_jetstream_v10", 10, map[string]any{
		"order": order, "fixed_binding": true, "suffix_recovery": true,
		"http_unknown_no_early_current": true, "current_failure_preserved": true,
		"changed_binding_refused": true, "gc_preserved_dependency": true,
		"output_ids_stable": true, "epoch_equals_state_generation": true,
		"broker_ack_cut_verified": true, "tested_cases": []string{"r1-fixed", "r2-r3-latest-drift", "http-held-kill9", "CURRENT.tmp", "semantic-binding-refusal"},
	})
	fmt.Println("REFERENCE_JS_LINEAR_OK", order)
	return true
}

func ptrInt(value int) *int { return &value }

func dagBranchSpec(file, sinkA, sinkB, checkpoint string, binding map[string]any, interval any) map[string]any {
	source := fileSource(file)
	return map[string]any{
		"version": 1, "stream": "sensors",
		"reference_tables": map[string]any{"limits": map[string]any{"revision": binding["revision"], "sha256": binding["sha256"]}},
		"source":           source,
		"sink":             map[string]any{"kind": "http", "url": sinkA, "batch_rows": 2, "linger_ms": 1, "max_inflight": 1, "outbox_capacity": 8},
		"delivery":         "live_best_effort", "recovery": "aligned", "checkpoint_dir": checkpoint,
		"checkpoint": checkpointPolicy(interval, true),
		"graph_io": map[string]any{
			"sources": map[string]any{"1": source},
			"sinks": map[string]any{
				"4": map[string]any{"kind": "http", "url": sinkA, "batch_rows": 2, "linger_ms": 1, "max_inflight": 1, "outbox_capacity": 8},
				"5": map[string]any{"kind": "http", "url": sinkB, "batch_rows": 2, "linger_ms": 1, "max_inflight": 1, "outbox_capacity": 8},
			},
		},
		"graph": map[string]any{"version": 1, "pipeline_id": 11001, "revision_id": 1, "nodes": []any{
			map[string]any{"id": 1, "kind": "memory_source", "table": "sensors", "out": []uint32{2}},
			map[string]any{"id": 2, "kind": "lookup", "table": "limits", "on": []any{map[string]any{"stream": "device_id", "table": "device_id"}}, "keep": []string{"threshold"}, "out": []uint32{3}},
			map[string]any{"id": 3, "kind": "branch", "out": []uint32{4, 5}},
			map[string]any{"id": 4, "kind": "capture_sink", "name": "branch_a"},
			map[string]any{"id": 5, "kind": "capture_sink", "name": "branch_b"},
		}},
	}
}

func dagUnionSpec(fileA, fileB, sink, checkpoint string, binding map[string]any, interval any) map[string]any {
	sourceA := fileSource(fileA)
	sourceB := fileSource(fileB)
	return map[string]any{
		"version": 1, "stream": "sensors",
		"reference_tables": map[string]any{"limits": map[string]any{"revision": binding["revision"], "sha256": binding["sha256"]}},
		"source":           sourceA,
		"sink":             map[string]any{"kind": "http", "url": sink, "batch_rows": 2, "linger_ms": 1, "max_inflight": 1, "outbox_capacity": 8},
		"delivery":         "live_best_effort", "recovery": "aligned", "checkpoint_dir": checkpoint,
		"checkpoint": checkpointPolicy(interval, true),
		"graph_io": map[string]any{
			"sources": map[string]any{"1": sourceA, "2": sourceB},
			"sinks":   map[string]any{"6": map[string]any{"kind": "http", "url": sink, "batch_rows": 2, "linger_ms": 1, "max_inflight": 1, "outbox_capacity": 8}},
		},
		"graph": map[string]any{"version": 1, "pipeline_id": 11002, "revision_id": 1, "nodes": []any{
			map[string]any{"id": 1, "kind": "memory_source", "table": "sensors", "out": []uint32{3}},
			map[string]any{"id": 2, "kind": "memory_source", "table": "sensors", "out": []uint32{4}},
			map[string]any{"id": 3, "kind": "lookup", "table": "limits", "on": []any{map[string]any{"stream": "device_id", "table": "device_id"}}, "keep": []string{"threshold"}, "out": []uint32{5}},
			map[string]any{"id": 4, "kind": "lookup", "table": "limits", "on": []any{map[string]any{"stream": "device_id", "table": "device_id"}}, "keep": []string{"threshold"}, "out": []uint32{5}},
			map[string]any{"id": 5, "kind": "union_all", "out": []uint32{6}},
			map[string]any{"id": 6, "kind": "capture_sink", "name": "union"},
		}},
	}
}

func dagExpected(values ...int) []wantedRow {
	rows := make([]wantedRow, 0, len(values))
	for _, value := range values {
		rows = append(rows, wantedRow{key: "a", value: value, field: "v", threshold: ptrInt(10)})
	}
	return rows
}

func runDAG(root, serverBin, kind string) bool {
	must(os.Mkdir(root, 0700))
	branch := kind == "branch"
	first := newCapture()
	defer first.close()
	var second *capture
	if branch {
		second = newCapture()
		defer second.close()
	}
	fileA := filepath.Join(root, "sensors-a.ndjson")
	fileB := filepath.Join(root, "sensors-b.ndjson")
	checkpoint := filepath.Join(root, "checkpoint-v11")
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
		waitHealth(a, "reference DAG server health")
	}

	start()
	if branch {
		configure(a, first, 0)
		allowCaptures(a, second)
	} else {
		configure(a, first, 0)
	}
	r1 := publishTable(a, 10, 0)
	r2 := publishTable(a, 90, 1)
	r3 := publishTable(a, 30, 2)
	save(filepath.Join(root, "table-r1.json"), r1)
	save(filepath.Join(root, "table-r2.json"), r2)
	save(filepath.Join(root, "table-r3.json"), r3)

	var spec map[string]any
	if branch {
		appendRows(fileA, []struct {
			key   string
			value int
		}{{"a", 1}, {"b", 2}})
		spec = dagBranchSpec(fileA, first.url(), second.url(), checkpoint, r1, float64(1000))
	} else {
		appendRows(fileA, []struct {
			key   string
			value int
		}{{"a", 1}})
		appendRows(fileB, []struct {
			key   string
			value int
		}{{"b", 2}})
		spec = dagUnionSpec(fileA, fileB, first.url(), checkpoint, r1, float64(1000))
	}
	save(filepath.Join(root, "spec-r1.json"), spec)
	startPipeline(a, "reference", spec)
	initialExpected := []wantedRow{{"a", 1, "v", ptrInt(10)}, {"b", 2, "v", ptrInt(20)}}
	if branch {
		assertRows(first, 0, initialExpected)
		assertRows(second, 0, initialExpected)
	} else {
		assertRowSet(first, 0, initialExpected)
	}
	periodicStatus := waitPeriodicStatus(a, "reference")
	stopPipeline(a, "reference")
	save(filepath.Join(root, "periodic-stopped-status.json"), statusOf(a, "reference"))
	periodicInv := inventory(a, "reference")
	requireCurrentSnapshotVersion(periodicInv, 11)
	periodicInfo := parseSnapshotInfo(checkpoint, periodicInv, r1, 11, 0)
	require(periodicInfo.SourceKind == "file-dag-v1", "DAG checkpoint did not carry file-dag-v1 source identity")
	require(periodicInfo.StateCount == 0, "stateless reference DAG unexpectedly carried state participants")
	save(filepath.Join(root, "periodic-status.json"), periodicStatus)
	save(filepath.Join(root, "periodic-checkpoints.json"), periodicInv)
	save(filepath.Join(root, "periodic-snapshot-info.json"), periodicInfo)
	baselineID := storageCurrent(periodicInv)
	baselineCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	baselineTree := committedTreeHash(checkpoint)
	stopPipeline(a, "reference")
	manual := clone(spec).(map[string]any)
	manual["checkpoint"].(map[string]any)["interval_ms"] = nil
	startPipeline(a, "reference", manual)
	require(hash(filepath.Join(checkpoint, "CURRENT")) == baselineCurrent, "DAG scheduler-only restart changed CURRENT")
	require(committedTreeHash(checkpoint) == baselineTree, "DAG scheduler-only restart changed committed history")

	if branch {
		firstStart, secondStart := first.rowCount(), second.rowCount()
		appendRows(fileA, []struct {
			key   string
			value int
		}{{"a", 3}})
		assertRows(first, firstStart, []wantedRow{{"a", 3, "v", ptrInt(10)}})
		assertRows(second, secondStart, []wantedRow{{"a", 3, "v", ptrInt(10)}})
	} else {
		unionStart := first.rowCount()
		appendRow(fileA, "a", 3)
		appendRow(fileB, "b", 4)
		assertRowSet(first, unionStart, []wantedRow{{"a", 3, "v", ptrInt(10)}, {"b", 4, "v", ptrInt(20)}})
	}

	// The required sink body is held while a checkpoint is requested.  For a
	// branch hold both sinks so neither half can accidentally make a partial
	// commit appear successful; Union has one required sink.
	var faultStart int
	if branch {
		faultStart = first.rowCount()
		faultReceived := first.receivedRows()
		first.setHold()
		second.setHold()
		appendRow(fileA, "b", 5)
		wait("DAG branch held first output", func() bool { return first.receivedRows() >= faultReceived+1 })
		first.waitHeld()
		second.waitHeld()
	} else {
		faultStart = first.rowCount()
		faultReceived := first.receivedRows()
		first.setHold()
		appendRow(fileA, "a", 5)
		wait("DAG union held output", func() bool { return first.receivedRows() >= faultReceived+1 })
		first.waitHeld()
	}
	beforeHold := statusOf(a, "reference")
	_, holdSucceeded, _, holdLast, _ := checkpointFields(beforeHold)
	holdCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	holdCall := make(chan callResult, 1)
	go func() {
		result, code := a.call(http.MethodPost, "/v1/pipelines/reference/checkpoint", nil)
		holdCall <- callResult{result: result, code: code}
	}()
	blocked := waitActiveCheckpoint(a, "reference", checkpoint, holdCurrent, holdSucceeded)
	_, blockedSucceeded, _, blockedLast, _ := checkpointFields(blocked)
	require(blockedSucceeded == holdSucceeded && blockedLast == holdLast, "DAG held body crossed checkpoint success")
	save(filepath.Join(root, "http-held-status.json"), blocked)
	stop(syscall.SIGKILL)
	if branch {
		first.releaseHold(true)
		second.releaseHold(true)
	} else {
		first.releaseHold(true)
	}
	select {
	case result := <-holdCall:
		save(filepath.Join(root, "http-held-checkpoint.json"), result)
		require(result.code == 0 || result.code >= 400, "held DAG checkpoint unexpectedly succeeded before SIGKILL")
	case <-time.After(4 * time.Second):
		panic("DAG held checkpoint did not terminate after SIGKILL")
	}
	require(first.rowCount() == faultStart && (!branch || second.rowCount() == faultStart), "DAG held body was counted as success")
	replayStart := first.rowCount()
	var replayExpected []wantedRow
	if branch {
		replayExpected = []wantedRow{{"a", 3, "v", ptrInt(10)}, {"b", 5, "v", ptrInt(20)}}
	} else {
		replayExpected = []wantedRow{{"a", 3, "v", ptrInt(10)}, {"b", 4, "v", ptrInt(20)}, {"a", 5, "v", ptrInt(10)}}
	}
	start()
	a.ok(http.MethodPost, "/v1/pipelines/reference/start", map[string]any{})
	waitRunning(a, "reference")
	if branch {
		assertRows(first, replayStart, replayExpected)
		assertRows(second, replayStart, replayExpected)
	} else {
		assertRowSet(first, replayStart, replayExpected)
	}
	restored := statusOf(a, "reference")
	require(number(nested(restored, "checkpoint", "restored_from_checkpoint")) == baselineID, "DAG suffix restore used the wrong checkpoint")
	_, restoredSucceeded, _, _, _ := checkpointFields(restored)
	_, replayCode := manualCheckpoint(a, "reference")
	require(replayCode >= 200 && replayCode < 300, "DAG replay checkpoint failed")
	replayStatus := waitCheckpointSuccess(a, "reference", restoredSucceeded)
	save(filepath.Join(root, "replay-checkpoint-status.json"), replayStatus)
	replayInv := inventory(a, "reference")
	requireCurrentSnapshotVersion(replayInv, 11)
	replayInfo := parseSnapshotInfo(checkpoint, replayInv, r1, 11, 0)
	require(replayInfo.ReferenceCRC32 == periodicInfo.ReferenceCRC32, "DAG r1 CRC identity changed")

	// Exercise an actual CURRENT.tmp I/O failure for this graph as well.  The
	// second held body is accepted only after the failed publication returns,
	// then must be replayed from the previous durable graph checkpoint.
	faultCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	faultTree := committedTreeHash(checkpoint)
	if branch {
		first.setHold()
		second.setHold()
		appendRow(fileA, "a", 6)
		first.waitHeld()
		second.waitHeld()
	} else {
		first.setHold()
		appendRow(fileA, "a", 6)
		first.waitHeld()
	}
	makeDirErr := os.Mkdir(filepath.Join(checkpoint, "CURRENT.tmp"), 0700)
	must(makeDirErr)
	_, beforeFailureSucceeded, _, _, _ := checkpointFields(statusOf(a, "reference"))
	failureCall := make(chan callResult, 1)
	go func() {
		result, code := a.call(http.MethodPost, "/v1/pipelines/reference/checkpoint", nil)
		failureCall <- callResult{result: result, code: code}
	}()
	waitActiveCheckpoint(a, "reference", checkpoint, faultCurrent, beforeFailureSucceeded)
	if branch {
		first.releaseHold(false)
		second.releaseHold(false)
	} else {
		first.releaseHold(false)
	}
	var failure callResult
	select {
	case failure = <-failureCall:
	case <-time.After(4 * time.Second):
		panic("DAG CURRENT.tmp checkpoint did not return")
	}
	text := strings.ToLower(fmt.Sprint(nested(failure.result, "error", "message")))
	require(failure.code >= 400 && strings.Contains(text, "checkpoint io:") && strings.Contains(text, "is a directory"), "DAG CURRENT.tmp error was imprecise")
	failureStatus := waitCheckpointFailure(a, "reference")
	require(hash(filepath.Join(checkpoint, "CURRENT")) == faultCurrent && committedTreeHash(checkpoint) == faultTree, "DAG CURRENT.tmp changed durable history")
	save(filepath.Join(root, "current-tmp-response.json"), failure)
	save(filepath.Join(root, "current-tmp-status.json"), failureStatus)
	must(os.Remove(filepath.Join(checkpoint, "CURRENT.tmp")))
	stop(syscall.SIGKILL)
	failureReplayStart := first.rowCount()
	start()
	a.ok(http.MethodPost, "/v1/pipelines/reference/start", map[string]any{})
	waitRunning(a, "reference")
	if branch {
		assertRows(first, failureReplayStart, []wantedRow{{"a", 6, "v", ptrInt(10)}})
		assertRows(second, failureReplayStart, []wantedRow{{"a", 6, "v", ptrInt(10)}})
	} else {
		assertRows(first, failureReplayStart, []wantedRow{{"a", 6, "v", ptrInt(10)}})
	}

	semanticCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	semanticRows := first.rowCount()
	stopPipeline(a, "reference")
	changed := replaceBinding(manual, r2)
	a.ok(http.MethodPut, "/v1/pipelines/reference", changed)
	startResult, startCode := a.call(http.MethodPost, "/v1/pipelines/reference/start", map[string]any{})
	require(startCode >= 200 && startCode < 300, fmt.Sprintf("changed DAG binding start returned %d", startCode))
	save(filepath.Join(root, "changed-binding-start.json"), map[string]any{"code": startCode, "response": startResult})
	changedStatus := waitActualError(a, "reference", "checkpoint participant set, schema, codec or pipeline semantics changed")
	require(hash(filepath.Join(checkpoint, "CURRENT")) == semanticCurrent && first.rowCount() == semanticRows, "changed DAG binding touched CURRENT or output")
	save(filepath.Join(root, "changed-binding-status.json"), changedStatus)
	deps := a.ok(http.MethodGet, "/v1/tables/limits/dependencies", nil)
	save(filepath.Join(root, "dependencies.json"), deps)
	a.ok(http.MethodPost, "/v1/tables/limits/gc", map[string]any{})
	rev1 := a.ok(http.MethodGet, "/v1/tables/limits/revisions/1", nil)
	require(rev1["sha256"] == r1["sha256"], "DAG GC deleted the r1 dependency")
	save(filepath.Join(root, "r1-after-gc.json"), rev1)
	stop(syscall.SIGTERM)
	saveScenarioSummary(root, "reference_dag_file_v11", 11, map[string]any{
		"dag_kind": kind, "source_kind": "file-dag-v1", "fixed_binding": true,
		"suffix_recovery": true, "http_unknown_no_early_current": true,
		"current_failure_preserved": true, "changed_binding_refused": true,
		"gc_preserved_dependency": true, "required_sinks": boolToInt(branch, 2, 1),
		"tested_cases": []string{"r1-fixed", "r2-r3-latest-drift", "http-held-kill9", "CURRENT.tmp", "semantic-binding-refusal"},
	})
	fmt.Println("REFERENCE_DAG_OK", kind)
	return true
}

func boolToInt(condition bool, yes, no int) int {
	if condition {
		return yes
	}
	return no
}

func hysteresisConfig() map[string]any {
	return map[string]any{
		"keys": []string{"device_id"}, "fields": []string{"temperature"},
		"emit_first": true, "ttl_micros": 0, "max_keys": 1024, "invalid": "ignore",
		"hysteresis": map[string]any{"direction": "high", "enter": 60.0, "exit": 55.0},
	}
}

func hysteresisFields() []any {
	return []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "temperature", "type": "float64", "nullable": true},
	}
}

func configureHysteresis(a api, c *capture, brokerPort int) {
	a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": c.listener.Addr().(*net.TCPAddr).Port})
	if brokerPort != 0 {
		a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": brokerPort})
	}
	a.ok(http.MethodPut, "/v1/streams/telemetry", map[string]any{"fields": hysteresisFields()})
}

func appendTemperature(path string, key string, value float64) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	_, err = file.Write(append(data(map[string]any{"device_id": key, "temperature": value}), '\n'))
	must(err)
	must(file.Sync())
	must(file.Close())
}

func appendTemperatures(path string, values []float64) {
	for _, value := range values {
		appendTemperature(path, "a", value)
	}
}

func publishTemperatures(producer *nats, values []float64, total int) {
	for _, value := range values {
		body := data(map[string]any{"device_id": "a", "temperature": value})
		_, err := fmt.Fprintf(producer.conn, "PUB input.rows %d\r\n%s\r\n", len(body), body)
		must(err)
	}
	wait("JetStream temperature persistence fence", func() bool {
		info := producer.request("$JS.API.STREAM.INFO.INPUT", []byte("{}"))
		return number(nested(info, "state", "messages")) >= uint64(total)
	})
}

func hysteresisFileSpec(file, sink, checkpoint string) map[string]any {
	return map[string]any{
		"version": 1, "stream": "telemetry",
		"source":   map[string]any{"kind": "file", "path": file, "file_contract": "append_only", "inbox_capacity": 8},
		"sink":     map[string]any{"kind": "http", "url": sink, "batch_rows": 1, "linger_ms": 0, "max_inflight": 1, "outbox_capacity": 8},
		"delivery": "live_best_effort", "recovery": "aligned", "checkpoint_dir": checkpoint,
		"checkpoint": checkpointPolicy(float64(1000), true),
		"graph": map[string]any{"version": 1, "pipeline_id": 12001, "revision_id": 1, "nodes": []any{
			map[string]any{"id": 1, "kind": "memory_source", "table": "telemetry", "out": []uint32{2}},
			map[string]any{"id": 2, "kind": "hysteresis", "iot": hysteresisConfig(), "out": []uint32{3}},
			map[string]any{"id": 3, "kind": "capture_sink", "name": "hysteresis"},
		}},
	}
}

func hysteresisJSSpec(brokerPort int, sink, checkpoint, consumer string) map[string]any {
	return map[string]any{
		"version": 1, "stream": "telemetry", "source": map[string]any{"kind": "jetstream", "inbox_capacity": 8, "jetstream": map[string]any{
			"servers": []string{fmt.Sprintf("nats://127.0.0.1:%d", brokerPort)}, "namespace": "hysteresis_v13", "stream": "INPUT", "consumer": consumer,
			"ownership_bucket": "OWNERS", "max_pending": 32, "pending_bytes": 262144, "pull_messages": 8, "pull_bytes": 73728,
		}},
		"sink":     map[string]any{"kind": "http", "url": sink, "batch_rows": 1, "linger_ms": 0, "max_inflight": 1, "outbox_capacity": 8},
		"delivery": "checkpointed_at_least_once", "recovery": "aligned", "checkpoint_dir": checkpoint,
		"checkpoint": checkpointPolicy(float64(86400000), true),
		"graph": map[string]any{"version": 1, "pipeline_id": 13001, "revision_id": 1, "nodes": []any{
			map[string]any{"id": 1, "kind": "memory_source", "table": "telemetry", "out": []uint32{2}},
			map[string]any{"id": 2, "kind": "hysteresis", "iot": hysteresisConfig(), "out": []uint32{3}},
			map[string]any{"id": 3, "kind": "capture_sink", "name": "hysteresis"},
		}},
	}
}

func snapshotVersion(checkpoint string, inv map[string]any) uint16 {
	payload := snapshotPayload(checkpoint, storageCurrent(inv))
	require(len(payload) >= 6 && bytes.Equal(payload[:4], []byte("SPV1")), "hysteresis snapshot header is not SPV1")
	return binary.LittleEndian.Uint16(payload[4:6])
}

func snapshotManifestMagic(checkpoint string, inv map[string]any, version uint16) string {
	payload := snapshotPayload(checkpoint, storageCurrent(inv))
	require(len(payload) >= 38 && bytes.Equal(payload[:4], []byte("SPV1")), "snapshot manifest header is not SPV1")
	offset := 38
	for i := 0; i < 2; i++ {
		length, ok := readU32(payload, &offset)
		require(ok && length <= 64*1024, "snapshot source metadata exceeds bound")
		_, ok = readBytes(payload, &offset, int(length))
		require(ok, "snapshot source metadata truncated")
	}
	_, ok := readBytes(payload, &offset, 16+8+8+16)
	require(ok, "snapshot provenance truncated")
	if version == 13 {
		_, ok = readBytes(payload, &offset, 24)
		require(ok, "reliable hysteresis cursor truncated")
	}
	length, ok := readU32(payload, &offset)
	require(ok && length <= 256*1024, "snapshot plan exceeds bound")
	plan, ok := readBytes(payload, &offset, int(length))
	require(ok && len(plan) >= 4, "snapshot plan truncated")
	return string(plan[:4])
}

func hysteresisOutputs(c *capture, from int, expected []float64) {
	wait("hysteresis HTTP output", func() bool { return c.rowCount() >= from+len(expected) })
	rows := c.snapshot().Rows
	require(len(rows) == from+len(expected), fmt.Sprintf("hysteresis rows=%d want=%d", len(rows), from+len(expected)))
	for i, value := range expected {
		row := rowData(rows[from+i])
		got, ok := row["temperature"].(float64)
		require(ok && got == value, fmt.Sprintf("hysteresis row %d temperature=%v want=%v", i, row["temperature"], value))
	}
}

func runHysteresisFile(root, serverBin string) bool {
	must(os.Mkdir(root, 0700))
	capture := newCapture()
	defer capture.close()
	file := filepath.Join(root, "telemetry.ndjson")
	checkpoint := filepath.Join(root, "checkpoint-v12")
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
		waitHealth(a, "hysteresis File health")
	}
	start()
	configureHysteresis(a, capture, 0)
	values := []float64{57, 60, 59, 56, 55, 56, 60, 60, 54}
	appendTemperatures(file, values[:3])
	spec := hysteresisFileSpec(file, capture.url(), checkpoint)
	save(filepath.Join(root, "spec.json"), spec)
	startPipeline(a, "hysteresis", spec)
	hysteresisOutputs(capture, 0, []float64{57, 60})
	status := waitPeriodicStatus(a, "hysteresis")
	stopPipeline(a, "hysteresis")
	save(filepath.Join(root, "periodic-stopped-status.json"), statusOf(a, "hysteresis"))
	inv := inventory(a, "hysteresis")
	requireCurrentSnapshotVersion(inv, 12)
	require(snapshotVersion(checkpoint, inv) == 12, "File hysteresis did not write profile v12")
	require(snapshotManifestMagic(checkpoint, inv, 12) == "CPL1", "File hysteresis unexpectedly wrote a reference CPL3 manifest")
	_, _, _, observedID, baselineGeneration := checkpointFields(status)
	baselineID := storageCurrent(inv)
	require(observedID != 0 && baselineID >= observedID && baselineGeneration == checkpointGeneration(inv), "File hysteresis baseline lacks identity")
	save(filepath.Join(root, "periodic-status.json"), status)
	save(filepath.Join(root, "periodic-checkpoints.json"), inv)
	manual := clone(spec).(map[string]any)
	manual["checkpoint"].(map[string]any)["interval_ms"] = nil
	startPipeline(a, "hysteresis", manual)
	appendTemperatures(file, values[3:])
	hysteresisOutputs(capture, 2, []float64{55, 60, 54})
	stop(syscall.SIGKILL)
	replayStart := capture.rowCount()
	start()
	a.ok(http.MethodPost, "/v1/pipelines/hysteresis/start", map[string]any{})
	waitRunning(a, "hysteresis")
	hysteresisOutputs(capture, replayStart, []float64{55, 60, 54})
	restored := statusOf(a, "hysteresis")
	require(number(nested(restored, "checkpoint", "restored_from_checkpoint")) == baselineID, "File hysteresis restored wrong checkpoint")
	_, _, _, _, restoredGeneration := checkpointFields(restored)
	require(restoredGeneration == baselineGeneration, "File hysteresis restore changed state generation")
	save(filepath.Join(root, "restore-status.json"), restored)
	save(filepath.Join(root, "outputs.json"), capture.snapshot())
	stop(syscall.SIGTERM)
	save(filepath.Join(root, "summary.json"), map[string]any{
		"valid": true, "profile": "hysteresis_file_v12", "snapshot_version": 12, "manifest": "CPL1", "ttl_micros": 0,
		"periodic_checkpoint": true, "initial_outputs": []float64{57, 60}, "restored_suffix": []float64{55, 60, 54},
		"state_generation_preserved": true, "semantic_state_continuity": true, "certified": false,
	})
	fmt.Println("REFERENCE_HYSTERESIS_FILE_OK")
	return true
}

func runHysteresisJS(root, serverBin, natsBin string) bool {
	must(os.Mkdir(root, 0700))
	capture := newCapture()
	defer capture.close()
	broker, producer, brokerPort := startBroker(root, natsBin)
	defer producer.close()
	defer broker.stop(syscall.SIGKILL)
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
		waitHealth(a, "hysteresis JetStream health")
	}
	start()
	configureHysteresis(a, capture, brokerPort)
	consumer := "hysteresis_v13"
	checkpoint := filepath.Join(root, "checkpoint-v13")
	spec := hysteresisJSSpec(brokerPort, capture.url(), checkpoint, consumer)
	save(filepath.Join(root, "spec.json"), spec)
	startPipeline(a, "hysteresis", spec)
	values := []float64{57, 60, 59, 56, 55, 56, 60, 60, 54}
	publishTemperatures(producer, values[:3], 3)
	hysteresisOutputs(capture, 0, []float64{57, 60})
	waitPublished(a, "hysteresis", 3)
	beforeStatus := statusOf(a, "hysteresis")
	_, beforeSucceeded, _, _, _ := checkpointFields(beforeStatus)
	_, code := manualCheckpoint(a, "hysteresis")
	require(code >= 200 && code < 300, "JetStream hysteresis baseline checkpoint failed")
	baselineStatus := waitCheckpointSuccess(a, "hysteresis", beforeSucceeded)
	baselineStatus = waitReliableCommitted(a, "hysteresis", 3)
	baselineBroker := consumerInfo(producer)
	require(number(nested(baselineBroker, "ack_floor", "stream_seq")) == 3 && number(baselineBroker["num_ack_pending"]) == 0, "hysteresis baseline broker ACK cut differs from checkpoint")
	save(filepath.Join(root, "baseline-broker.json"), baselineBroker)
	inv := inventory(a, "hysteresis")
	requireCurrentSnapshotVersion(inv, 13)
	require(snapshotVersion(checkpoint, inv) == 13, "JetStream hysteresis did not write profile v13")
	require(snapshotManifestMagic(checkpoint, inv, 13) == "CPL1", "JetStream hysteresis unexpectedly wrote a reference CPL3 manifest")
	_, _, _, baselineID, baselineGeneration := checkpointFields(baselineStatus)
	verifyOutputIDs(capture.snapshot().Rows, baselineGeneration, 1)
	save(filepath.Join(root, "baseline-status.json"), baselineStatus)
	save(filepath.Join(root, "baseline-checkpoints.json"), inv)
	publishTemperatures(producer, values[3:], 9)
	hysteresisOutputs(capture, 2, []float64{55, 60, 54})
	waitPublished(a, "hysteresis", 9)
	stop(syscall.SIGKILL)
	replayStart := capture.rowCount()
	start()
	a.ok(http.MethodPost, "/v1/pipelines/hysteresis/start", map[string]any{})
	waitRunning(a, "hysteresis")
	// The suffix was deliberately appended after the v13 baseline and must
	// replay as 55,60,54.  Equal in-band values remain suppressed by the
	// restored latch rather than being emitted a second time.
	hysteresisOutputs(capture, replayStart, []float64{55, 60, 54})
	verifyOutputIDs(capture.snapshot().Rows[replayStart:], baselineGeneration, 3)
	restored := statusOf(a, "hysteresis")
	require(number(nested(restored, "checkpoint", "restored_from_checkpoint")) == baselineID, "JetStream hysteresis restored wrong checkpoint")
	_, _, _, _, restoredGeneration := checkpointFields(restored)
	require(restoredGeneration == baselineGeneration, "JetStream hysteresis changed generation on restore")
	waitPublished(a, "hysteresis", 9)
	_, replaySucceeded, _, _, _ := checkpointFields(restored)
	_, code = manualCheckpoint(a, "hysteresis")
	require(code >= 200 && code < 300, "JetStream hysteresis replay checkpoint failed")
	replayStatus := waitCheckpointSuccess(a, "hysteresis", replaySucceeded)
	_ = waitReliableCommitted(a, "hysteresis", 9)
	save(filepath.Join(root, "replay-status.json"), replayStatus)
	brokerInfo := consumerInfo(producer)
	require(number(nested(brokerInfo, "ack_floor", "stream_seq")) == 9 && number(brokerInfo["num_ack_pending"]) == 0, "JetStream hysteresis ACK cut is not durable")
	save(filepath.Join(root, "restore-status.json"), restored)
	save(filepath.Join(root, "restore-broker.json"), brokerInfo)
	committedRows := capture.rowCount()
	stop(syscall.SIGKILL)
	start()
	a.ok(http.MethodPost, "/v1/pipelines/hysteresis/start", map[string]any{})
	waitRunning(a, "hysteresis")
	committedRestart := waitReliableCommitted(a, "hysteresis", 9)
	// A restored ACK cut at 9 plus a new valid in-band value proves that
	// already committed suffix rows are neither replayed nor emitted fresh.
	publishTemperatures(producer, []float64{56}, 10)
	waitPublished(a, "hysteresis", 10)
	time.Sleep(100 * time.Millisecond)
	require(capture.rowCount() == committedRows, "committed hysteresis suffix replayed or lost its Normal latch")
	save(filepath.Join(root, "committed-restart-status.json"), committedRestart)
	save(filepath.Join(root, "outputs.json"), capture.snapshot())
	stop(syscall.SIGTERM)
	save(filepath.Join(root, "summary.json"), map[string]any{
		"valid": true, "profile": "hysteresis_reliable_jetstream_v13", "snapshot_version": 13, "manifest": "CPL1", "ttl_micros": 0,
		"periodic_checkpoint": false, "initial_outputs": []float64{57, 60}, "suffix_outputs": []float64{55, 60, 54},
		"committed_suffix_not_replayed": true, "output_ids_epoch_generation": true, "broker_ack_cut_verified": true, "certified": false,
	})
	fmt.Println("REFERENCE_HYSTERESIS_JS_OK")
	return true
}

func waitProfileGuard(a api, name, expected string) map[string]any {
	var status map[string]any
	wait("profile guard="+expected, func() bool {
		status = statusOf(a, name)
		return nested(status, "actual", "status") == "failed" &&
			strings.Contains(strings.ToLower(fmt.Sprint(nested(status, "actual", "last_error"))), strings.ToLower(expected))
	})
	return status
}

func runProfileGuard(root, binary, highCheckpoint, highInput, expected string, label string) map[string]any {
	must(os.Mkdir(root, 0700))
	checkpoint := filepath.Join(root, "checkpoint-high")
	copyTree(highCheckpoint, checkpoint)
	input := filepath.Join(root, "sensors.ndjson")
	raw, err := os.ReadFile(highInput)
	if err != nil || !bytes.Contains(raw, []byte(`"v"`)) {
		// A source path is only used to make the legacy catalog/spec valid.  The
		// guard must fail before source activation, so a tiny valid fixture is
		// sufficient if the original artifact was pruned by the runner.
		raw = []byte("{\"device_id\":\"a\",\"v\":1}\n")
	}
	must(os.WriteFile(input, raw, 0600))
	capture := newCapture()
	defer capture.close()
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
		process = spawnServer(binary, root, filepath.Join(root, "server.log"), port)
		waitHealth(a, label+" health")
	}
	start()
	configure(a, capture, 0)
	legacy := legacyFileSpec(input, capture.url(), checkpoint)
	save(filepath.Join(root, "legacy-spec.json"), legacy)
	// Create the catalog with this binary itself.  This isolates the profile
	// check from catalog schema/version compatibility and from any old writer.
	a.ok(http.MethodPut, "/v1/pipelines/legacy", legacy)
	stop(syscall.SIGTERM)
	start()
	beforeTree := fullTreeHash(checkpoint)
	beforeCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	beforeRows := capture.rowCount()
	startResult, startCode := a.call(http.MethodPost, "/v1/pipelines/legacy/start", map[string]any{})
	var failed map[string]any
	if startCode >= 200 && startCode < 300 {
		failed = waitProfileGuard(a, "legacy", expected)
	} else {
		failed = startResult
	}
	text := strings.ToLower(fmt.Sprint(nested(failed, "actual", "last_error")))
	text += " " + strings.ToLower(fmt.Sprint(nested(startResult, "error", "message")))
	if !strings.Contains(text, strings.ToLower(expected)) {
		logBytes, readErr := os.ReadFile(filepath.Join(root, "server.log"))
		if readErr == nil {
			text += " " + strings.ToLower(string(logBytes))
		}
	}
	require(strings.Contains(text, strings.ToLower(expected)), fmt.Sprintf("%s did not reach exact profile guard: %s", label, text))
	stop(syscall.SIGTERM)
	require(fullTreeHash(checkpoint) == beforeTree, label+" changed checkpoint history")
	require(hash(filepath.Join(checkpoint, "CURRENT")) == beforeCurrent, label+" changed CURRENT")
	require(capture.rowCount() == beforeRows, label+" emitted output from incompatible checkpoint")
	evidence := map[string]any{
		"checked": true, "label": label, "start_code": startCode,
		"expected_error": expected, "observed_error": text,
		"history_preserved": true, "current_preserved": true, "output_preserved": true,
	}
	save(filepath.Join(root, "evidence.json"), evidence)
	return evidence
}

func main() {
	serverBin := flag.String("server-bin", "", "new server binary with reference v9-v13 support")
	oldServerBin := flag.String("old-server-bin", "", "old B2 server binary for profile guards")
	natsBin := flag.String("nats-server", "", "pinned NATS Server binary")
	out := flag.String("out", "", "new isolated artifact directory")
	pausedOnly := flag.Bool("paused-time-only", false, "run independent v14/v15 time/process oracles instead of the reference matrix")
	pausedFileOnly := flag.Bool("paused-file-only", false, "run the three timed File scenarios on a feature-off production binary")
	completionOnly := flag.Bool("time-completion-only", false, "run v16/v17 PT, TTL and multi-state crash oracles")
	completionFileOnly := flag.Bool("time-completion-file-only", false, "run v16 crash oracles on the default feature-off binary")
	graphOnly := flag.Bool("time-graph-only", false, "run File time DAG v18/v19 multi-source/multi-Sink SIGKILL oracles")
	alarmOnly := flag.Bool("alarm-only", false, "run v20/v21 alarm activate/resolve and episode SIGKILL oracles")
	alarmFileOnly := flag.Bool("alarm-file-only", false, "run v20 alarm oracles on the default feature-off binary")
	flag.Parse()
	require(*serverBin != "" && *oldServerBin != "" && *natsBin != "" && *out != "",
		"server-bin, old-server-bin, nats-server, and out are required")
	root, err := filepath.Abs(*out)
	must(err)
	if _, statErr := os.Stat(root); statErr == nil {
		panic("out directory already exists; refusing to reuse artifacts")
	} else if !os.IsNotExist(statErr) {
		panic(statErr)
	}
	must(os.Mkdir(root, 0700))
	self, err := os.Executable()
	must(err)
	save(filepath.Join(root, "binaries.json"), map[string]any{
		"server_sha256": hash(*serverBin), "old_server_sha256": hash(*oldServerBin),
		"nats_sha256": hash(*natsBin), "driver_sha256": hash(self),
	})
	if *graphOnly {
		runTimeGraphMatrix(root, *serverBin, *oldServerBin)
		return
	}
	if *alarmOnly || *alarmFileOnly {
		runAlarmMatrix(root, *serverBin, *oldServerBin, *natsBin, *alarmFileOnly)
		return
	}
	if *completionOnly || *completionFileOnly {
		runTimeCompletionMatrix(root, *serverBin, *oldServerBin, *natsBin, *completionFileOnly)
		return
	}
	if *pausedFileOnly {
		for _, kind := range []string{"hold_for", "debounce", "debounce_leading"} {
			runTimedProcess(filepath.Join(root, "file-"+kind), *serverBin, *natsBin, kind, false)
		}
		save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "file_process_scenarios": 3, "jetstream_required": false, "certified": false})
		fmt.Println("PAUSED_TIME_DEFAULT_FILE_PROCESS_OK")
		return
	}
	if *pausedOnly {
		runPausedTimeMatrix(root, *serverBin, *oldServerBin, *natsBin)
		return
	}

	fileLookupCount := filepath.Join(root, "profile9-file-lookup-count-iot")
	fileCountLookup := filepath.Join(root, "profile9-file-count-lookup")
	jsLookupCount := filepath.Join(root, "profile10-js-lookup-count")
	jsLookupIot := filepath.Join(root, "profile10-js-lookup-iot")
	dagBranch := filepath.Join(root, "profile11-file-dag-branch")
	dagUnion := filepath.Join(root, "profile11-file-dag-union")
	hysteresisFile := filepath.Join(root, "profile12-hysteresis-file")
	hysteresisJS := filepath.Join(root, "profile13-hysteresis-jetstream")

	runFileLinear(fileLookupCount, *serverBin, "lookup-count-iot")
	runFileLinear(fileCountLookup, *serverBin, "count-lookup")
	runJSLinear(jsLookupCount, *serverBin, *natsBin, "lookup-count")
	runJSLinear(jsLookupIot, *serverBin, *natsBin, "lookup-iot")
	runDAG(dagBranch, *serverBin, "branch")
	runDAG(dagUnion, *serverBin, "union")
	runStatefulDAG(filepath.Join(root, "profile11-file-dag-count-hysteresis"), *serverBin)
	runHysteresisFile(hysteresisFile, *serverBin)
	runHysteresisJS(hysteresisJS, *serverBin, *natsBin)

	// Each compatibility check creates its own catalog with the binary being
	// tested.  The high-version checkpoint and its full history are copied,
	// never opened in place by a legacy writer.
	oldV9 := runProfileGuard(filepath.Join(root, "old-b2-v9-guard"), *oldServerBin,
		filepath.Join(fileLookupCount, "checkpoint-v9"), filepath.Join(fileLookupCount, "sensors.ndjson"),
		"checkpoint source profile mismatch", "old B2 v9 guard")
	oldV10 := runProfileGuard(filepath.Join(root, "old-b2-v10-guard"), *oldServerBin,
		filepath.Join(jsLookupCount, "checkpoint-v10"), filepath.Join(jsLookupCount, "sensors.ndjson"),
		"checkpoint source profile mismatch", "old B2 v10 guard")
	oldV11 := runProfileGuard(filepath.Join(root, "old-b2-v11-guard"), *oldServerBin,
		filepath.Join(dagBranch, "checkpoint-v11"), filepath.Join(dagBranch, "sensors.ndjson"),
		"checkpoint source profile mismatch", "old B2 v11 guard")
	newNoReference := runProfileGuard(filepath.Join(root, "new-no-reference-v10-guard"), *serverBin,
		filepath.Join(jsLookupCount, "checkpoint-v10"), filepath.Join(jsLookupCount, "sensors.ndjson"),
		"checkpoint source profile mismatch", "new no-reference v10 guard")
	oldV12 := runProfileGuard(filepath.Join(root, "old-b2-v12-guard"), *oldServerBin,
		filepath.Join(hysteresisFile, "checkpoint-v12"), filepath.Join(hysteresisFile, "telemetry.ndjson"),
		"checkpoint source profile mismatch", "old B2 v12 guard")
	oldV13 := runProfileGuard(filepath.Join(root, "old-b2-v13-guard"), *oldServerBin,
		filepath.Join(hysteresisJS, "checkpoint-v13"), filepath.Join(hysteresisJS, "telemetry.ndjson"),
		"checkpoint source profile mismatch", "old B2 v13 guard")
	save(filepath.Join(root, "summary.json"), map[string]any{
		"valid": true, "manifest": "CPL3 plus legacy CPL1 hysteresis profiles", "ttl_micros": 0,
		"profile9_file_lookup_count_iot": true, "profile9_file_count_lookup": true,
		"profile10_jetstream_lookup_count": true, "profile10_jetstream_lookup_iot": true,
		"profile11_file_dag_branch": true, "profile11_file_dag_union": true,
		"profile11_file_dag_count_hysteresis": true,
		"profile12_hysteresis_file":           true, "profile13_hysteresis_jetstream": true,
		"old_profile8_v9_guard": oldV9, "new_profile8_v10_guard": newNoReference,
		"old_profile8_v10_guard": oldV10, "old_profile8_v11_guard": oldV11,
		"old_profile8_v12_guard": oldV12, "old_profile8_v13_guard": oldV13,
		"snapshot_versions": map[string]any{"profile9": 9, "profile10": 10, "profile11": 11, "profile12": 12, "profile13": 13},
		"tested_cases": []string{
			"File Lookup-Count-IoT", "File Count-Lookup", "JetStream Lookup-Count", "JetStream Lookup-IoT",
			"File DAG Lookup-Branch-required-HTTP", "File DAG Lookup-Union-required-HTTP",
			"File DAG Lookup-Count-Hysteresis-Branch-required-HTTP",
			"File hysteresis v12", "JetStream hysteresis v13", "legacy profile guards",
		},
		"exactly_once_claimed": false, "at_least_once_output_replay_explicit": true, "certified": false,
	})
	fmt.Println("K1_K4_REFERENCE_PROCESS_OK")
}
