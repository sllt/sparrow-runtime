// Core-A process oracle for the reliable JetStream -> state -> HTTP profile.
// Standard library only.  The broker, server, and HTTP receiver are all
// children/listeners created by this driver on loopback; no existing service
// or deployment is touched.
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

// treeHash covers published checkpoint history and CURRENT, but not the
// writer lock, a deliberately injected CURRENT.tmp, or an unpublished failed
// generation.  It is an evidence digest, not restore authorization.
func treeHash(root string) string {
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
			if _, statErr := os.Stat(filepath.Join(path, "PUBLISHED")); os.IsNotExist(statErr) {
				return filepath.SkipDir
			}
		}
		if info.IsDir() {
			return nil
		}
		relative, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
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
		if info.IsDir() && strings.HasPrefix(name, "chk-") {
			if _, statErr := os.Stat(filepath.Join(path, "PUBLISHED")); os.IsNotExist(statErr) {
				return filepath.SkipDir
			}
		}
		relative, relErr := filepath.Rel(source, path)
		if relErr != nil {
			return relErr
		}
		target := filepath.Join(destination, relative)
		if info.IsDir() {
			return os.MkdirAll(target, 0700)
		}
		if !info.Mode().IsRegular() {
			return fmt.Errorf("checkpoint copy contains non-regular file %s", path)
		}
		raw, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		return os.WriteFile(target, raw, 0600)
	}))
}

func freePort() int {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	port := listener.Addr().(*net.TCPAddr).Port
	must(listener.Close())
	return port
}

func waitFor(label string, condition func() bool) {
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("deadline: " + label)
}

type child struct {
	cmd *exec.Cmd
	log *os.File
}

func launch(binary, root, logPath string, args ...string) *child {
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	command := exec.Command(binary, args...)
	command.Env = append(os.Environ(),
		"SPARROW_TOKEN=core-a-private-process-fixture",
		"SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef",
		"SPARROW_REQUIRE_SECRETS_KEY=1",
		"SPARROW_DATA_ROOTS="+root,
	)
	command.Stdout = logFile
	command.Stderr = logFile
	must(command.Start())
	return &child{cmd: command, log: logFile}
}

func (c *child) stop(signal os.Signal) {
	if c == nil || c.cmd == nil {
		return
	}
	_ = c.cmd.Process.Signal(signal)
	done := make(chan struct{})
	go func() {
		_ = c.cmd.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		_ = c.cmd.Process.Kill()
		<-done
	}
	_ = c.log.Close()
	c.cmd = nil
}

// nats is a bounded Core NATS request/reply client used only for JetStream
// admin calls and fixture publication.  The Sparrow reader uses the same
// broker through its normal JetStream SDK path.
type nats struct {
	conn   net.Conn
	in     *bufio.Reader
	serial uint64
}

func dialNATS(port int) *nats {
	var conn net.Conn
	waitFor("isolated NATS ready", func() bool {
		var err error
		conn, err = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		return err == nil
	})
	n := &nats{conn: conn, in: bufio.NewReader(conn)}
	_, err := fmt.Fprint(conn, "CONNECT {\"verbose\":false,\"pedantic\":true,\"lang\":\"go-core-a-fixture\",\"version\":\"1\"}\r\n")
	must(err)
	return n
}

func (n *nats) request(subject string, payload []byte) map[string]any {
	n.serial++
	inbox := fmt.Sprintf("_INBOX.coreafixture.%d.%d", os.Getpid(), n.serial)
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
			require(size >= 0 && size <= 1024*1024, "NATS response body bound")
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
	broker := launch(binary, root, filepath.Join(root, "broker.log"), "-c", filepath.Join(root, "nats.conf"))
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
		body := data(map[string]any{"device_id": "d1", "v": value})
		_, err := fmt.Fprintf(producer.conn, "PUB input.rows %d\r\n%s\r\n", len(body), body)
		must(err)
	}
	waitFor("JetStream persistence fence", func() bool {
		info := producer.request("$JS.API.STREAM.INFO.INPUT", []byte("{}"))
		return number(nested(info, "state", "messages")) >= uint64(total)
	})
}

func readerName(producer *nats) string {
	var result map[string]any
	waitFor("single active JetStream reader", func() bool {
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

type httpRequest struct {
	rows   []map[string]any
	status int
	at     time.Time
}

type capture struct {
	mu        sync.Mutex
	requests  []httpRequest
	responses []int
	respondedRows int
	server    *http.Server
	listener  net.Listener

	holdMu      sync.Mutex
	hold        bool
	holdStarted chan struct{}
	holdRelease chan struct{}
	status      int
}

func newCapture() *capture {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	capture := &capture{listener: listener, status: http.StatusOK}
	capture.holdRelease = make(chan struct{})
	close(capture.holdRelease)
	capture.server = &http.Server{
		ReadHeaderTimeout: time.Second,
		Handler: http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
			defer request.Body.Close()
			raw, readErr := io.ReadAll(io.LimitReader(request.Body, 1024*1024+1))
			if readErr != nil || len(raw) > 1024*1024 {
				writer.WriteHeader(http.StatusRequestEntityTooLarge)
				return
			}
			var rows []map[string]any
			if json.Unmarshal(raw, &rows) != nil {
				writer.WriteHeader(http.StatusBadRequest)
				return
			}

			// Record receipt before the optional hold.  A held body is an external
			// side effect without a 2xx response and must not be mistaken for a
			// durable Sparrow commit.
			capture.mu.Lock()
			capture.requests = append(capture.requests, httpRequest{
				rows: rows, status: capture.status, at: time.Now(),
			})
			capture.mu.Unlock()

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
			status := capture.status
			capture.mu.Unlock()
			writer.WriteHeader(status)
			capture.mu.Lock()
			capture.responses = append(capture.responses, status)
			capture.respondedRows += len(rows)
			capture.mu.Unlock()
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
	rows := make([]map[string]any, 0)
	for _, request := range c.requests {
		rows = append(rows, request.rows...)
	}
	return rows
}

func (c *capture) responseRowCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.respondedRows
}

func (c *capture) setHold() {
	c.holdMu.Lock()
	defer c.holdMu.Unlock()
	require(!c.hold, "HTTP capture is already held")
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
	case <-time.After(20 * time.Second):
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

func (c *capture) setStatus(status int) {
	c.mu.Lock()
	c.status = status
	c.mu.Unlock()
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
	request.Header.Set("Authorization", "Bearer core-a-private-process-fixture")
	request.Header.Set("Content-Type", "application/json")
	if method == http.MethodPut && strings.HasPrefix(path, "/v1/pipelines/") && strings.Count(path, "/") == 3 {
		current, code := a.call(http.MethodGet, path, nil)
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
	if len(raw) != 0 {
		_ = json.Unmarshal(raw, &result)
	}
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
		parsed, err := value.Int64()
		if err == nil && parsed >= 0 {
			return uint64(parsed)
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

func generationText(value any) string {
	switch value := value.(type) {
	case string:
		if len(value) == 32 {
			if raw, err := hex.DecodeString(value); err == nil && len(raw) == 16 && !bytes.Equal(raw, make([]byte, 16)) {
				return value
			}
		}
	case []any:
		if len(value) != 16 {
			return ""
		}
		raw := make([]byte, len(value))
		for i, item := range value {
			n, ok := item.(float64)
			if !ok || n < 0 || n > 255 || n != float64(uint8(n)) {
				return ""
			}
			raw[i] = byte(n)
		}
		if bytes.Equal(raw, make([]byte, 16)) {
			return ""
		}
		return hex.EncodeToString(raw)
	case []uint8:
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
	generation = generationText(checkpoint["state_generation"])
	return number(checkpoint["started_total"]),
		number(checkpoint["succeeded_total"]),
		checkpoint["active"] == true,
		number(checkpoint["last_success_id"]),
		generation
}

func statusReliable(status map[string]any) (restored, published, committed, pending uint64) {
	return number(nested(status, "checkpoint", "reliable_source", "restored_cut")),
		number(nested(status, "checkpoint", "reliable_source", "published_cut")),
		number(nested(status, "checkpoint", "reliable_source", "committed_cut")),
		number(nested(status, "checkpoint", "reliable_source", "pending_messages"))
}

func waitHealth(a api, label string) {
	waitFor(label, func() bool {
		_, code := a.call(http.MethodGet, "/v1/health", nil)
		return code == http.StatusOK
	})
}

func healthWithin(a api, duration time.Duration) bool {
	deadline := time.Now().Add(duration)
	for time.Now().Before(deadline) {
		if _, code := a.call(http.MethodGet, "/v1/health", nil); code == http.StatusOK {
			return true
		}
		time.Sleep(100 * time.Millisecond)
	}
	return false
}

func waitRunning(a api, name string) map[string]any {
	var last map[string]any
	waitFor(name+" running", func() bool {
		var code int
		last, code = a.call(http.MethodGet, "/v1/pipelines/"+name+"/status", nil)
		return code == http.StatusOK && nested(last, "actual", "status") == "running"
	})
	return last
}

func waitCommitted(a api, name string, cut uint64) map[string]any {
	var last map[string]any
	waitFor(name+" committed source cut", func() bool {
		last = a.ok(http.MethodGet, "/v1/pipelines/"+name+"/status", nil)
		_, _, committed, pending := statusReliable(last)
		return committed == cut && pending == 0
	})
	return last
}

func waitPublished(a api, name string, cut uint64) map[string]any {
	var last map[string]any
	waitFor(name+" published source cut", func() bool {
		last = a.ok(http.MethodGet, "/v1/pipelines/"+name+"/status", nil)
		_, published, _, _ := statusReliable(last)
		return published >= cut
	})
	return last
}

func manualCheckpoint(a api, name string) (map[string]any, int) {
	var result map[string]any
	var code int
	for attempt := 0; attempt < 120; attempt++ {
		result, code = a.call(http.MethodPost, "/v1/pipelines/"+name+"/checkpoint", nil)
		if code >= 200 && code < 300 {
			return result, code
		}
		if code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable || code == 0 {
			time.Sleep(25 * time.Millisecond)
			continue
		}
		return result, code
	}
	return result, code
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

type shape struct {
	name          string
	iotKind       string
	order         string
	iotField      string
	emitFirst     bool
	outputField   string
	initial       []int
	tail          []int
	fault         []int
	initialOutput []int
	tailOutput    []int
	faultOutput   []int
}

func shapeFor(name string) shape {
	switch name {
	case "change":
		return shape{
			name: "change", iotKind: "change_detect", order: "iot", iotField: "v", emitFirst: true, outputField: "v",
			initial: []int{10, 10, 11, 11, 12}, tail: []int{12, 13}, fault: []int{15},
			initialOutput: []int{10, 11, 12}, tailOutput: []int{13}, faultOutput: []int{15},
		}
	case "deadband":
		return shape{
			name: "deadband", iotKind: "deadband", order: "iot", iotField: "v", emitFirst: true, outputField: "v",
			initial: []int{10, 10, 11, 11, 12}, tail: []int{12, 14}, fault: []int{16},
			initialOutput: []int{10, 12}, tailOutput: []int{14}, faultOutput: []int{16},
		}
	case "iot-count":
		return shape{
			name: "iot-count", iotKind: "change_detect", order: "iot-count", iotField: "v", emitFirst: true, outputField: "s",
			initial: []int{10, 10, 11, 12}, tail: []int{12, 13}, fault: []int{15, 16},
			initialOutput: []int{21}, tailOutput: []int{25}, faultOutput: []int{31},
		}
	case "count-iot":
		return shape{
			name: "count-iot", iotKind: "change_detect", order: "count-iot", iotField: "s", emitFirst: true, outputField: "s",
			initial: []int{10, 10, 11, 12}, tail: []int{12, 13}, fault: []int{15, 16},
			initialOutput: []int{20, 23}, tailOutput: []int{25}, faultOutput: []int{31},
		}
	case "all-suppressed":
		return shape{
			name: "all-suppressed", iotKind: "change_detect", order: "iot", iotField: "v", emitFirst: false, outputField: "v",
			initial: []int{10, 10, 10}, tail: []int{10, 11}, fault: []int{12},
			initialOutput: []int{}, tailOutput: []int{11}, faultOutput: []int{12},
		}
	default:
		panic("unknown Core-A shape: " + name)
	}
}

func iotConfig(kind, field string, emitFirst bool) map[string]any {
	value := map[string]any{
		"keys": []string{"device_id"}, "fields": []string{field}, "emit_first": emitFirst,
		"ttl_micros": 0, "max_keys": 64, "invalid": "ignore",
	}
	if kind == "deadband" {
		value["invalid"] = "error"
		value["deadband"] = map[string]any{
			"mode": "absolute", "baseline": "last_output", "threshold": 1.0,
		}
	}
	return value
}

func countNode(id, output uint32) map[string]any {
	return map[string]any{
		"id": id, "kind": "window_agg", "keys": []string{"device_id"},
		"window": map[string]any{"kind": "count", "size": 2},
		"aggs": []any{map[string]any{
			"fn": "sum", "expr": map[string]any{"k": "col", "name": "v"}, "alias": "s",
		}},
		"out": []uint32{output},
	}
}

func makeSpec(brokerPort, sinkPort int, checkpoint, consumer string, s shape, resume bool) map[string]any {
	js := map[string]any{
		"servers":   []string{fmt.Sprintf("nats://127.0.0.1:%d", brokerPort)},
		"namespace": "core_a", "stream": "INPUT", "consumer": consumer,
		"ownership_bucket": "OWNERS", "max_pending": 32, "pending_bytes": 262144,
		"pull_messages": 8, "pull_bytes": 73728,
	}
	source := map[string]any{"kind": "jetstream", "inbox_capacity": 8, "jetstream": js}
	sink := map[string]any{
		"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/telemetry", sinkPort),
		"outbox_capacity": 8, "batch_rows": 1, "linger_ms": 0, "max_inflight": 1,
	}
	nodes := []any{map[string]any{"id": uint32(1), "kind": "memory_source", "table": "sensors"}}
	if s.order == "iot-count" {
		nodes[0].(map[string]any)["out"] = []uint32{2}
		nodes = append(nodes,
			map[string]any{"id": uint32(2), "kind": s.iotKind, "iot": iotConfig(s.iotKind, s.iotField, s.emitFirst), "out": []uint32{3}},
			countNode(3, 4),
			map[string]any{"id": uint32(4), "kind": "capture_sink", "name": s.name},
		)
	} else if s.order == "count-iot" {
		nodes[0].(map[string]any)["out"] = []uint32{2}
		nodes = append(nodes,
			countNode(2, 3),
			map[string]any{"id": uint32(3), "kind": s.iotKind, "iot": iotConfig(s.iotKind, s.iotField, s.emitFirst), "out": []uint32{4}},
			map[string]any{"id": uint32(4), "kind": "capture_sink", "name": s.name},
		)
	} else {
		nodes[0].(map[string]any)["out"] = []uint32{2}
		nodes = append(nodes,
			map[string]any{"id": uint32(2), "kind": s.iotKind, "iot": iotConfig(s.iotKind, s.iotField, s.emitFirst), "out": []uint32{3}},
			map[string]any{"id": uint32(3), "kind": "capture_sink", "name": s.name},
		)
	}
	return map[string]any{
		"version": 1, "stream": "sensors", "source": source, "sink": sink,
		"delivery": "checkpointed_at_least_once", "recovery": "aligned",
		"checkpoint_dir": checkpoint,
		"checkpoint": map[string]any{
			// Keep the required periodic field valid but far outside this short
			// run.  source_full remains automatic; the HTTP hold below is the
			// synchronization that prevents it from committing the fault suffix.
			"interval_ms": 86400000, "timeout_ms": 5000, "retain_generations": 3,
			"max_store_bytes": 33554432, "resume_latest": resume,
		},
		"graph": map[string]any{
			"version": 1, "pipeline_id": 700, "revision_id": 1,
			"nodes": nodes,
		},
	}
}

func makeLegacyCountSpec(brokerPort, sinkPort int, checkpoint, consumer string) map[string]any {
	return map[string]any{
		"version": 1, "stream": "sensors",
		"sql": "SELECT SUM(v) AS s FROM sensors GROUP BY COUNT_WINDOW(3)",
		"source": map[string]any{
			"kind": "jetstream", "inbox_capacity": 8,
			"jetstream": map[string]any{
				"servers":   []string{fmt.Sprintf("nats://127.0.0.1:%d", brokerPort)},
				"namespace": "core_a", "stream": "INPUT", "consumer": consumer,
				"ownership_bucket": "OWNERS", "max_pending": 32, "pending_bytes": 262144,
				"pull_messages": 8, "pull_bytes": 73728,
			},
		},
		"sink": map[string]any{
			"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/telemetry", sinkPort),
			"outbox_capacity": 8, "batch_rows": 1, "linger_ms": 0,
		},
		"delivery": "checkpointed_at_least_once", "recovery": "aligned",
		"checkpoint_dir": checkpoint,
		"checkpoint": map[string]any{
			"interval_ms": 86400000, "timeout_ms": 5000, "retain_generations": 3,
			"max_store_bytes": 33554432, "resume_latest": true,
		},
	}
}

func segment(c *capture, from int) []map[string]any {
	rows := c.snapshot()
	require(from >= 0 && from <= len(rows), "invalid HTTP capture segment")
	return rows[from:]
}

func rowsEqual(left, right []map[string]any) bool {
	return bytes.Equal(data(left), data(right))
}

func outputValues(rows []map[string]any, field string) []float64 {
	values := make([]float64, 0, len(rows))
	for _, row := range rows {
		body, ok := row["data"].(map[string]any)
		require(ok, "HTTP output is missing data envelope")
		value, ok := body[field].(float64)
		require(ok, fmt.Sprintf("HTTP output field %s is not numeric: %v", field, row))
		values = append(values, value)
	}
	return values
}

func requireValues(rows []map[string]any, field string, expected []int) {
	actual := outputValues(rows, field)
	require(len(actual) == len(expected), fmt.Sprintf("output count=%d want=%d", len(actual), len(expected)))
	for i, want := range expected {
		require(actual[i] == float64(want), fmt.Sprintf("output value[%d]=%v want=%d", i, actual[i], want))
	}
}

func verifyOutputIDs(rows []map[string]any, generation string) map[string]map[string]any {
	seen := make(map[string]map[string]any)
	for _, row := range rows {
		id, ok := row["id"].(string)
		require(ok && len(id) == 48 && strings.ToLower(id) == id, "output ID is not lowercase 24-byte hex")
		decoded, err := hex.DecodeString(id)
		require(err == nil && len(decoded) == 24, "output ID encoding is invalid")
		if generation != "" {
			require(id[:32] == generation, "output ID epoch differs from durable state generation")
		}
		body, ok := row["data"].(map[string]any)
		require(ok, "output data envelope is missing")
		if previous, exists := seen[id]; exists {
			require(rowsEqual([]map[string]any{{"data": previous}}, []map[string]any{{"data": body}}),
				"same output ID has conflicting content")
		} else {
			seen[id] = body
		}
	}
	return seen
}

func requireOutputOrdinals(rows []map[string]any, generation string, first uint64) {
	for offset, row := range rows {
		id, ok := row["id"].(string)
		require(ok && len(id) == 48, "output ordinal check requires a valid ID")
		decoded, err := hex.DecodeString(id)
		require(err == nil && len(decoded) == 24, "output ordinal ID encoding is invalid")
		require(generation == "" || id[:32] == generation, "output ordinal epoch differs from state generation")
		ordinal := binary.BigEndian.Uint64(decoded[16:])
		require(ordinal == first+uint64(offset), fmt.Sprintf("output ordinal=%d want=%d", ordinal, first+uint64(offset)))
	}
}

func mutateSemantic(spec map[string]any, s shape) map[string]any {
	changed := clone(spec).(map[string]any)
	graph := changed["graph"].(map[string]any)
	nodes := graph["nodes"].([]any)
	iotIndex := 1
	if s.order == "count-iot" {
		iotIndex = 2
	}
	iot := nodes[iotIndex].(map[string]any)["iot"].(map[string]any)
	if s.iotKind == "deadband" {
		iot["deadband"].(map[string]any)["threshold"] = 2.0
	} else {
		iot["emit_first"] = !s.emitFirst
	}
	return changed
}

func clone(value any) any {
	var result any
	must(json.Unmarshal(data(value), &result))
	return result
}

type callResult struct {
	result map[string]any
	code   int
}

func oldProfileCheck(root, oldBinary string, brokerPort int, sink *capture, consumer string, checkpoint string) map[string]any {
	legacyRoot := filepath.Join(root, "legacy-v7-root")
	legacyCheckpoint := filepath.Join(legacyRoot, "checkpoint-v7")
	must(os.MkdirAll(legacyRoot, 0700))
	copyTree(checkpoint, legacyCheckpoint)
	beforeTree := treeHash(legacyCheckpoint)
	beforeCurrent := hash(filepath.Join(legacyCheckpoint, "CURRENT"))
	beforeOutputs := len(sink.snapshot())
	port := freePort()
	legacyAPI := api{base: fmt.Sprintf("http://127.0.0.1:%d", port), client: &http.Client{Timeout: 2 * time.Second}}
	command := launch(oldBinary, legacyRoot, filepath.Join(root, "old-server.log"), "--bind", fmt.Sprintf("127.0.0.1:%d", port), "--catalog", filepath.Join(legacyRoot, "catalog.db"), "--max-jobs", "1", "--safe-mode")
	defer command.stop(syscall.SIGTERM)
	healthy := healthWithin(legacyAPI, 8*time.Second)
	require(healthy, "old K4JS server did not become healthy for v7 profile guard")
	legacyAPI.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": brokerPort})
	legacyAPI.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": sink.port()})
	legacyAPI.ok(http.MethodPut, "/v1/streams/sensors", map[string]any{"fields": []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "v", "type": "int64", "nullable": false},
	}})
	legacy := makeLegacyCountSpec(brokerPort, sink.port(), legacyCheckpoint, consumer)
	save(filepath.Join(root, "old-v7-count-spec.json"), legacy)
	legacyAPI.ok(http.MethodPut, "/v1/pipelines/old-count", legacy)
	startResult, startCode := legacyAPI.call(http.MethodPost, "/v1/pipelines/old-count/start", map[string]any{})
	var oldStatus map[string]any
	if startCode >= 200 && startCode < 300 {
		waitFor("old binary v7 profile rejection", func() bool {
			var code int
			oldStatus, code = legacyAPI.call(http.MethodGet, "/v1/pipelines/old-count/status", nil)
			return code == http.StatusOK && nested(oldStatus, "actual", "status") == "failed"
		})
	}
	command.stop(syscall.SIGTERM)
	errorText := strings.ToLower(fmt.Sprint(nested(oldStatus, "actual", "last_error")))
	errorText += " " + strings.ToLower(fmt.Sprint(nested(startResult, "error", "message")))
	guard := strings.Contains(errorText, "checkpoint source profile mismatch")
	require(guard, fmt.Sprintf("old binary did not reach v7 profile guard: code=%d status=%v start=%v", startCode, oldStatus, startResult))
	require(treeHash(legacyCheckpoint) == beforeTree, "old binary changed v7 checkpoint history")
	require(hash(filepath.Join(legacyCheckpoint, "CURRENT")) == beforeCurrent, "old binary changed v7 CURRENT")
	require(len(sink.snapshot()) == beforeOutputs, "old binary emitted outputs from an incompatible v7 checkpoint")
	evidence := map[string]any{
		"checked": true, "healthy": healthy, "start_code": startCode,
		"error_oracle": "checkpoint source profile mismatch", "error_response": errorText,
		"history_preserved": true, "current_preserved": true, "outputs_unchanged": true,
	}
	save(filepath.Join(root, "old-v7-profile-evidence.json"), evidence)
	return evidence
}

func runScenario(root, serverBin, natsBin, oldBinary string, s shape) {
	must(os.Mkdir(root, 0700))
	broker, producer, brokerPort := startBroker(root, natsBin)
	defer producer.conn.Close()
	defer broker.stop(syscall.SIGKILL)

	sink := newCapture()
	defer sink.server.Close()
	defer func() {
		sink.releaseHold()
		save(filepath.Join(root, "capture-final.json"), map[string]any{
			"rows": sink.snapshot(), "responded_rows": sink.responseRowCount(),
		})
	}()
	serverPort := freePort()
	a := api{base: fmt.Sprintf("http://127.0.0.1:%d", serverPort), client: &http.Client{Timeout: 15 * time.Second}}
	checkpoint := filepath.Join(root, "checkpoint-v7")
	spec := makeSpec(brokerPort, sink.port(), checkpoint, "core_a_"+s.name, s, true)
	save(filepath.Join(root, "spec.json"), spec)

	var server *child
	start := func() {
		server = launch(serverBin, root, filepath.Join(root, "server.log"), "--bind", fmt.Sprintf("127.0.0.1:%d", serverPort), "--catalog", filepath.Join(root, "catalog.db"), "--max-jobs", "1", "--safe-mode")
		waitHealth(a, "Core-A server health")
	}
	stop := func(signal os.Signal) {
		if server != nil {
			server.stop(signal)
			server = nil
		}
	}
	defer stop(syscall.SIGKILL)

	start()
	a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": brokerPort})
	a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": sink.port()})
	a.ok(http.MethodPut, "/v1/streams/sensors", map[string]any{"fields": []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "v", "type": "int64", "nullable": false},
	}})
	save(filepath.Join(root, "explain.json"), a.ok(http.MethodPost, "/v1/explain", spec))
	a.ok(http.MethodPut, "/v1/pipelines/core", spec)
	a.ok(http.MethodPost, "/v1/pipelines/core/start", map[string]any{})
	status := func() map[string]any { return a.ok(http.MethodGet, "/v1/pipelines/core/status", nil) }
	waitRunning(a, "core")
	waitFor("v7 bootstrap checkpoint", func() bool {
		return number(nested(status(), "checkpoint", "last_success_id")) > 0
	})

	// Initial output deliberately contains suppressed rows.  The source cut
	// below must nevertheless cover every input, not only emitted outputs.
	publishRows(producer, s.initial, len(s.initial))
	waitFor("initial Core-A outputs", func() bool { return len(sink.snapshot()) == len(s.initialOutput) })
	waitFor("initial HTTP responses", func() bool { return sink.responseRowCount() == len(s.initialOutput) })
	initialRows := sink.snapshot()
	requireValues(initialRows, s.outputField, s.initialOutput)
	waitPublished(a, "core", uint64(len(s.initial)))
	baselineCheckpoint, baselineCode := manualCheckpoint(a, "core")
	require(baselineCode >= 200 && baselineCode < 300,
		fmt.Sprintf("v7 baseline checkpoint failed: %d %v", baselineCode, baselineCheckpoint))
	initialStatus := waitCommitted(a, "core", uint64(len(s.initial)))
	initialCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	initialInventory := a.ok(http.MethodGet, "/v1/pipelines/core/checkpoints", nil)
	requireCurrentSnapshotVersion(initialInventory, 7)
	_, _, _, initialID, initialGeneration := checkpointFields(initialStatus)
	require(initialID > 0 && initialGeneration != "", "v7 baseline lacks checkpoint identity")
	verifyOutputIDs(initialRows, initialGeneration)
	requireOutputOrdinals(initialRows, initialGeneration, 1)
	initialInfo := consumerInfo(producer)
	require(number(initialInfo["num_ack_pending"]) == 0 && number(nested(initialInfo, "ack_floor", "stream_seq")) == uint64(len(s.initial)),
		"suppressed source rows were not ACKed after the durable v7 baseline")
	save(filepath.Join(root, "baseline-status.json"), initialStatus)
	save(filepath.Join(root, "baseline-checkpoints.json"), initialInventory)
	save(filepath.Join(root, "baseline-broker.json"), initialInfo)

	// Hold the first post-cut HTTP body before it can return 2xx.  This both
	// prevents source_full's automatic checkpoint from committing the suffix
	// and gives a deterministic SIGKILL cut without a timing sleep race.
	beforeTail := len(sink.snapshot())
	beforeTailResponses := sink.responseRowCount()
	sink.setHold()
	publishRows(producer, s.tail, len(s.initial)+len(s.tail))
	waitFor("held uncommitted output body", func() bool { return len(sink.snapshot()) == beforeTail+len(s.tailOutput) })
	tailObserved := append([]map[string]any(nil), segment(sink, beforeTail)...)
	requireValues(tailObserved, s.outputField, s.tailOutput)
	requireOutputOrdinals(tailObserved, initialGeneration, uint64(len(s.initialOutput)+1))
	require(sink.responseRowCount() == beforeTailResponses, "HTTP-held body returned a response before the checkpoint cut")
	beforeHold := status()
	_, beforeHoldSucceeded, _, _, _ := checkpointFields(beforeHold)
	checkpointChannel := make(chan callResult, 1)
	go func() {
		result, code := a.call(http.MethodPost, "/v1/pipelines/core/checkpoint", nil)
		checkpointChannel <- callResult{result: result, code: code}
	}()
	waitFor("HTTP-held checkpoint active", func() bool {
		current := status()
		_, succeeded, active, last, _ := checkpointFields(current)
		_, _, committed, _ := statusReliable(current)
		return active && succeeded == beforeHoldSucceeded && last == initialID && committed == uint64(len(s.initial)) &&
			hash(filepath.Join(checkpoint, "CURRENT")) == initialCurrent
	})
	holdInfo := consumerInfo(producer)
	require(number(nested(holdInfo, "ack_floor", "stream_seq")) == uint64(len(s.initial)) &&
		number(holdInfo["num_ack_pending"]) == uint64(len(s.tail)),
		"HTTP-held uncommitted suffix crossed the JetStream ACK cut")
	save(filepath.Join(root, "http-held-status.json"), map[string]any{
		"pipeline": status(), "responded_rows_before_release": sink.responseRowCount(),
	})
	stop(syscall.SIGKILL)
	// The separate capture server can now finish the abandoned HTTP handler;
	// its already-recorded body remains the pre-restart duplicate oracle.
	sink.releaseHold()
	select {
	case result := <-checkpointChannel:
		save(filepath.Join(root, "held-checkpoint-response.json"), result)
	case <-time.After(2 * time.Second):
		// Killing the server may close the API request after the evidence cut.
	}
	replayStart := len(sink.snapshot())
	start()
	a.ok(http.MethodPost, "/v1/pipelines/core/start", map[string]any{})
	waitFor("v7 restore from baseline", func() bool {
		current := waitRunning(a, "core")
		return number(nested(current, "checkpoint", "restored_from_checkpoint")) == initialID
	})
	waitFor("uncommitted IoT suffix replay", func() bool { return len(sink.snapshot()) == replayStart+len(s.tailOutput) })
	replayedTail := segment(sink, replayStart)
	require(rowsEqual(tailObserved, replayedTail), "uncommitted suffix replay changed output IDs or values")
	requireValues(replayedTail, s.outputField, s.tailOutput)
	requireOutputOrdinals(replayedTail, initialGeneration, uint64(len(s.initialOutput)+1))
	waitFor("replayed HTTP responses", func() bool { return sink.responseRowCount() == len(s.initialOutput)+len(s.tailOutput)*2 })
	replayCheckpoint, replayCode := manualCheckpoint(a, "core")
	require(replayCode >= 200 && replayCode < 300, fmt.Sprintf("replay checkpoint failed: %d %v", replayCode, replayCheckpoint))
	replayStatus := waitCommitted(a, "core", uint64(len(s.initial)+len(s.tail)))
	_, _, _, committedID, _ := checkpointFields(replayStatus)
	require(committedID > initialID, "v7 restore checkpoint did not advance")
	committedInfo := consumerInfo(producer)
	require(number(committedInfo["num_ack_pending"]) == 0 &&
		number(nested(committedInfo, "ack_floor", "stream_seq")) == uint64(len(s.initial)+len(s.tail)),
		"replayed suppressed/changed rows were not ACKed after v7 commit")
	save(filepath.Join(root, "replay-status.json"), replayStatus)
	save(filepath.Join(root, "replay-broker.json"), committedInfo)

	// A second held body is released only after CURRENT.tmp is installed.  The
	// body may be accepted by HTTP, but the failed publication must not advance
	// CURRENT or the broker ACK floor.
	faultStart := len(sink.snapshot())
	faultResponses := sink.responseRowCount()
	faultCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	_, _, _, faultBaselineID, _ := checkpointFields(replayStatus)
	sink.setHold()
	publishRows(producer, s.fault, len(s.initial)+len(s.tail)+len(s.fault))
	waitFor("held CURRENT.tmp output body", func() bool { return len(sink.snapshot()) == faultStart+len(s.faultOutput) })
	faultObserved := append([]map[string]any(nil), segment(sink, faultStart)...)
	requireValues(faultObserved, s.outputField, s.faultOutput)
	requireOutputOrdinals(faultObserved, initialGeneration, uint64(len(s.initialOutput)+len(s.tailOutput)+1))
	require(sink.responseRowCount() == faultResponses, "CURRENT.tmp held body returned a response before publication failure")
	// The held output is the complete independently expected fault suffix.
	// Do not wait for the 5 s sampled published_cut while holding a request
	// whose production HTTP timeout is shorter; that would inject a second,
	// unrelated failure before CURRENT.tmp can be exercised.
	must(os.Mkdir(filepath.Join(checkpoint, "CURRENT.tmp"), 0700))
	faultBefore := status()
	_, faultBeforeSucceeded, _, _, _ := checkpointFields(faultBefore)
	faultCheckpointChannel := make(chan callResult, 1)
	go func() {
		result, code := a.call(http.MethodPost, "/v1/pipelines/core/checkpoint", nil)
		faultCheckpointChannel <- callResult{result: result, code: code}
	}()
	waitFor("CURRENT.tmp checkpoint active", func() bool {
		current := status()
		_, succeeded, active, _, _ := checkpointFields(current)
		return active && succeeded == faultBeforeSucceeded && hash(filepath.Join(checkpoint, "CURRENT")) == faultCurrent
	})
	faultHeldInfo := consumerInfo(producer)
	require(number(nested(faultHeldInfo, "ack_floor", "stream_seq")) == uint64(len(s.initial)+len(s.tail)) &&
		number(faultHeldInfo["num_ack_pending"]) == uint64(len(s.fault)),
		"CURRENT.tmp checkpoint crossed the broker ACK cut before publication")
	sink.releaseHold()
	var faultResult callResult
	select {
	case faultResult = <-faultCheckpointChannel:
		save(filepath.Join(root, "current-tmp-checkpoint-response.json"), faultResult)
	case <-time.After(2 * time.Second):
		faultResult = callResult{code: 0}
	}
	require(faultResult.code == 0 || faultResult.code >= 400, fmt.Sprintf("CURRENT.tmp checkpoint returned success: %v", faultResult))
	require(hash(filepath.Join(checkpoint, "CURRENT")) == faultCurrent, "CURRENT.tmp failure changed CURRENT")
	var faultFailedStatus map[string]any
	waitFor("CURRENT.tmp checkpoint failure", func() bool {
		current := status()
		faultFailedStatus = current
		actualFailed := nested(current, "actual", "status") == "failed"
		errText := strings.ToLower(fmt.Sprint(nested(current, "actual", "last_error")))
		return actualFailed && strings.Contains(errText, "checkpoint io:") && strings.Contains(errText, "is a directory")
	})
	faultAfterInfo := consumerInfo(producer)
	require(number(nested(faultAfterInfo, "ack_floor", "stream_seq")) == uint64(len(s.initial)+len(s.tail)) &&
		number(faultAfterInfo["num_ack_pending"]) == uint64(len(s.fault)),
		"CURRENT.tmp failure ACKed an uncommitted suffix")
	save(filepath.Join(root, "current-tmp-status.json"), faultFailedStatus)
	save(filepath.Join(root, "current-tmp-broker.json"), faultAfterInfo)
	must(os.Remove(filepath.Join(checkpoint, "CURRENT.tmp")))
	stop(syscall.SIGKILL)
	faultReplayStart := len(sink.snapshot())
	start()
	a.ok(http.MethodPost, "/v1/pipelines/core/start", map[string]any{})
	waitFor("CURRENT.tmp suffix restore", func() bool {
		current := waitRunning(a, "core")
		return number(nested(current, "checkpoint", "restored_from_checkpoint")) == faultBaselineID
	})
	waitFor("CURRENT.tmp suffix replay", func() bool { return len(sink.snapshot()) == faultReplayStart+len(s.faultOutput) })
	faultReplay := segment(sink, faultReplayStart)
	require(rowsEqual(faultObserved, faultReplay), "CURRENT.tmp suffix replay changed output IDs or values")
	requireValues(faultReplay, s.outputField, s.faultOutput)
	requireOutputOrdinals(faultReplay, initialGeneration, uint64(len(s.initialOutput)+len(s.tailOutput)+1))
	finalCheckpoint, finalCode := manualCheckpoint(a, "core")
	require(finalCode >= 200 && finalCode < 300, fmt.Sprintf("final v7 checkpoint failed: %d %v", finalCode, finalCheckpoint))
	finalStatus := waitCommitted(a, "core", uint64(len(s.initial)+len(s.tail)+len(s.fault)))
	_, _, _, _, finalGeneration := checkpointFields(finalStatus)
	finalInfo := consumerInfo(producer)
	require(number(finalInfo["num_ack_pending"]) == 0 &&
		number(nested(finalInfo, "ack_floor", "stream_seq")) == uint64(len(s.initial)+len(s.tail)+len(s.fault)),
		"final v7 source cut did not ACK all input rows")
	finalCurrent := hash(filepath.Join(checkpoint, "CURRENT"))
	finalRows := sink.snapshot()
	verifyOutputIDs(finalRows, finalGeneration)
	save(filepath.Join(root, "final-status.json"), finalStatus)
	save(filepath.Join(root, "final-broker.json"), finalInfo)
	save(filepath.Join(root, "outputs.json"), finalRows)

	// A semantic change is a restore incompatibility, not an implicit fresh
	// start.  It must fail before replacing CURRENT or producing output.
	semantic := mutateSemantic(spec, s)
	save(filepath.Join(root, "semantic-change-spec.json"), semantic)
	a.ok(http.MethodPut, "/v1/pipelines/core", semantic)
	startResult, startCode := a.call(http.MethodPost, "/v1/pipelines/core/start", map[string]any{})
	var semanticStatus map[string]any
	if startCode >= 200 && startCode < 300 {
		waitFor("semantic v7 restore refusal", func() bool {
			var code int
			semanticStatus, code = a.call(http.MethodGet, "/v1/pipelines/core/status", nil)
			return code == http.StatusOK && nested(semanticStatus, "actual", "status") == "failed"
		})
	} else {
		semanticStatus = startResult
	}
	semanticError := strings.ToLower(fmt.Sprint(nested(semanticStatus, "actual", "last_error")))
	semanticError += " " + strings.ToLower(fmt.Sprint(nested(startResult, "error", "message")))
	require(strings.Contains(semanticError, "semantic"),
		fmt.Sprintf("semantic v7 change was not refused: code=%d status=%v start=%v", startCode, semanticStatus, startResult))
	require(hash(filepath.Join(checkpoint, "CURRENT")) == finalCurrent, "semantic refusal changed CURRENT")
	require(len(sink.snapshot()) == len(finalRows), "semantic refusal produced output")
	save(filepath.Join(root, "semantic-change-status.json"), semanticStatus)

	oldEvidence := map[string]any{"checked": false, "reason": "old K4JS server binary not supplied"}
	if oldBinary != "" && s.name == "change" {
		oldEvidence = oldProfileCheck(root, oldBinary, brokerPort, sink, "core_a_"+s.name, checkpoint)
	}
	save(filepath.Join(root, "summary.json"), map[string]any{
		"valid": true, "profile": "reliable_iot_v7", "shape": s.name,
		"operator": s.iotKind, "order": s.order, "ttl_micros": 0,
		"input_rows":                            len(s.initial) + len(s.tail) + len(s.fault),
		"initial_suppressed_rows":               len(s.initial) - len(s.initialOutput),
		"durable_ack_includes_suppressed_rows":  true,
		"http_held_without_2xx_cannot_commit":   true,
		"uncommitted_sigkill_replayed_same_ids": true,
		"current_tmp_failure_preserved_ack_cut": true,
		"semantic_change_refused":               true,
		"snapshot_version":                      7,
		"old_k4js_profile_guard":                oldEvidence,
		"certified":                             false,
	})
	stop(syscall.SIGTERM)
	fmt.Println("CORE_A_PROCESS_OK", s.name)
}

func main() {
	serverBin := flag.String("server-bin", "", "v7 reliable-IoT production server")
	natsBin := flag.String("nats-server", "", "pinned NATS Server binary")
	oldBinary := flag.String("old-server-bin", "", "optional pre-v7 JetStream server for profile rejection")
	out := flag.String("out", "", "new evidence directory")
	flag.Parse()
	require(*serverBin != "" && *natsBin != "" && *out != "", "server-bin, nats-server, and out are required")

	root, err := filepath.Abs(*out)
	must(err)
	must(os.Mkdir(root, 0700))
	self, err := os.Executable()
	must(err)
	save(filepath.Join(root, "binaries.json"), map[string]any{
		"server_sha256": hash(*serverBin), "nats_sha256": hash(*natsBin), "driver_sha256": hash(self),
	})
	for _, name := range []string{"change", "deadband", "iot-count", "count-iot", "all-suppressed"} {
		runScenario(filepath.Join(root, name), *serverBin, *natsBin, *oldBinary, shapeFor(name))
	}
}
