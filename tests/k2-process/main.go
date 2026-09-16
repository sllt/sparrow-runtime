// Isolated process-fault oracle. Standard library only; no system service,
// firewall, qdisc or existing broker is changed. Build on the test server.
package main

import (
	"bufio"
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
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
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
func encode(v any) []byte     { b, e := json.Marshal(v); must(e); return b }
func save(path string, v any) { must(os.WriteFile(path, append(encode(v), '\n'), 0600)) }
func hash(path string) string {
	f, e := os.Open(path)
	must(e)
	defer f.Close()
	h := sha256.New()
	_, e = io.Copy(h, f)
	must(e)
	return hex.EncodeToString(h.Sum(nil))
}
func port() int {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	p := l.Addr().(*net.TCPAddr).Port
	must(l.Close())
	return p
}

type child struct {
	cmd *exec.Cmd
	log *os.File
}

func launch(binary, logPath string, env []string, args ...string) *child {
	log, e := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(e)
	c := exec.Command(binary, args...)
	c.Env = append(os.Environ(), env...)
	c.Stdout = log
	c.Stderr = log
	must(c.Start())
	return &child{c, log}
}
func (c *child) stop(signal os.Signal) {
	if c == nil || c.cmd == nil {
		return
	}
	_ = c.cmd.Process.Signal(signal)
	done := make(chan struct{})
	go func() { _ = c.cmd.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		_ = c.cmd.Process.Kill()
		<-done
	}
	_ = c.log.Close()
	c.cmd = nil
}
func eventually(label string, fn func() bool) {
	until := time.Now().Add(10 * time.Second)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("deadline: " + label)
}

// Fixture-only Core NATS request/reply. One bounded response at a time; all
// connection targets are listeners created by this driver on loopback.
type nats struct {
	conn   net.Conn
	in     *bufio.Reader
	serial uint64
}

func dialNATS(p int) *nats {
	var conn net.Conn
	eventually("isolated NATS ready", func() bool {
		var e error
		conn, e = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", p), 100*time.Millisecond)
		return e == nil
	})
	n := &nats{conn: conn, in: bufio.NewReader(conn)}
	_, e := fmt.Fprint(conn, "CONNECT {\"verbose\":false,\"pedantic\":true,\"lang\":\"go-k2-fixture\",\"version\":\"1\"}\r\n")
	must(e)
	return n
}
func (n *nats) request(subject string, payload []byte) map[string]any {
	n.serial++
	inbox := fmt.Sprintf("_INBOX.k2fixture.%d.%d", os.Getpid(), n.serial)
	must(n.conn.SetDeadline(time.Now().Add(5 * time.Second)))
	_, e := fmt.Fprintf(n.conn, "SUB %s 1\r\nUNSUB 1 1\r\nPUB %s %s %d\r\n", inbox, subject, inbox, len(payload))
	must(e)
	_, e = n.conn.Write(append(payload, '\r', '\n'))
	must(e)
	for {
		line, e := n.in.ReadString('\n')
		must(e)
		require(len(line) <= 65536, "NATS fixture header bound")
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "PING":
			_, e = fmt.Fprint(n.conn, "PONG\r\n")
			must(e)
		case "-ERR":
			panic("isolated NATS error: " + line)
		case "MSG":
			require(len(fields) >= 4, "NATS MSG framing")
			size, e := strconv.Atoi(fields[len(fields)-1])
			must(e)
			require(size >= 0 && size <= 1024*1024, "NATS response bound")
			body := make([]byte, size+2)
			_, e = io.ReadFull(n.in, body)
			must(e)
			require(fields[1] == inbox, "NATS fixture reply mismatch")
			var value map[string]any
			must(json.Unmarshal(body[:size], &value))
			if problem := value["error"]; problem != nil {
				panic(fmt.Sprintf("NATS API error: %v", problem))
			}
			return value
		}
	}
}

// Drop ONLY source +ACK commands in the selected fault phase. Requests,
// progress, stream/KV operations and producer PubAcks keep their normal flow.
type ackProxy struct {
	listener net.Listener
	target   int
	drop     atomic.Bool
	dropped  atomic.Uint64
	wg       sync.WaitGroup
	mu       sync.Mutex
	conns    []net.Conn
	closed   bool
}

func newProxy(target int) *ackProxy {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	p := &ackProxy{listener: l, target: target}
	p.wg.Add(1)
	go func() {
		defer p.wg.Done()
		for {
			client, e := l.Accept()
			if e != nil {
				return
			}
			p.wg.Add(1)
			go p.handle(client)
		}
	}()
	return p
}
func (p *ackProxy) handle(client net.Conn) {
	defer p.wg.Done()
	defer client.Close()
	server, e := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", p.target))
	if e != nil {
		return
	}
	defer server.Close()
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.conns = append(p.conns, client, server)
	p.mu.Unlock()
	done := make(chan struct{})
	go func() { _, _ = io.Copy(client, server); _ = client.Close(); close(done) }()
	in := bufio.NewReader(client)
	for {
		line, e := in.ReadString('\n')
		if e != nil {
			break
		}
		if len(line) > 65536 {
			break
		}
		fields := strings.Fields(line)
		if len(fields) > 2 && (fields[0] == "PUB" || fields[0] == "HPUB") {
			n, e := strconv.Atoi(fields[len(fields)-1])
			if e != nil || n < 0 || n > 1024*1024 {
				break
			}
			body := make([]byte, n+2)
			if _, e = io.ReadFull(in, body); e != nil {
				break
			}
			if p.drop.Load() && strings.HasPrefix(fields[1], "$JS.ACK.") && bytes.Equal(body[:n], []byte("+ACK")) {
				p.dropped.Add(1)
				continue
			}
			if _, e = io.WriteString(server, line); e != nil {
				break
			}
			if _, e = server.Write(body); e != nil {
				break
			}
		} else {
			if _, e = io.WriteString(server, line); e != nil {
				break
			}
		}
	}
	_ = server.Close()
	_ = client.Close()
	<-done
}
func (p *ackProxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	p.closed = true
	for _, c := range p.conns {
		_ = c.Close()
	}
	p.mu.Unlock()
	p.wg.Wait()
}
func (p *ackProxy) port() int { return p.listener.Addr().(*net.TCPAddr).Port }

type capture struct {
	mu       sync.Mutex
	rows     []map[string]any
	received []time.Time
	server   *http.Server
	listener net.Listener
}

func newCapture() *capture {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	c := &capture{listener: l}
	c.server = &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, e := io.ReadAll(io.LimitReader(r.Body, 1024*1024+1))
		if e != nil || len(body) > 1024*1024 {
			w.WriteHeader(413)
			return
		}
		var rows []map[string]any
		if json.Unmarshal(body, &rows) != nil {
			w.WriteHeader(400)
			return
		}
		c.mu.Lock()
		c.rows = append(c.rows, rows...)
		for range rows {
			c.received = append(c.received, time.Now())
		}
		c.mu.Unlock()
		w.WriteHeader(200)
	})}
	go func() { _ = c.server.Serve(l) }()
	return c
}
func (c *capture) snapshot() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.rows...)
}
func (c *capture) port() int { return c.listener.Addr().(*net.TCPAddr).Port }

type apiClient struct {
	base   string
	client *http.Client
}

func (a apiClient) call(method, path string, body any) (map[string]any, int) {
	var payload io.Reader
	if body != nil {
		payload = bytes.NewReader(encode(body))
	}
	r, e := http.NewRequest(method, a.base+path, payload)
	if e != nil {
		return nil, 0
	}
	r.Header.Set("Authorization", "Bearer k2-private-process-fixture")
	r.Header.Set("Content-Type", "application/json")
	response, e := a.client.Do(r)
	if e != nil {
		return nil, 0
	}
	defer response.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(response.Body, 2*1024*1024))
	if e != nil {
		return nil, response.StatusCode
	}
	var value map[string]any
	_ = json.Unmarshal(raw, &value)
	return value, response.StatusCode
}
func (a apiClient) ok(method, path string, body any) map[string]any {
	v, status := a.call(method, path, body)
	require(status >= 200 && status < 300, fmt.Sprintf("API %s %s returned %d: %v", method, path, status, v))
	return v
}
func number(value map[string]any, keys ...string) float64 {
	var v any = value
	for _, key := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return -1
		}
		v = m[key]
	}
	n, _ := v.(float64)
	return n
}
func text(value map[string]any, keys ...string) string {
	var v any = value
	for _, key := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return ""
		}
		v = m[key]
	}
	s, _ := v.(string)
	return s
}

func scenario(root, shape, serverBin, natsBin string) {
	must(os.Mkdir(root, 0700))
	natsPort, serverPort := port(), port()
	config := fmt.Sprintf("host: 127.0.0.1\nport: %d\nmax_payload: 65536\njetstream {\nstore_dir: %q\nmax_file_store: 256MB\nmax_memory_store: 16MB\nsync_interval: always\n}\n", natsPort, filepath.Join(root, "broker-data"))
	must(os.WriteFile(filepath.Join(root, "nats.conf"), []byte(config), 0600))
	broker := launch(natsBin, filepath.Join(root, "broker.log"), nil, "-c", filepath.Join(root, "nats.conf"))
	defer broker.stop(syscall.SIGKILL)
	producer := dialNATS(natsPort)
	defer producer.conn.Close()
	inputConfig := map[string]any{"name": "INPUT", "subjects": []string{"input.rows"}, "storage": "file", "retention": "limits", "max_bytes": 16 * 1024 * 1024, "max_msg_size": 65536, "max_consumers": 32, "num_replicas": 1, "deny_delete": true, "deny_purge": true}
	producer.request("$JS.API.STREAM.CREATE.INPUT", encode(inputConfig))
	producer.request("$JS.API.STREAM.CREATE.KV_OWNERS", encode(map[string]any{"name": "KV_OWNERS", "subjects": []string{"$KV.OWNERS.>"}, "storage": "file", "retention": "limits", "max_bytes": 1024 * 1024, "max_msg_size": 1024, "max_msgs_per_subject": 1, "num_replicas": 1, "discard": "new", "allow_direct": true, "allow_rollup_hdrs": true}))
	proxy := newProxy(natsPort)
	defer proxy.close()
	sink := newCapture()
	defer sink.server.Close()
	a := apiClient{fmt.Sprintf("http://127.0.0.1:%d", serverPort), &http.Client{Timeout: 5 * time.Second}}
	env := []string{"SPARROW_TOKEN=k2-private-process-fixture", "SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef", "SPARROW_REQUIRE_SECRETS_KEY=1", "SPARROW_DATA_ROOTS=" + root}
	var server *child
	defer func() { server.stop(syscall.SIGKILL) }()
	start := func() {
		server = launch(serverBin, filepath.Join(root, "server.log"), env, "--bind", fmt.Sprintf("127.0.0.1:%d", serverPort), "--catalog", filepath.Join(root, "catalog.db"), "--max-jobs", "1", "--safe-mode")
		eventually("server health", func() bool { _, s := a.call("GET", "/v1/health", nil); return s == 200 })
	}
	status := func() map[string]any { return a.ok("GET", "/v1/pipelines/check/status", nil) }
	start()
	a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": proxy.port()})
	a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": sink.port()})
	a.ok("PUT", "/v1/streams/sensors", map[string]any{"fields": []any{map[string]any{"name": "device_id", "type": "utf8", "nullable": false}, map[string]any{"name": "v", "type": "int64", "nullable": false}}})
	checkpointDir := filepath.Join(root, "checkpoint")
	spec := map[string]any{"stream": "sensors", "sql": "SELECT device_id, v FROM sensors", "source": map[string]any{"kind": "jetstream", "jetstream": map[string]any{
		"servers": []string{fmt.Sprintf("nats://127.0.0.1:%d", proxy.port())}, "namespace": "process_fixture", "stream": "INPUT", "consumer": "check", "ownership_bucket": "OWNERS", "max_pending": 128, "pull_messages": 8}},
		"sink":     map[string]any{"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/output", sink.port()), "batch_rows": 64, "linger_ms": 5},
		"recovery": "aligned", "delivery": "checkpointed_at_least_once", "checkpoint_dir": checkpointDir,
		"checkpoint": map[string]any{"interval_ms": 86400000, "timeout_ms": 2000, "resume_latest": true}}
	expected := 6
	initial := 6
	field := "v"
	if shape == "two-count" {
		delete(spec, "sql")
		expected = 1
		initial = 7
		field = "total"
		spec["graph"] = map[string]any{"version": 1, "pipeline_id": 1, "revision_id": 1, "nodes": []any{
			map[string]any{"id": 1, "kind": "memory_source", "table": "sensors", "out": []int{10}},
			map[string]any{"id": 10, "kind": "window_agg", "keys": []string{"device_id"}, "window": map[string]any{"kind": "count", "size": 3}, "aggs": []any{map[string]any{"fn": "sum", "expr": map[string]any{"k": "col", "name": "v"}, "alias": "s"}}, "out": []int{11}},
			map[string]any{"id": 11, "kind": "window_agg", "keys": []string{"device_id"}, "window": map[string]any{"kind": "count", "size": 2}, "aggs": []any{map[string]any{"fn": "sum", "expr": map[string]any{"k": "col", "name": "s"}, "alias": "total"}}, "out": []int{20}},
			map[string]any{"id": 20, "kind": "capture_sink", "name": "out"}}}
	}
	save(filepath.Join(root, "spec.json"), spec)
	save(filepath.Join(root, "explain.json"), a.ok("POST", "/v1/explain", spec))
	a.ok("PUT", "/v1/pipelines/check", spec)
	a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	eventually("durable empty bootstrap", func() bool { return number(status(), "checkpoint", "last_success_id") >= 1 })
	initialCurrent := hash(filepath.Join(checkpointDir, "CURRENT"))
	publish := func(from, to int) {
		for i := from; i <= to; i++ {
			producer.request("input.rows", encode(map[string]any{"device_id": "d1", "v": i}))
		}
	}
	publish(1, initial)
	eventually("first required output", func() bool { return len(sink.snapshot()) == expected })
	first := sink.snapshot()
	eventually("initial input prefix applied", func() bool {
		return number(status(), "observation", "runtime_progress", "ingested_rows") == float64(initial)
	})
	// A real filesystem publication error after HTTP succeeded, before ACK.
	readerName := func() string {
		v := producer.request("$JS.API.CONSUMER.NAMES.INPUT", []byte("{}"))
		names := v["consumers"].([]any)
		require(len(names) == 1, "exactly one owned attempt consumer expected")
		return names[0].(string)
	}
	oldReader := readerName()
	beforeACK := producer.request("$JS.API.CONSUMER.INFO.INPUT."+oldReader, []byte("{}"))
	require(number(beforeACK, "num_ack_pending") == float64(initial), "broker pending prefix missing before failure")
	must(os.Mkdir(filepath.Join(checkpointDir, "CURRENT.tmp"), 0700))
	failed, code := a.call("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
	require(code >= 400, "injected checkpoint failure unexpectedly succeeded")
	save(filepath.Join(root, "checkpoint-failed.json"), failed)
	require(hash(filepath.Join(checkpointDir, "CURRENT")) == initialCurrent, "failed checkpoint changed CURRENT")
	server.stop(syscall.SIGKILL)
	afterFailure := producer.request("$JS.API.CONSUMER.INFO.INPUT."+oldReader, []byte("{}"))
	require(number(afterFailure, "num_ack_pending") == float64(initial) && number(afterFailure, "ack_floor", "stream_seq") == 0, "failed CURRENT publication ACKed broker input")
	save(filepath.Join(root, "broker-after-commit-failure.json"), afterFailure)
	must(os.Remove(filepath.Join(checkpointDir, "CURRENT.tmp")))
	start()
	a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	eventually("uncommitted replay", func() bool { return len(sink.snapshot()) == 2*expected })
	require(readerName() != oldReader, "old attempt consumer was not deleted before replacement")
	replayed := sink.snapshot()
	require(bytes.Equal(encode(first), encode(replayed[expected:])), "uncommitted replay changed output IDs or values")
	eventually("replayed input prefix applied", func() bool {
		return number(status(), "observation", "runtime_progress", "ingested_rows") == float64(initial)
	})
	// Observe a durable new CURRENT while ACK commands are actually dropped,
	// then SIGKILL the Sparrow process inside that committed/unacked interval.
	proxy.drop.Store(true)
	save(filepath.Join(root, "committed-with-ack-loss.json"), a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{}))
	eventually("actual dropped post-commit ACK", func() bool { return proxy.dropped.Load() > 0 })
	committedCurrent := hash(filepath.Join(checkpointDir, "CURRENT"))
	require(committedCurrent != initialCurrent, "checkpoint never committed before ACK fault")
	server.stop(syscall.SIGKILL)
	proxy.drop.Store(false)
	start()
	a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	eventually("restore committed unacked cut", func() bool {
		return number(status(), "checkpoint", "reliable_source", "restored_cut") == float64(initial)
	})
	time.Sleep(150 * time.Millisecond)
	require(len(sink.snapshot()) == 2*expected, "committed prefix replayed after ACK loss")
	publish(initial+1, 12)
	eventually("suffix output", func() bool { return len(sink.snapshot()) == 3*expected })
	a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
	eventually("source ACK drain", func() bool {
		s := status()
		return number(s, "checkpoint", "reliable_source", "committed_cut") == 12 && number(s, "checkpoint", "reliable_source", "pending_messages") == 0
	})
	save(filepath.Join(root, "final-status.json"), status())
	brokerFinal := producer.request("$JS.API.CONSUMER.INFO.INPUT."+readerName(), []byte("{}"))
	require(number(brokerFinal, "num_ack_pending") == 0 && number(brokerFinal, "ack_floor", "stream_seq") == 12, "broker ACK floor differs from committed cut")
	save(filepath.Join(root, "broker-final.json"), brokerFinal)
	save(filepath.Join(root, "inventory.json"), a.ok("GET", "/v1/pipelines/check/checkpoints", nil))
	all := sink.snapshot()
	unique := map[string]any{}
	for i, row := range all {
		id, ok := row["id"].(string)
		require(ok && len(id) == 48, "output ID encoding invalid")
		decoded, err := hex.DecodeString(id)
		require(err == nil && len(decoded) == 24 && strings.ToLower(id) == id, "output ID must be lowercase hex")
		data, ok := row["data"].(map[string]any)
		require(ok, "missing output data envelope")
		want := float64(i%expected + 1)
		if i >= 2*expected {
			want += 6
		}
		if shape == "two-count" {
			want = 21
			if i == 2 {
				want = 57
			}
		}
		require(data[field] == want, fmt.Sprintf("independent numeric oracle mismatch at %d: %v", i, row))
		if old, found := unique[id]; found {
			require(bytes.Equal(encode(old), encode(data)), "same output ID has different content")
		}
		unique[id] = data
	}
	require(len(unique) == expected*2, "unexpected unique output ID count")
	save(filepath.Join(root, "outputs.json"), all)
	server.stop(syscall.SIGTERM)
	// Prefix retention loss must refuse restore, not deliver the latest row.
	inputConfig["max_msgs"] = 1
	producer.request("$JS.API.STREAM.UPDATE.INPUT", encode(inputConfig))
	publish(13, 15)
	beforeExpiry := hash(filepath.Join(checkpointDir, "CURRENT"))
	start()
	a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	eventually("retention refusal", func() bool { return strings.Contains(text(status(), "actual", "last_error"), "required replay range") })
	require(hash(filepath.Join(checkpointDir, "CURRENT")) == beforeExpiry, "retention refusal mutated CURRENT")
	require(len(sink.snapshot()) == len(all), "retention gap produced output")
	save(filepath.Join(root, "retention-refused.json"), status())
	server.stop(syscall.SIGTERM)
	save(filepath.Join(root, "summary.json"), map[string]any{"shape": shape, "valid": true, "unique_outputs": len(unique), "expected_uncommitted_duplicate_outputs": expected,
		"checkpoint_failure_after_HTTP": true, "process_sigkill": true, "checkpoint_committed_ACK_lost": true, "dropped_ACK_commands": proxy.dropped.Load(), "retention_refused": true,
		"scope": "isolated_process_crash_not_OS_power_loss_TLS_WAN_or_soak"})
}

// Capacity baseline and paced latency use the same real pipeline. Preloading
// excludes producer persistence time from backlog-drain throughput; paced
// latency starts at the producer write, not at a fabricated device timestamp.
func benchmark(root, serverBin, natsBin, shape string, rows, rate int) {
	np, sp := port(), port()
	config := fmt.Sprintf("host: 127.0.0.1\nport: %d\nmax_payload: 65536\njetstream {\nstore_dir: %q\nmax_file_store: 256MB\nmax_memory_store: 16MB\nsync_interval: always\n}\n", np, filepath.Join(root, "broker-data"))
	must(os.WriteFile(filepath.Join(root, "nats.conf"), []byte(config), 0600))
	broker := launch(natsBin, filepath.Join(root, "broker.log"), nil, "-c", filepath.Join(root, "nats.conf"))
	defer broker.stop(syscall.SIGKILL)
	n := dialNATS(np)
	defer n.conn.Close()
	n.request("$JS.API.STREAM.CREATE.INPUT", encode(map[string]any{"name": "INPUT", "subjects": []string{"input.rows"}, "storage": "file", "retention": "limits", "max_bytes": 64 * 1024 * 1024, "max_msg_size": 65536, "max_consumers": 32, "num_replicas": 1, "deny_delete": true, "deny_purge": true}))
	n.request("$JS.API.STREAM.CREATE.KV_OWNERS", encode(map[string]any{"name": "KV_OWNERS", "subjects": []string{"$KV.OWNERS.>"}, "storage": "file", "retention": "limits", "max_bytes": 1024 * 1024, "max_msg_size": 1024, "max_msgs_per_subject": 1, "num_replicas": 1, "discard": "new", "allow_direct": true, "allow_rollup_hdrs": true}))
	sink := newCapture()
	defer sink.server.Close()
	a := apiClient{fmt.Sprintf("http://127.0.0.1:%d", sp), &http.Client{Timeout: 10 * time.Second}}
	server := launch(serverBin, filepath.Join(root, "server.log"), []string{"SPARROW_TOKEN=k2-private-process-fixture", "SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef", "SPARROW_REQUIRE_SECRETS_KEY=1", "SPARROW_DATA_ROOTS=" + root}, "--bind", fmt.Sprintf("127.0.0.1:%d", sp), "--catalog", filepath.Join(root, "catalog.db"), "--max-jobs", "1", "--safe-mode")
	defer server.stop(syscall.SIGKILL)
	eventually("benchmark server", func() bool { _, code := a.call("GET", "/v1/health", nil); return code == 200 })
	for _, p := range []int{np, sink.port()} {
		a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": p})
	}
	a.ok("PUT", "/v1/streams/sensors", map[string]any{"fields": []any{map[string]any{"name": "device_id", "type": "utf8", "nullable": false}, map[string]any{"name": "v", "type": "int64", "nullable": false}}})
	sql := "SELECT device_id, v FROM sensors"
	expected := rows
	if shape == "count" {
		sql = "SELECT SUM(v) AS s FROM sensors GROUP BY COUNT_WINDOW(3)"
		require(rows%3 == 0, "count benchmark rows must be divisible by 3")
		expected = rows / 3
	}
	spec := map[string]any{"stream": "sensors", "sql": sql, "source": map[string]any{"kind": "jetstream", "inbox_capacity": 16, "jetstream": map[string]any{"servers": []string{fmt.Sprintf("nats://127.0.0.1:%d", np)}, "namespace": "benchmark", "stream": "INPUT", "consumer": "bench", "ownership_bucket": "OWNERS", "max_pending": 128, "pull_messages": 8, "pull_bytes": 73728, "pending_bytes": 262144}}, "sink": map[string]any{"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/output", sink.port()), "batch_rows": 64, "batch_bytes": 262144, "linger_ms": 5, "max_inflight": 1}, "delivery": "checkpointed_at_least_once", "recovery": "aligned", "checkpoint_dir": filepath.Join(root, "checkpoint"), "checkpoint": map[string]any{"interval_ms": 1000, "timeout_ms": 5000, "resume_latest": true}}
	save(filepath.Join(root, "spec.json"), spec)
	a.ok("PUT", "/v1/pipelines/check", spec)
	status := func() map[string]any { return a.ok("GET", "/v1/pipelines/check/status", nil) }
	writeTimes := make([]time.Time, rows)
	publish := func() {
		begin := time.Now()
		for i := 1; i <= rows; i++ {
			if rate > 0 {
				due := begin.Add(time.Duration(float64(i-1) / float64(rate) * float64(time.Second)))
				if wait := time.Until(due); wait > 0 {
					time.Sleep(wait)
				}
			}
			body := encode(map[string]any{"device_id": "d1", "v": i})
			writeTimes[i-1] = time.Now()
			_, e := fmt.Fprintf(n.conn, "PUB input.rows %d\r\n%s\r\n", len(body), body)
			must(e)
		}
		// Core PUB enqueues stream storage work; a later control INFO reply
		// is NOT a persistence fence. Wait for the independently observed
		// stream count before timing a preloaded drain.
		var persisted map[string]any
		eventually("producer persistence count", func() bool {
			persisted = n.request("$JS.API.STREAM.INFO.INPUT", []byte("{}"))
			return number(persisted, "state", "messages") == float64(rows)
		})
		save(filepath.Join(root, "producer-persisted.json"), persisted)
	}
	if rate == 0 {
		publish()
	}
	started := time.Now()
	a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	eventually("benchmark bootstrap", func() bool { return number(status(), "checkpoint", "last_success_id") >= 1 })
	attempt := number(status(), "actual", "attempt_id")
	if rate > 0 {
		started = time.Now()
		publish()
	}
	deadline := time.Now().Add(60 * time.Second)
	for len(sink.snapshot()) < expected && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	all := sink.snapshot()
	require(len(all) == expected, "benchmark lost/duplicated output")
	sink.mu.Lock()
	received := append([]time.Time(nil), sink.received...)
	sink.mu.Unlock()
	httpElapsed := received[len(received)-1].Sub(started).Seconds()
	for {
		_, code := a.call("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
		if code >= 200 && code < 300 {
			break
		}
		require(time.Now().Before(deadline), "final checkpoint deadline")
		time.Sleep(10 * time.Millisecond)
	}
	eventually("benchmark committed and ACKed", func() bool {
		s := status()
		return number(s, "checkpoint", "reliable_source", "committed_cut") == float64(rows) && number(s, "checkpoint", "reliable_source", "pending_messages") == 0
	})
	commitElapsed := time.Since(started).Seconds()
	ids := map[string]bool{}
	latency := make([]float64, 0, expected)
	for i, row := range all {
		id, ok := row["id"].(string)
		decoded, e := hex.DecodeString(id)
		require(ok && e == nil && len(decoded) == 24 && !ids[id], "invalid/duplicate benchmark output ID")
		ids[id] = true
		data := row["data"].(map[string]any)
		seq := i + 1
		want := float64(seq)
		field := "v"
		if shape == "count" {
			seq = 3 * (i + 1)
			want = float64(3*seq - 3)
			field = "s"
		}
		require(data[field] == want, "benchmark numeric/order oracle mismatch")
		if rate > 0 {
			latency = append(latency, float64(received[i].Sub(writeTimes[seq-1]).Microseconds())/1000)
		}
	}
	final := status()
	require(number(final, "actual", "attempt_id") == attempt, "benchmark unexpectedly restarted")
	save(filepath.Join(root, "final-status.json"), final)
	save(filepath.Join(root, "outputs.json"), all)
	processStatus, e := os.ReadFile(fmt.Sprintf("/proc/%d/status", server.cmd.Process.Pid))
	must(e)
	must(os.WriteFile(filepath.Join(root, "process-status.txt"), processStatus, 0600))
	percentiles := map[string]any{"scope": "not_measured_for_preloaded_backlog"}
	if len(latency) > 0 {
		sort.Float64s(latency)
		percentiles = map[string]any{"scope": "producer_write_to_HTTP_capture; count_uses_last_input_of_window", "p50_ms": latency[(len(latency)-1)*50/100], "p95_ms": latency[(len(latency)-1)*95/100], "p99_ms": latency[(len(latency)-1)*99/100]}
	}
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "rows": rows, "outputs": expected, "shape": shape, "requested_rate": rate, "http_elapsed_s": httpElapsed, "committed_ACKed_elapsed_s": commitElapsed, "input_rows_per_s_HTTP": float64(rows) / httpElapsed, "input_rows_per_s_committed": float64(rows) / commitElapsed, "latency": percentiles, "scope": "single_pipeline_default_4MiB_loopback_no_TLS_WAN_or_soak; rate0_preloaded_includes_startup"})
	server.stop(syscall.SIGTERM)
	fmt.Println("K2_BENCHMARK_OK", shape, rows, rate)
}

func main() {
	server := flag.String("server-bin", "", "JetStream-enabled Sparrow binary")
	natsBin := flag.String("nats-server", "", "pinned NATS Server binary")
	out := flag.String("out", "", "new evidence directory")
	benchRows := flag.Int("bench-rows", 0, "optional bounded benchmark input rows")
	rate := flag.Int("rate", 0, "paced benchmark input rows/s; 0 preloads backlog")
	shape := flag.String("bench-shape", "zero", "zero or count")
	flag.Parse()
	require(*server != "" && *natsBin != "" && *out != "", "all flags required")
	root, e := filepath.Abs(*out)
	must(e)
	must(os.Mkdir(root, 0700))
	binary, e := os.Executable()
	must(e)
	save(filepath.Join(root, "binaries.json"), map[string]any{"server_sha256": hash(*server), "nats_sha256": hash(*natsBin), "driver_sha256": hash(binary)})
	if *benchRows > 0 {
		require(*benchRows <= 100000 && *rate >= 0 && (*shape == "zero" || *shape == "count"), "invalid benchmark bounds")
		benchmark(root, *server, *natsBin, *shape, *benchRows, *rate)
		return
	}
	for _, shape := range []string{"zero", "two-count"} {
		scenario(filepath.Join(root, shape), shape, *server, *natsBin)
		fmt.Println("K2_PROCESS_OK", shape)
	}
}
