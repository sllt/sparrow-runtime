// One bounded end-to-end input quarantine / recovery operation scenario per
// source. Reuses the durable-output fixture protocol helpers; no rate sweep.
package main

import (
	"bufio"
	"bytes"
	"encoding/base64"
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
	"sync/atomic"
	"syscall"
	"time"
)

const token = "durable-output-test-token"

func must(err error) {
	if err != nil {
		panic(err)
	}
}
func require(ok bool, text string) {
	if !ok {
		panic(text)
	}
}
func encode(v any) []byte     { b, e := json.Marshal(v); must(e); return b }
func save(path string, v any) { must(os.WriteFile(path, append(encode(v), '\n'), 0600)) }
func until(label string, f func() bool) {
	end := time.Now().Add(15 * time.Second)
	for time.Now().Before(end) {
		if f() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	panic("deadline: " + label)
}

type api struct {
	url    string
	client *http.Client
}

func (a api) call(method, path string, v any) (map[string]any, int) {
	var body io.Reader
	if v != nil {
		body = bytes.NewReader(encode(v))
	}
	r, e := http.NewRequest(method, a.url+path, body)
	must(e)
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	response, e := a.client.Do(r)
	if e != nil {
		return nil, 0
	}
	defer response.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(response.Body, 2*1024*1024))
	must(e)
	var out map[string]any
	_ = json.Unmarshal(raw, &out)
	return out, response.StatusCode
}
func (a api) ok(method, path string, v any) map[string]any {
	out, status := a.call(method, path, v)
	require(status >= 200 && status < 300, fmt.Sprintf("%s %s: %d %v", method, path, status, out))
	return out
}
func at(v map[string]any, keys ...string) any {
	var cur any = v
	for _, k := range keys {
		m, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = m[k]
	}
	return cur
}
func number(v map[string]any, keys ...string) float64 { x, _ := at(v, keys...).(float64); return x }

type child struct {
	cmd  *exec.Cmd
	done chan error
	log  *os.File
}

func launch(bin, log string, env []string, args ...string) *child {
	f, e := os.OpenFile(log, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(e)
	c := exec.Command(bin, args...)
	c.Env = append(os.Environ(), env...)
	c.Stdout = f
	c.Stderr = f
	must(c.Start())
	out := &child{c, make(chan error, 1), f}
	go func() { out.done <- c.Wait() }()
	return out
}
func (c *child) stop(signal os.Signal) {
	if c == nil || c.cmd == nil {
		return
	}
	_ = c.cmd.Process.Signal(signal)
	select {
	case <-c.done:
	case <-time.After(8 * time.Second):
		_ = c.cmd.Process.Kill()
		<-c.done
	}
	_ = c.log.Close()
	c.cmd = nil
}

type received struct {
	ID     string `json:"id"`
	Body   string `json:"body"`
	Status int    `json:"status"`
}
type capture struct {
	listener net.Listener
	server   *http.Server
	code     atomic.Int32
	mu       sync.Mutex
	items    []received
}

func newCapture() *capture {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	c := &capture{listener: l}
	c.code.Store(503)
	c.server = &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		body, e := io.ReadAll(io.LimitReader(r.Body, 1024*1024+1))
		must(e)
		require(len(body) <= 1024*1024, "request bound")
		status := int(c.code.Load())
		c.mu.Lock()
		c.items = append(c.items, received{r.Header.Get("Idempotency-Key"), string(body), status})
		c.mu.Unlock()
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(status)
	})}
	go func() { _ = c.server.Serve(l) }()
	return c
}
func (c *capture) port() int { return c.listener.Addr().(*net.TCPAddr).Port }
func (c *capture) rows() []received {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]received(nil), c.items...)
}
func (c *capture) totals() []float64 {
	var totals []float64
	for _, r := range c.rows() {
		if r.Status < 200 || r.Status >= 300 {
			continue
		}
		var rows []map[string]any
		must(json.Unmarshal([]byte(r.Body), &rows))
		for _, row := range rows {
			if data, ok := row["data"].(map[string]any); ok {
				row = data
			}
			v, ok := row["total"].(float64)
			require(ok, "missing SUM result")
			totals = append(totals, v)
		}
	}
	return totals
}

// Minimal isolated-broker request/reply. There is no dependency on the client
// under test; ACK floor checks query the broker itself.
type nats struct {
	conn   net.Conn
	in     *bufio.Reader
	serial int
}

func connect(port int) *nats {
	var conn net.Conn
	until("NATS ready", func() bool {
		var e error
		conn, e = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		return e == nil
	})
	_, e := fmt.Fprint(conn, "CONNECT {\"verbose\":false,\"pedantic\":true}\r\n")
	must(e)
	return &nats{conn: conn, in: bufio.NewReader(conn)}
}
func (n *nats) request(subject string, payload []byte) map[string]any {
	n.serial++
	inbox := fmt.Sprintf("_INBOX.outbox.%d.%d", os.Getpid(), n.serial)
	must(n.conn.SetDeadline(time.Now().Add(5 * time.Second)))
	_, e := fmt.Fprintf(n.conn, "SUB %s 1\r\nUNSUB 1 1\r\nPUB %s %s %d\r\n", inbox, subject, inbox, len(payload))
	must(e)
	_, e = n.conn.Write(append(payload, '\r', '\n'))
	must(e)
	for {
		line, e := n.in.ReadString('\n')
		must(e)
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "PING":
			_, e = fmt.Fprint(n.conn, "PONG\r\n")
			must(e)
		case "-ERR":
			panic(line)
		case "MSG":
			size, e := strconv.Atoi(fields[len(fields)-1])
			must(e)
			require(size >= 0 && size <= 1024*1024, "NATS reply bound")
			b := make([]byte, size+2)
			_, e = io.ReadFull(n.in, b)
			must(e)
			require(fields[1] == inbox, "wrong NATS reply")
			var value map[string]any
			must(json.Unmarshal(b[:size], &value))
			require(value["error"] == nil, fmt.Sprint(value))
			return value
		}
	}
}
func cli(bin, base, command, name string, extra ...string) map[string]any {
	args := append([]string{command, name, "--url", base}, extra...)
	cmd := exec.Command(bin, args...)
	cmd.Env = append(os.Environ(), "SPARROW_TOKEN="+token)
	out, e := cmd.CombinedOutput()
	require(e == nil, fmt.Sprintf("CLI %v: %v %s", args, e, out))
	var v map[string]any
	must(json.Unmarshal(out, &v))
	return v
}

func clone(v map[string]any) map[string]any {
	var out map[string]any
	must(json.Unmarshal(encode(v), &out))
	return out
}
func scenario(root, source, serverBin, cliBin, natsBin string) {
	must(os.Mkdir(root, 0700))
	capture := newCapture()
	capture.code.Store(200)
	defer capture.server.Close()
	var producer *nats
	brokerPort := 0
	if source == "jetstream" {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		must(e)
		brokerPort = l.Addr().(*net.TCPAddr).Port
		must(l.Close())
		config := fmt.Sprintf("host: 127.0.0.1\nport: %d\nmax_payload: 65536\njetstream {\nstore_dir: %q\nmax_file_store: 256MB\nmax_memory_store: 16MB\nsync_interval: always\n}\n", brokerPort, filepath.Join(root, "broker-data"))
		conf := filepath.Join(root, "broker.conf")
		must(os.WriteFile(conf, []byte(config), 0600))
		broker := launch(natsBin, filepath.Join(root, "broker.log"), nil, "-c", conf)
		defer broker.stop(syscall.SIGKILL)
		producer = connect(brokerPort)
		defer producer.conn.Close()
		producer.request("$JS.API.STREAM.CREATE.INPUT", encode(map[string]any{"name": "INPUT", "subjects": []string{"input.rows"}, "storage": "file", "retention": "limits", "max_bytes": 16 * 1024 * 1024, "max_msg_size": 65536, "max_consumers": 32, "num_replicas": 1, "deny_delete": true, "deny_purge": true}))
		producer.request("$JS.API.STREAM.CREATE.KV_OWNERS", encode(map[string]any{"name": "KV_OWNERS", "subjects": []string{"$KV.OWNERS.>"}, "storage": "file", "retention": "limits", "max_bytes": 1024 * 1024, "max_msg_size": 1024, "max_msgs_per_subject": 1, "num_replicas": 1, "discard": "new", "allow_direct": true, "allow_rollup_hdrs": true}))
	}
	hold, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	port := hold.Addr().(*net.TCPAddr).Port
	a := api{fmt.Sprintf("http://127.0.0.1:%d", port), &http.Client{Timeout: 8 * time.Second}}
	input := filepath.Join(root, "input.ndjson")
	must(os.WriteFile(input, nil, 0600))
	var server *child
	defer func() {
		if server != nil {
			server.stop(syscall.SIGKILL)
		}
	}()
	defer func() {
		if why := recover(); why != nil {
			s, code := a.call("GET", "/v1/pipelines/p/status", nil)
			save(filepath.Join(root, "failure.json"), map[string]any{"panic": fmt.Sprint(why), "status": s, "status_code": code, "requests": capture.rows()})
			panic(why)
		}
	}()
	startServer := func() {
		if hold != nil {
			must(hold.Close())
			hold = nil
		}
		server = launch(serverBin, filepath.Join(root, "server.log"), []string{"SPARROW_TOKEN=" + token, "SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef", "SPARROW_REQUIRE_SECRETS_KEY=1", "SPARROW_DATA_ROOTS=" + root}, "--bind", fmt.Sprintf("127.0.0.1:%d", port), "--catalog", filepath.Join(root, "catalog.db"), "--safe-mode")
		until("server health", func() bool { _, s := a.call("GET", "/v1/health", nil); return s == 200 })
	}
	publishRaw := func(row []byte) {
		if producer != nil {
			producer.request("input.rows", row)
		} else {
			f, e := os.OpenFile(input, os.O_APPEND|os.O_WRONLY, 0600)
			must(e)
			_, e = f.Write(append(row, '\n'))
			must(e)
			must(f.Sync())
			must(f.Close())
		}
	}
	publish := func(v int) { publishRaw(encode(map[string]any{"device_id": "d", "v": v})) }
	start := func(name string) {
		a.ok("POST", "/v1/pipelines/"+name+"/start", map[string]any{})
		until(name+" running", func() bool {
			return at(a.ok("GET", "/v1/pipelines/"+name+"/status", nil), "actual", "status") == "running"
		})
	}
	stop := func(name string) {
		a.ok("POST", "/v1/pipelines/"+name+"/stop", map[string]any{})
		until(name+" stopped", func() bool {
			return at(a.ok("GET", "/v1/pipelines/"+name+"/status", nil), "actual", "status") == "stopped"
		})
	}
	checkpoint := func(name string) float64 {
		return number(a.ok("POST", "/v1/pipelines/"+name+"/checkpoint", map[string]any{}), "checkpoint_id")
	}
	waitTotals := func(want []float64) {
		until("expected output count", func() bool { return len(capture.totals()) >= len(want) })
		got := capture.totals()
		require(fmt.Sprint(got) == fmt.Sprint(want), fmt.Sprintf("outputs: got %v want %v", got, want))
	}
	startServer()
	a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": capture.port()})
	if producer != nil {
		a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": brokerPort})
	}
	a.ok("PUT", "/v1/streams/sensors", map[string]any{"fields": []any{map[string]any{"name": "device_id", "type": "utf8", "nullable": false}, map[string]any{"name": "v", "type": "int64", "nullable": false}}})
	sink := map[string]any{"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/out", capture.port()), "batch_bytes": 16384}
	spec := map[string]any{"stream": "sensors", "sql": "SELECT device_id, SUM(v) AS total FROM sensors GROUP BY device_id, COUNT_WINDOW(2)", "recovery": "aligned", "fail_on_decode": true, "checkpoint_dir": filepath.Join(root, "checkpoint"), "checkpoint": map[string]any{"interval_ms": 60000, "timeout_ms": 5000, "resume_latest": true, "retain_generations": 4},
		"source": map[string]any{"kind": "file", "path": input, "file_contract": "append_only"},
		"sink":   clone(sink)}
	if producer != nil {
		spec["delivery"] = "checkpointed_at_least_once"
		spec["source"] = map[string]any{"kind": "jetstream", "jetstream": map[string]any{"servers": []string{fmt.Sprintf("nats://127.0.0.1:%d", brokerPort)}, "namespace": "input_recovery", "stream": "INPUT", "consumer": "delivery", "ownership_bucket": "OWNERS", "max_pending": 128, "pull_messages": 8}}
	}
	spec["source"].(map[string]any)["input_dlq"] = map[string]any{"directory": filepath.Join(root, "input-dlq"), "max_disk_bytes": 8388608, "max_payload_bytes": 262144, "max_entries": 1}
	spec["sink"].(map[string]any)["durable_outbox"] = map[string]any{"directory": filepath.Join(root, "output-outbox"), "max_disk_bytes": 8388608, "max_pending_bytes": 262144, "max_dlq_bytes": 262144, "max_pending_entries": 16, "max_dlq_entries": 8, "max_record_bytes": 16384, "max_attempts": 32, "max_retry_elapsed_ms": 120000, "retry_base_ms": 250, "retry_max_ms": 1000}
	capture.code.Store(503)
	a.ok("PUT", "/v1/pipelines/p", spec)
	start("p")
	publish(1)
	bad := []byte("{\"device_id\":\"d\",\"v\":\"wrong\"}")
	publishRaw(bad)
	publish(2)
	bad = []byte("{\"device_id\":\"d\",\"v\":\"second-poison\"}")
	publishRaw(bad)
	publish(3)
	until("full quarantine leaves checkpoint control responsive", func() bool {
		return at(a.ok("GET", "/v1/pipelines/p/status", nil), "observation", "source", "reason") == "input_dlq_full_waiting_for_operator"
	})
	if producer != nil {
		until("full queue commits accepted prefix", func() bool {
			return number(a.ok("GET", "/v1/pipelines/p/status", nil), "checkpoint", "reliable_source", "committed_cut") == 3
		})
	}
	blockedCut := checkpoint("p")
	require(len(capture.totals()) == 0, "initial checkpoint unexpectedly relied on remote success")
	require(number(a.ok("GET", "/v1/pipelines/p/outbox", nil), "outbox", "accepted_total") == 1, "checkpoint before durable output acceptance")
	if producer != nil {
		until("full queue does not ACK unaccepted poison", func() bool {
			return number(a.ok("GET", "/v1/pipelines/p/status", nil), "checkpoint", "reliable_source", "committed_cut") == 3
		})
	}
	capture.code.Store(200)
	waitTotals([]float64{3})
	stop("p")
	firstQueue := a.ok("GET", "/v1/pipelines/p/input-dlq", nil)
	firstEntry := a.ok("GET", "/v1/pipelines/p/input-dlq/entries", nil)["entries"].([]any)[0].(map[string]any)
	a.ok("POST", "/v1/pipelines/p/input-dlq/purge", map[string]any{"approve_uuid": at(firstQueue, "input_dlq", "uuid"), "position": firstEntry["position"], "approve_replay_floor": firstEntry["position"], "approve_checkpoint": blockedCut, "reason": "explicit full-queue disposition after checkpoint"})
	start("p")
	until("poison stored and valid input applied", func() bool {
		return number(a.ok("GET", "/v1/pipelines/p/input-dlq", nil), "input_dlq", "captured_total") == 2 && number(a.ok("GET", "/v1/pipelines/p/status", nil), "observation", "runtime_progress", "ingested_rows") == 1
	})
	waitTotals([]float64{3})
	cp1 := checkpoint("p")
	if producer != nil {
		until("checkpoint ACK includes durable poison disposition", func() bool {
			return number(a.ok("GET", "/v1/pipelines/p/status", nil), "checkpoint", "reliable_source", "committed_cut") == 5 && number(a.ok("GET", "/v1/pipelines/p/status", nil), "checkpoint", "reliable_source", "pending_messages") == 0
		})
		names := producer.request("$JS.API.CONSUMER.NAMES.INPUT", []byte("{}"))["consumers"].([]any)
		info := producer.request("$JS.API.CONSUMER.INFO.INPUT."+names[0].(string), []byte("{}"))
		require(number(info, "ack_floor", "stream_seq") == 5, "broker ACK floor includes poison before/after durable ordering")
		save(filepath.Join(root, "broker-ack.json"), info)
	}
	entry := cli(cliBin, a.url, "input-dlq-entries", "p", "0", "10")["entries"].([]any)[0].(map[string]any)
	pos := entry["position"].(float64)
	posText := strconv.FormatFloat(pos, 'f', 0, 64)
	peek := cli(cliBin, a.url, "input-dlq-entry", "p", posText)
	raw, e := base64.StdEncoding.DecodeString(peek["body_base64"].(string))
	must(e)
	require(bytes.Equal(raw, bad), "quarantine changed raw payload")
	until("outbox settled before checkpoint restart", func() bool {
		return number(a.ok("GET", "/v1/pipelines/p/outbox", nil), "outbox", "pending_entries") == 0
	})
	server.stop(syscall.SIGKILL)
	startServer()
	start("p")
	require(number(a.ok("GET", "/v1/pipelines/p/input-dlq", nil), "input_dlq", "captured_total") == 2, "restart duplicated quarantine")
	publish(4)
	waitTotals([]float64{3, 7})
	cp2 := checkpoint("p")
	stop("p")
	// Compatible revision migration preserves the exact checkpoint and state.
	resumeSpec := clone(spec)
	resumeSpec["checkpoint"].(map[string]any)["timeout_ms"] = 6000
	request := func(op, mode, target string, cut float64, targetSpec map[string]any) map[string]any {
		return map[string]any{"operation": op, "mode": mode, "approve_parent_revision": number(a.ok("GET", "/v1/pipelines/p/status", nil), "revision"), "checkpoint_id": cut, "target": target, "spec": targetSpec, "reason": "focused recovery verification", "accept_state_reset": true, "accept_duplicate_outputs": true}
	}
	execute := func(req map[string]any) map[string]any {
		file := filepath.Join(root, req["operation"].(string)+".json")
		save(file, req)
		preview := cli(cliBin, a.url, "recovery-preview", "p", file)
		req["approve_digest"] = preview["approve_digest"]
		save(file, req)
		result := cli(cliBin, a.url, "recovery-execute", "p", file)
		require(result["phase"] == "ready", "operation did not reach ready")
		again := cli(cliBin, a.url, "recovery-execute", "p", file)
		require(again["published_revision"] == result["published_revision"], "idempotent execute published another revision")
		return result
	}
	execute(request("resume", "resume", "p", cp2, resumeSpec))
	require(number(a.ok("GET", "/v1/pipelines/p/status", nil), "revision") == 2, "resume revision count")
	spec = resumeSpec
	start("p")
	publish(5)
	publish(6)
	waitTotals([]float64{3, 7, 11})
	cp3 := checkpoint("p")
	stop("p")
	// Corrected poison is a separate immutable input artifact/new lineage.
	fixed := clone(spec)
	fixed["source"] = map[string]any{"kind": "file", "path": filepath.Join(root, "placeholder"), "file_contract": "append_only"}
	fixed["delivery"] = "live_best_effort"
	fixed["sql"] = "SELECT v AS total FROM sensors"
	fixed["checkpoint_dir"] = filepath.Join(root, "fixed-checkpoint")
	fixed["sink"] = clone(sink)
	req := request("fix", "dlq_replay", "fixed", cp3, fixed)
	req["dlq_positions"] = []float64{pos}
	req["corrections"] = map[string]any{posText: map[string]any{"device_id": "d", "v": 42}}
	req["artifact_directory"] = filepath.Join(root, "fixed-artifact")
	execute(req)
	start("fixed")
	waitTotals([]float64{3, 7, 11, 42})
	cli(cliBin, a.url, "recovery-finish", "p", "fix")
	until("fixed stopped", func() bool {
		return at(a.ok("GET", "/v1/pipelines/fixed/status", nil), "actual", "status") == "stopped"
	})
	// Finite history rebuild: original partial window state is NOT copied.
	history := clone(spec)
	delete(history["source"].(map[string]any), "input_dlq")
	history["sql"] = "SELECT v AS total FROM sensors"
	history["checkpoint_dir"] = filepath.Join(root, "history-checkpoint")
	history["sink"] = clone(sink)
	if producer != nil {
		history["source"].(map[string]any)["jetstream"].(map[string]any)["consumer"] = "history"
	}
	req = request("history", "replay", "history", cp3, history)
	req["from_checkpoint"] = cp1
	historyOperation := execute(req)
	until("outbox settled before lineage restart", func() bool {
		return number(a.ok("GET", "/v1/pipelines/p/outbox", nil), "outbox", "pending_entries") == 0
	})
	server.stop(syscall.SIGKILL)
	startServer()
	require(at(cli(cliBin, a.url, "recovery-operation", "p", "history"), "phase") == "ready", "published lineage lost on crash")
	start("history")
	waitTotals([]float64{3, 7, 11, 42, 4, 5, 6})
	if producer != nil {
		// The bounded JetStream reader checkpoints its terminal cut itself.
		// Wait for that one coordinator before issuing a second checkpoint;
		// a retryable "already queued or active" is not a correctness failure.
		until("history terminal checkpoint complete", func() bool {
			return number(a.ok("GET", "/v1/pipelines/history/status", nil), "checkpoint", "reliable_source", "committed_cut") == number(historyOperation, "target_spec", "source", "replay_start", "end")
		})
	}
	cli(cliBin, a.url, "recovery-finish", "p", "history")
	// Open-ended semantic fork starts AFTER the parent cut, with empty state.
	fork := clone(spec)
	delete(fork["source"].(map[string]any), "input_dlq")
	fork["checkpoint_dir"] = filepath.Join(root, "fork-checkpoint")
	fork["sink"] = clone(sink)
	if producer != nil {
		fork["source"].(map[string]any)["jetstream"].(map[string]any)["consumer"] = "fork"
	}
	execute(request("fork", "fork", "fork", cp3, fork))
	start("fork")
	publish(7)
	publish(8)
	waitTotals([]float64{3, 7, 11, 42, 4, 5, 6, 15})
	checkpoint("fork")
	stop("fork")
	// Explicit purge cannot outrun CURRENT and retires source replay history.
	q := cli(cliBin, a.url, "input-dlq", "p")
	purge := map[string]any{"approve_uuid": at(q, "input_dlq", "uuid"), "position": pos, "approve_replay_floor": pos, "approve_checkpoint": cp3, "reason": "corrected in approved separate lineage"}
	purgeFile := filepath.Join(root, "purge.json")
	save(purgeFile, purge)
	cli(cliBin, a.url, "input-dlq-purge", "p", purgeFile)
	q = cli(cliBin, a.url, "input-dlq", "p")
	require(number(q, "input_dlq", "entries") == 0 && number(q, "input_dlq", "replay_floor") == pos, "purge/floor did not persist")
	denied := clone(history)
	denied["checkpoint_dir"] = filepath.Join(root, "denied-checkpoint")
	if producer != nil {
		denied["source"].(map[string]any)["jetstream"].(map[string]any)["consumer"] = "denied"
	}
	req = request("denied", "replay", "denied", cp3, denied)
	_, code := a.call("POST", "/v1/pipelines/p/recovery/preview", req)
	require(code >= 400 && code < 500, "replay below retired poison floor was accepted")
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "source": source, "totals": capture.totals(), "input_dlq": q, "operations": cli(cliBin, a.url, "recovery-operations", "p"), "SIGKILL": 2, "power_loss_tested": false})
	save(filepath.Join(root, "requests.json"), capture.rows())
	server.stop(syscall.SIGTERM)
	fmt.Println("INPUT_RECOVERY_OK", source)
}

func main() {
	server := flag.String("server-bin", "", "candidate server")
	client := flag.String("cli-bin", "", "candidate CLI")
	broker := flag.String("nats-server", "", "pinned NATS server")
	out := flag.String("out", "", "new evidence directory")
	flag.Parse()
	require(*server != "" && *client != "" && *broker != "" && *out != "", "all flags required")
	root, e := filepath.Abs(*out)
	must(e)
	must(os.Mkdir(root, 0700))
	for _, source := range []string{"file", "jetstream"} {
		scenario(filepath.Join(root, source), source, *server, *client, *broker)
	}
}
