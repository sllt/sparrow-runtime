// One focused scenario per source: local durable receipt while HTTP is down,
// committed checkpoint + SIGKILL, restart delivery, permanent rejection, DLQ
// replay/purge and authenticated CLI. No rate sweep or repetition matrix.
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

func scenario(root, source, serverBin, cliBin, natsBin string) {
	must(os.Mkdir(root, 0700))
	capture := newCapture()
	defer capture.server.Close()
	var broker *child
	var producer *nats
	brokerPort := 0
	if source == "jetstream" {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		must(e)
		brokerPort = l.Addr().(*net.TCPAddr).Port
		must(l.Close())
		brokerConfig := filepath.Join(root, "broker.conf")
		config := fmt.Sprintf("host: 127.0.0.1\nport: %d\nmax_payload: 65536\njetstream {\nstore_dir: %q\nmax_file_store: 256MB\nmax_memory_store: 16MB\nsync_interval: always\n}\n", brokerPort, filepath.Join(root, "broker-data"))
		must(os.WriteFile(brokerConfig, []byte(config), 0600))
		broker = launch(natsBin, filepath.Join(root, "broker.log"), nil, "-c", brokerConfig)
		defer broker.stop(syscall.SIGKILL)
		producer = connect(brokerPort)
		defer producer.conn.Close()
		producer.request("$JS.API.STREAM.CREATE.INPUT", encode(map[string]any{"name": "INPUT", "subjects": []string{"input.rows"}, "storage": "file", "retention": "limits", "max_bytes": 16 * 1024 * 1024, "max_msg_size": 65536, "max_consumers": 32, "num_replicas": 1, "deny_delete": true, "deny_purge": true}))
		producer.request("$JS.API.STREAM.CREATE.KV_OWNERS", encode(map[string]any{"name": "KV_OWNERS", "subjects": []string{"$KV.OWNERS.>"}, "storage": "file", "retention": "limits", "max_bytes": 1024 * 1024, "max_msg_size": 1024, "max_msgs_per_subject": 1, "num_replicas": 1, "discard": "new", "allow_direct": true, "allow_rollup_hdrs": true}))
	}
	// All other listeners are bound before reserving the management port.
	hold, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	serverPort := hold.Addr().(*net.TCPAddr).Port
	a := api{fmt.Sprintf("http://127.0.0.1:%d", serverPort), &http.Client{Timeout: 3 * time.Second}}
	input := filepath.Join(root, "input.ndjson")
	must(os.WriteFile(input, nil, 0600))
	var server *child
	defer func() {
		if server != nil {
			server.stop(syscall.SIGKILL)
		}
	}()
	defer func() {
		if reason := recover(); reason != nil {
			status, code := a.call("GET", "/v1/pipelines/p/status", nil)
			save(filepath.Join(root, "failure.json"), map[string]any{"panic": fmt.Sprint(reason), "status": status, "status_code": code, "requests": capture.rows()})
			panic(reason)
		}
	}()
	start := func() {
		if hold != nil {
			must(hold.Close())
			hold = nil
		}
		server = launch(serverBin, filepath.Join(root, "server.log"), []string{"SPARROW_TOKEN=" + token, "SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef", "SPARROW_REQUIRE_SECRETS_KEY=1", "SPARROW_DATA_ROOTS=" + root}, "--bind", fmt.Sprintf("127.0.0.1:%d", serverPort), "--catalog", filepath.Join(root, "catalog.db"), "--safe-mode")
		until("server health", func() bool { _, s := a.call("GET", "/v1/health", nil); return s == 200 })
	}
	publish := func(first, last int) {
		for i := first; i <= last; i++ {
			row := encode(map[string]any{"device_id": "d", "v": i})
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
	}
	start()
	a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": capture.port()})
	if producer != nil {
		a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": brokerPort})
	}
	a.ok("PUT", "/v1/streams/sensors", map[string]any{"fields": []any{map[string]any{"name": "device_id", "type": "utf8", "nullable": false}, map[string]any{"name": "v", "type": "int64", "nullable": false}}})
	spec := map[string]any{"stream": "sensors", "sql": "SELECT device_id, SUM(v) AS total FROM sensors GROUP BY device_id, COUNT_WINDOW(2)", "recovery": "aligned", "fail_on_decode": true,
		"checkpoint_dir": filepath.Join(root, "checkpoint"), "checkpoint": map[string]any{"timeout_ms": 5000, "resume_latest": true},
		"source": map[string]any{"kind": "file", "path": input, "file_contract": "append_only"},
		"sink": map[string]any{"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/out", capture.port()), "batch_bytes": 16384,
			"durable_outbox": map[string]any{"directory": filepath.Join(root, "outbox"), "max_disk_bytes": 8388608, "max_pending_bytes": 262144, "max_dlq_bytes": 262144, "max_pending_entries": 8, "max_dlq_entries": 8, "max_record_bytes": 16384, "max_attempts": 16, "max_retry_elapsed_ms": 60000, "retry_base_ms": 250, "retry_max_ms": 1000}}}
	if producer != nil {
		spec["delivery"] = "checkpointed_at_least_once"
		spec["checkpoint"].(map[string]any)["interval_ms"] = 60000
		spec["source"] = map[string]any{"kind": "jetstream", "jetstream": map[string]any{"servers": []string{fmt.Sprintf("nats://127.0.0.1:%d", brokerPort)}, "namespace": "outbox_fixture", "stream": "INPUT", "consumer": "delivery", "ownership_bucket": "OWNERS", "max_pending": 128, "pull_messages": 8}}
	}
	save(filepath.Join(root, "spec.json"), spec)
	a.ok("PUT", "/v1/pipelines/p", spec)
	a.ok("POST", "/v1/pipelines/p/start", map[string]any{})
	publish(1, 3)
	view := func() map[string]any {
		v, s := a.call("GET", "/v1/pipelines/p/outbox", nil)
		if s != 200 {
			return nil
		}
		return v
	}
	until("durable local receipt while HTTP fails", func() bool { return number(view(), "outbox", "accepted_total") >= 1 })
	until("all input applied", func() bool {
		return number(a.ok("GET", "/v1/pipelines/p/status", nil), "observation", "runtime_progress", "ingested_rows") == 3
	})
	statusBefore := a.ok("GET", "/v1/pipelines/p/status", nil)
	require(at(statusBefore, "observation", "delivery", "completion_basis") == "local_outbox_FULL_commit_not_remote_HTTP_2xx", "observation mislabels durable completion")
	require(at(statusBefore, "effective", "checkpoint_participants", "confirmation") == "local_outbox_FULL_commit_before_checkpoint_publication; not_remote_2xx", "effective contract mislabels durable completion")
	a.ok("POST", "/v1/pipelines/p/checkpoint", map[string]any{})
	if producer != nil {
		until("source ACK after local durable receipt", func() bool {
			return number(a.ok("GET", "/v1/pipelines/p/status", nil), "checkpoint", "reliable_source", "committed_cut") == 3 && number(a.ok("GET", "/v1/pipelines/p/status", nil), "checkpoint", "reliable_source", "pending_messages") == 0
		})
		names := producer.request("$JS.API.CONSUMER.NAMES.INPUT", []byte("{}"))["consumers"].([]any)
		require(len(names) == 1, "one owned reader")
		info := producer.request("$JS.API.CONSUMER.INFO.INPUT."+names[0].(string), []byte("{}"))
		require(number(info, "ack_floor", "stream_seq") == 3, "broker ACK floor must equal checkpoint cut")
		save(filepath.Join(root, "broker-acked-while-http-down.json"), info)
	}
	require(len(capture.totals()) == 0, "HTTP unexpectedly accepted output")
	uuid := at(view(), "outbox", "uuid").(string)
	unauth, e := a.client.Get(a.url + "/v1/pipelines/p/outbox")
	must(e)
	must(unauth.Body.Close())
	require(unauth.StatusCode == 401, "outbox inspection must require authentication")
	_, rejected := a.call("POST", "/v1/pipelines/p/outbox/command", map[string]any{"action": "pause", "approve_uuid": "wrong", "reason": "wrong queue"})
	require(rejected >= 400 && rejected < 500, "wrong UUID command must be rejected")
	command := func(action string, entry map[string]any) {
		req := map[string]any{"action": action, "approve_uuid": uuid, "reason": "bounded process verification"}
		if entry != nil {
			req["id"] = entry["id"]
			req["replay_generation"] = entry["replay_generation"]
		}
		a.ok("POST", "/v1/pipelines/p/outbox/command", req)
	}
	command("pause", nil)
	before := a.ok("GET", "/v1/pipelines/p/outbox/entries?state=pending", nil)["entries"].([]any)
	require(len(before) == 1, "one durable pending batch")
	entry := before[0].(map[string]any)
	attempts := entry["attempts"]
	current, e := os.ReadFile(filepath.Join(root, "checkpoint", "CURRENT"))
	must(e)
	server.stop(syscall.SIGKILL)
	start()
	a.ok("POST", "/v1/pipelines/p/start", map[string]any{})
	until("restored pipeline running", func() bool { return at(a.ok("GET", "/v1/pipelines/p/status", nil), "actual", "status") == "running" })
	got, e := os.ReadFile(filepath.Join(root, "checkpoint", "CURRENT"))
	must(e)
	require(bytes.Equal(got, current), "restart changed committed checkpoint before new input")
	after := a.ok("GET", "/v1/pipelines/p/outbox/entries?state=pending", nil)["entries"].([]any)
	require(len(after) == 1 && after[0].(map[string]any)["attempts"] == attempts, "restart reset retry state")
	peek := cli(cliBin, a.url, "outbox-entry", "p", entry["id"].(string))
	raw, e := base64.StdEncoding.DecodeString(peek["body_base64"].(string))
	must(e)
	require(len(raw) > 0, "CLI lost body")
	capture.code.Store(200)
	command("resume", nil)
	publish(4, 4)
	until("recovered window and outbox delivered", func() bool {
		v := capture.totals()
		return len(v) >= 2 && number(view(), "outbox", "pending_entries") == 0
	})
	totals := capture.totals()
	require(len(totals) == 2 && totals[0] == 3 && totals[1] == 7, fmt.Sprint("unexpected restored sums ", totals))
	a.ok("POST", "/v1/pipelines/p/checkpoint", map[string]any{})
	capture.code.Store(400)
	publish(5, 6)
	until("permanent failure DLQ", func() bool { return number(view(), "outbox", "dlq_entries") == 1 })
	a.ok("POST", "/v1/pipelines/p/checkpoint", map[string]any{})
	dlq := cli(cliBin, a.url, "outbox-entries", "p", "dlq", "0", "10")["entries"].([]any)[0].(map[string]any)
	capture.code.Store(200)
	command("replay", dlq)
	until("manual replay delivered", func() bool { return len(capture.totals()) >= 3 && number(view(), "outbox", "dlq_entries") == 0 })
	var body string
	matches := 0
	for _, r := range capture.rows() {
		if r.ID == dlq["id"] {
			if body == "" {
				body = r.Body
			}
			require(r.Body == body, "replay changed request bytes")
			matches++
		}
	}
	require(matches >= 2, "replay did not reuse its Idempotency-Key")
	capture.code.Store(400)
	publish(7, 8)
	until("second DLQ entry", func() bool { return number(view(), "outbox", "dlq_entries") == 1 })
	a.ok("POST", "/v1/pipelines/p/checkpoint", map[string]any{})
	dlq = a.ok("GET", "/v1/pipelines/p/outbox/entries?state=dlq", nil)["entries"].([]any)[0].(map[string]any)
	command("purge", dlq)
	status := cli(cliBin, a.url, "outbox", "p")
	require(number(status, "outbox", "pending_entries") == 0 && number(status, "outbox", "dlq_entries") == 0, "queue did not drain")
	require(number(status, "outbox", "purged_total") == 1 && number(status, "outbox", "replayed_total") == 1, "missing disposition audit")
	final := capture.totals()
	require(len(final) == 3 && final[2] == 11, fmt.Sprint("unexpected delivered results ", final))
	save(filepath.Join(root, "requests.json"), capture.rows())
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "source": source, "totals": final, "outbox": status, "SIGKILL": true, "power_loss_tested": false})
	server.stop(syscall.SIGTERM)
	fmt.Println("DURABLE_OUTPUT_OK", source)
}

func main() {
	server := flag.String("server-bin", "", "candidate server")
	client := flag.String("cli-bin", "", "candidate CLI")
	broker := flag.String("nats-server", "", "pinned NATS server (required)")
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
