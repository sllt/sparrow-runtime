// One real process recovery per declared profile. No repetitions or rate sweep.
package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
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

const token = "analysis-recovery-test-token"

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func require(ok bool, s string) {
	if !ok {
		panic(s)
	}
}
func encode(v any) []byte  { b, e := json.Marshal(v); must(e); return b }
func save(p string, v any) { must(os.WriteFile(p, encode(v), 0600)) }
func until(label string, f func() bool) {
	end := time.Now().Add(20 * time.Second)
	for time.Now().Before(end) {
		if f() {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	panic("deadline: " + label)
}

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
	p := &child{c, make(chan error, 1), f}
	go func() { p.done <- c.Wait() }()
	return p
}
func (p *child) stop() {
	if p == nil || p.cmd == nil {
		return
	}
	_ = p.cmd.Process.Signal(syscall.SIGKILL)
	select {
	case <-p.done:
	case <-time.After(8 * time.Second):
		panic("child did not terminate")
	}
	_ = p.log.Close()
	p.cmd = nil
}

type api struct {
	base   string
	client *http.Client
}

func (a api) call(method, path string, v any) (map[string]any, int) {
	var body io.Reader
	if v != nil {
		body = bytes.NewReader(encode(v))
	}
	q, e := http.NewRequest(method, a.base+path, body)
	must(e)
	q.Header.Set("Authorization", "Bearer "+token)
	q.Header.Set("Content-Type", "application/json")
	r, e := a.client.Do(q)
	if e != nil {
		return nil, 0
	}
	defer r.Body.Close()
	b, e := io.ReadAll(io.LimitReader(r.Body, 2<<20))
	must(e)
	var out map[string]any
	_ = json.Unmarshal(b, &out)
	return out, r.StatusCode
}
func (a api) ok(method, path string, v any) map[string]any {
	out, n := a.call(method, path, v)
	require(n >= 200 && n < 300, fmt.Sprintf("%s %s: %d %v", method, path, n, out))
	return out
}

type captured struct {
	ID   string         `json:"id"`
	Data map[string]any `json:"data"`
}
type capture struct {
	mu       sync.Mutex
	rows     []captured
	block    atomic.Bool
	release  chan struct{}
	listener net.Listener
	server   *http.Server
}

func newCapture() *capture {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	c := &capture{listener: l, release: make(chan struct{})}
	c.server = &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		b, e := io.ReadAll(io.LimitReader(r.Body, 1<<20))
		must(e)
		var rows []map[string]any
		must(json.Unmarshal(b, &rows))
		c.mu.Lock()
		for _, row := range rows {
			id, _ := row["id"].(string)
			data, ok := row["data"].(map[string]any)
			if !ok {
				data = row
				id = ""
			}
			c.rows = append(c.rows, captured{id, data})
		}
		c.mu.Unlock()
		if c.block.Load() {
			select {
			case <-c.release:
			case <-r.Context().Done():
			}
		}
		w.Header().Set("Content-Length", "0")
		w.WriteHeader(200)
	})}
	go func() { _ = c.server.Serve(l) }()
	return c
}
func (c *capture) all() []captured {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]captured(nil), c.rows...)
}
func (c *capture) port() int { return c.listener.Addr().(*net.TCPAddr).Port }
func appendRow(path string, v any) {
	f, e := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0600)
	must(e)
	_, e = f.Write(append(encode(v), '\n'))
	must(e)
	must(f.Sync())
	must(f.Close())
}
func cut(dir string) (uint16, uint64) {
	current, e := os.ReadFile(filepath.Join(dir, "CURRENT"))
	if e != nil {
		return 0, 0
	}
	b, e := os.ReadFile(filepath.Join(dir, strings.TrimSpace(string(current)), "0000.bin"))
	if e != nil || len(b) < 38 {
		return 0, 0
	}
	return binary.LittleEndian.Uint16(b[4:6]), binary.LittleEndian.Uint64(b[30:38])
}

// Independent minimal broker client, only for fixture publication/ACK inspection.
type nats struct {
	c net.Conn
	r *bufio.Reader
	n int
}

func connect(port int) *nats {
	var c net.Conn
	until("broker", func() bool {
		var e error
		c, e = net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 100*time.Millisecond)
		return e == nil
	})
	_, e := fmt.Fprint(c, "CONNECT {\"verbose\":false,\"pedantic\":true}\r\n")
	must(e)
	return &nats{c: c, r: bufio.NewReader(c)}
}
func (n *nats) request(subject string, v any) map[string]any {
	n.n++
	inbox := fmt.Sprintf("_INBOX.analysis.%d.%d", os.Getpid(), n.n)
	b := encode(v)
	must(n.c.SetDeadline(time.Now().Add(5 * time.Second)))
	_, e := fmt.Fprintf(n.c, "SUB %s 1\r\nUNSUB 1 1\r\nPUB %s %s %d\r\n", inbox, subject, inbox, len(b))
	must(e)
	_, e = n.c.Write(append(b, '\r', '\n'))
	must(e)
	for {
		line, e := n.r.ReadString('\n')
		must(e)
		p := strings.Fields(line)
		if len(p) == 0 {
			continue
		}
		switch p[0] {
		case "PING":
			_, e = fmt.Fprint(n.c, "PONG\r\n")
			must(e)
		case "-ERR":
			panic(line)
		case "MSG":
			size, e := strconv.Atoi(p[len(p)-1])
			must(e)
			require(size >= 0 && size <= 1<<20, "broker reply bound")
			body := make([]byte, size+2)
			_, e = io.ReadFull(n.r, body)
			must(e)
			var out map[string]any
			must(json.Unmarshal(body[:size], &out))
			require(out["error"] == nil, fmt.Sprint(out))
			return out
		}
	}
}

func scenario(root, kind, bin, natsBin string) {
	must(os.Mkdir(root, 0700))
	c := newCapture()
	defer c.server.Close()
	var broker *child
	var producer *nats
	var brokerPort int
	if kind == "jetstream" {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		must(e)
		brokerPort = l.Addr().(*net.TCPAddr).Port
		must(l.Close())
		conf := filepath.Join(root, "nats.conf")
		must(os.WriteFile(conf, []byte(fmt.Sprintf("host: 127.0.0.1\nport: %d\nmax_payload: 65536\njetstream { store_dir: %q, max_file_store: 256MB, max_memory_store: 16MB, sync_interval: always }\n", brokerPort, filepath.Join(root, "broker"))), 0600))
		broker = launch(natsBin, filepath.Join(root, "broker.log"), nil, "-c", conf)
		defer broker.stop()
		producer = connect(brokerPort)
		defer producer.c.Close()
		producer.request("$JS.API.STREAM.CREATE.INPUT", map[string]any{"name": "INPUT", "subjects": []string{"input.rows"}, "storage": "file", "retention": "limits", "max_bytes": 16777216, "max_msg_size": 65536, "max_consumers": 32, "num_replicas": 1, "deny_delete": true, "deny_purge": true})
		producer.request("$JS.API.STREAM.CREATE.KV_OWNERS", map[string]any{"name": "KV_OWNERS", "subjects": []string{"$KV.OWNERS.>"}, "storage": "file", "retention": "limits", "max_bytes": 1048576, "max_msg_size": 1024, "max_msgs_per_subject": 1, "num_replicas": 1, "discard": "new", "allow_direct": true, "allow_rollup_hdrs": true})
	}
	l, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	port := l.Addr().(*net.TCPAddr).Port
	a := api{fmt.Sprintf("http://127.0.0.1:%d", port), &http.Client{Timeout: 8 * time.Second}}
	paths := []string{filepath.Join(root, "left.ndjson"), filepath.Join(root, "right.ndjson")}
	for _, p := range paths {
		must(os.WriteFile(p, nil, 0600))
	}
	checkpoint := filepath.Join(root, "checkpoint")
	var server *child
	defer func() {
		if server != nil {
			server.stop()
		}
	}()
	defer func() {
		if why := recover(); why != nil {
			status, code := a.call("GET", "/v1/pipelines/p/status", nil)
			save(filepath.Join(root, "failure.json"), map[string]any{"panic": fmt.Sprint(why), "status": status, "code": code, "requests": c.all()})
			panic(why)
		}
	}()
	startServer := func() {
		if l != nil {
			must(l.Close())
			l = nil
		}
		server = launch(bin, filepath.Join(root, "server.log"), []string{"SPARROW_TOKEN=" + token, "SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef", "SPARROW_REQUIRE_SECRETS_KEY=1", "SPARROW_DATA_ROOTS=" + root}, "--bind", fmt.Sprintf("127.0.0.1:%d", port), "--catalog", filepath.Join(root, "catalog.db"), "--safe-mode")
		until("health", func() bool { _, n := a.call("GET", "/v1/health", nil); return n == 200 })
	}
	start := func() { a.ok("POST", "/v1/pipelines/p/start", map[string]any{}) }
	waitCut := func(want uint64) {
		until(fmt.Sprintf("committed input %d", want), func() bool { _, n := cut(checkpoint); return n >= want })
	}
	waitRows := func(want int) { until(fmt.Sprintf("output %d", want), func() bool { return len(c.all()) >= want }) }
	source := func(index int) map[string]any {
		return map[string]any{"kind": "file", "path": paths[index], "file_contract": "append_only"}
	}
	sink := map[string]any{"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/out", c.port())}
	spec := map[string]any{"version": 1, "stream": "l", "recovery": "aligned", "fail_on_decode": true, "checkpoint_dir": checkpoint, "checkpoint": map[string]any{"interval_ms": 100, "timeout_ms": 5000, "resume_latest": true}, "source": source(0), "sink": sink}
	startServer()
	a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": c.port()})
	if kind == "join" {
		fields := []any{map[string]any{"name": "k", "type": "utf8", "nullable": false}, map[string]any{"name": "v", "type": "int64", "nullable": false}, map[string]any{"name": "ts", "type": "int64", "nullable": false}}
		for _, name := range []string{"l", "r"} {
			a.ok("PUT", "/v1/streams/"+name, map[string]any{"fields": fields})
		}
		spec["sql"] = "SELECT a.v AS lv,b.v AS rv FROM l a LEFT JOIN r b ON a.k=b.k AND INTERVAL_MATCH(a.ts,b.ts,3,5,10)"
		spec["graph_io"] = map[string]any{"sources": map[string]any{"1": source(0), "2": source(1)}, "sinks": map[string]any{"6": sink}}
		a.ok("PUT", "/v1/pipelines/p", spec)
		start()
		emit := func(side, k, v, ts int) { appendRow(paths[side], map[string]any{"k": fmt.Sprint(k), "v": v, "ts": ts}) }
		emit(0, 1, 1, 12)
		waitCut(1)
		emit(1, 1, 101, 15)
		waitRows(1)
		waitCut(2)
		emit(0, 2, 2, 22)
		waitCut(3)
		// Committed state contains a matched left row AND an unmatched left row.
		server.stop()
		startServer()
		start()
		emit(1, 1, 102, 16)
		waitRows(2)
		waitCut(4)
		emit(1, 9, 109, 50)
		waitRows(3)
		waitCut(5)
		rows := c.all()
		require(len(rows) == 3, "unexpected Join duplicates/loss")
		want := []string{"1/101", "1/102", "2/<nil>"}
		for i, r := range rows {
			require(fmt.Sprint(r.Data["lv"])+"/"+fmt.Sprint(r.Data["rv"]) == want[i], fmt.Sprintf("Join row %d: %v", i, r))
			require(r.ID != "", "Join output lacks stable ID")
		}
		require(rows[0].ID != rows[1].ID && rows[1].ID != rows[2].ID, "Join ID collision")
		version, _ := cut(checkpoint)
		require(version == 38, "Join snapshot version")
	} else {
		a.ok("PUT", "/v1/streams/l", map[string]any{"fields": []any{map[string]any{"name": "items", "type": "dynamic", "nullable": true}}})
		spec["sql"] = "SELECT to_int64(u.item) AS item,u.unnest_input AS input_n,u.unnest_ordinal AS ordinal FROM l CROSS JOIN UNNEST(items) AS u(item)"
		if producer != nil {
			a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": brokerPort})
			spec["delivery"] = "checkpointed_at_least_once"
			spec["source"] = map[string]any{"kind": "jetstream", "jetstream": map[string]any{"servers": []string{fmt.Sprintf("nats://127.0.0.1:%d", brokerPort)}, "namespace": "analysis_recovery", "stream": "INPUT", "consumer": "rows", "ownership_bucket": "OWNERS", "max_pending": 128, "pull_messages": 8}}
		}
		publish := func(items []int) {
			v := map[string]any{"items": items}
			if producer != nil {
				producer.request("input.rows", v)
			} else {
				appendRow(paths[0], v)
			}
		}
		a.ok("PUT", "/v1/pipelines/p", spec)
		start()
		publish([]int{7, 8})
		waitRows(2)
		waitCut(1)
		publish([]int{})
		waitCut(2)
		c.block.Store(true)
		publish([]int{9})
		waitRows(3)
		server.stop()
		c.block.Store(false)
		close(c.release)
		startServer()
		start()
		waitRows(4)
		waitCut(3)
		publish([]int{10})
		waitRows(5)
		waitCut(4)
		rows := c.all()
		require(len(rows) == 5, "unexpected UNNEST output count")
		want := []int{7, 8, 9, 9, 10}
		inputs := []int{1, 1, 3, 3, 4}
		ordinals := []int{1, 2, 1, 1, 1}
		for i, r := range rows {
			require(r.Data["item"] == float64(want[i]) && r.Data["input_n"] == float64(inputs[i]) && r.Data["ordinal"] == float64(ordinals[i]), fmt.Sprintf("UNNEST row %d: %v", i, r))
		}
		require(bytes.Equal(encode(rows[2].Data), encode(rows[3].Data)), "replayed expansion changed")
		version, _ := cut(checkpoint)
		if producer != nil {
			require(version == 37, "JetStream snapshot version")
			require(rows[2].ID != "" && rows[2].ID == rows[3].ID, "replayed JetStream output ID changed")
		} else {
			require(version == 36, "File snapshot version")
		}
	}
	a.ok("POST", "/v1/pipelines/p/stop", map[string]any{})
	save(filepath.Join(root, "outputs.json"), c.all())
	save(filepath.Join(root, "status.json"), a.ok("GET", "/v1/pipelines/p/status", nil))
	fmt.Println("ANALYSIS_RECOVERY_OK", kind)
}
func main() {
	bin := flag.String("server-bin", "", "server with jetstream feature")
	broker := flag.String("nats-server", "", "nats-server binary")
	out := flag.String("out", "", "new evidence directory")
	flag.Parse()
	require(*bin != "" && *broker != "" && *out != "", "--server-bin --nats-server --out required")
	must(os.Mkdir(*out, 0700))
	for _, kind := range []string{"file", "jetstream", "join"} {
		scenario(filepath.Join(*out, kind), kind, *bin, *broker)
	}
}
