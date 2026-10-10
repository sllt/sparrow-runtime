// Sub-batch 1 process oracle: File/JetStream -> Count/ET tumbling/ET hopping
// window with FIRST/LAST/VAR_*/STDDEV_* -> required HTTP JSON. Standard
// library only. Cuts that need an in-process stop use the server built with
// the off-by-default `process-fault-pause` feature: the driver arms a marker
// file, waits for `<point>.reached`, then SIGKILLs (no timed sleep-then-kill).
// Expected rows are computed here independently (Go float64 Welford in the
// same per-key arrival order, plus a math/big two-pass cross-check) and
// compared bit-for-bit. Each run SIGKILLs the real server at one confirmable
// cut point. SIGKILL evidence is not OS power-loss or media certification.
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
	"math"
	"math/big"
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

const token = "agg-recovery-private-process-fixture"

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
	b, e := os.ReadFile(path)
	must(e)
	s := sha256.Sum256(b)
	return hex.EncodeToString(s[:])
}
func port() int {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	p := l.Addr().(*net.TCPAddr).Port
	must(l.Close())
	return p
}
func eventually(label string, fn func() bool) {
	until := time.Now().Add(20 * time.Second)
	for time.Now().Before(until) {
		if fn() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("deadline: " + label)
}

type child struct {
	cmd  *exec.Cmd
	log  *os.File
	done chan error
}

func launch(binary, logPath string, env []string, args ...string) *child {
	log, e := os.OpenFile(logPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(e)
	c := exec.Command(binary, args...)
	c.Env = append(os.Environ(), env...)
	c.Stdout = log
	c.Stderr = log
	must(c.Start())
	ch := &child{c, log, make(chan error, 1)}
	go func() { ch.done <- c.Wait() }()
	return ch
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

// ------------------------------------------------------------------ oracle

type input struct {
	Device string
	V      *int64
	X      float64
	Ts     int64
}

func row(i int, shape string) input {
	// Dyadic inputs isolate checkpoint fidelity from cross-parser JSON
	// rounding differences without changing production parsing semantics.
	r := input{Device: fmt.Sprintf("d%d", i%2), X: float64(i)*0.375 - 2.0, Ts: int64(i)*100 + 50}
	if shape != "count" && shape != "slide" {
		r.Device = "d0"
	}
	if i%4 != 0 {
		v := int64((i*7)%11 - 3)
		r.V = &v
	}
	return r
}
func (r input) json() []byte {
	m := map[string]any{"device_id": r.Device, "x": r.X, "ts": r.Ts}
	if r.V != nil {
		m["v"] = *r.V
	} else {
		m["v"] = nil
	}
	return encode(m)
}

type acc struct {
	c       int64
	f, l    *int64
	n       uint64
	mean    float64
	m2      float64
	xs      []float64
	started bool
}

func (a *acc) add(r input) {
	a.c++
	if r.V != nil {
		if a.f == nil {
			v := *r.V
			a.f = &v
		}
		v := *r.V
		a.l = &v
	}
	n := a.n + 1
	d := r.X - a.mean
	mean := a.mean + d/float64(n)
	a.m2 += d * (r.X - mean)
	a.mean = mean
	a.n = n
	a.xs = append(a.xs, r.X)
}

type expected struct {
	Device string
	C      int64
	F, L   *int64
	Vp, Vs *float64
	Sp, Ss *float64
	Win    string // sliding count: "start|end" (empty for other shapes)
}

func moment(a *acc, sample, sqrt bool) *float64 {
	s := uint64(0)
	if sample {
		s = 1
	}
	if a.n <= s {
		return nil
	}
	v := math.Max(a.m2/float64(a.n-s), 0)
	if sqrt {
		v = math.Sqrt(v)
	}
	// Independent two-pass exact-rational cross-check (tolerance only here).
	sum := new(big.Rat)
	for _, x := range a.xs {
		sum.Add(sum, new(big.Rat).SetFloat64(x))
	}
	mean := new(big.Rat).Quo(sum, new(big.Rat).SetInt64(int64(len(a.xs))))
	ss := new(big.Rat)
	for _, x := range a.xs {
		d := new(big.Rat).Sub(new(big.Rat).SetFloat64(x), mean)
		ss.Add(ss, new(big.Rat).Mul(d, d))
	}
	exact, _ := new(big.Rat).Quo(ss, new(big.Rat).SetInt64(int64(a.n-s))).Float64()
	if sqrt {
		exact = math.Sqrt(exact)
	}
	require(math.Abs(exact-v) <= 1e-9*math.Max(1, math.Abs(exact)), "Welford diverged from exact two-pass cross-check")
	return &v
}
func finish(key string, a *acc) expected {
	return expected{key, a.c, a.f, a.l, moment(a, false, false), moment(a, true, false), moment(a, false, true), moment(a, true, true), ""}
}

// oracle: full uninterrupted output for rows 1..n. Count: per key every 3rd
// arrival. ET tumble 300us, lateness 0: a window closes when a later event
// has ts > window end (ts never equals a boundary).
func oracle(shape string, n int) []expected {
	var out []expected
	if shape == "count" {
		keys := map[string]*acc{}
		for i := 1; i <= n; i++ {
			r := row(i, shape)
			a := keys[r.Device]
			if a == nil {
				a = &acc{}
				keys[r.Device] = a
			}
			a.add(r)
			if a.c == 3 {
				out = append(out, finish(r.Device, a))
				keys[r.Device] = &acc{}
			}
		}
		return out
	}
	if shape == "slide" {
		// COUNT_WINDOW(3, 2): per key keep the last 3 raw rows; on the n-th
		// arrival with n >= 3 and n % 2 == 0 emit [n-3, n) over those rows.
		type ring struct {
			n    int
			rows []input
		}
		keys := map[string]*ring{}
		for i := 1; i <= n; i++ {
			r := row(i, shape)
			k := keys[r.Device]
			if k == nil {
				k = &ring{}
				keys[r.Device] = k
			}
			k.n++
			k.rows = append(k.rows, r)
			if len(k.rows) > 3 {
				k.rows = k.rows[1:]
			}
			if k.n >= 3 && k.n%2 == 0 {
				a := &acc{}
				for _, x := range k.rows {
					a.add(x)
				}
				e := finish(r.Device, a)
				e.Win = fmt.Sprintf("%d|%d", k.n-3, k.n)
				out = append(out, e)
			}
		}
		return out
	}
	if shape == "hop" {
		// HOP(size 600, slide 300), lateness 0, single key: window [s,s+600)
		// with s a multiple of 300 (negative starts included) closes when the
		// first event with ts > s+600 arrives; at most one closes per event
		// because ts advances by 100 and never lands on a boundary.
		open := map[int64]*acc{}
		for i := 1; i <= n; i++ {
			r := row(i, shape)
			for _, s := range []int64{floorDiv(r.Ts, 300)*300 - 600, floorDiv(r.Ts, 300)*300 - 300} {
				if a, ok := open[s]; ok && r.Ts > s+600 {
					out = append(out, finish("d0", a))
					delete(open, s)
				}
			}
			for _, s := range []int64{floorDiv(r.Ts, 300)*300 - 300, floorDiv(r.Ts, 300) * 300} {
				if open[s] == nil {
					open[s] = &acc{}
				}
				open[s].add(r)
			}
		}
		return out
	}
	var cur *acc
	var curStart int64 = -1
	for i := 1; i <= n; i++ {
		r := row(i, shape)
		start := r.Ts / 300 * 300
		if cur != nil && start != curStart {
			out = append(out, finish("d0", cur))
			cur = nil
		}
		if cur == nil {
			cur = &acc{}
			curStart = start
		}
		cur.add(r)
	}
	return out
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if a%b != 0 && (a < 0) != (b < 0) {
		q--
	}
	return q
}

func intText(v *int64) string {
	if v == nil {
		return "null"
	}
	return strconv.FormatInt(*v, 10)
}
func floatText(v *float64) string {
	if v == nil {
		return "null"
	}
	return fmt.Sprintf("%016x", math.Float64bits(*v))
}
func (e expected) text() string {
	parts := []string{e.Device, strconv.FormatInt(e.C, 10), intText(e.F), intText(e.L), floatText(e.Vp), floatText(e.Vs), floatText(e.Sp), floatText(e.Ss)}
	if e.Win != "" {
		parts = append(parts, e.Win)
	}
	return strings.Join(parts, "|")
}

// render one received payload with the same canonical text (exact float bits).
func render(data map[string]any) string {
	i := func(k string) string {
		v, ok := data[k]
		require(ok, "missing output field "+k)
		if v == nil {
			return "null"
		}
		f := v.(float64)
		require(f == math.Trunc(f), "non-integral integer field "+k)
		return strconv.FormatInt(int64(f), 10)
	}
	f := func(k string) string {
		v, ok := data[k]
		require(ok, "missing output field "+k)
		if v == nil {
			return "null"
		}
		x := v.(float64)
		return fmt.Sprintf("%016x", math.Float64bits(x))
	}
	d, _ := data["device_id"].(string)
	parts := []string{d, i("c"), i("f"), i("l"), f("vp"), f("vs"), f("sp"), f("ss")}
	if _, ok := data["count_start"]; ok {
		parts = append(parts, i("count_start")+"|"+i("count_end"))
	}
	return strings.Join(parts, "|")
}

// ------------------------------------------------------------------ fixtures

type capture struct {
	mu       sync.Mutex
	raw      [][]byte
	held     [][]byte // bodies of requests left unanswered (in flight at SIGKILL)
	hold     atomic.Bool
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
		body, e := io.ReadAll(io.LimitReader(r.Body, 1024*1024+1))
		if e != nil || len(body) > 1024*1024 {
			w.WriteHeader(413)
			return
		}
		var rows []json.RawMessage
		if json.Unmarshal(body, &rows) != nil {
			w.WriteHeader(400)
			return
		}
		if c.hold.Load() {
			// Never acknowledged: the request stays in flight until the
			// server is SIGKILLed, then the driver releases it with 503.
			c.mu.Lock()
			for _, r := range rows {
				c.held = append(c.held, append([]byte(nil), r...))
			}
			c.mu.Unlock()
			<-c.release
			w.WriteHeader(503)
			return
		}
		c.mu.Lock()
		for _, r := range rows {
			c.raw = append(c.raw, append([]byte(nil), r...))
		}
		c.mu.Unlock()
		w.WriteHeader(200)
	})}
	go func() { _ = c.server.Serve(l) }()
	return c
}
func (c *capture) count() int { c.mu.Lock(); defer c.mu.Unlock(); return len(c.raw) }
func (c *capture) rows() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.raw...)
}
func (c *capture) heldRows() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.held...)
}
func (c *capture) port() int { return c.listener.Addr().(*net.TCPAddr).Port }

type api struct {
	base   string
	client *http.Client
}

func (a api) call(method, path string, body any) (map[string]any, int) {
	var payload io.Reader
	if body != nil {
		payload = bytes.NewReader(encode(body))
	}
	r, e := http.NewRequest(method, a.base+path, payload)
	if e != nil {
		return nil, 0
	}
	r.Header.Set("Authorization", "Bearer "+token)
	r.Header.Set("Content-Type", "application/json")
	if method == http.MethodPut && strings.HasPrefix(path, "/v1/pipelines/") {
		if cur, code := a.call(http.MethodGet, path, nil); code == 200 {
			r.Header.Set("If-Match", fmt.Sprint(cur["etag"]))
		}
	}
	resp, e := a.client.Do(r)
	if e != nil {
		return nil, 0
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	var v map[string]any
	_ = json.Unmarshal(raw, &v)
	return v, resp.StatusCode
}
func (a api) ok(method, path string, body any) map[string]any {
	v, s := a.call(method, path, body)
	require(s >= 200 && s < 300, fmt.Sprintf("API %s %s returned %d: %v", method, path, s, v))
	return v
}
func nested(v any, keys ...string) any {
	for _, k := range keys {
		m, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = m[k]
	}
	return v
}
func number(v map[string]any, keys ...string) float64 {
	n, ok := nested(v, keys...).(float64)
	if !ok {
		return -1
	}
	return n
}

// Minimal Core NATS request/reply for fixture setup and publishing.
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
	_, e := fmt.Fprint(conn, "CONNECT {\"verbose\":false,\"pedantic\":true,\"lang\":\"go-agg-fixture\",\"version\":\"1\"}\r\n")
	must(e)
	return n
}
func (n *nats) request(subject string, payload []byte) map[string]any {
	n.serial++
	inbox := fmt.Sprintf("_INBOX.aggfixture.%d.%d", os.Getpid(), n.serial)
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
			panic("isolated NATS error: " + line)
		case "MSG":
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

// Drops only source +ACK commands while armed.
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
			c, e := l.Accept()
			if e != nil {
				return
			}
			p.wg.Add(1)
			go p.handle(c)
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
		if e != nil || len(line) > 65536 {
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
		} else if _, e = io.WriteString(server, line); e != nil {
			break
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

// snapshotVersion reads the outer pipeline version from the CURRENT
// generation's first chunk on disk (independent of server status JSON).
func snapshotVersion(dir string) int {
	cur, e := os.ReadFile(filepath.Join(dir, "CURRENT"))
	must(e)
	chunk, e := os.ReadFile(filepath.Join(dir, strings.TrimSpace(string(cur)), "0000.bin"))
	must(e)
	require(len(chunk) >= 6, "short checkpoint chunk")
	return int(binary.LittleEndian.Uint16(chunk[4:6]))
}

func fields() []any {
	return []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "v", "type": "int64", "nullable": true},
		map[string]any{"name": "x", "type": "float64", "nullable": false},
		map[string]any{"name": "ts", "type": "int64", "nullable": false},
	}
}
func col(n string) map[string]any { return map[string]any{"k": "col", "name": n} }
func aggs() []any {
	return []any{
		map[string]any{"fn": "count", "alias": "c"},
		map[string]any{"fn": "first", "expr": col("v"), "alias": "f"},
		map[string]any{"fn": "last", "expr": col("v"), "alias": "l"},
		map[string]any{"fn": "var_pop", "expr": col("x"), "alias": "vp"},
		map[string]any{"fn": "var_samp", "expr": col("x"), "alias": "vs"},
		map[string]any{"fn": "stddev_pop", "expr": col("x"), "alias": "sp"},
		map[string]any{"fn": "stddev_samp", "expr": col("x"), "alias": "ss"},
	}
}
func graph(shape string, ext bool) map[string]any {
	window := map[string]any{"kind": "count", "size": 3}
	if shape == "slide" {
		window = map[string]any{"kind": "sliding_count", "size": 3, "step": 2}
	}
	source := map[string]any{"id": 1, "kind": "memory_source", "table": "sensors", "out": []int{10}}
	node := map[string]any{"id": 10, "kind": "window_agg", "keys": []string{"device_id"}, "out": []int{20}}
	node["window"] = window
	if ext {
		node["aggs"] = aggs()
	} else {
		node["aggs"] = []any{map[string]any{"fn": "sum", "expr": col("ts"), "alias": "s"}}
	}
	return map[string]any{"version": 1, "pipeline_id": 1, "revision_id": 1, "nodes": []any{source, node, map[string]any{"id": 20, "kind": "capture_sink", "name": "out"}}}
}

type env struct {
	root, serverBin, natsBin string
	a                        api
	serverPort               int
	server                   *child
	sink                     *capture
	producer                 *nats
	proxy                    *ackProxy
	broker                   *child
	file                     string
	checkpoint               string
	faults                   string
}

func (v *env) start(binary string) {
	v.server = launch(binary, filepath.Join(v.root, "server.log"), []string{"SPARROW_TOKEN=" + token, "SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef", "SPARROW_REQUIRE_SECRETS_KEY=1", "SPARROW_DATA_ROOTS=" + v.root, "SPARROW_FAULT_MARKER_DIR=" + v.faults},
		"--bind", fmt.Sprintf("127.0.0.1:%d", v.serverPort), "--catalog", filepath.Join(v.root, "catalog.db"), "--max-jobs", "1", "--safe-mode")
	eventually("server health", func() bool { _, s := v.a.call("GET", "/v1/health", nil); return s == 200 })
}

// arm makes the next arrival at `point` park the server; waitReached
// confirms the cut from the `.reached` marker the parked thread wrote.
func (v *env) arm(point, content string) {
	must(os.WriteFile(filepath.Join(v.faults, point+".arm"), []byte(content), 0600))
}
func (v *env) disarm(point string) {
	must(os.Remove(filepath.Join(v.faults, point+".arm")))
	_ = os.Remove(filepath.Join(v.faults, point+".reached"))
}
func (v *env) waitReached(point string) string {
	var pid string
	eventually("fault point reached: "+point, func() bool {
		b, e := os.ReadFile(filepath.Join(v.faults, point+".reached"))
		pid = strings.TrimSpace(string(b))
		return e == nil && pid != ""
	})
	require(pid == strconv.Itoa(v.server.cmd.Process.Pid), "fault marker written by a different process")
	return pid
}

// generations lists checkpoint generation directories and whether each has
// a published MANIFEST (independent of the server's own inventory).
func generations(dir string) map[string]bool {
	out := map[string]bool{}
	entries, e := os.ReadDir(dir)
	must(e)
	for _, entry := range entries {
		if !entry.IsDir() || strings.HasSuffix(entry.Name(), ".tmp") {
			continue
		}
		_, e := os.Stat(filepath.Join(dir, entry.Name(), "MANIFEST"))
		out[entry.Name()] = e == nil
	}
	return out
}
func currentName(dir string) string {
	b, e := os.ReadFile(filepath.Join(dir, "CURRENT"))
	must(e)
	return strings.TrimSpace(string(b))
}

func (v *env) status() map[string]any { return v.a.ok("GET", "/v1/pipelines/check/status", nil) }
func (v *env) publish(from, to int, shape string) {
	for i := from; i <= to; i++ {
		r := row(i, shape)
		if v.producer != nil {
			v.producer.request("input.rows", r.json())
			continue
		}
		f, e := os.OpenFile(v.file, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		must(e)
		_, e = f.Write(append(r.json(), '\n'))
		must(e)
		must(f.Sync())
		must(f.Close())
	}
}
func (v *env) spec(source, shape string, ext bool) map[string]any {
	spec := map[string]any{"stream": "sensors",
		"sink":     map[string]any{"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/output", v.sink.port()), "batch_rows": 8, "linger_ms": 2},
		"recovery": "aligned", "checkpoint_dir": v.checkpoint,
		"checkpoint": map[string]any{"interval_ms": 86400000, "timeout_ms": 3000, "resume_latest": true}}
	if shape == "count" || shape == "slide" {
		spec["graph"] = graph(shape, ext)
	} else {
		// Event-time windows go through the SQL front end (single-source
		// linear graphs with an event-time source binding are DAG-only).
		require(ext, "plain ET spec not used")
		window := "TUMBLE(ts, 300)"
		if shape == "hop" {
			window = "HOP(ts, 300, 600)" // slide 300us, size 600us, lateness 0
		}
		spec["sql"] = "SELECT device_id, COUNT(*) AS c, FIRST(v) AS f, LAST(v) AS l, VAR_POP(x) AS vp, VAR_SAMP(x) AS vs, " +
			"STDDEV_POP(x) AS sp, STDDEV_SAMP(x) AS ss FROM sensors GROUP BY device_id, " + window
	}
	if source == "jetstream" {
		spec["delivery"] = "checkpointed_at_least_once"
		spec["source"] = map[string]any{"kind": "jetstream", "jetstream": map[string]any{
			"servers": []string{fmt.Sprintf("nats://127.0.0.1:%d", v.proxy.port())}, "namespace": "agg_fixture", "stream": "INPUT", "consumer": "check", "ownership_bucket": "OWNERS", "max_pending": 128, "pull_messages": 8}}
	} else {
		spec["source"] = map[string]any{"kind": "file", "path": v.file, "file_contract": "append_only", "inbox_capacity": 4}
	}
	return spec
}

func setup(root, source, serverBin, natsBin string) *env {
	must(os.MkdirAll(root, 0700))
	v := &env{root: root, serverBin: serverBin, natsBin: natsBin, serverPort: port(), sink: newCapture(),
		file: filepath.Join(root, "input.ndjson"), checkpoint: filepath.Join(root, "checkpoint"), faults: filepath.Join(root, "faults")}
	must(os.MkdirAll(v.faults, 0700))
	v.a = api{fmt.Sprintf("http://127.0.0.1:%d", v.serverPort), &http.Client{Timeout: 5 * time.Second}}
	if source == "jetstream" {
		np := port()
		config := fmt.Sprintf("host: 127.0.0.1\nport: %d\nmax_payload: 65536\njetstream {\nstore_dir: %q\nmax_file_store: 256MB\nmax_memory_store: 16MB\nsync_interval: always\n}\n", np, filepath.Join(root, "broker-data"))
		must(os.WriteFile(filepath.Join(root, "nats.conf"), []byte(config), 0600))
		v.broker = launch(natsBin, filepath.Join(root, "broker.log"), nil, "-c", filepath.Join(root, "nats.conf"))
		v.producer = dialNATS(np)
		v.producer.request("$JS.API.STREAM.CREATE.INPUT", encode(map[string]any{"name": "INPUT", "subjects": []string{"input.rows"}, "storage": "file", "retention": "limits", "max_bytes": 16 * 1024 * 1024, "max_msg_size": 65536, "max_consumers": 32, "num_replicas": 1, "deny_delete": true, "deny_purge": true}))
		v.producer.request("$JS.API.STREAM.CREATE.KV_OWNERS", encode(map[string]any{"name": "KV_OWNERS", "subjects": []string{"$KV.OWNERS.>"}, "storage": "file", "retention": "limits", "max_bytes": 1024 * 1024, "max_msg_size": 1024, "max_msgs_per_subject": 1, "num_replicas": 1, "discard": "new", "allow_direct": true, "allow_rollup_hdrs": true}))
		v.proxy = newProxy(np)
	}
	return v
}
func (v *env) close() {
	v.server.stop(syscall.SIGKILL)
	if v.proxy != nil {
		v.proxy.close()
	}
	if v.producer != nil {
		_ = v.producer.conn.Close()
	}
	v.broker.stop(syscall.SIGKILL)
	_ = v.sink.server.Close()
}
func (v *env) register() {
	if v.proxy != nil {
		v.a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": v.proxy.port()})
	}
	v.a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": v.sink.port()})
	v.a.ok("PUT", "/v1/streams/sensors", map[string]any{"fields": fields()})
}
func (v *env) readerName() string {
	names := v.producer.request("$JS.API.CONSUMER.NAMES.INPUT", []byte("{}"))["consumers"].([]any)
	require(len(names) == 1, "exactly one owned attempt consumer expected")
	return names[0].(string)
}

// decode returns the canonical text and (for JetStream) the output id.
func decode(raw []byte, reliable bool) (string, string) {
	var m map[string]any
	must(json.Unmarshal(raw, &m))
	if !reliable {
		_, hasID := m["id"]
		require(!hasID, "File v29 must not claim stable output IDs")
		return render(m), ""
	}
	id, ok := m["id"].(string)
	require(ok && len(id) == 48 && strings.ToLower(id) == id, "output ID encoding invalid")
	_, e := hex.DecodeString(id)
	must(e)
	data, ok := m["data"].(map[string]any)
	require(ok, "missing output data envelope")
	return render(data), id
}

// run executes one cut-point scenario. Phases (rows): P1=1..12 then a manual
// committed checkpoint C1; P2 depends on the cut; then SIGKILL; restart and
// append to N=36; final commit.
func run(root, source, shape, cut, serverBin, natsBin string) map[string]any {
	const n = 36
	p1 := 12
	// Sliding count state-shape cuts: C1 itself is the cut (killed after
	// commit, no phase 2). empty: no input; not_full: every key has 2 < size
	// arrivals and nothing was emitted; at_boundary: every key is exactly at
	// a trigger (n=4, output just emitted, the 3 retained rows still needed).
	stateCut := cut == "empty" || cut == "not_full" || cut == "at_boundary"
	switch cut {
	case "empty":
		p1 = 0
	case "not_full":
		p1 = 4
	case "at_boundary":
		p1 = 8
	}
	reliable := source == "jetstream"
	v := setup(root, source, serverBin, natsBin)
	defer v.close()
	want := oracle(shape, n)
	wantText := make([]string, len(want))
	for i, e := range want {
		wantText[i] = e.text()
	}
	outputsAt := func(rows int) int { return len(oracle(shape, rows)) }
	v.start(serverBin)
	v.register()
	spec := v.spec(source, shape, true)
	save(filepath.Join(root, "spec.json"), spec)
	explain := v.a.ok("POST", "/v1/explain", spec)
	save(filepath.Join(root, "explain.json"), explain)
	v.a.ok("PUT", "/v1/pipelines/check", spec)
	v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	if reliable {
		eventually("durable empty bootstrap", func() bool { return number(v.status(), "checkpoint", "last_success_id") >= 1 })
	}
	if p1 == 0 && !reliable {
		must(os.WriteFile(v.file, nil, 0600))
	}
	v.publish(1, p1, shape)
	eventually("phase-1 outputs", func() bool { return v.sink.count() == outputsAt(p1) })
	eventually("phase-1 input applied", func() bool {
		return number(v.status(), "observation", "runtime_progress", "ingested_rows") == float64(p1)
	})
	v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
	expectVersion := 29
	if reliable {
		expectVersion = 30
	}
	if shape == "slide" {
		expectVersion += 2
	}
	require(snapshotVersion(v.checkpoint) == expectVersion, fmt.Sprintf("CURRENT outer version want %d", expectVersion))
	c1 := hash(filepath.Join(v.checkpoint, "CURRENT"))
	committedRows := p1
	p2 := 18
	if stateCut {
		p2 = p1
		switch cut {
		case "not_full":
			require(outputsAt(p1) == 0, "not_full cut must precede every output")
		case "at_boundary":
			require(outputsAt(p1) == 2 && outputsAt(p1-1) == 1, "at_boundary must sit exactly on a trigger")
		}
	}
	if cut == "input_after" {
		// Rows applied to window state, no window completes: Count +2 rows
		// per key (one short of 3); ET/HOP ts 1450 < next end 1500.
		p2 = 16
		if shape != "count" {
			p2 = 14 // ET/HOP: ts 1450 < next end; slide: each key at n=7 (odd)
		}
		require(outputsAt(p2) == outputsAt(p1), "input_after must not complete a window")
	}
	var oldReader string
	detail := map[string]any{}
	currentPath := filepath.Join(v.checkpoint, "CURRENT")
	switch cut {
	case "input_after":
		// Confirmable: the window stage parks after applying row p2 (absolute
		// count of rows applied in this process).
		v.arm("window_rows_applied", strconv.Itoa(p2))
		v.publish(p1+1, p2, shape)
		detail["paused_pid"] = v.waitReached("window_rows_applied")
		require(v.sink.count() == outputsAt(p1), "input_after produced output")
	case "output_inflight":
		v.sink.hold.Store(true)
		v.publish(p1+1, p2, shape)
		// Confirmable: at least one output request is held unanswered.
		eventually("phase-2 output request in flight", func() bool { return len(v.sink.heldRows()) >= 1 })
		require(v.sink.count() == outputsAt(p1), "held output was acknowledged")
	default:
		v.publish(p1+1, p2, shape)
		eventually("phase-2 input applied", func() bool {
			return number(v.status(), "observation", "runtime_progress", "ingested_rows") == float64(p2)
		})
		// Confirmable: every phase-2 output was acknowledged by the HTTP peer.
		eventually("phase-2 outputs", func() bool { return v.sink.count() == outputsAt(p2) })
	}
	if reliable {
		// Consumer listing goes to the broker, not the (possibly parked) server.
		oldReader = v.readerName()
	}
	switch cut {
	case "input_after", "output_after", "output_inflight", "restore_kill", "empty", "not_full", "at_boundary", "low_budget":
		require(hash(currentPath) == c1, "no commit expected before this cut")
	case "commit_before":
		must(os.Mkdir(filepath.Join(v.checkpoint, "CURRENT.tmp"), 0700))
		failed, code := v.a.call("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
		require(code >= 400, "injected CURRENT publication failure unexpectedly succeeded")
		detail["failed_commit"] = failed
		require(hash(currentPath) == c1, "failed commit changed CURRENT")
	case "manifest_renamed":
		before := generations(v.checkpoint)
		v.arm("checkpoint_after_manifest_rename", "")
		go func() { _, _ = v.a.call("POST", "/v1/pipelines/check/checkpoint", map[string]any{}) }()
		detail["paused_pid"] = v.waitReached("checkpoint_after_manifest_rename")
		after := generations(v.checkpoint)
		var orphans []string
		for name, published := range after {
			if _, old := before[name]; !old && published && name != currentName(v.checkpoint) {
				orphans = append(orphans, name)
			}
		}
		require(len(orphans) == 1, fmt.Sprintf("expected exactly one renamed-but-unpublished MANIFEST, got %v", orphans))
		require(hash(currentPath) == c1, "CURRENT moved before the cut")
		detail["orphan_generation"] = orphans[0]
	case "commit_after":
		v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
		require(hash(currentPath) != c1, "commit did not publish CURRENT")
		committedRows = p2
	case "ack_lost":
		require(reliable, "ack_lost is JetStream-only")
		v.proxy.drop.Store(true)
		v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
		eventually("actual dropped post-commit ACK", func() bool { return v.proxy.dropped.Load() > 0 })
		require(hash(currentPath) != c1, "commit did not publish CURRENT")
		committedRows = p2
	default:
		panic("unknown cut " + cut)
	}
	preKill := v.sink.count()
	currentBeforeKill := hash(currentPath)
	v.server.stop(syscall.SIGKILL)
	var held [][]byte
	if cut == "output_inflight" {
		held = v.sink.heldRows()
		v.sink.hold.Store(false)
		close(v.sink.release)
		detail["inflight_rows"] = len(held)
	}
	switch cut {
	case "commit_before":
		must(os.Remove(filepath.Join(v.checkpoint, "CURRENT.tmp")))
	case "input_after":
		v.disarm("window_rows_applied")
	case "manifest_renamed":
		v.disarm("checkpoint_after_manifest_rename")
	}
	checkBroker := func(label string) {
		if !reliable {
			return
		}
		info := v.producer.request("$JS.API.CONSUMER.INFO.INPUT."+oldReader, []byte("{}"))
		detail[label] = info
		floor := number(info, "ack_floor", "stream_seq")
		// ACK never passes the durable cut: C1 rows were committed+ACKed;
		// later rows may be ACKed only after their own commit.
		require(floor <= float64(committedRows), "broker ACK floor passed the committed cut")
		if cut == "ack_lost" {
			require(floor < float64(committedRows), "ACK for the last commit was not actually lost")
		}
	}
	checkBroker("broker_after_kill")
	if reliable {
		v.proxy.drop.Store(false)
	}
	require(hash(currentPath) == currentBeforeKill, "SIGKILL changed CURRENT")
	if cut == "restore_kill" {
		// Second crash inside owned restore: payload/header/participant
		// credit charged, materialize not yet done.
		v.arm("restore_after_credit", "")
		v.start(serverBin)
		go func() { _, _ = v.a.call("POST", "/v1/pipelines/check/start", map[string]any{}) }()
		detail["paused_pid"] = v.waitReached("restore_after_credit")
		require(v.sink.count() == preKill, "output emitted during interrupted restore")
		require(hash(currentPath) == currentBeforeKill, "interrupted restore changed CURRENT")
		v.server.stop(syscall.SIGKILL)
		v.disarm("restore_after_credit")
		require(hash(currentPath) == currentBeforeKill, "SIGKILL during restore changed CURRENT")
		checkBroker("broker_after_restore_kill")
	}
	if cut == "low_budget" {
		// Real reservation pressure on the restoring Job owner: owned restore
		// must refuse (restore_credit), refund, leave CURRENT and outputs alone.
		v.arm("restore_pressure", "1024")
		v.start(serverBin)
		_, code := v.a.call("POST", "/v1/pipelines/check/start", map[string]any{})
		st := v.refused("low-budget restore", "")
		detail["low_budget_start_code"] = code
		detail["low_budget_status"] = st
		_, e := os.Stat(filepath.Join(v.faults, "restore_pressure.reached"))
		require(e == nil, "restore pressure hook did not engage")
		require(v.sink.count() == preKill, "output emitted by refused low-budget restore")
		require(hash(currentPath) == currentBeforeKill, "refused low-budget restore changed CURRENT")
		metrics := v.a.ok("GET", "/v1/metrics", nil)
		detail["low_budget_metrics"] = metrics
		v.server.stop(syscall.SIGKILL)
		v.disarm("restore_pressure")
		checkBroker("broker_after_low_budget")
	}
	v.start(serverBin)
	v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	if reliable {
		eventually("restore committed cut", func() bool {
			return number(v.status(), "checkpoint", "reliable_source", "restored_cut") == float64(committedRows)
		})
		require(v.readerName() != oldReader, "old attempt consumer was not replaced")
	}
	replayFrom := outputsAt(committedRows)
	expectedTotal := preKill + (len(want) - replayFrom)
	v.publish(p2+1, n, shape)
	eventually("final outputs", func() bool { return v.sink.count() == expectedTotal })
	v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
	require(snapshotVersion(v.checkpoint) == expectVersion, "final CURRENT outer version")
	if reliable {
		eventually("source ACK drain", func() bool {
			s := v.status()
			return number(s, "checkpoint", "reliable_source", "committed_cut") == n && number(s, "checkpoint", "reliable_source", "pending_messages") == 0
		})
		final := v.producer.request("$JS.API.CONSUMER.INFO.INPUT."+v.readerName(), []byte("{}"))
		require(number(final, "num_ack_pending") == 0 && number(final, "ack_floor", "stream_seq") == n, "broker ACK floor differs from committed cut")
		detail["broker_final"] = final
	}
	time.Sleep(100 * time.Millisecond)
	raw := v.sink.rows()
	require(len(raw) == expectedTotal, fmt.Sprintf("late extra output: %d want %d", len(raw), expectedTotal))
	// Exact sequence: pre-kill prefix A[:preKill] then replay A[replayFrom:].
	got := make([]string, len(raw))
	ids := make([]string, len(raw))
	for i, r := range raw {
		got[i], ids[i] = decode(r, reliable)
	}
	wantSeq := append(append([]string(nil), wantText[:preKill]...), wantText[replayFrom:]...)
	for i := range wantSeq {
		require(got[i] == wantSeq[i], fmt.Sprintf("oracle mismatch at %d: got %s want %s", i, got[i], wantSeq[i]))
	}
	duplicates := preKill - replayFrom
	if reliable {
		unique := map[string]string{}
		var order []string
		for i, id := range ids {
			if old, ok := unique[id]; ok {
				require(old == got[i], "same output ID has different content")
				continue
			}
			unique[id] = got[i]
			order = append(order, id)
		}
		require(len(unique) == len(want), "unique output IDs differ from oracle count (loss or extra)")
		for i, id := range order {
			require(unique[id] == wantText[i], "ID order differs from oracle order")
		}
		// Replayed rows must reuse exactly the IDs of the uncommitted suffix.
		for k := 0; k < duplicates; k++ {
			require(ids[preKill+k] == ids[replayFrom+k], "replayed output changed ID")
		}
	}
	if cut == "output_inflight" {
		// The unacknowledged request carried oracle rows of the uncommitted
		// suffix, and every one of them was replayed (same content and ID).
		require(len(held) >= 1, "no in-flight output captured")
		for k, r := range held {
			text, id := decode(r, reliable)
			require(replayFrom+k < len(wantText) && text == wantText[replayFrom+k], "in-flight output differs from oracle")
			require(got[preKill+k] == text, "in-flight output was not replayed in order")
			if reliable {
				require(ids[preKill+k] == id, "replayed in-flight output changed ID")
			}
		}
	}
	if cut == "manifest_renamed" {
		orphan := detail["orphan_generation"].(string)
		require(currentName(v.checkpoint) != orphan, "renamed-but-unpublished MANIFEST was promoted")
	}
	finalStatus := v.status()
	save(filepath.Join(root, "final-status.json"), finalStatus)
	save(filepath.Join(root, "inventory.json"), v.a.ok("GET", "/v1/pipelines/check/checkpoints", nil))
	save(filepath.Join(root, "outputs.json"), got)
	// Resource release: stop the job; tracked credits must drain to zero.
	v.a.ok("POST", "/v1/pipelines/check/stop", map[string]any{})
	var metrics map[string]any
	eventually("job credits released after stop", func() bool {
		metrics = v.a.ok("GET", "/v1/metrics", nil)
		return number(metrics, "process_credits", "reservation_bytes") == 0 &&
			number(metrics, "process_credits", "physical_bytes") == 0 &&
			number(metrics, "process_credits", "live_handles") == 0
	})
	require(number(metrics, "state_accounting_errors_total") == 0, "state accounting errors after recovery")
	save(filepath.Join(root, "metrics-after-stop.json"), metrics)
	v.server.stop(syscall.SIGTERM)
	log, _ := os.ReadFile(filepath.Join(root, "server.log"))
	require(!bytes.Contains(log, []byte("accounting_error")), "memory accounting error logged")
	summary := map[string]any{"valid": true, "source": source, "shape": shape, "cut": cut, "outputs": len(raw), "oracle_outputs": len(want),
		"pre_kill_outputs": preKill, "committed_rows": committedRows, "replayed_duplicates": duplicates, "snapshot_version": expectVersion, "detail": detail,
		"scope": "isolated_process_sigkill_not_power_loss"}
	save(filepath.Join(root, "summary.json"), summary)
	return summary
}

// refused waits (no fixed sleep) until the start attempt is held as failed
// with the expected reason and returns the status document.
func (v *env) refused(label, reason string) map[string]any {
	var st map[string]any
	eventually(label+" refused", func() bool {
		st, _ = v.a.call("GET", "/v1/pipelines/check/status", nil)
		return nested(st, "actual", "status") == "failed"
	})
	msg, _ := nested(st, "actual", "last_error").(string)
	require(strings.Contains(msg, reason), label+": unexpected refusal reason: "+msg)
	return st
}

// compat: upgrade/rollback with an old (pre-v29) binary, plus new-binary
// directory profile mismatches. Nothing may change CURRENT or emit output.
func compat(root, serverBin, oldBin string) map[string]any {
	v := setup(root, "file", serverBin, "")
	defer v.close()
	result := map[string]any{}
	// (1) New binary writes a v29 directory.
	v.start(serverBin)
	v.register()
	ext := v.spec("file", "count", true)
	v.a.ok("PUT", "/v1/pipelines/check", ext)
	v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	v.publish(1, 12, "count")
	eventually("v29 outputs", func() bool { return v.sink.count() == 4 })
	eventually("v29 applied", func() bool { return number(v.status(), "observation", "runtime_progress", "ingested_rows") == 12 })
	v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
	require(snapshotVersion(v.checkpoint) == 29, "v29 expected")
	v29Current := hash(filepath.Join(v.checkpoint, "CURRENT"))
	v.server.stop(syscall.SIGTERM)
	outputs := v.sink.count()
	// (2) Rollback: old binary on the same catalog + v29 directory must refuse
	// before touching CURRENT or emitting output.
	if oldBin != "" {
		v.start(oldBin)
		resp, code := v.a.call("POST", "/v1/pipelines/check/start", map[string]any{})
		// The old binary has no FIRST/LAST/VAR_* recovery; any held refusal
		// counts, the reason text is recorded as evidence.
		st := v.refused("old binary on v29", "")
		result["rollback_start_code"] = code
		result["rollback_start"] = resp
		result["rollback_status"] = st
		require(hash(filepath.Join(v.checkpoint, "CURRENT")) == v29Current, "old binary changed v29 CURRENT")
		require(v.sink.count() == outputs, "old binary produced output from v29 history")
		// Old binary on a fresh directory also rejects the extended aligned profile.
		fresh := v.spec("file", "count", true)
		fresh["checkpoint_dir"] = filepath.Join(root, "old-fresh")
		_, vcode := v.a.call("POST", "/v1/validate", fresh)
		result["old_validate_ext_code"] = vcode
		require(vcode >= 400, "old binary accepted extended aggregate aligned spec")
		v.server.stop(syscall.SIGTERM)
		// (3) Upgrade: old binary writes a v3 directory (plain SUM).
		v.checkpoint = filepath.Join(root, "checkpoint-v3")
		plain := v.spec("file", "count", false)
		v.start(oldBin)
		v.a.ok("POST", "/v1/pipelines/check/stop", map[string]any{})
		v.a.ok("PUT", "/v1/pipelines/check", plain)
		v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
		// Fresh v3 directory replays the 12 file rows: 2 Count windows per key.
		eventually("old v3 outputs", func() bool { return v.sink.count() == outputs+4 })
		eventually("old applied", func() bool {
			return number(v.status(), "observation", "runtime_progress", "ingested_rows") == 12
		})
		v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
		require(snapshotVersion(v.checkpoint) == 3, "old binary should write v3")
		v3Current := hash(filepath.Join(v.checkpoint, "CURRENT"))
		v.server.stop(syscall.SIGKILL)
		// New binary restores the old v3 directory unchanged in semantics.
		before := v.sink.count()
		v.start(serverBin)
		v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
		v.publish(13, 18, "count")
		eventually("upgraded v3 continues", func() bool { return v.sink.count() >= before+2 })
		v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
		require(snapshotVersion(v.checkpoint) == 3, "new binary must keep v3 format for the old profile")
		v.a.ok("POST", "/v1/pipelines/check/stop", map[string]any{})
		v3Current = hash(filepath.Join(v.checkpoint, "CURRENT"))
		// (4) New binary: extended plan on the v3 directory is refused.
		mismatch := v.spec("file", "count", true)
		v.a.ok("PUT", "/v1/pipelines/check", mismatch)
		before = v.sink.count()
		_, code = v.a.call("POST", "/v1/pipelines/check/start", map[string]any{})
		st = v.refused("extended plan on v3", "profile mismatch")
		result["ext_on_v3_code"] = code
		result["ext_on_v3_status"] = st
		require(hash(filepath.Join(v.checkpoint, "CURRENT")) == v3Current, "mismatch changed v3 CURRENT")
		require(v.sink.count() == before, "mismatch produced output")
		v.server.stop(syscall.SIGTERM)
		result["old_binary_sha256"] = hash(oldBin)
	}
	// (5) New binary: plain plan on the v29 directory is refused.
	v.checkpoint = filepath.Join(root, "checkpoint")
	v.start(serverBin)
	_, _ = v.a.call("POST", "/v1/pipelines/check/stop", map[string]any{})
	plainOnV29 := v.spec("file", "count", false)
	v.a.ok("PUT", "/v1/pipelines/check", plainOnV29)
	before := v.sink.count()
	_, code := v.a.call("POST", "/v1/pipelines/check/start", map[string]any{})
	st := v.refused("plain plan on v29", "profile mismatch")
	result["plain_on_v29_code"] = code
	result["plain_on_v29_status"] = st
	require(hash(filepath.Join(v.checkpoint, "CURRENT")) == v29Current, "mismatch changed v29 CURRENT")
	require(v.sink.count() == before, "mismatch produced output")
	// (6) Semantic change (FIRST <-> LAST) on the v29 directory is refused.
	changed := v.spec("file", "count", true)
	node := changed["graph"].(map[string]any)["nodes"].([]any)[1].(map[string]any)
	list := node["aggs"].([]any)
	list[1].(map[string]any)["fn"], list[2].(map[string]any)["fn"] = "last", "first"
	v.a.ok("PUT", "/v1/pipelines/check", changed)
	_, code = v.a.call("POST", "/v1/pipelines/check/start", map[string]any{})
	st = v.refused("FIRST/LAST swap", "semantics changed")
	result["first_last_swap_code"] = code
	result["first_last_swap_status"] = st
	require(hash(filepath.Join(v.checkpoint, "CURRENT")) == v29Current, "semantic mismatch changed CURRENT")
	require(v.sink.count() == before, "semantic mismatch produced output")
	// (7) Original plan still restores (the refusals did not damage history).
	v.a.ok("PUT", "/v1/pipelines/check", ext)
	v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	v.publish(19, 24, "count")
	eventually("original plan resumes", func() bool { return v.sink.count() > before })
	v.server.stop(syscall.SIGTERM)
	result["valid"] = true
	save(filepath.Join(root, "summary.json"), result)
	return result
}

// compatSlide: v31 (sliding count) against older binaries and neighbouring
// profiles. old = pre-v29 (424cf95), prev = #29 (v29/v30, pre-v31). Nothing
// refused may change CURRENT or emit output.
func compatSlide(root, serverBin, oldBin, prevBin string) map[string]any {
	v := setup(root, "file", serverBin, "")
	defer v.close()
	result := map[string]any{}
	cur := func() string { return hash(filepath.Join(v.checkpoint, "CURRENT")) }
	// (1) New binary writes a v31 directory.
	v.start(serverBin)
	v.register()
	slide := v.spec("file", "slide", true)
	v.a.ok("PUT", "/v1/pipelines/check", slide)
	v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	v.publish(1, 12, "slide")
	eventually("v31 outputs", func() bool { return v.sink.count() == len(oracle("slide", 12)) })
	eventually("v31 applied", func() bool { return number(v.status(), "observation", "runtime_progress", "ingested_rows") == 12 })
	v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
	require(snapshotVersion(v.checkpoint) == 31, "v31 expected")
	v31Current := cur()
	v.server.stop(syscall.SIGTERM)
	outputs := v.sink.count()
	// (2) Older binaries on the same catalog + v31 directory refuse, and
	// refuse the sliding-count aligned spec on a fresh directory.
	for label, bin := range map[string]string{"old_424cf95": oldBin, "prev_29": prevBin} {
		if bin == "" {
			continue
		}
		v.start(bin)
		resp, code := v.a.call("POST", "/v1/pipelines/check/start", map[string]any{})
		st := v.refused(label+" on v31", "")
		result[label+"_on_v31"] = map[string]any{"start_code": code, "start": resp, "status": st, "binary_sha256": hash(bin)}
		require(cur() == v31Current, label+" changed v31 CURRENT")
		require(v.sink.count() == outputs, label+" produced output from v31 history")
		fresh := v.spec("file", "slide", true)
		fresh["checkpoint_dir"] = filepath.Join(root, label+"-fresh")
		_, vcode := v.a.call("POST", "/v1/validate", fresh)
		result[label+"_validate_slide_code"] = vcode
		require(vcode >= 400, label+" accepted sliding-count aligned spec")
		_, e := os.Stat(fresh["checkpoint_dir"].(string))
		require(os.IsNotExist(e), label+" created a checkpoint directory for a refused spec")
		v.server.stop(syscall.SIGTERM)
	}
	// (3) #29 binary writes a v29 directory; the new binary keeps it v29.
	if prevBin != "" {
		v.checkpoint = filepath.Join(root, "checkpoint-v29")
		ext := v.spec("file", "count", true)
		v.start(prevBin)
		_, _ = v.a.call("POST", "/v1/pipelines/check/stop", map[string]any{})
		v.a.ok("PUT", "/v1/pipelines/check", ext)
		v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
		before := v.sink.count()
		eventually("prev v29 outputs", func() bool { return v.sink.count() == before+len(oracle("count", 12)) })
		eventually("prev applied", func() bool { return number(v.status(), "observation", "runtime_progress", "ingested_rows") == 12 })
		v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
		require(snapshotVersion(v.checkpoint) == 29, "#29 binary should write v29")
		v.server.stop(syscall.SIGKILL)
		// New binary: sliding-count plan on the v29 directory is refused.
		v.start(serverBin)
		v.a.ok("PUT", "/v1/pipelines/check", v.spec("file", "slide", true))
		v29Current := cur()
		before = v.sink.count()
		_, code := v.a.call("POST", "/v1/pipelines/check/start", map[string]any{})
		result["slide_on_v29"] = map[string]any{"start_code": code, "status": v.refused("slide plan on v29", "profile mismatch")}
		require(cur() == v29Current && v.sink.count() == before, "slide-on-v29 refusal changed state")
		// The original v29 plan continues on the new binary and stays v29.
		v.a.ok("PUT", "/v1/pipelines/check", ext)
		v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
		v.publish(13, 18, "slide")
		eventually("v29 continues on new binary", func() bool { return v.sink.count() >= before+2 })
		v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
		require(snapshotVersion(v.checkpoint) == 29, "new binary must keep writing v29 for the v29 profile")
		v.a.ok("POST", "/v1/pipelines/check/stop", map[string]any{})
		v.server.stop(syscall.SIGTERM)
		result["v29_dir_continued_by_new_binary"] = true
	}
	// (4) New binary on the v31 directory: neighbouring profiles and changed
	// sliding-count parameters (old params vs new) are refused.
	v.checkpoint = filepath.Join(root, "checkpoint")
	v.start(serverBin)
	_, _ = v.a.call("POST", "/v1/pipelines/check/stop", map[string]any{})
	param := func(size, step int) map[string]any {
		sp := v.spec("file", "slide", true)
		w := sp["graph"].(map[string]any)["nodes"].([]any)[1].(map[string]any)["window"].(map[string]any)
		w["size"], w["step"] = size, step
		return sp
	}
	cases := []struct {
		label, reason string
		spec          map[string]any
	}{
		{"ext Count plan on v31", "profile mismatch", v.spec("file", "count", true)},
		{"slide size 4 on v31 (saved size 3)", "semantics changed", param(4, 2)},
		{"slide step 3 on v31 (saved step 2)", "semantics changed", param(3, 3)},
	}
	for _, c := range cases {
		v.a.ok("PUT", "/v1/pipelines/check", c.spec)
		before := v.sink.count()
		_, code := v.a.call("POST", "/v1/pipelines/check/start", map[string]any{})
		result[c.label] = map[string]any{"start_code": code, "status": v.refused(c.label, c.reason)}
		require(cur() == v31Current, c.label+" changed v31 CURRENT")
		require(v.sink.count() == before, c.label+" produced output")
	}
	// (5) Original plan still restores from the untouched v31 history and
	// continues exactly where C1 left off (rows 13..18 in this directory's
	// source file were already appended by step 3; replay from the cut).
	v.a.ok("PUT", "/v1/pipelines/check", slide)
	before := v.sink.count()
	v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	if prevBin == "" {
		v.publish(13, 18, "slide")
	}
	want := oracle("slide", 18)
	eventually("original v31 plan resumes", func() bool { return v.sink.count() == before+len(want)-len(oracle("slide", 12)) })
	tail := v.sink.rows()[before:]
	for i, r := range tail {
		text, _ := decode(r, false)
		require(text == want[len(oracle("slide", 12))+i].text(), "resumed v31 output differs from oracle")
	}
	v.a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
	require(snapshotVersion(v.checkpoint) == 31, "v31 profile kept")
	v.server.stop(syscall.SIGTERM)
	result["valid"] = true
	save(filepath.Join(root, "summary.json"), result)
	return result
}

func main() {
	server := flag.String("server-bin", "", "Sparrow server (jetstream feature for JetStream runs)")
	natsBin := flag.String("nats-server", "", "pinned NATS server (JetStream runs)")
	oldBin := flag.String("old-server-bin", "", "pre-v29 server for rollback/upgrade")
	prevBin := flag.String("prev-server-bin", "", "v29/v30-capable (#29) server, pre-v31, for compat_slide")
	out := flag.String("out", "", "new evidence directory")
	source := flag.String("source", "file", "file or jetstream")
	shape := flag.String("shape", "count", "count, et (tumbling), hop or slide (COUNT_WINDOW(3,2))")
	cut := flag.String("cut", "", "input_after|output_after|output_inflight|commit_before|manifest_renamed|commit_after|restore_kill|ack_lost|compat")
	flag.Parse()
	require(*server != "" && *out != "" && *cut != "", "server-bin, out and cut required")
	root, e := filepath.Abs(*out)
	must(e)
	must(os.Mkdir(root, 0700))
	self, e := os.Executable()
	must(e)
	bins := map[string]any{"server_sha256": hash(*server), "driver_sha256": hash(self)}
	if *natsBin != "" {
		bins["nats_sha256"] = hash(*natsBin)
	}
	save(filepath.Join(root, "binaries.json"), bins)
	if *cut == "compat" {
		compat(filepath.Join(root, "compat"), *server, *oldBin)
		fmt.Println("AGG_RECOVERY_COMPAT_OK")
		return
	}
	require(*source == "file" || *source == "jetstream", "source")
	if *cut == "compat_slide" {
		compatSlide(filepath.Join(root, "compat"), *server, *oldBin, *prevBin)
		fmt.Println("AGG_RECOVERY_COMPAT_SLIDE_OK")
		return
	}
	require(*shape == "count" || *shape == "slide" || ((*shape == "et" || *shape == "hop") && *source == "file"), "shape")
	switch *cut {
	case "empty", "not_full", "at_boundary", "low_budget":
		require(*shape == "slide", *cut+" is a sliding-count cut")
	}
	s := run(filepath.Join(root, "run"), *source, *shape, *cut, *server, *natsBin)
	fmt.Println("AGG_RECOVERY_PROCESS_OK", *source, *shape, *cut, s["outputs"])
}
