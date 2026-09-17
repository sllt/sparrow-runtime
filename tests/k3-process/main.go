// Private loopback File/DAG/HTTP process-fault oracle. No broker, service,
// firewall or existing deployment is modified. Standard library only.
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
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

func must(e error) {
	if e != nil {
		panic(e)
	}
}
func require(v bool, s string) {
	if !v {
		panic(s)
	}
}
func data(v any) []byte    { b, e := json.Marshal(v); must(e); return b }
func save(p string, v any) { must(os.WriteFile(p, append(data(v), '\n'), 0600)) }
func hash(p string) string {
	f, e := os.Open(p)
	must(e)
	defer f.Close()
	h := sha256.New()
	_, e = io.Copy(h, f)
	must(e)
	return hex.EncodeToString(h.Sum(nil))
}
func wait(label string, f func() bool) {
	until := time.Now().Add(12 * time.Second)
	for time.Now().Before(until) {
		if f() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	panic("deadline: " + label)
}

type capture struct {
	mu       sync.Mutex
	rows     []map[string]any
	at       []time.Time
	server   *http.Server
	listener net.Listener
	fail     atomic.Bool
}

func newCapture() *capture {
	l, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	c := &capture{listener: l}
	c.server = &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		b, e := io.ReadAll(io.LimitReader(r.Body, 1024*1024+1))
		if e != nil || len(b) > 1024*1024 {
			w.WriteHeader(413)
			return
		}
		var rows []map[string]any
		if json.Unmarshal(b, &rows) != nil {
			w.WriteHeader(400)
			return
		}
		if c.fail.Load() {
			time.Sleep(200 * time.Millisecond)
			w.WriteHeader(400)
			return
		}
		c.mu.Lock()
		c.rows = append(c.rows, rows...)
		for range rows {
			c.at = append(c.at, time.Now())
		}
		c.mu.Unlock()
		w.WriteHeader(200)
	})}
	go func() { _ = c.server.Serve(l) }()
	return c
}
func (c *capture) port() int { return c.listener.Addr().(*net.TCPAddr).Port }
func (c *capture) snapshot() []map[string]any {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]map[string]any(nil), c.rows...)
}

type api struct {
	base   string
	client http.Client
}

func (a api) call(method, p string, v any) (map[string]any, int) {
	var body io.Reader
	if v != nil {
		body = bytes.NewReader(data(v))
	}
	r, e := http.NewRequest(method, a.base+p, body)
	must(e)
	r.Header.Set("Authorization", "Bearer k3-private-process-fixture")
	r.Header.Set("Content-Type", "application/json")
	resp, e := a.client.Do(r)
	if e != nil {
		return nil, 0
	}
	defer resp.Body.Close()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, 2*1024*1024))
	if e != nil {
		return nil, resp.StatusCode
	}
	var result map[string]any
	_ = json.Unmarshal(raw, &result)
	return result, resp.StatusCode
}
func (a api) ok(method, p string, v any) map[string]any {
	result, code := a.call(method, p, v)
	require(code >= 200 && code < 300, fmt.Sprintf("%s %s: %d %v", method, p, code, result))
	return result
}
func nested(m map[string]any, keys ...string) any {
	var v any = m
	for _, k := range keys {
		next, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = next[k]
	}
	return v
}
func appendRows(p, device string, values []int) {
	f, e := os.OpenFile(p, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(e)
	for _, v := range values {
		_, e = f.Write(append(data(map[string]any{"device_id": device, "v": v}), '\n'))
		must(e)
	}
	must(f.Sync())
	must(f.Close())
}
func values(rows []map[string]any, field string) []int {
	result := make([]int, 0, len(rows))
	for _, r := range rows {
		v, ok := r[field].(float64)
		require(ok, "output schema/value mismatch")
		result = append(result, int(v))
	}
	sort.Ints(result)
	return result
}
func equal(a, b []int) bool { return bytes.Equal(data(a), data(b)) }
func scenario(root, binary, shape string) {
	must(os.Mkdir(root, 0700))
	first := newCapture()
	second := newCapture()
	defer first.server.Close()
	defer second.server.Close()
	l, e := net.Listen("tcp", "127.0.0.1:0")
	must(e)
	port := l.Addr().(*net.TCPAddr).Port
	must(l.Close())
	a := api{fmt.Sprintf("http://127.0.0.1:%d", port), http.Client{Timeout: 5 * time.Second}}
	var child *exec.Cmd
	var log *os.File
	stop := func(signal os.Signal) {
		if child == nil {
			return
		}
		_ = child.Process.Signal(signal)
		done := make(chan struct{})
		go func() { _ = child.Wait(); close(done) }()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			_ = child.Process.Kill()
			<-done
		}
		_ = log.Close()
		child = nil
	}
	defer func() { stop(syscall.SIGKILL) }()
	start := func() {
		log, e = os.OpenFile(filepath.Join(root, "server.log"), os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
		must(e)
		child = exec.Command(binary, "--bind", fmt.Sprintf("127.0.0.1:%d", port), "--catalog", filepath.Join(root, "catalog.db"), "--max-jobs", "1", "--safe-mode")
		child.Env = append(os.Environ(), "SPARROW_TOKEN=k3-private-process-fixture", "SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef", "SPARROW_REQUIRE_SECRETS_KEY=1", "SPARROW_DATA_ROOTS="+root)
		child.Stdout = log
		child.Stderr = log
		must(child.Start())
		wait("health", func() bool { _, code := a.call("GET", "/v1/health", nil); return code == 200 })
	}
	pa, pb := filepath.Join(root, "a.ndjson"), filepath.Join(root, "b.ndjson")
	appendRows(pa, "a", []int{1, 2, 3, 4})
	appendRows(pb, "b", []int{10, 20, 30})
	source := map[string]any{"kind": "file", "path": pa, "file_contract": "append_only", "inbox_capacity": 4}
	sourceB := map[string]any{"kind": "file", "path": pb, "file_contract": "append_only", "inbox_capacity": 4}
	sink := func(c *capture) map[string]any {
		return map[string]any{"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/out", c.port()), "batch_rows": 8, "linger_ms": 2, "outbox_capacity": 4}
	}
	nodes := []any{map[string]any{"id": 1, "kind": "memory_source", "table": "s", "out": []int{3}}, map[string]any{"id": 2, "kind": "memory_source", "table": "s", "out": []int{3}}, map[string]any{"id": 3, "kind": "union_all", "out": []int{4}}, map[string]any{"id": 4, "kind": "branch", "out": []int{5, 6}}, map[string]any{"id": 5, "kind": "capture_sink", "name": "a"}, map[string]any{"id": 6, "kind": "capture_sink", "name": "b"}}
	initial := []int{1, 2, 3, 4, 10, 20, 30}
	later := []int{5, 6, 40}
	field := "v"
	if shape == "two-count" {
		nodes[0].(map[string]any)["out"] = []int{10}
		nodes[1].(map[string]any)["out"] = []int{11}
		for i, size := range []int{3, 2} {
			nodes = append(nodes, map[string]any{"id": 10 + i, "kind": "window_agg", "keys": []string{"device_id"}, "window": map[string]any{"kind": "count", "size": size}, "aggs": []any{map[string]any{"fn": "sum", "expr": map[string]any{"k": "col", "name": "v"}, "alias": "s"}}, "out": []int{3}})
		}
		initial = []int{6, 30}
		later = []int{15, 70}
		field = "s"
	}
	checkpoint := filepath.Join(root, "checkpoint")
	spec := map[string]any{"stream": "s", "source": source, "sink": sink(first), "recovery": "aligned", "checkpoint_dir": checkpoint, "checkpoint": map[string]any{"timeout_ms": 2000, "resume_latest": true}, "graph_io": map[string]any{"sources": map[string]any{"1": source, "2": sourceB}, "sinks": map[string]any{"5": sink(first), "6": sink(second)}}, "graph": map[string]any{"version": 1, "pipeline_id": 44, "revision_id": 1, "nodes": nodes}}
	start()
	for _, p := range []int{first.port(), second.port()} {
		a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": p})
	}
	a.ok("PUT", "/v1/streams/s", map[string]any{"fields": []any{map[string]any{"name": "device_id", "type": "utf8", "nullable": false}, map[string]any{"name": "v", "type": "int64", "nullable": false}}})
	save(filepath.Join(root, "spec.json"), spec)
	save(filepath.Join(root, "explain.json"), a.ok("POST", "/v1/explain", spec))
	a.ok("PUT", "/v1/pipelines/check", spec)
	a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	status := func() map[string]any { return a.ok("GET", "/v1/pipelines/check/status", nil) }
	await := func(n int) {
		wait("both required outputs", func() bool { return len(first.snapshot()) == n && len(second.snapshot()) == n })
	}
	await(len(initial))
	for _, c := range []*capture{first, second} {
		require(equal(values(c.snapshot(), field), initial), "independent initial oracle")
	}
	a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
	current := hash(filepath.Join(checkpoint, "CURRENT"))
	save(filepath.Join(root, "committed-status.json"), status())
	appendRows(pa, "a", []int{5, 6})
	appendRows(pb, "b", []int{40})
	await(len(initial) + len(later))
	must(os.Mkdir(filepath.Join(checkpoint, "CURRENT.tmp"), 0700))
	failed, code := a.call("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
	require(code >= 400, "publication fault must fail")
	save(filepath.Join(root, "failed-commit.json"), failed)
	require(hash(filepath.Join(checkpoint, "CURRENT")) == current, "failed commit changed CURRENT")
	stop(syscall.SIGKILL)
	must(os.Remove(filepath.Join(checkpoint, "CURRENT.tmp")))
	start()
	a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	await(len(initial) + 2*len(later))
	for _, c := range []*capture{first, second} {
		out := c.snapshot()
		require(equal(values(out[len(initial):len(initial)+len(later)], field), later), "pre-kill output oracle")
		require(equal(values(out[len(initial)+len(later):], field), later), "restored partial-state/cursor oracle")
	}
	a.ok("POST", "/v1/pipelines/check/checkpoint", map[string]any{})
	save(filepath.Join(root, "restored-status.json"), status())
	current = hash(filepath.Join(checkpoint, "CURRENT"))
	// External A may succeed while B rejects. Do not roll back A or publish a cut.
	second.fail.Store(true)
	appendRows(pa, "a", []int{7, 8, 9})
	appendRows(pb, "b", []int{50, 60})
	wait("required sink failure", func() bool { return nested(status(), "actual", "status") == "failed" })
	require(len(first.snapshot()) > len(initial)+2*len(later), "required A must have succeeded before B's injected failure")
	require(hash(filepath.Join(checkpoint, "CURRENT")) == current, "required output failure published checkpoint")
	save(filepath.Join(root, "sink-failed-status.json"), status())
	save(filepath.Join(root, "outputs-a.json"), first.snapshot())
	save(filepath.Join(root, "outputs-b.json"), second.snapshot())
	stop(syscall.SIGTERM)
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "shape": shape, "required_sources": 2, "required_sinks": 2, "checkpoint_commit_failure": true, "sigkill_restore": true, "restored_state_participants": map[string]int{"zero": 0, "two-count": 2}[shape], "partial_external_success_no_rollback": true, "certified": false})
	fmt.Println("K3_PROCESS_OK", shape)
}
func main() {
	binary := flag.String("server-bin", "", "production server")
	out := flag.String("out", "", "new artifact directory")
	flag.Parse()
	require(*binary != "" && *out != "", "server-bin and out required")
	root, e := filepath.Abs(*out)
	must(e)
	must(os.Mkdir(root, 0700))
	self, e := os.Executable()
	must(e)
	save(filepath.Join(root, "binaries.json"), map[string]any{"server_sha256": hash(*binary), "driver_sha256": hash(self)})
	for _, shape := range []string{"zero", "two-count"} {
		scenario(filepath.Join(root, shape), *binary, shape)
	}
}
