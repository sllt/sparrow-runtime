// Sub-batch 2c process oracle: File v34 / JetStream v35 PT windows (PT
// hopping, PT sliding, PT session, PT tumbling + new aggregates) on the
// durable logical clock -> required HTTP JSON. Standard library only. The
// server is built with the off-by-default `process-fault-pause` feature:
// every crash is at a confirmable cut (the driver arms a marker, waits for
// `<point>.reached`, then SIGKILLs). The server logs every logical tick and
// row decision (`pt_clock.log`); the driver rebuilds the effective decision
// timeline across crashes (entries <= the restored cut, then the restored
// process) and an independent oracle computes every window from it. Input
// pacing uses sleeps; kill points never do. SIGKILL is not power loss.
package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
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
	"time"
)

const token = "pt-window-private-process-fixture"

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
