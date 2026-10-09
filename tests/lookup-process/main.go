// Independent, standard-library-only HTTP peer for the real Server/CLI
// Lookup process oracle. This is a bounded test fixture, never a deployment.
package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math"
	"net"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

const (
	maxFrame = 64 * 1024
	maxRows  = 4096
	maxCalls = 8192
	maxLog   = 8 * 1024 * 1024
)

type options struct {
	threshold int64
	mode      string
	delayMS   int
}

type fixture struct {
	mu          sync.Mutex
	config      options
	rows        []map[string]any
	lookupConns map[string]bool
	log         *os.File
	logBytes    int
	calls       atomic.Uint64
	requests    atomic.Uint64
	inflight    atomic.Int64
	peak        atomic.Int64
	connections atomic.Uint64
	activeConns atomic.Int64
	overflow    atomic.Bool
}

func require(err error) {
	if err != nil {
		panic(err)
	}
}

func jsonReply(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}

func reject(w http.ResponseWriter, status int, code string) {
	jsonReply(w, status, map[string]any{"error": code})
}

func readBody(w http.ResponseWriter, r *http.Request) ([]byte, error) {
	defer r.Body.Close()
	return io.ReadAll(http.MaxBytesReader(w, r.Body, maxFrame))
}

func decode(raw []byte, value any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(value); err != nil {
		return err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return errors.New("trailing JSON")
	}
	return nil
}

type responseWriter struct {
	http.ResponseWriter
	status int
}

func (w *responseWriter) WriteHeader(status int) {
	if w.status == 0 {
		w.status = status
		w.ResponseWriter.WriteHeader(status)
	}
}
func (w *responseWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	return w.ResponseWriter.Write(p)
}

func (f *fixture) logEvent(event map[string]any) error {
	raw, err := json.Marshal(event)
	if err != nil {
		return err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(raw)+1 > maxLog-f.logBytes {
		f.overflow.Store(true)
		return errors.New("fixture evidence limit exceeded")
	}
	_, err = f.log.Write(append(raw, '\n'))
	if err != nil {
		f.overflow.Store(true)
	}
	f.logBytes += len(raw) + 1
	return err
}

func (f *fixture) lookup(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		reject(w, 405, "post_required")
		return
	}
	raw, err := readBody(w, r)
	if err != nil {
		reject(w, 413, "frame_bound")
		return
	}
	var query struct {
		Keys struct {
			DeviceID *string `json:"device_id"`
		} `json:"keys"`
	}
	if decode(raw, &query) != nil || query.Keys.DeviceID == nil || len(*query.Keys.DeviceID) > 128 {
		reject(w, 400, "invalid_typed_key")
		return
	}
	f.mu.Lock()
	config := f.config
	if len(f.lookupConns) == maxRows && !f.lookupConns[r.RemoteAddr] {
		f.mu.Unlock()
		f.overflow.Store(true)
		reject(w, 503, "connection_inventory_bound")
		return
	}
	f.lookupConns[r.RemoteAddr] = true
	f.mu.Unlock()
	if err := f.logEvent(map[string]any{"event": "lookup", "body": json.RawMessage(raw), "mode": config.mode, "threshold": config.threshold, "delay_ms": config.delayMS}); err != nil {
		reject(w, 503, "evidence_bound")
		return
	}
	f.requests.Add(1)
	active := f.inflight.Add(1)
	defer f.inflight.Add(-1)
	for {
		previous := f.peak.Load()
		if active <= previous || f.peak.CompareAndSwap(previous, active) {
			break
		}
	}
	if config.delayMS > 0 {
		timer := time.NewTimer(time.Duration(config.delayMS) * time.Millisecond)
		defer timer.Stop()
		select {
		case <-r.Context().Done():
			return
		case <-timer.C:
		}
	}
	id := *query.Keys.DeviceID
	switch config.mode {
	case "error":
		reject(w, 503, "controlled_lookup_failure")
	case "wrong_schema":
		jsonReply(w, 200, map[string]any{"row": map[string]any{"device_id": id}})
	case "wrong_key":
		jsonReply(w, 200, map[string]any{"row": map[string]any{"device_id": "different-key", "threshold": config.threshold}})
	case "oversized":
		w.Header().Set("Content-Type", "application/json")
		body := `{"row":{"device_id":"` + strings.Repeat("x", maxFrame) + `","threshold":1}}`
		w.Header().Set("Content-Length", fmt.Sprint(len(body)))
		_, _ = io.WriteString(w, body)
	case "miss":
		jsonReply(w, 200, map[string]any{"row": nil})
	default:
		if id != "d1" && id != "d2" {
			jsonReply(w, 200, map[string]any{"row": nil})
			return
		}
		threshold := config.threshold
		if id == "d2" {
			threshold += 10
		}
		jsonReply(w, 200, map[string]any{"row": map[string]any{"device_id": id, "threshold": threshold}})
	}
}

func (f *fixture) update(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		reject(w, 405, "post_required")
		return
	}
	raw, err := readBody(w, r)
	if err != nil {
		reject(w, 413, "frame_bound")
		return
	}
	var update struct {
		Threshold *int64  `json:"threshold"`
		Mode      *string `json:"mode"`
		DelayMS   *int    `json:"delay_ms"`
	}
	if decode(raw, &update) != nil || (update.DelayMS != nil && (*update.DelayMS < 0 || *update.DelayMS > 5000)) || (update.Threshold != nil && *update.Threshold > math.MaxInt64-10) {
		reject(w, 400, "invalid_fixture_update")
		return
	}
	if update.Mode != nil {
		switch *update.Mode {
		case "ok", "error", "wrong_schema", "wrong_key", "oversized", "miss":
		default:
			reject(w, 400, "invalid_fixture_mode")
			return
		}
	}
	if err := f.logEvent(map[string]any{"event": "update", "body": json.RawMessage(raw)}); err != nil {
		reject(w, 503, "evidence_bound")
		return
	}
	f.mu.Lock()
	if update.Threshold != nil {
		f.config.threshold = *update.Threshold
	}
	if update.Mode != nil {
		f.config.mode = *update.Mode
	}
	if update.DelayMS != nil {
		f.config.delayMS = *update.DelayMS
	}
	config := f.config
	f.mu.Unlock()
	jsonReply(w, 200, map[string]any{"threshold": config.threshold, "mode": config.mode, "delay_ms": config.delayMS})
}

func (f *fixture) ingest(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		reject(w, 405, "post_required")
		return
	}
	raw, err := readBody(w, r)
	if err != nil {
		reject(w, 413, "frame_bound")
		return
	}
	var rows []map[string]json.RawMessage
	if decode(raw, &rows) != nil || len(rows) == 0 || len(rows) > 32 {
		reject(w, 400, "expected_bounded_row_array")
		return
	}
	checked := make([]map[string]any, 0, len(rows))
	for _, row := range rows {
		var sequence int64
		var device *string
		var threshold *int64
		if len(row) != 3 || decode(row["seq"], &sequence) != nil || sequence <= 0 || decode(row["device_id"], &device) != nil || (device != nil && len(*device) > 128) || decode(row["threshold"], &threshold) != nil {
			reject(w, 400, "unexpected_output_schema")
			return
		}
		checked = append(checked, map[string]any{"seq": sequence, "device_id": device, "threshold": threshold})
	}
	if err := f.logEvent(map[string]any{"event": "ingest", "body": json.RawMessage(raw)}); err != nil {
		reject(w, 503, "evidence_bound")
		return
	}
	f.mu.Lock()
	if len(checked) > maxRows-len(f.rows) {
		f.mu.Unlock()
		f.overflow.Store(true)
		reject(w, 429, "capture_row_bound")
		return
	}
	f.rows = append(f.rows, checked...)
	f.mu.Unlock()
	jsonReply(w, 200, map[string]any{"accepted": len(checked)})
}

func (f *fixture) result(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		reject(w, 405, "get_required")
		return
	}
	f.mu.Lock()
	rows := append([]map[string]any{}, f.rows...)
	lookupConnections := len(f.lookupConns)
	config := f.config
	f.mu.Unlock()
	jsonReply(w, 200, map[string]any{
		"rows":   rows,
		"config": map[string]any{"threshold": config.threshold, "mode": config.mode, "delay_ms": config.delayMS},
		"stats": map[string]any{
			"requests": f.requests.Load(), "inflight": f.inflight.Load(), "peak": f.peak.Load(),
			"lookup_connections": lookupConnections, "connections_total": f.connections.Load(),
			"active_connections": f.activeConns.Load(), "calls": f.calls.Load(), "overflow": f.overflow.Load(),
		},
	})
}

func main() {
	dir := flag.String("dir", "", "new fixture evidence directory (must not exist)")
	flag.Parse()
	if *dir == "" || flag.NArg() != 0 {
		flag.Usage()
		os.Exit(2)
	}
	root, err := filepath.Abs(*dir)
	require(err)
	require(os.Mkdir(root, 0700))
	log, err := os.OpenFile(filepath.Join(root, "requests.ndjson"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	require(err)
	defer log.Close()
	f := &fixture{config: options{threshold: 100, mode: "ok"}, rows: make([]map[string]any, 0), lookupConns: make(map[string]bool), log: log}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require(err)
	port := listener.Addr().(*net.TCPAddr).Port
	ports, err := json.Marshal(map[string]any{"port": port, "url": fmt.Sprintf("http://127.0.0.1:%d", port), "pid": os.Getpid()})
	require(err)
	require(os.WriteFile(filepath.Join(root, "ports.json"), append(ports, '\n'), 0600))
	mux := http.NewServeMux()
	mux.HandleFunc("/lookup", f.lookup)
	mux.HandleFunc("/update", f.update)
	mux.HandleFunc("/ingest", f.ingest)
	mux.HandleFunc("/result", f.result)
	server := &http.Server{
		ReadHeaderTimeout: 2 * time.Second, ReadTimeout: 3 * time.Second,
		WriteTimeout: 6 * time.Second, IdleTimeout: 10 * time.Second, MaxHeaderBytes: 8192,
		ConnState: func(connection net.Conn, state http.ConnState) {
			if state == http.StateNew {
				f.connections.Add(1)
				if f.activeConns.Add(1) > 32 {
					f.overflow.Store(true)
					_ = connection.Close()
				}
			}
			if state == http.StateClosed || state == http.StateHijacked {
				f.activeConns.Add(-1)
			}
		},
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			call := f.calls.Add(1)
			if call > maxCalls || f.overflow.Load() {
				f.overflow.Store(true)
				reject(w, 503, "fixture_capacity_exhausted")
				return
			}
			observed := &responseWriter{ResponseWriter: w}
			mux.ServeHTTP(observed, r)
			// Request/status evidence is bounded; business request bodies are
			// retained separately in the same stream, without silent truncation.
			_ = f.logEvent(map[string]any{"event": "http", "call": call, "method": r.Method, "path": r.URL.Path, "status": observed.status})
		}),
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	done := make(chan error, 1)
	go func() { done <- server.Serve(listener) }()
	select {
	case err := <-done:
		if err != http.ErrServerClosed {
			require(err)
		}
	case <-ctx.Done():
		shutdown, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if server.Shutdown(shutdown) != nil {
			_ = server.Close()
		}
		err := <-done
		if err != http.ErrServerClosed {
			require(err)
		}
	}
	require(log.Sync())
}
