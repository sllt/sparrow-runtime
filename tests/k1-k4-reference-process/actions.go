package main

// Independent wire/disk goldens for production (no demo feature) builds.
import (
	"bufio"
	"bytes"
	wirebinary "encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

func actionsSubmit(a api, name string, spec map[string]any) {
	a.ok(http.MethodPost, "/v1/validate", spec)
	a.ok(http.MethodPut, "/v1/pipelines/"+name, spec)
	a.ok(http.MethodPost, "/v1/pipelines/"+name+"/start", map[string]any{})
}
func actionsSchema(a api) {
	a.ok(http.MethodPut, "/v1/streams/actions", map[string]any{"fields": []any{
		map[string]any{"name": "device", "type": "utf8", "nullable": false},
		map[string]any{"name": "n", "type": "uint64", "nullable": false},
	}})
}
func actionsSpec(root string, sink map[string]any, contract string) map[string]any {
	return map[string]any{"version": 1, "stream": "actions", "recovery": "restart_fresh", "fail_on_decode": true,
		"sql":    "SELECT concat(device, '-out') AS label, to_string(n) AS number FROM actions",
		"source": map[string]any{"kind": "file", "path": filepath.Join(root, "input.ndjson"), "file_contract": contract}, "sink": sink}
}
func actionsSegments(dir string) ([]string, [][]byte) {
	files, err := filepath.Glob(filepath.Join(dir, "part-*.ndjson"))
	must(err)
	sort.Strings(files)
	var rows [][]byte
	for _, name := range files {
		file, err := os.Open(name)
		must(err)
		scan := bufio.NewScanner(file)
		for scan.Scan() {
			rows = append(rows, append([]byte(nil), scan.Bytes()...))
		}
		must(scan.Err())
		must(file.Close())
	}
	return files, rows
}
func actionsStart(root, binary string) (api, *child) {
	port := freePort()
	a := newAPI(port)
	process := spawnServer(binary, root, filepath.Join(root, "server.log"), port)
	waitHealth(a, "actions server")
	return a, process
}
func runActionsProcess(root, binary, address string) {
	absolute, err := filepath.Abs(root)
	must(err)
	root = absolute
	must(os.Mkdir(root, 0700))
	self, err := os.Executable()
	must(err)
	save(filepath.Join(root, "binaries.json"), map[string]any{"server_sha256": hash(binary), "driver_sha256": hash(self)})
	runActionsFile(filepath.Join(root, "file"), binary)
	runActionsHTTP(filepath.Join(root, "http"), binary)
	runActionsMQTT(filepath.Join(root, "mqtt"), binary, address)
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "scenarios": 3, "actual_sigkill": true, "file_restart_no_overwrite": true, "partial_tail_rejected": true, "quota_failed": true, "http_query_retry_stable": true, "mqtt_real_broker": true, "aligned_rejected": true, "performance": false, "soak": false, "certified": false})
	fmt.Println("ACTIONS_PROCESS_OK")
}
func runActionsFile(root, binary string) {
	must(os.Mkdir(root, 0700))
	output := filepath.Join(root, "output")
	must(os.Mkdir(output, 0700))
	input := bytes.Repeat([]byte("{\"device\":\"sensor\",\"n\":18446744073709551615}\n"), 8)
	must(os.WriteFile(filepath.Join(root, "input.ndjson"), input, 0600))
	a, process := actionsStart(root, binary)
	defer func() {
		if process != nil {
			silenceStop(&process, syscall.SIGKILL)
		}
	}()
	actionsSchema(a)
	sink := map[string]any{"kind": "file", "file": map[string]any{"directory": output, "segment_bytes": 1024, "max_bytes": 8192, "max_files": 16, "row_bytes": 1000, "sync_data": true},
		"action": map[string]any{"body": map[string]any{"label": map[string]any{"$field": "label"}, "number": map[string]any{"$field": "number"}, "pad": strings.Repeat("p", 150)}}}
	spec := actionsSpec(root, sink, "append_only")
	save(filepath.Join(root, "spec.json"), spec)
	actionsSubmit(a, "actions", spec)
	wait("first file writes", func() bool { return mqttCounter(a, "file_written") == 8 })
	files, rows := actionsSegments(output)
	require(len(files) >= 2 && len(rows) == 8, "File rotation")
	expected := map[string]any{"label": "sensor-out", "number": "18446744073709551615", "pad": strings.Repeat("p", 150)}
	for _, row := range rows {
		var got map[string]any
		must(json.Unmarshal(row, &got))
		require(bytes.Equal(data(got), data(expected)), "File typed function/action golden")
	}
	originals := map[string]string{}
	for _, file := range files {
		originals[file] = hash(file)
	}
	// Kill between acknowledged batches; this is NOT a torn-write/power-loss test.
	silenceStop(&process, syscall.SIGKILL)
	a, process = actionsStart(root, binary)
	wait("fresh replay into new segments", func() bool { _, all := actionsSegments(output); return len(all) == 16 })
	stopPipeline(a, "actions")
	for name, digest := range originals {
		require(hash(name) == digest, "fresh restart overwrote old file")
	}
	for _, field := range []string{"checkpoint_dir", "recovery"} {
		bad := actionsSpec(root, sink, "append_only")
		if field == "recovery" {
			bad[field] = "aligned"
		} else {
			bad[field] = filepath.Join(root, "checkpoints")
		}
		a.rejected(http.MethodPost, "/v1/validate", bad)
	}
	torn := filepath.Join(output, "part-00000000000000009999.ndjson")
	must(os.WriteFile(torn, []byte("{incomplete"), 0600))
	before := fullTreeHash(output)
	a.ok(http.MethodPost, "/v1/pipelines/actions/start", map[string]any{})
	waitActual(a, "actions", "failed")
	stopPipeline(a, "actions")
	require(fullTreeHash(output) == before, "partial-tail failure mutated existing evidence")
	quota := filepath.Join(root, "quota")
	must(os.Mkdir(quota, 0700))
	limited := map[string]any{"kind": "file", "file": map[string]any{"directory": quota, "segment_bytes": 1024, "max_bytes": 1024, "max_files": 1, "row_bytes": 1000, "sync_data": true}, "action": sink["action"]}
	actionsSubmit(a, "quota", actionsSpec(root, limited, "sealed"))
	waitActual(a, "quota", "failed")
	stopPipeline(a, "quota")
	quotaFiles, quotaRows := actionsSegments(quota)
	require(len(quotaFiles) == 1 && len(quotaRows) > 0 && len(quotaRows) < 8, "quota fails without all-or-nothing claim")
	info, err := os.Stat(quotaFiles[0])
	must(err)
	require(info.Size() <= 1024, "quota exceeded")
	save(filepath.Join(root, "metrics.json"), mqttMetrics(a))
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "initial_rows": 8, "after_fresh_restart_rows": 16, "actual_sigkill": true, "initial_segments": len(files), "quota_rows": len(quotaRows)})
	silenceStop(&process, syscall.SIGTERM)
}
func runActionsHTTP(root, binary string) {
	must(os.Mkdir(root, 0700))
	must(os.WriteFile(filepath.Join(root, "input.ndjson"), []byte("{\"device\":\"x&host=http://evil/#测\",\"n\":18446744073709551615}\n{\"device\":\"two\",\"n\":2}\n"), 0600))
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	port := listener.Addr().(*net.TCPAddr).Port
	var mu sync.Mutex
	var requests []map[string]any
	connections := 0
	server := &http.Server{ReadHeaderTimeout: 2 * time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(io.LimitReader(r.Body, 65537))
		must(err)
		must(r.Body.Close())
		require(len(raw) <= 65536, "HTTP fixture body bound")
		mu.Lock()
		defer mu.Unlock()
		requests = append(requests, map[string]any{"path": r.URL.Path, "query": r.URL.Query(), "body": string(raw), "method": r.Method})
		if len(requests) == 1 {
			w.WriteHeader(503)
		} else {
			w.WriteHeader(200)
		}
		_, _ = w.Write([]byte("ok"))
	}), ConnState: func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			mu.Lock()
			connections++
			mu.Unlock()
		}
	}}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	a, process := actionsStart(root, binary)
	defer process.stop(syscall.SIGKILL)
	actionsSchema(a)
	a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": port})
	sink := map[string]any{"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/fixed?base=1", port), "batch_rows": 1, "max_inflight": 1, "linger_ms": 0,
		"action": map[string]any{"single": true, "body": map[string]any{"number": map[string]any{"$field": "number"}}, "query": map[string]any{"key": []any{map[string]any{"$field": "label"}}}}}
	spec := actionsSpec(root, sink, "sealed")
	actionsSubmit(a, "actions", spec)
	waitActual(a, "actions", "completed")
	mu.Lock()
	require(len(requests) == 3, "HTTP expected one retry and two rows")
	require(bytes.Equal(data(requests[0]), data(requests[1])), "HTTP retry recomputed query/body")
	require(connections == 1, "HTTP action did not reuse connection")
	query := requests[0]["query"].(url.Values)
	require(query["key"][0] == "x&host=http://evil/#测-out" && len(query) == 2, "escaped query changed destination shape")
	require(requests[0]["path"] == "/fixed" && requests[0]["body"] == "{\"number\":\"18446744073709551615\"}", "HTTP mapping golden")
	save(filepath.Join(root, "requests.json"), requests)
	mu.Unlock()
	save(filepath.Join(root, "metrics.json"), mqttMetrics(a))
	process.stop(syscall.SIGTERM)
}
func actionsMQTTFrame(conn net.Conn) (byte, []byte) {
	must(conn.SetReadDeadline(time.Now().Add(5 * time.Second)))
	var first [1]byte
	_, err := io.ReadFull(conn, first[:])
	must(err)
	n, mult := 0, 1
	for i := 0; i < 4; i++ {
		var b [1]byte
		_, err = io.ReadFull(conn, b[:])
		must(err)
		n += int(b[0]&127) * mult
		if b[0]&128 == 0 {
			require(n <= 65536, "MQTT oracle frame bound")
			payload := make([]byte, n)
			_, err = io.ReadFull(conn, payload)
			must(err)
			return first[0], payload
		}
		mult *= 128
	}
	panic("invalid MQTT remaining length")
}
func runActionsMQTT(root, binary, address string) {
	must(os.Mkdir(root, 0700))
	must(os.WriteFile(filepath.Join(root, "input.ndjson"), []byte("{\"device\":\"测\",\"n\":18446744073709551615}\n"), 0600))
	host, portText, err := net.SplitHostPort(address)
	must(err)
	port, err := strconv.Atoi(portText)
	must(err)
	prefix := fmt.Sprintf("sparrow/actions/%d/", time.Now().UnixNano())
	subscriber := mqttPublisher(address)
	defer subscriber.Close()
	mqttPacket(subscriber, 0x82, append(append([]byte{0, 1}, mqttString(prefix+"#")...), 0))
	header, payload := actionsMQTTFrame(subscriber)
	require(header == 0x90 && bytes.Equal(payload, []byte{0, 1, 0}), "MQTT SUBACK")
	a, process := actionsStart(root, binary)
	defer process.stop(syscall.SIGKILL)
	actionsSchema(a)
	a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": host, "port": port})
	sink := map[string]any{"kind": "mqtt", "host": host, "port": port, "topic": "unused-static", "client_id": fmt.Sprintf("actions-sink-%d", time.Now().UnixNano()),
		"action": map[string]any{"topic": []any{prefix, map[string]any{"$field": "label"}}, "body": map[string]any{"number": map[string]any{"$field": "number"}}}}
	actionsSubmit(a, "actions", actionsSpec(root, sink, "sealed"))
	header, payload = actionsMQTTFrame(subscriber)
	require(header == 0x30 && len(payload) > 2, "MQTT publish QoS0")
	size := int(wirebinary.BigEndian.Uint16(payload[:2]))
	require(size+2 <= len(payload), "MQTT topic bound")
	topic := string(payload[2 : 2+size])
	body := string(payload[2+size:])
	require(topic == prefix+"测-out" && body == "{\"number\":\"18446744073709551615\"}", "MQTT typed topic/body golden")
	waitActual(a, "actions", "completed")
	save(filepath.Join(root, "publish.json"), map[string]any{"topic": topic, "body": body})
	process.stop(syscall.SIGTERM)
}
