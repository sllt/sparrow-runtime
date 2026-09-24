package main

// Independent GTC1/GTD1 decoder and actual SIGKILL oracle. No Rust evaluator or
// test-only server hooks; one required HTTP leg accepts, the other withholds ACK.
import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type graphCut struct {
	ID       uint64            `json:"checkpoint_id"`
	Version  uint16            `json:"version"`
	Sequence uint64            `json:"sequence"`
	Micros   uint64            `json:"micros"`
	Observed uint64            `json:"observed_micros"`
	Ingested uint64            `json:"ingested"`
	EOF      map[uint32]bool   `json:"eof"`
	Idle     map[uint32]bool   `json:"idle"`
	Outputs  map[uint32]uint64 `json:"outputs"`
}
type graphReader struct {
	raw    []byte
	offset int
}

func (r *graphReader) take(n int) []byte {
	b, ok := readBytes(r.raw, &r.offset, n)
	require(ok, "truncated graph cut")
	return b
}
func (r *graphReader) u32() uint32 { return binary.LittleEndian.Uint32(r.take(4)) }
func (r *graphReader) u64() uint64 { return binary.LittleEndian.Uint64(r.take(8)) }
func (r *graphReader) text() string {
	n := r.u32()
	require(n <= 65536, "graph string bound")
	return string(r.take(int(n)))
}
func decodeGraphCut(path string) graphCut {
	raw, err := hex.DecodeString(path)
	must(err)
	require(len(raw) <= 30*1024, "graph cut bound")
	r := graphReader{raw: raw}
	require(string(r.take(4)) == "GTC1", "graph cut magic")
	cut := graphCut{Sequence: r.u64(), Micros: r.u64(), Observed: r.u64(), Ingested: r.u64(), EOF: map[uint32]bool{}, Idle: map[uint32]bool{}, Outputs: map[uint32]uint64{}}
	_ = r.u64()
	_ = r.u64()
	n := r.u32()
	require(n >= 1 && n <= 16, "graph source count")
	for i := uint32(0); i < n; i++ {
		id := r.u32()
		r.take(16)
		require(r.text() == "file", "graph source kind")
		_ = r.text()
		r.take(16)
		r.take(8)
		flags := r.take(2)
		require(flags[0] <= 1 && flags[1] <= 1, "graph source flags")
		cut.Idle[id] = flags[0] == 1
		cut.EOF[id] = flags[1] == 1
		r.take(8)
		require(r.take(1)[0] <= 2, "graph File contract")
	}
	n = r.u32()
	require(n <= 64, "graph Union bound")
	for i := uint32(0); i < n; i++ {
		r.take(4)
		ports := r.u32()
		require(ports >= 2 && ports <= 16, "graph Union ports")
		r.take(int(ports+1) * 10)
	}
	n = r.u32()
	require(n >= 1 && n <= 16, "graph Sink count")
	for i := uint32(0); i < n; i++ {
		id := r.u32()
		ordinal := r.u64()
		require(ordinal > 0, "graph output cursor")
		cut.Outputs[id] = ordinal
	}
	require(r.offset == len(raw), "trailing graph cut")
	return cut
}
func currentGraphCut(dir string, requiredVersion ...uint16) graphCut {
	current, err := os.ReadFile(filepath.Join(dir, "CURRENT"))
	must(err)
	id, err := strconv.ParseUint(strings.TrimPrefix(strings.TrimSpace(string(current)), "chk-"), 10, 64)
	must(err)
	raw := snapshotPayload(dir, id)
	require(len(raw) >= 38, "graph snapshot header")
	version := binary.LittleEndian.Uint16(raw[4:6])
	if len(requiredVersion) == 0 {
		require(version == 18 || version == 19, "graph snapshot version")
	} else {
		// Only an explicit alarm-graph caller may widen the accepted profile;
		// the v18/v19 call sites stay exactly as strict as before.
		require(len(requiredVersion) == 1 && requiredVersion[0] == 22 && version == 22,
			"alarm graph snapshot version")
	}
	r := graphReader{raw: raw, offset: 38}
	require(r.text() == "time-file-dag-v1", "graph source profile")
	cut := decodeGraphCut(r.text())
	cut.ID = id
	cut.Version = version
	require(cut.Ingested == binary.LittleEndian.Uint64(raw[14:22]), "graph ingested mirror")
	return cut
}
func waitGraphCut(dir string, condition func(graphCut) bool, requiredVersion ...uint16) graphCut {
	var cut graphCut
	wait("committed graph cut", func() (ready bool) {
		defer func() {
			if problem := recover(); problem != nil {
				if err, ok := problem.(error); ok && os.IsNotExist(err) {
					ready = false
					return
				}
				panic(problem)
			}
		}()
		cut = currentGraphCut(dir, requiredVersion...)
		return condition(cut)
	})
	return cut
}
func graphDecision(dir string) (map[string]any, graphCut) {
	raw, err := os.ReadFile(filepath.Join(dir, "TIME_PENDING"))
	must(err)
	require(len(raw) >= 36 && len(raw) <= 128*1024 && string(raw[:4]) == "GTD1", "graph decision envelope")
	sum := sha256.Sum256(raw[36:])
	require(bytes.Equal(sum[:], raw[4:36]), "graph decision checksum")
	var decision map[string]any
	must(json.Unmarshal(raw[36:], &decision))
	return decision, decodeGraphCut(decision["position"].(string))
}
func appendGraphRow(file string, v, ts int) {
	f, err := os.OpenFile(file, os.O_WRONLY|os.O_APPEND, 0600)
	must(err)
	_, err = fmt.Fprintf(f, "{\"v\":%d,\"ts\":%d}\n", v, ts)
	must(err)
	must(f.Sync())
	must(f.Close())
}
func graphSpec(root string, a, b *capture, kind string) map[string]any {
	event := strings.HasPrefix(kind, "et-")
	contract := "append_only"
	if kind == "et-eof" || kind == "et-hop-eof" {
		contract = "sealed"
	}
	source := func(name string) any {
		return map[string]any{"kind": "file", "path": filepath.Join(root, name), "file_contract": contract, "inbox_capacity": 1}
	}
	sink := func(c *capture) any {
		return map[string]any{"kind": "http", "url": c.url(), "batch_rows": 1, "linger_ms": 0, "outbox_capacity": 1}
	}
	sources := []map[string]any{{"id": 1, "kind": "memory_source", "table": "telemetry", "out": []int{3}}, {"id": 2, "kind": "memory_source", "table": "telemetry", "out": []int{3}}}
	window := map[string]any{"kind": "processing_time", "size_micros": 1000000}
	if event {
		window = map[string]any{"kind": "event_time", "size_micros": 100, "event_time_field": "ts", "lateness_micros": 10}
		for _, s := range sources {
			s["event_time_field"] = "ts"
			s["out_of_orderness_micros"] = 0
		}
	}
	if kind == "et-hop-eof" {
		window["kind"] = "hop"
		window["slide_micros"] = 50
	}
	agg := []any{map[string]any{"fn": "sum", "expr": map[string]any{"k": "col", "name": "v"}, "alias": "v"}}
	nodes := []any{sources[0], sources[1], map[string]any{"id": 3, "kind": "union_all", "out": []int{4}},
		map[string]any{"id": 4, "kind": "window_agg", "window": window, "keys": []string{}, "aggs": agg, "out": []int{5}},
		map[string]any{"id": 5, "kind": "branch", "out": []int{10, 11}}}
	if kind == "pt-rejoin" || kind == "pt-budget-refusal" {
		nodes = nodes[:3]
		nodes = append(nodes, map[string]any{"id": 4, "kind": "branch", "out": []int{5, 6}},
			map[string]any{"id": 5, "kind": "window_agg", "window": window, "keys": []string{}, "aggs": agg, "out": []int{7}},
			map[string]any{"id": 6, "kind": "window_agg", "window": window, "keys": []string{}, "aggs": agg, "out": []int{7}},
			map[string]any{"id": 7, "kind": "union_all", "out": []int{8}},
			map[string]any{"id": 8, "kind": "window_agg", "window": map[string]any{"kind": "count", "size": 2}, "keys": []string{}, "aggs": agg, "out": []int{9}},
			map[string]any{"id": 9, "kind": "branch", "out": []int{10, 11}})
	}
	nodes = append(nodes, map[string]any{"id": 10, "kind": "capture_sink", "name": "a"}, map[string]any{"id": 11, "kind": "capture_sink", "name": "b"})
	io := map[string]any{"sources": map[string]any{"1": source("a.ndjson"), "2": source("b.ndjson")}, "sinks": map[string]any{"10": sink(a), "11": sink(b)}}
	primarySink := sink(a)
	if kind == "pt-rejoin" {
		// Keep the real server's Compact quota, not the large embedding test
		// topology. One source -> two PT branches -> Union -> Count -> Sink
		// has seven edges; the twelve-edge variant is tested as a refusal.
		sources[0]["out"] = []int{4}
		nodes = []any{sources[0], nodes[3], nodes[4], nodes[5], nodes[6], nodes[7], nodes[9]}
		nodes[5].(map[string]any)["out"] = []int{10}
		io["sources"] = map[string]any{"1": source("a.ndjson")}
		io["sinks"] = map[string]any{"10": sink(b)}
		primarySink = sink(b)
	}
	if kind == "et-idle" {
		io["idle_after_ms"] = 300
	}
	return map[string]any{"stream": "telemetry", "source": source("a.ndjson"), "sink": primarySink, "fail_on_decode": true, "recovery": "aligned",
		"checkpoint_dir": filepath.Join(root, "checkpoint"), "checkpoint": checkpointPolicy(float64(100), true), "graph_io": io,
		"graph": map[string]any{"version": 1, "pipeline_id": 818, "revision_id": 1, "nodes": nodes}}
}
func runGraphProcess(root, serverBin, kind string) {
	must(os.Mkdir(root, 0700))
	a, b := newCapture(), newCapture()
	defer a.close()
	defer b.close()
	if kind != "et-idle" {
		b.setHold()
	}
	first, second := filepath.Join(root, "a.ndjson"), filepath.Join(root, "b.ndjson")
	must(os.WriteFile(first, nil, 0600))
	must(os.WriteFile(second, nil, 0600))
	ts := 10
	if kind == "et-hop-eof" {
		ts = 60
	}
	appendGraphRow(first, 3, ts)
	appendGraphRow(second, 7, ts+10)
	multiSink := kind != "pt-rejoin"
	if !multiSink {
		appendGraphRow(first, 7, ts+10)
	}
	if kind == "et-idle" {
		must(os.WriteFile(second, nil, 0600))
	}
	port := freePort()
	api := newAPI(port)
	var process *child
	stop := func(signal os.Signal) {
		if process != nil {
			command := process.cmd
			process.stop(signal)
			if signal == syscall.SIGKILL {
				status, ok := command.ProcessState.Sys().(syscall.WaitStatus)
				require(ok && status.Signaled() && status.Signal() == syscall.SIGKILL, "not an actual graph SIGKILL")
			}
			process = nil
		}
	}
	defer func() { stop(syscall.SIGKILL) }()
	start := func() {
		process = spawnServer(serverBin, root, filepath.Join(root, "server.log"), port)
		waitHealth(api, "graph server")
	}
	start()
	allowCaptures(api, a, b)
	api.ok(http.MethodPut, "/v1/streams/telemetry", map[string]any{"fields": []any{
		map[string]any{"name": "v", "type": "int64", "nullable": false}, map[string]any{"name": "ts", "type": "int64", "nullable": false}}})
	spec := graphSpec(root, a, b, kind)
	save(filepath.Join(root, "spec.json"), spec)
	startPipeline(api, "graph", spec)
	dir := filepath.Join(root, "checkpoint")
	baseA, baseB := 0, 0
	if strings.HasPrefix(kind, "pt-") {
		waitGraphCut(dir, func(c graphCut) bool { return c.Ingested == 2 })
		stop(syscall.SIGKILL)
		require(a.receivedRows() == 0 && b.receivedRows() == 0, "PT fixture missed pending window")
		time.Sleep(1100 * time.Millisecond)
		start()
		api.ok(http.MethodPost, "/v1/pipelines/graph/start", map[string]any{})
		waitRunning(api, "graph")
		time.Sleep(100 * time.Millisecond)
		require(a.receivedRows() == 0 && b.receivedRows() == 0, "graph downtime advanced PT")
	} else if kind == "et-union" {
		waitGraphCut(dir, func(c graphCut) bool { return c.Ingested == 2 })
		require(a.receivedRows() == 0, "ET prematurely closed")
		appendGraphRow(first, 5, 210)
		appendGraphRow(second, 9, 220)
	} else if kind == "et-idle" {
		// Do not hold the first idle transition: the empty port can time out
		// slightly before the active port and legitimately advance the union.
		// Establish an acknowledged prefix, then hold one definite successor.
		waitGraphCut(dir, func(c graphCut) bool { return c.Idle[1] && c.Idle[2] })
		appendGraphRow(first, 7, 210)
		waitGraphCut(dir, func(c graphCut) bool { return c.Ingested == 2 && c.Outputs[10] == 2 && c.Outputs[11] == 2 })
		waitGraphCut(dir, func(c graphCut) bool { return c.Idle[1] && c.Idle[2] })
		baseA, baseB = a.rowCount(), b.rowCount()
		require(baseA == 1 && baseB == 1, "idle prefix cut")
		require(number(rowData(a.snapshot().Rows[0])["v"]) == 3 && number(rowData(b.snapshot().Rows[0])["v"]) == 3, "idle prefix aggregate")
		b.setHold()
		appendGraphRow(first, 5, 310)
	}
	want := []uint64{10}
	if kind == "pt-rejoin" {
		want = []uint64{20}
	}
	if kind == "et-hop-eof" {
		want = []uint64{10, 10}
	}
	if kind == "et-idle" {
		want = []uint64{7}
	}
	b.waitHeld()
	// Freeze the whole accepted leg, not only its first row. Otherwise a
	// second row accepted between sampling and SIGKILL pollutes the suffix.
	if multiSink {
		wait("first graph Sink accepted whole round", func() bool { return a.rowCount() == baseA+len(want) })
	}
	before := currentGraphCut(dir)
	decision, target := graphDecision(dir)
	require(target.Sequence == before.Sequence+1, "graph pending is not immediate successor")
	currentHash, pendingHash := hash(filepath.Join(dir, "CURRENT")), hash(filepath.Join(dir, "TIME_PENDING"))
	time.Sleep(100 * time.Millisecond)
	require(hash(filepath.Join(dir, "CURRENT")) == currentHash, "partial Sink flush committed graph")
	accepted, held := a.snapshot().Rows[baseA:], b.snapshot().Received[baseB:]
	if multiSink {
		require(accepted[0]["id"] != held[0]["id"], "Sink ID namespace collision")
	}
	save(filepath.Join(root, "held-cut.json"), before)
	save(filepath.Join(root, "held-decision.json"), decision)
	stop(syscall.SIGKILL)
	require(hash(filepath.Join(dir, "CURRENT")) == currentHash && hash(filepath.Join(dir, "TIME_PENDING")) == pendingHash, "kill changed durable graph state")
	b.releaseHold(true)
	start()
	api.ok(http.MethodPost, "/v1/pipelines/graph/start", map[string]any{})
	waitRunning(api, "graph")
	committed := waitGraphCut(dir, func(c graphCut) bool {
		return c.Sequence >= target.Sequence && c.Outputs[10] == uint64(baseB+len(want)+1) && (!multiSink || c.Outputs[11] == uint64(baseB+len(want)+1))
	})
	wait("replayed required graph Sinks", func() bool {
		return (!multiSink || a.rowCount() >= baseA+len(accepted)+len(want)) && b.rowCount() >= baseB+len(want)
	})
	replayA, replayB := a.snapshot().Rows[baseA+len(accepted):], b.snapshot().Rows[baseB:]
	require(bytes.Equal(data(held[0]), data(replayB[0])), "graph replay bytes/IDs changed")
	require((!multiSink || len(replayA) == len(want)) && len(replayB) == len(want), "unexpected graph result count")
	for i, total := range want {
		if multiSink {
			require(bytes.Equal(data(accepted[i]), data(replayA[i])), "accepted graph leg replay changed")
			require(number(rowData(replayA[i])["v"]) == total, "accepted graph aggregate oracle mismatch")
		}
		require(number(rowData(replayB[i])["v"]) == total, "graph aggregate oracle mismatch")
	}
	stop(syscall.SIGKILL)
	countA, countB := a.receivedRows(), b.receivedRows()
	start()
	api.ok(http.MethodPost, "/v1/pipelines/graph/start", map[string]any{})
	waitRunning(api, "graph")
	waitGraphCut(dir, func(c graphCut) bool { return c.Sequence > committed.Sequence })
	time.Sleep(250 * time.Millisecond)
	require(a.receivedRows() == countA && b.receivedRows() == countB, "committed graph repeated output")
	stop(syscall.SIGTERM)
	save(filepath.Join(root, "capture-a.json"), a.snapshot())
	save(filepath.Join(root, "capture-b.json"), b.snapshot())
	save(filepath.Join(root, "committed-cut.json"), committed)
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "kind": kind, "real_sigkill": true, "partial_sink_flush_checked": multiSink, "pending_replay_identical": true, "committed_restart_no_repeat": true, "certified": false})
}

func runGraphBudgetRefusal(root, serverBin string) {
	must(os.Mkdir(root, 0700))
	a, b := newCapture(), newCapture()
	defer a.close()
	defer b.close()
	for _, file := range []string{"a.ndjson", "b.ndjson"} {
		must(os.WriteFile(filepath.Join(root, file), []byte("{\"v\":3,\"ts\":10}\n"), 0600))
	}
	port := freePort()
	api := newAPI(port)
	process := spawnServer(serverBin, root, filepath.Join(root, "server.log"), port)
	defer process.stop(syscall.SIGTERM)
	waitHealth(api, "graph budget fixture")
	allowCaptures(api, a, b)
	api.ok(http.MethodPut, "/v1/streams/telemetry", map[string]any{"fields": []any{map[string]any{"name": "v", "type": "int64", "nullable": false}, map[string]any{"name": "ts", "type": "int64", "nullable": false}}})
	spec := graphSpec(root, a, b, "pt-budget-refusal")
	save(filepath.Join(root, "spec.json"), spec)
	api.ok(http.MethodPut, "/v1/pipelines/graph", spec)
	api.ok(http.MethodPost, "/v1/pipelines/graph/start", map[string]any{})
	status := waitExactError(api, "graph", "job queue quota")
	save(filepath.Join(root, "status.json"), status)
	require(a.receivedRows() == 0 && b.receivedRows() == 0, "over-budget graph published output")
	_, err := os.Stat(filepath.Join(root, "checkpoint", "CURRENT"))
	require(os.IsNotExist(err), "over-budget graph advanced CURRENT")
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "default_budget_preserved": true, "explicit_refusal": true, "no_output_or_current": true})
}
func runTimeGraphMatrix(root, serverBin, oldServerBin string) {
	runGraphBudgetRefusal(filepath.Join(root, "budget-refusal"), serverBin)
	kinds := []string{"pt-union", "pt-rejoin", "et-union", "et-eof", "et-hop-eof", "et-idle"}
	for _, kind := range kinds {
		runGraphProcess(filepath.Join(root, kind), serverBin, kind)
	}
	guards := map[string]any{}
	for _, kind := range []string{"pt-union", "et-eof"} {
		source := filepath.Join(root, kind)
		guards[kind] = runProfileGuard(filepath.Join(root, "old-"+kind), oldServerBin, filepath.Join(source, "checkpoint"), filepath.Join(source, "a.ndjson"), "checkpoint source profile mismatch", "v18/v19 old binary guard")
	}
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "process_scenarios": len(kinds) + 1, "crash_scenarios": len(kinds), "budget_refusal": true, "old_profile_guards": guards, "snapshot_versions": []int{18, 19}, "exactly_once_claimed": false, "certified": false})
	fmt.Println("TIME_GRAPH_PROCESS_OK")
}
