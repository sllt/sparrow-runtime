package main

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"math"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// ------------------------------------------------------------------ shapes

const (
	rowsTotal = 30
	hopSlide  = 100000
	hopSize   = 300000
	tumble    = 200000
	slideSize = 300000
	slideDel  = 120000
	sessGap   = 250000
	sessMax   = 500000
	downtime  = 800 * time.Millisecond
)

var shapes = []string{"pthop", "pttumble", "ptslide", "ptsess"}

// Input pacing only (wall clock between appends); never used to time a kill.
var pace = []int{10, 40, 90, 15, 130, 60, 270, 5}

func rowKey(i int) string { return map[bool]string{true: "b", false: "a"}[i%3 == 0] }
func rowJSON(i int) []byte {
	return encode(map[string]any{"device_id": rowKey(i), "v": 2*i + 1})
}
func ext(shape string) bool { return shape != "pthop" }

func sqlFor(shape string) string {
	group := map[string]string{
		"pthop":    fmt.Sprintf("HOP(PROCESSING_TIME, %d, %d)", hopSlide, hopSize),
		"pttumble": fmt.Sprintf("TUMBLE(PROCESSING_TIME, %d)", tumble),
		"ptslide":  fmt.Sprintf("SLIDING(PROCESSING_TIME, %d, %d)", slideSize, slideDel),
		"ptsess":   fmt.Sprintf("SESSION(PROCESSING_TIME, %d, %d)", sessGap, sessMax),
	}[shape]
	aggs := "COUNT(*) AS c, SUM(v) AS s"
	if ext(shape) {
		aggs += ", FIRST(v) AS f, LAST(v) AS l"
	}
	return "SELECT device_id, window_start, window_end, " + aggs + " FROM sensors GROUP BY device_id, " + group
}

// ------------------------------------------------------------------ timeline

type entry struct {
	tick   bool
	micros int64
	rows   int
}
type timeline struct {
	entries  []entry
	cuts     []int64 // restored cut per process start (first is the bootstrap 0)
	firstNew []int64 // per restart: first two ticks after the restored cut (catch-up check)
}

// parseTimeline rebuilds the effective decision history: each process start
// "start C" truncates to entries <= C (committed), then appends its own.
func parseTimeline(path string) timeline {
	var t timeline
	raw, e := os.ReadFile(path)
	if os.IsNotExist(e) {
		return t
	}
	must(e)
	// A live server may be mid-append: only complete lines count.
	raw = raw[:bytes.LastIndexByte(raw, '\n')+1]
	sc := bufio.NewScanner(bytes.NewReader(raw))
	lastPid := ""
	newTicks := 0
	for sc.Scan() {
		p := strings.Fields(sc.Text())
		require(len(p) == 4, "pt_clock.log line shape")
		micros, e := strconv.ParseInt(p[2], 10, 64)
		must(e)
		n, e := strconv.Atoi(p[3])
		must(e)
		if p[0] != lastPid {
			require(p[1] == "start", "process does not begin with a clock start: "+sc.Text())
			lastPid = p[0]
		}
		switch p[1] {
		case "start":
			keep := t.entries[:0:0]
			for _, x := range t.entries {
				if x.micros <= micros {
					keep = append(keep, x)
				}
			}
			if len(t.cuts) > 0 {
				ok := micros == 0
				for _, x := range keep {
					if x.tick && x.micros == micros {
						ok = true
					}
				}
				require(ok, fmt.Sprintf("restored cut %d is not a logged decision time", micros))
				t.firstNew = append(t.firstNew, math.MinInt64)
			}
			t.entries, newTicks = keep, 0
			t.cuts = append(t.cuts, micros)
		case "tick":
			if len(t.entries) > 0 {
				require(micros >= t.entries[len(t.entries)-1].micros, "logical clock moved backwards")
			}
			t.entries = append(t.entries, entry{true, micros, 0})
			if len(t.cuts) > 1 && micros > t.cuts[len(t.cuts)-1] && newTicks < 2 {
				newTicks++
				t.firstNew[len(t.firstNew)-1] = micros
			}
		case "rows":
			require(len(t.entries) > 0 && t.entries[len(t.entries)-1].tick && t.entries[len(t.entries)-1].micros == micros,
				"row decision not preceded by its tick")
			t.entries = append(t.entries, entry{false, micros, n})
		default:
			panic("pt_clock.log kind " + p[1])
		}
	}
	must(sc.Err())
	return t
}
func (t timeline) rows() int {
	n := 0
	for _, x := range t.entries {
		n += x.rows
	}
	return n
}
func (t timeline) rowsAt(cut int64) int {
	n := 0
	for _, x := range t.entries {
		if x.micros <= cut {
			n += x.rows
		}
	}
	return n
}
func (t timeline) lastTick() int64 {
	last := int64(math.MinInt64)
	for _, x := range t.entries {
		if x.tick {
			last = x.micros
		}
	}
	return last
}

// ------------------------------------------------------------------ oracle

type window struct {
	key        string
	start, end int64
	vals       []int64
	emit       int64 // first tick >= end (math.MaxInt64: not yet due)
}

func (w window) text(shape string) string {
	s := int64(0)
	for _, v := range w.vals {
		s += v
	}
	out := fmt.Sprintf("%s|%d|%d|%d|%d", w.key, w.start, w.end, len(w.vals), s)
	if ext(shape) {
		out += fmt.Sprintf("|%d|%d", w.vals[0], w.vals[len(w.vals)-1])
	}
	return out
}
func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// oracle computes every window the timeline will ever produce (independent
// of the operators), each tagged with the first logged tick that closes it.
func oracle(shape string, t timeline) []window {
	type arr struct{ t, v int64 }
	per := map[string][]arr{}
	var keys []string
	idx := 0
	for _, x := range t.entries {
		for k := 0; k < x.rows; k++ {
			idx++
			key := rowKey(idx)
			if _, ok := per[key]; !ok {
				keys = append(keys, key)
			}
			per[key] = append(per[key], arr{x.micros, int64(2*idx + 1)})
		}
	}
	sort.Strings(keys)
	var out []window
	collect := func(key string, s, e int64, rows []arr) {
		w := window{key: key, start: s, end: e}
		for _, r := range rows {
			if r.t >= s && r.t < e {
				w.vals = append(w.vals, r.v)
			}
		}
		require(len(w.vals) > 0, "oracle produced an empty window")
		out = append(out, w)
	}
	for _, key := range keys {
		rows := per[key]
		switch shape {
		case "ptslide":
			for _, r := range rows {
				collect(key, r.t-slideSize+1, r.t+slideDel+1, rows)
			}
		case "ptsess":
			for i := 0; i < len(rows); {
				first := rows[i].t
				end := first + sessGap
				j := i + 1
				for j < len(rows) && rows[j].t < end {
					end = min(rows[j].t+sessGap, first+sessMax)
					j++
				}
				collect(key, first, end, rows[i:j])
				i = j
			}
		case "pthop":
			starts := map[int64]bool{}
			var list []int64
			for _, r := range rows {
				for s := floorDiv(r.t, hopSlide) * hopSlide; s+hopSize > r.t; s -= hopSlide {
					if !starts[s] {
						starts[s] = true
						list = append(list, s)
					}
				}
			}
			sort.Slice(list, func(a, b int) bool { return list[a] < list[b] })
			for _, s := range list {
				collect(key, s, s+hopSize, rows)
			}
		case "pttumble":
			starts := map[int64]bool{}
			var list []int64
			for _, r := range rows {
				s := floorDiv(r.t, tumble) * tumble
				if !starts[s] {
					starts[s] = true
					list = append(list, s)
				}
			}
			sort.Slice(list, func(a, b int) bool { return list[a] < list[b] })
			for _, s := range list {
				collect(key, s, s+tumble, rows)
			}
		}
	}
	for i := range out {
		out[i].emit = math.MaxInt64
		for _, x := range t.entries {
			if x.tick && x.micros >= out[i].end {
				out[i].emit = x.micros
				break
			}
		}
	}
	sort.SliceStable(out, func(a, b int) bool {
		x, y := out[a], out[b]
		if x.emit != y.emit {
			return x.emit < y.emit
		}
		if x.end != y.end {
			return x.end < y.end
		}
		return x.key < y.key
	})
	return out
}

// ------------------------------------------------------------------ fixture

type env struct {
	root           string
	a              api
	serverPort     int
	serverPortHold interface{ Close() error }
	server         *child
	sink           *capture
	producer       *nats
	proxy          *ackProxy
	broker         *child
	file           string
	checkpoint     string
	faults         string
	dataRoot       string
}

func setup(root, source, natsBin string) *env {
	must(os.MkdirAll(root, 0700))
	hold, e := netListen()
	must(e)
	v := &env{root: root, serverPort: holdPort(hold), serverPortHold: hold, sink: newCapture(),
		file: filepath.Join(root, "input.ndjson"), checkpoint: filepath.Join(root, "checkpoint"), faults: filepath.Join(root, "faults")}
	must(os.MkdirAll(v.faults, 0700))
	v.dataRoot = root
	v.a = newAPI(v.serverPort)
	if source == "jetstream" {
		np := port()
		config := fmt.Sprintf("host: 127.0.0.1\nport: %d\nmax_payload: 65536\njetstream {\nstore_dir: %q\nmax_file_store: 256MB\nmax_memory_store: 16MB\nsync_interval: always\n}\n", np, filepath.Join(root, "broker-data"))
		must(os.WriteFile(filepath.Join(root, "nats.conf"), []byte(config), 0600))
		v.broker = launch(natsBin, filepath.Join(root, "broker.log"), nil, "-c", filepath.Join(root, "nats.conf"))
		v.producer = dialNATS(np)
		v.producer.request("$JS.API.STREAM.CREATE.INPUT", encode(map[string]any{"name": "INPUT", "subjects": []string{"input.rows"}, "storage": "file", "retention": "limits", "max_bytes": 16 * 1024 * 1024, "max_msg_size": 65536, "max_consumers": 32, "num_replicas": 1, "deny_delete": true, "deny_purge": true}))
		v.producer.request("$JS.API.STREAM.CREATE.KV_OWNERS", encode(map[string]any{"name": "KV_OWNERS", "subjects": []string{"$KV.OWNERS.>"}, "storage": "file", "retention": "limits", "max_bytes": 1024 * 1024, "max_msg_size": 1024, "max_msgs_per_subject": 1, "num_replicas": 1, "discard": "new", "allow_direct": true, "allow_rollup_hdrs": true}))
		v.proxy = newProxy(np)
	} else {
		// Provision the append-only input before start (lesson: inputs before start).
		f, e := os.OpenFile(v.file, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
		must(e)
		must(f.Close())
	}
	return v
}
func (v *env) start(binary string) {
	if v.serverPortHold != nil {
		must(v.serverPortHold.Close())
		v.serverPortHold = nil
	}
	v.server = launch(binary, filepath.Join(v.root, "server.log"), []string{"SPARROW_TOKEN=" + token, "SPARROW_SECRETS_KEY=0123456789abcdef0123456789abcdef", "SPARROW_REQUIRE_SECRETS_KEY=1", "SPARROW_DATA_ROOTS=" + v.dataRoot, "SPARROW_FAULT_MARKER_DIR=" + v.faults},
		"--bind", fmt.Sprintf("127.0.0.1:%d", v.serverPort), "--catalog", filepath.Join(v.root, "catalog.db"), "--max-jobs", "1", "--safe-mode")
	eventually("server health", func() bool { _, s := v.a.call("GET", "/v1/health", nil); return s == 200 })
}
func (v *env) arm(point, content string) {
	must(os.WriteFile(filepath.Join(v.faults, point+".arm"), []byte(content), 0600))
}
func (v *env) disarm(point string) {
	_ = os.Remove(filepath.Join(v.faults, point+".arm"))
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
func (v *env) status() map[string]any { return v.a.ok("GET", "/v1/pipelines/check/status", nil) }
func (v *env) publish(i int) {
	if v.producer != nil {
		v.producer.request("input.rows", rowJSON(i))
		return
	}
	f, e := os.OpenFile(v.file, os.O_APPEND|os.O_WRONLY, 0600)
	must(e)
	_, e = f.Write(append(rowJSON(i), '\n'))
	must(e)
	must(f.Sync())
	must(f.Close())
}
func (v *env) spec(source, shape string) map[string]any {
	spec := map[string]any{"version": 1, "stream": "sensors", "sql": sqlFor(shape),
		"sink":     map[string]any{"kind": "http", "url": fmt.Sprintf("http://127.0.0.1:%d/output", v.sink.port()), "batch_rows": 1, "linger_ms": 0},
		"recovery": "aligned", "checkpoint_dir": v.checkpoint, "fail_on_decode": true,
		"checkpoint": map[string]any{"interval_ms": 100, "timeout_ms": 5000, "resume_latest": true}}
	if source == "jetstream" {
		spec["delivery"] = "checkpointed_at_least_once"
		spec["source"] = map[string]any{"kind": "jetstream", "jetstream": map[string]any{
			"servers": []string{fmt.Sprintf("nats://127.0.0.1:%d", v.proxy.port())}, "namespace": "pt_fixture", "stream": "INPUT", "consumer": "check", "ownership_bucket": "OWNERS",
			"max_pending": 16, "pending_bytes": 262144, "pull_messages": 4, "pull_bytes": 73728}}
	} else {
		spec["source"] = map[string]any{"kind": "file", "path": v.file, "file_contract": "append_only"}
	}
	return spec
}
func (v *env) register() {
	if v.proxy != nil {
		v.a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": v.proxy.port()})
	}
	v.a.ok("PUT", "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": v.sink.port()})
	v.a.ok("PUT", "/v1/streams/sensors", map[string]any{"fields": []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "v", "type": "int64", "nullable": false}}})
}
func (v *env) readerName() string {
	names := v.producer.request("$JS.API.CONSUMER.NAMES.INPUT", []byte("{}"))["consumers"].([]any)
	require(len(names) == 1, "exactly one owned attempt consumer expected")
	return names[0].(string)
}
func (v *env) captureFailure() {
	if cause := recover(); cause != nil {
		status, code := v.a.call("GET", "/v1/pipelines/check/status", nil)
		detail := map[string]any{"panic": fmt.Sprint(cause), "status_code": code, "status": status, "outputs": len(v.sink.rows())}
		if raw, err := json.Marshal(detail); err == nil {
			_ = os.WriteFile(filepath.Join(v.root, "failure.json"), append(raw, '\n'), 0600)
		}
		panic(cause)
	}
}
func (v *env) close() {
	if v.serverPortHold != nil {
		_ = v.serverPortHold.Close()
		v.serverPortHold = nil
	}
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
func (v *env) refused(label string) map[string]any {
	var st map[string]any
	eventually(label+" refused", func() bool {
		st, _ = v.a.call("GET", "/v1/pipelines/check/status", nil)
		return nested(st, "actual", "status") == "failed"
	})
	return st
}
func (v *env) clockLog() string { return filepath.Join(v.faults, "pt_clock.log") }
func (v *env) timeline() timeline { return parseTimeline(v.clockLog()) }

// decode -> (canonical text, output id); every v34/v35 row carries an id.
func decode(raw []byte, shape string) (string, string) {
	var m map[string]any
	must(json.Unmarshal(raw, &m))
	id, ok := m["id"].(string)
	require(ok && len(id) == 48 && strings.ToLower(id) == id, "output ID encoding invalid")
	_, e := hex.DecodeString(id)
	must(e)
	d, ok := m["data"].(map[string]any)
	require(ok, "missing output data envelope")
	i := func(k string) int64 {
		f, ok := d[k].(float64)
		require(ok && f == math.Trunc(f), "integer output field "+k)
		return int64(f)
	}
	key, _ := d["device_id"].(string)
	text := fmt.Sprintf("%s|%d|%d|%d|%d", key, i("window_start"), i("window_end"), i("c"), i("s"))
	if ext(shape) {
		text += fmt.Sprintf("|%d|%d", i("f"), i("l"))
	}
	return text, id
}

// ------------------------------------------------------------------ run

var stateCuts = map[string]bool{"input_after": true, "restore_kill": true, "low_budget": true, "neg_start": true, "no_new_input": true}

func run(root, source, shape, cut, serverBin, natsBin string) map[string]any {
	reliable := source == "jetstream"
	v := setup(root, source, natsBin)
	defer v.close()
	defer v.captureFailure()
	expectVersion := 34
	if reliable {
		expectVersion = 35
	}
	// Row 1 exists before start (lesson: inputs before start): its logical
	// time is the first tick (< 200ms), so PT hopping holds windows with
	// negative starts from the beginning. "empty" starts with no input.
	next := 1
	if cut != "empty" {
		v.publish(1)
		next = 2
	}
	v.start(serverBin)
	v.register()
	spec := v.spec(source, shape)
	save(filepath.Join(root, "spec.json"), spec)
	explain := v.a.ok("POST", "/v1/explain", spec)
	save(filepath.Join(root, "explain.json"), explain)
	v.a.ok("PUT", "/v1/pipelines/check", spec)
	detail := map[string]any{}
	switch cut {
	case "input_after", "restore_kill", "low_budget":
		v.arm("window_rows_applied", "8")
	case "neg_start":
		// Park the commit of the first due tick (~100ms): CURRENT then holds
		// the row-1 decision (t < 100000) with hop windows starting at
		// -200000 and -100000 still open in committed state.
		v.arm("checkpoint_after_manifest_rename", "after_due")
	case "no_new_input":
		v.arm("window_rows_applied", strconv.Itoa(rowsTotal))
	case "fire_tick":
		v.arm("pt_time_applied", "due 6")
	case "about_to_fire":
		v.arm("pt_time_applied", "near 60000 6")
	}
	v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	pausePoint := map[string]string{"input_after": "window_rows_applied", "restore_kill": "window_rows_applied", "low_budget": "window_rows_applied",
		"neg_start": "checkpoint_after_manifest_rename", "no_new_input": "window_rows_applied", "fire_tick": "pt_time_applied", "about_to_fire": "pt_time_applied"}[cut]
	reached := func() bool {
		if pausePoint == "" {
			return false
		}
		_, e := os.Stat(filepath.Join(v.faults, pausePoint+".reached"))
		return e == nil
	}
	feed := func(until func() bool) {
		for next <= rowsTotal && !until() {
			v.publish(next)
			next++
			deadline := time.Now().Add(time.Duration(pace[next%len(pace)]) * time.Millisecond)
			for time.Now().Before(deadline) && !until() {
				time.Sleep(2 * time.Millisecond)
			}
		}
	}
	applied := func(n int) func() bool { return func() bool { return v.timeline().rows() >= n } }
	switch cut {
	case "output_after":
		feed(func() bool { return v.sink.count() >= 2 })
		v.arm("checkpoint_after_manifest_rename", "after_due")
		pausePoint = "checkpoint_after_manifest_rename"
		feed(reached)
	case "empty":
		// Several committed tick-only decisions, then park the next commit.
		eventually("tick-only decisions", func() bool { t := v.timeline(); return len(t.entries) >= 3 && t.rows() == 0 })
		v.arm("checkpoint_after_manifest_rename", "")
		pausePoint = "checkpoint_after_manifest_rename"
	case "manifest_renamed":
		feed(applied(6))
		v.arm("checkpoint_after_manifest_rename", "")
		pausePoint = "checkpoint_after_manifest_rename"
		feed(reached)
	case "output_inflight":
		feed(func() bool { return v.sink.count() >= 2 })
		v.sink.hold.Store(true)
		feed(func() bool { return len(v.sink.heldRows()) >= 1 })
	case "ack_lost":
		require(reliable, "ack_lost is JetStream-only")
		feed(applied(6))
		v.proxy.drop.Store(true)
		feed(func() bool { return v.proxy.dropped.Load() > 0 })
		v.arm("checkpoint_after_manifest_rename", "")
		pausePoint = "checkpoint_after_manifest_rename"
		feed(reached)
	case "neg_start":
		// No further input until the cut.
	default:
		feed(reached)
	}
	if cut == "empty" {
		require(v.timeline().rows() == 0 && v.sink.count() == 0, "empty cut saw input")
	}
	if pausePoint != "" {
		detail["paused_pid"] = v.waitReached(pausePoint)
	} else {
		eventually("phase-1 output request in flight", func() bool { return len(v.sink.heldRows()) >= 1 })
	}
	require(snapshotVersion(v.checkpoint) == expectVersion, fmt.Sprintf("CURRENT outer version want %d", expectVersion))
	var oldReader string
	if reliable {
		oldReader = v.readerName()
	}
	before := v.timeline()
	detail["rows_applied_before_kill"] = before.rows()
	if cut == "neg_start" {
		require(before.rows() == 1 && before.entries[len(before.entries)-1].micros < hopSize-hopSlide,
			"neg_start: first row must sit before the first non-negative hop start")
	}
	if cut == "fire_tick" || cut == "about_to_fire" {
		w := oracle(shape, before)
		last := before.lastTick()
		due, near := 0, false
		for _, x := range w {
			if x.end <= last && x.emit == last {
				due++
			}
			if x.end > last && x.end-last <= 60000 {
				near = true
			}
		}
		// The independent oracle confirms the tick the server parked on.
		if cut == "fire_tick" {
			require(due >= 1, "fire_tick: no window due at the parked tick")
		} else {
			require(near && due == 0, "about_to_fire: no window within 60ms or one already due")
		}
		detail["parked_tick"] = last
	}
	preKill := v.sink.count()
	currentPath := filepath.Join(v.checkpoint, "CURRENT")
	currentBeforeKill := hash(currentPath)
	v.server.stop(syscall.SIGKILL)
	var held [][]byte
	if cut == "output_inflight" {
		held = v.sink.heldRows()
		v.sink.hold.Store(false)
		close(v.sink.release)
		detail["inflight_rows"] = len(held)
	}
	for _, p := range []string{"window_rows_applied", "pt_time_applied", "checkpoint_after_manifest_rename"} {
		v.disarm(p)
	}
	if reliable {
		v.proxy.drop.Store(false)
	}
	require(hash(currentPath) == currentBeforeKill, "SIGKILL changed CURRENT")
	// The broker state of the killed attempt is read before any restart (the
	// restored attempt replaces its consumer); checked once the restored
	// process reveals the committed cut.
	var brokerAfterKill map[string]any
	if reliable {
		brokerAfterKill = v.producer.request("$JS.API.CONSUMER.INFO.INPUT."+oldReader, []byte("{}"))
	}
	checkBroker := func(label string, committed int) {
		if !reliable {
			return
		}
		info := brokerAfterKill
		detail[label] = info
		floor := number(info, "ack_floor", "stream_seq")
		require(floor <= float64(committed), fmt.Sprintf("broker ACK floor %v passed the committed cut %d", floor, committed))
		if cut == "ack_lost" {
			require(floor < float64(committed), "ACK for the last commit was not actually lost")
		}
	}
	time.Sleep(downtime) // deliberate downtime: must not advance the logical clock
	if cut == "restore_kill" {
		v.arm("restore_after_credit", "")
		v.start(serverBin)
		go func() { _, _ = v.a.call("POST", "/v1/pipelines/check/start", map[string]any{}) }()
		detail["restore_paused_pid"] = v.waitReached("restore_after_credit")
		require(v.sink.count() == preKill, "output emitted during interrupted restore")
		require(hash(currentPath) == currentBeforeKill, "interrupted restore changed CURRENT")
		v.server.stop(syscall.SIGKILL)
		v.disarm("restore_after_credit")
		require(hash(currentPath) == currentBeforeKill, "SIGKILL during restore changed CURRENT")
	}
	if cut == "low_budget" {
		v.arm("restore_pressure", "1024")
		v.start(serverBin)
		_, code := v.a.call("POST", "/v1/pipelines/check/start", map[string]any{})
		detail["low_budget_start_code"] = code
		detail["low_budget_status"] = v.refused("low-budget restore")
		_, e := os.Stat(filepath.Join(v.faults, "restore_pressure.reached"))
		require(e == nil, "restore pressure hook did not engage")
		require(v.sink.count() == preKill, "output emitted by refused low-budget restore")
		require(hash(currentPath) == currentBeforeKill, "refused low-budget restore changed CURRENT")
		v.server.stop(syscall.SIGKILL)
		v.disarm("restore_pressure")
	}
	v.start(serverBin)
	v.a.ok("POST", "/v1/pipelines/check/start", map[string]any{})
	eventually("restored process logged its clock start", func() bool { return len(v.timeline().cuts) >= 2 })
	restored := v.timeline()
	cutMicros := restored.cuts[len(restored.cuts)-1]
	committedRows := restored.rowsAt(cutMicros)
	detail["restored_cut_micros"] = cutMicros
	detail["committed_rows"] = committedRows
	checkBroker("broker_after_kill", committedRows)
	if reliable {
		eventually("old attempt consumer replaced", func() bool { return v.readerName() != oldReader })
	}
	// Remaining input (none for no_new_input: ticks alone must close windows).
	if cut != "no_new_input" {
		feed(func() bool { return false })
	} else {
		require(next > rowsTotal, "no_new_input must have consumed all input before the kill")
	}
	var final timeline
	var want []window
	eventually("all windows closed and delivered", func() bool {
		final = v.timeline()
		if final.rows() != rowsTotal {
			return false
		}
		want = oracle(shape, final)
		for _, w := range want {
			if w.emit == math.MaxInt64 {
				return false
			}
		}
		ids := map[string]bool{}
		for _, r := range v.sink.rows() {
			_, id := decode(r, shape)
			ids[id] = true
		}
		return len(ids) >= len(want)
	})
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
	require(snapshotVersion(v.checkpoint) == expectVersion, "final CURRENT outer version")
	final = v.timeline()
	want = oracle(shape, final)
	wantText := make([]string, len(want))
	negative := false
	for i, w := range want {
		wantText[i] = w.text(shape)
		require(w.emit != math.MaxInt64, "window left open at stop")
		negative = negative || w.start < 0
	}
	if shape == "pthop" && cut != "empty" {
		require(negative, "PT hopping fixture produced no negative window start")
	}
	// No wall-clock catch-up: the first ticks after each restore sit within
	// the restored process's own elapsed time of the cut, not the downtime.
	for i, first := range final.firstNew {
		if first != math.MinInt64 {
			gap := first - final.cuts[i+1]
			require(gap < int64(downtime/time.Microsecond)-200000, fmt.Sprintf("logical clock caught up across downtime: %dus", gap))
		}
	}
	raw := v.sink.rows()
	got := make([]string, len(raw))
	ids := make([]string, len(raw))
	unique := map[string]string{}
	var order []string
	for i, r := range raw {
		got[i], ids[i] = decode(r, shape)
		if old, ok := unique[ids[i]]; ok {
			require(old == got[i], "same output ID has different content")
			continue
		}
		unique[ids[i]] = got[i]
		order = append(order, ids[i])
	}
	require(len(unique) == len(want), fmt.Sprintf("unique outputs %d differ from oracle %d (loss or extra)", len(unique), len(want)))
	for i, id := range order {
		require(unique[id] == wantText[i], fmt.Sprintf("oracle mismatch at %d: got %s want %s", i, unique[id], wantText[i]))
	}
	// Committed outputs = windows closed by ticks <= the restored cut.
	committedOutputs := 0
	for _, w := range want {
		if w.emit <= cutMicros {
			committedOutputs++
		}
	}
	duplicates := len(raw) - len(unique)
	require(preKill >= committedOutputs, "delivered fewer outputs than the committed cut implies")
	require(duplicates == preKill-committedOutputs, fmt.Sprintf("replayed duplicates %d != uncommitted delivered %d", duplicates, preKill-committedOutputs))
	for k := 0; k < duplicates; k++ {
		require(ids[preKill+k] == ids[committedOutputs+k], "replayed output changed ID or order")
	}
	if cut == "neg_start" {
		require(committedRows == 1 && cutMicros < hopSlide, "neg_start: committed state is not the row-1 decision")
		open := 0
		for _, w := range want {
			if w.start < 0 && w.emit > cutMicros {
				open++
			}
		}
		require(open >= 2, "neg_start: committed state lacks open negative-start windows")
		detail["open_negative_windows_at_cut"] = open
	}
	if cut == "fire_tick" || cut == "about_to_fire" {
		require(preKill == committedOutputs, cut+": the parked tick emitted before the kill")
	}
	if cut == "output_after" {
		require(duplicates >= 1, "output_after: no output delivered after the last committed cut")
	}
	if cut == "output_inflight" {
		require(len(held) >= 1, "no in-flight output captured")
		for k, r := range held {
			text, id := decode(r, shape)
			require(committedOutputs+k < len(wantText) && text == wantText[committedOutputs+k], "in-flight output differs from oracle")
			require(ids[preKill+k] == id, "in-flight output was not replayed in order with its ID")
		}
	}
	if cut == "no_new_input" {
		after := 0
		for _, w := range want {
			if w.emit > cutMicros {
				after++
			}
		}
		require(after >= 1, "no_new_input: no window left for ticks to close after restore")
		detail["closed_by_ticks_after_restore"] = after
	}
	if reliable {
		eventually("source ACK drain", func() bool {
			info := v.producer.request("$JS.API.CONSUMER.INFO.INPUT."+v.readerName(), []byte("{}"))
			return number(info, "num_ack_pending") == 0 && number(info, "ack_floor", "stream_seq") == rowsTotal
		})
	}
	save(filepath.Join(root, "outputs.json"), got)
	save(filepath.Join(root, "timeline.json"), map[string]any{"cuts": final.cuts, "first_new": final.firstNew, "rows": final.rows(), "last_tick": final.lastTick()})
	v.server.stop(syscall.SIGTERM)
	log, _ := os.ReadFile(filepath.Join(root, "server.log"))
	require(!strings.Contains(string(log), "accounting_error"), "memory accounting error logged")
	summary := map[string]any{"valid": true, "source": source, "shape": shape, "cut": cut, "outputs": len(raw), "oracle_outputs": len(want),
		"pre_kill_outputs": preKill, "committed_outputs": committedOutputs, "replayed_duplicates": duplicates, "snapshot_version": expectVersion,
		"detail": detail, "scope": "isolated_process_sigkill_not_power_loss"}
	save(filepath.Join(root, "summary.json"), summary)
	return summary
}

func main() {
	server := flag.String("server-bin", "", "Sparrow server (--features jetstream,process-fault-pause)")
	natsBin := flag.String("nats-server", "", "pinned NATS server (JetStream runs)")
	out := flag.String("out", "", "new evidence directory")
	source := flag.String("source", "file", "file or jetstream")
	shape := flag.String("shape", "pthop", strings.Join(shapes, "|"))
	cut := flag.String("cut", "", "input_after|output_after|output_inflight|manifest_renamed|restore_kill|fire_tick|about_to_fire|no_new_input|empty|low_budget|neg_start|ack_lost|compat")
	oldBin := flag.String("old-server-bin", "", "424cf95")
	mainBin := flag.String("main-server-bin", "", "#29-merged main")
	pr30 := flag.String("pr30-server-bin", "", "#30")
	pr31 := flag.String("pr31-server-bin", "", "#31")
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
		compat(filepath.Join(root, "compat"), *server, map[string]string{"old-424cf95": *oldBin, "main-29": *mainBin, "pr30": *pr30, "pr31": *pr31})
		fmt.Println("PT_WINDOW_COMPAT_OK")
		return
	}
	require(*source == "file" || *source == "jetstream", "source")
	ok := false
	for _, s := range shapes {
		ok = ok || s == *shape
	}
	require(ok, "shape")
	require(*cut != "neg_start" || *shape == "pthop", "neg_start is a PT hopping cut")
	s := run(filepath.Join(root, "run"), *source, *shape, *cut, *server, *natsBin)
	fmt.Println("PT_WINDOW_PROCESS_OK", *source, *shape, *cut, s["outputs"])
}

func netListen() (interface{ Close() error }, error) { return net.Listen("tcp", "127.0.0.1:0") }
func holdPort(l interface{ Close() error }) int  { return l.(net.Listener).Addr().(*net.TCPAddr).Port }
func newAPI(p int) api {
	return api{fmt.Sprintf("http://127.0.0.1:%d", p), &http.Client{Timeout: 5 * time.Second}}
}

// ------------------------------------------------------------------ compat

// stage runs one binary on a fresh catalog against a shared checkpoint dir
// and input. Nothing a refused binary does may move CURRENT or emit output.
type stage struct {
	v    *env
	spec map[string]any
}

func newStage(root, shared, source, shape string) *stage {
	v := setup(root, source, "")
	v.dataRoot = filepath.Dir(shared)
	v.checkpoint = filepath.Join(shared, "checkpoint")
	v.file = filepath.Join(shared, "input.ndjson")
	if _, e := os.Stat(v.file); os.IsNotExist(e) {
		must(os.WriteFile(v.file, nil, 0600))
	}
	return &stage{v: v}
}

// tryStart returns "" when the job runs, else the refusal text.
func (s *stage) tryStart(binary, sql string, sqlShape string) string {
	v := s.v
	v.start(binary)
	v.register()
	spec := v.spec("file", sqlShape)
	if sql != "" {
		spec["sql"] = sql
	}
	s.spec = spec
	if r, code := v.a.call("PUT", "/v1/pipelines/check", spec); code >= 300 {
		return fmt.Sprintf("PUT %d %v", code, r)
	}
	if r, code := v.a.call("POST", "/v1/pipelines/check/start", map[string]any{}); code >= 300 {
		return fmt.Sprintf("start %d %v", code, r)
	}
	var st map[string]any
	var status any
	eventually("started or refused", func() bool {
		st, _ = v.a.call("GET", "/v1/pipelines/check/status", nil)
		status = nested(st, "actual", "status")
		return status == "running" || status == "failed"
	})
	if status == "failed" {
		return fmt.Sprint(nested(st, "actual", "last_error"))
	}
	// running: confirm a committed decision so a refusal-at-restore shows.
	time.Sleep(400 * time.Millisecond)
	st, _ = v.a.call("GET", "/v1/pipelines/check/status", nil)
	if nested(st, "actual", "status") == "failed" {
		return fmt.Sprint(nested(st, "actual", "last_error"))
	}
	return ""
}
func (s *stage) end() { s.v.close() }

const legacyTumble = "SELECT device_id, window_start, window_end, COUNT(*) AS c, SUM(v) AS s FROM sensors GROUP BY device_id, TUMBLE(PROCESSING_TIME, 200000)"

func compat(root, server string, others map[string]string) map[string]any {
	for name, b := range others {
		require(b != "", "compat requires --"+name+" binary")
	}
	must(os.MkdirAll(root, 0700))
	out := map[string]any{}
	appendRows := func(shared string, from, to int) {
		f, e := os.OpenFile(filepath.Join(shared, "input.ndjson"), os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
		must(e)
		for i := from; i <= to; i++ {
			_, e = f.Write(append(rowJSON(i), '\n'))
			must(e)
		}
		must(f.Sync())
		must(f.Close())
	}
	n := 0
	next := func(shared, source, shape string) *stage {
		n++
		return newStage(filepath.Join(root, fmt.Sprintf("stage-%02d", n)), shared, source, shape)
	}
	// 1. v34 directories (PT hopping codec 1 and PT session codec 4) are refused
	//    by every older binary, unchanged, then resumed by the new binary.
	for _, shape := range []string{"pthop", "ptsess"} {
		shared := filepath.Join(root, "v34-"+shape)
		must(os.MkdirAll(shared, 0700))
		appendRows(shared, 1, 6)
		s := next(shared, "file", shape)
		r0 := s.tryStart(server, "", shape)
		require(r0 == "", "new binary must run "+shape+": "+r0)
		eventually("v34 state committed", func() bool { _, e := os.Stat(filepath.Join(s.v.checkpoint, "CURRENT")); return e == nil && snapshotVersion(s.v.checkpoint) == 34 })
		time.Sleep(300 * time.Millisecond)
		s.v.server.stop(syscall.SIGKILL)
		s.end()
		cur := hash(filepath.Join(shared, "checkpoint", "CURRENT"))
		for _, name := range []string{"old-424cf95", "main-29", "pr30", "pr31"} {
			o := next(shared, "file", shape)
			reason := o.tryStart(others[name], "", shape)
			require(reason != "", name+" binary accepted a v34 directory/plan")
			require(o.v.sink.count() == 0, name+" emitted output on refusal")
			o.v.server.stop(syscall.SIGKILL)
			o.end()
			require(hash(filepath.Join(shared, "checkpoint", "CURRENT")) == cur, name+" changed CURRENT")
			out[shape+"_refused_by_"+name] = reason
			// The same binary with a plan it does admit (v16 PT tumbling,
			// legacy aggregates) must open the v34 directory and refuse it.
			o = next(shared, "file", shape)
			reason = o.tryStart(others[name], legacyTumble, "pttumble")
			require(reason != "" && !strings.HasPrefix(reason, "PUT"), name+" did not refuse the v34 directory at restore: "+reason)
			require(o.v.sink.count() == 0, name+" emitted output on v34 directory refusal")
			o.v.server.stop(syscall.SIGKILL)
			o.end()
			require(hash(filepath.Join(shared, "checkpoint", "CURRENT")) == cur, name+" changed CURRENT (v16 plan)")
			out[shape+"_dir_refused_by_"+name+"_v16_plan"] = reason
		}
		// Changed parameters on the same v34 directory: strict refusal.
		changed := map[string]string{"pthop": "SELECT device_id, window_start, window_end, COUNT(*) AS c, SUM(v) AS s FROM sensors GROUP BY device_id, HOP(PROCESSING_TIME, 50000, 300000)",
			"ptsess": "SELECT device_id, window_start, window_end, COUNT(*) AS c, SUM(v) AS s, FIRST(v) AS f, LAST(v) AS l FROM sensors GROUP BY device_id, SESSION(PROCESSING_TIME, 250000, 600000)"}[shape]
		o := next(shared, "file", shape)
		reason := o.tryStart(server, changed, shape)
		require(reason != "", "new binary accepted changed PT parameters on a v34 directory")
		require(o.v.sink.count() == 0, "param-mismatch emitted output")
		o.v.server.stop(syscall.SIGKILL)
		o.end()
		require(hash(filepath.Join(shared, "checkpoint", "CURRENT")) == cur, "param mismatch changed CURRENT")
		out[shape+"_param_change_refused"] = reason
		// New binary resumes (with new input): windows close, CURRENT stays v34.
		appendRows(shared, 7, 10)
		r := next(shared, "file", shape)
		require(r.tryStart(server, "", shape) == "", "new binary must resume its v34 directory")
		eventually("resumed v34 emits", func() bool { return r.v.sink.count() > 0 })
		require(snapshotVersion(r.v.checkpoint) == 34, "resumed directory changed version")
		r.v.server.stop(syscall.SIGKILL)
		r.end()
	}
	// 2. Regression: a v16 PT tumbling (legacy aggs) directory written by the
	//    #31 binary is resumed by the new binary as v16 (unchanged profile).
	{
		shared := filepath.Join(root, "v16-pr31")
		must(os.MkdirAll(shared, 0700))
		appendRows(shared, 1, 4)
		legacy := legacyTumble
		s := next(shared, "file", "pttumble")
		require(s.tryStart(others["pr31"], legacy, "pttumble") == "", "#31 must run v16")
		eventually("v16 committed", func() bool { _, e := os.Stat(filepath.Join(s.v.checkpoint, "CURRENT")); return e == nil && snapshotVersion(s.v.checkpoint) == 16 })
		s.v.server.stop(syscall.SIGKILL)
		s.end()
		appendRows(shared, 5, 8)
		r := next(shared, "file", "pttumble")
		require(r.tryStart(server, legacy, "pttumble") == "", "new binary must resume the #31 v16 directory")
		eventually("resumed v16 emits", func() bool { return r.v.sink.count() > 0 })
		require(snapshotVersion(r.v.checkpoint) == 16, "v16 directory was re-versioned")
		r.v.server.stop(syscall.SIGKILL)
		r.end()
		// The same v16 directory with new aggregates (v34 plan) is refused.
		cur := hash(filepath.Join(shared, "checkpoint", "CURRENT"))
		o := next(shared, "file", "pttumble")
		reason := o.tryStart(server, "", "pttumble")
		require(reason != "", "v34 plan accepted a v16 directory")
		o.v.server.stop(syscall.SIGKILL)
		o.end()
		require(hash(filepath.Join(shared, "checkpoint", "CURRENT")) == cur, "v16->v34 refusal changed CURRENT")
		out["v16_dir_v34_plan_refused"] = reason
	}
	save(filepath.Join(root, "compat.json"), out)
	return out
}
