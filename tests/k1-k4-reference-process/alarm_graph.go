package main

// Black-box v22 alarm-graph oracle.
//
//   branch-union: File(1) -> Branch(2) -> Alarm(3,4) -> Union(5) -> required HTTP(6)
//   partial-sinks: File(1) -> Alarm(2) -> Branch(3) -> required HTTP(10,11)
//
// The driver only uses the production binary's HTTP API plus the durable
// GTC1/GTD1 artifacts it writes itself.  There is no Rust evaluator, no
// test-only hook and no in-process kill: every crash is a real SIGKILL of the
// child process, checked through its wait status.
import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"
)

// The two lifecycle events every alarm operator namespace must produce.
const (
	alarmGraphActivate = "activate"
	alarmGraphResolve  = "resolve"
)

// alarmGraphIot mirrors the control-plane alarm fixture: paused clock, 1 s
// activate, 200 ms resolve, 1 s cooldown, 2 s notification lifetime.
func alarmGraphIot() map[string]any {
	return map[string]any{
		"keys": []string{"device_id"}, "fields": []string{"enter", "clear"},
		"emit_first": false, "ttl_micros": 0, "max_keys": 8, "invalid": "ignore",
		"timing": map[string]any{
			"kind": "alarm", "clock": "paused",
			"activate_micros": 1000000, "resolve_micros": 200000,
			"cooldown_micros": 1000000, "notification_max_age_micros": 2000000,
		},
	}
}

// alarmGraphSink publishes one row per POST so a withheld ACK freezes exactly
// one output leg.  max_inflight 2 keeps both Union arms in flight, so the two
// alarm namespaces reach the capture while one leg withholds its ACK.  The
// request order of those two legs is not part of the oracle.
func alarmGraphSink(url string) map[string]any {
	return map[string]any{"kind": "http", "url": url, "batch_rows": 1, "linger_ms": 0,
		"max_inflight": 2, "outbox_capacity": 1}
}

// alarmGraphSignalSource is the append-only File source both topologies share.
// The capacities match the accepted graph fixtures.
func alarmGraphSignalSource(root string) map[string]any {
	return map[string]any{"kind": "file", "path": filepath.Join(root, "a.ndjson"),
		"file_contract": "append_only", "inbox_capacity": 1}
}

// alarmGraphSpec builds the required graph, its graph_io ports and the
// paused-time checkpoint policy.  The default server budget is deliberately
// left untouched: both topologies stay at their documented edge count.
func alarmGraphSpec(root string, primary *capture, secondary *capture, kind string) map[string]any {
	source := alarmGraphSignalSource(root)
	nodes := []any{}
	io := map[string]any{"sources": map[string]any{"1": source}}
	sink := alarmGraphSink(primary.url())
	pipeline := 14002
	switch kind {
	case "branch-union":
		nodes = []any{
			map[string]any{"id": 1, "kind": "memory_source", "table": "signals", "out": []int{2}},
			map[string]any{"id": 2, "kind": "branch", "out": []int{3, 4}},
			map[string]any{"id": 3, "kind": "alarm", "iot": alarmGraphIot(), "out": []int{5}},
			map[string]any{"id": 4, "kind": "alarm", "iot": alarmGraphIot(), "out": []int{5}},
			map[string]any{"id": 5, "kind": "union_all", "out": []int{6}},
			map[string]any{"id": 6, "kind": "capture_sink", "name": "alarm_union"},
		}
		io["sinks"] = map[string]any{"6": alarmGraphSink(primary.url())}
		pipeline = 14001
	default:
		nodes = []any{
			map[string]any{"id": 1, "kind": "memory_source", "table": "signals", "out": []int{2}},
			map[string]any{"id": 2, "kind": "alarm", "iot": alarmGraphIot(), "out": []int{3}},
			map[string]any{"id": 3, "kind": "branch", "out": []int{10, 11}},
			map[string]any{"id": 10, "kind": "capture_sink", "name": "alarm_a"},
			map[string]any{"id": 11, "kind": "capture_sink", "name": "alarm_b"},
		}
		io["sinks"] = map[string]any{
			"10": alarmGraphSink(primary.url()),
			"11": alarmGraphSink(secondary.url()),
		}
	}
	return map[string]any{
		"version": 1, "stream": "signals", "source": source, "sink": sink,
		"delivery": "live_best_effort", "recovery": "aligned", "fail_on_decode": true,
		"checkpoint_dir": filepath.Join(root, "checkpoint"),
		"checkpoint":     checkpointPolicy(float64(100), true),
		"graph_io":       io,
		"graph":          map[string]any{"version": 1, "pipeline_id": pipeline, "revision_id": 1, "nodes": nodes},
	}
}

func appendAlarmSignal(path string, enter, clear bool) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	_, err = file.Write(append(data(map[string]any{"device_id": "a", "enter": enter, "clear": clear}), '\n'))
	must(err)
	must(file.Sync())
	must(file.Close())
}

func alarmOutputID(row map[string]any) string {
	id, _ := row["id"].(string)
	return id
}

// alarmCommittedGeneration reads the durable state generation marker the Sink
// output namespaces are derived from.
func alarmCommittedGeneration(checkpoint string) string {
	raw, err := os.ReadFile(filepath.Join(checkpoint, "STATE_GENERATION"))
	must(err)
	require(len(raw) == 20 && string(raw[:4]) == "SG01", "alarm graph state generation marker is invalid")
	require(!bytes.Equal(raw[4:], make([]byte, 16)), "alarm graph state generation is unset")
	return hex.EncodeToString(raw[4:])
}

// alarmSinkEpoch mirrors the runtime's per-Sink output namespace: the state
// generation with the Sink id XORed into the first four bytes and byte 15
// XORed with 0xd8.  Two required Sinks therefore never share one ID namespace,
// so a payload published for the other leg cannot satisfy this oracle.
func alarmSinkEpoch(generation string, sink uint32) string {
	raw, err := hex.DecodeString(generation)
	require(err == nil && len(raw) == 16, "alarm generation is not 16-byte hex")
	var id [4]byte
	binary.LittleEndian.PutUint32(id[:], sink)
	for i := 0; i < 4; i++ {
		raw[i] ^= id[i]
	}
	raw[15] ^= 0xd8
	return hex.EncodeToString(raw)
}

// alarmRow is one validated alarm output row.
type alarmRow struct {
	row        map[string]any
	data       map[string]any
	id         string
	generation string
	episode    uint64
	operator   uint64
	ordinal    uint64
}

// parseAlarmRow checks a row against the event, the key conditions and the Sink
// that must have published it: the output ID epoch has to be that Sink's
// namespace over the durable state generation, the ordinal has to be set, and
// the payload has to carry the same generation and the transition the event
// implies.
func parseAlarmRow(row map[string]any, event string, sink uint32, generation string) alarmRow {
	payload := rowData(row)
	id := alarmOutputID(row)
	require(len(id) == 48 && id == strings.ToLower(id), "alarm output ID is not lowercase 24-byte hex")
	raw, err := hex.DecodeString(id)
	require(err == nil && len(raw) == 24, "alarm output ID is not 24-byte hex")
	require(hex.EncodeToString(raw[:16]) == alarmSinkEpoch(generation, sink),
		"alarm output ID epoch is not this Sink's namespace for the durable state generation")
	ordinal := binary.BigEndian.Uint64(raw[16:])
	require(ordinal > 0, "alarm output ordinal is unset")
	require(generationText(payload["sparrow_alarm_generation"]) == generation,
		"alarm row generation differs from the durable state generation")
	require(fmt.Sprint(payload["device_id"]) == "a", "alarm output device key changed")
	require(fmt.Sprint(payload["sparrow_alarm_event"]) == event,
		fmt.Sprintf("alarm event=%v want=%s", payload["sparrow_alarm_event"], event))
	require(payload["sparrow_alarm_notify"] == true, "alarm row was emitted without a notification")
	require(number(payload["sparrow_alarm_time"]) > 0, "alarm event time is unset")
	enter, enterOK := payload["enter"].(bool)
	clear, clearOK := payload["clear"].(bool)
	require(enterOK && clearOK, "alarm row lacks boolean enter/clear conditions")
	phase := fmt.Sprint(payload["sparrow_alarm_phase"])
	switch event {
	case alarmGraphActivate:
		require(phase == "active" && enter && !clear,
			fmt.Sprintf("activate row is not an active enter/!clear transition: phase=%s enter=%v clear=%v", phase, enter, clear))
	case alarmGraphResolve:
		require(phase == "normal" && !enter && clear,
			fmt.Sprintf("resolve row is not a normal !enter/clear transition: phase=%s enter=%v clear=%v", phase, enter, clear))
	default:
		panic("unknown alarm event oracle")
	}
	return alarmRow{row: row, data: payload, id: id, generation: generation,
		episode: number(payload["sparrow_alarm_episode"]), operator: number(payload["sparrow_alarm_operator"]),
		ordinal: ordinal}
}

// requireAlarmOperators pins the alarm operator namespace set of one round.
func requireAlarmOperators(label string, group map[uint64]alarmRow, want []uint64) {
	require(len(group) == len(want), fmt.Sprintf("%s: alarm operator namespaces=%d want=%d", label, len(group), len(want)))
	for _, operator := range want {
		_, present := group[operator]
		require(present, fmt.Sprintf("%s: alarm operator namespace %d is missing", label, operator))
	}
}

// requireAlarmOrdinals proves one round consumed a contiguous block of one
// Sink's output ordinals: no missing row, no repeated row, no foreign row.
func requireAlarmOrdinals(label string, group map[uint64]alarmRow, first uint64) {
	ordinals := make([]uint64, 0, len(group))
	for _, row := range group {
		ordinals = append(ordinals, row.ordinal)
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })
	for i, ordinal := range ordinals {
		require(ordinal == first+uint64(i),
			fmt.Sprintf("%s: Sink ordinal %d is not the expected %d", label, ordinal, first+uint64(i)))
	}
}

// alarmGroup indexes one Sink round by operator namespace.  A round must carry
// exactly the expected namespaces, one contiguous ordinal block, one row per
// namespace, every row in episode 1 and one durable state generation.
func alarmGroup(label string, round []map[string]any, event string, want int, sink uint32, generation string, first uint64, operators []uint64) map[uint64]alarmRow {
	require(len(round) == want, fmt.Sprintf("alarm round rows=%d want=%d", len(round), want))
	group := map[uint64]alarmRow{}
	for _, row := range round {
		parsed := parseAlarmRow(row, event, sink, generation)
		require(parsed.episode == 1, fmt.Sprintf("alarm episode=%d want=1", parsed.episode))
		_, duplicate := group[parsed.operator]
		require(!duplicate, "alarm round repeated an operator namespace")
		group[parsed.operator] = parsed
	}
	require(len(group) == want, "alarm round lost an operator namespace")
	requireAlarmOperators(label, group, operators)
	requireAlarmOrdinals(label, group, first)
	return group
}

// requireAlarmReplay is the replay oracle for one leg, keyed by operator
// namespace: the request order of one round is not part of the contract, but
// every replayed row must repeat its operator's output ID and full payload.
func requireAlarmReplay(label string, first, replay map[uint64]alarmRow) {
	require(len(first) == len(replay), label+": alarm operator namespace set changed across restart")
	for operator, row := range first {
		other, present := replay[operator]
		require(present, label+": alarm operator namespace disappeared across restart")
		require(row.id == other.id, label+": replayed alarm output ID changed")
		require(row.generation == other.generation, label+": replayed alarm state generation changed")
		require(row.episode == other.episode, label+": replayed alarm episode changed")
		require(number(row.data["sparrow_alarm_time"]) == number(other.data["sparrow_alarm_time"]),
			label+": replayed alarm time changed")
		require(bytes.Equal(data(row.data), data(other.data)), label+": replayed alarm payload changed")
		require(bytes.Equal(data(row.row), data(other.row)), label+": replayed alarm row changed")
	}
}

// requireAlarmLifecycle ties the resolve round to the activate round of the
// same key and operator namespace: one episode, one state generation.
func requireAlarmLifecycle(activate, resolve map[uint64]alarmRow) {
	require(len(activate) == len(resolve), "activate/resolve operator namespace sets differ")
	for operator, row := range activate {
		other, present := resolve[operator]
		require(present, "resolve lost an alarm operator namespace")
		require(other.episode == row.episode, "resolve changed the episode of one operator")
		require(other.generation == row.generation, "resolve changed the state generation of one operator")
	}
}

// requireRowsReplay compares one single-row leg with the leg it replays: same
// output ID and same payload, event time included.
func requireRowsReplay(label string, first, replay []map[string]any) {
	require(len(first) == len(replay), label+": replayed leg row count changed")
	require(len(first) > 0, label+": replayed leg is empty")
	for i := range first {
		require(bytes.Equal(data(first[i]), data(replay[i])), label+": replayed leg changed its output ID or payload")
	}
}

// runAlarmGraphProcess drives one alarm topology and returns its checkpoint
// directory so the caller can reuse the frozen v22 history for the old-binary
// profile guard.
func runAlarmGraphProcess(root, serverBin, kind string) string {
	branch := kind == "branch-union"
	must(os.Mkdir(root, 0700))
	file := filepath.Join(root, "a.ndjson")
	must(os.WriteFile(file, nil, 0600))
	checkpoint := filepath.Join(root, "checkpoint")
	primary := newCapture()
	defer primary.close()
	var secondary *capture
	if !branch {
		secondary = newCapture()
		defer secondary.close()
	}
	arms, primarySink, secondarySink := 1, uint32(10), uint32(11)
	operators, peers := []uint64{2}, []*capture{primary}
	if branch {
		arms, primarySink, operators = 2, 6, []uint64{3, 4}
	} else {
		peers = append(peers, secondary)
	}
	sinkIDs := []uint32{primarySink}
	if !branch {
		sinkIDs = append(sinkIDs, secondarySink)
	}
	port := freePort()
	client := newAPI(port)
	var process *child
	stop := func(signal os.Signal) {
		if process == nil {
			return
		}
		command := process.cmd
		process.stop(signal)
		if signal == syscall.SIGKILL {
			require(command.ProcessState != nil, "alarm graph process lacks a wait status")
			status, ok := command.ProcessState.Sys().(syscall.WaitStatus)
			require(ok && status.Signaled() && status.Signal() == syscall.SIGKILL,
				"alarm graph process did not actually die from SIGKILL")
		}
		process = nil
	}
	defer func() { stop(syscall.SIGKILL) }()
	start := func() {
		process = spawnServer(serverBin, root, filepath.Join(root, "server.log"), port)
		waitHealth(client, "alarm graph server")
	}
	resume := func() {
		client.ok(http.MethodPost, "/v1/pipelines/alarms/start", map[string]any{})
		waitRunning(client, "alarms")
	}
	start()
	allowCaptures(client, peers...)
	client.ok(http.MethodPut, "/v1/streams/signals", map[string]any{"fields": []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "enter", "type": "bool", "nullable": true},
		map[string]any{"name": "clear", "type": "bool", "nullable": true}}})
	spec := alarmGraphSpec(root, primary, secondary, kind)
	save(filepath.Join(root, "spec.json"), spec)
	startPipeline(client, "alarms", spec)

	generation := ""
	// group verifies one Sink round completely: operator namespace set, the
	// contiguous ordinal block that round must have consumed, and every row's
	// event, phase, conditions, ID namespace, generation and episode.
	group := func(label string, rows []map[string]any, event string, sink uint32, first uint64) map[uint64]alarmRow {
		return alarmGroup(label, rows, event, arms, sink, generation, first, operators)
	}
	// One round delivers `arms` rows to every required Sink: the two Union arms
	// in branch-union, the two duplicated Branch legs in partial-sinks.
	consumed := 0
	waitRound := func(label string) {
		wait(label, func() bool {
			for _, peer := range peers {
				if peer.receivedRows() < consumed+arms {
					return false
				}
			}
			return true
		})
		consumed += arms
	}
	round := func(peer *capture) []map[string]any {
		return peer.snapshot().Received[consumed-arms : consumed]
	}
	// withhold freezes the Sink leg whose ACK must stay uncommitted.  In
	// branch-union the Union is the only required Sink; in partial-sinks the
	// first leg keeps accepting while the second leg withholds its ACK.
	withhold := func() {
		if branch {
			primary.setHold()
		} else {
			secondary.setHold()
		}
	}
	// heldCut reads the durable cut and the GTD1 decision while a required Sink
	// withholds its ACK, then proves the withheld ACK published nothing.
	heldCut := func(label string, ingestAdvance uint64) (graphCut, map[string]any) {
		before := currentGraphCut(checkpoint, uint16(22))
		decision, target := graphDecision(checkpoint)
		require(target.Sequence == before.Sequence+1, label+": pending decision is not the immediate successor")
		require(target.Ingested == before.Ingested+ingestAdvance, label+": pending decision ingested cut differs")
		for _, id := range sinkIDs {
			require(target.Outputs[id] >= before.Outputs[id] && target.Outputs[id] <= before.Outputs[id]+uint64(arms),
				label+": pending decision Sink ordinal left the round")
		}
		currentHash := hash(filepath.Join(checkpoint, "CURRENT"))
		pendingHash := hash(filepath.Join(checkpoint, "TIME_PENDING"))
		time.Sleep(100 * time.Millisecond)
		require(hash(filepath.Join(checkpoint, "CURRENT")) == currentHash,
			label+": withheld Sink ACK committed the alarm graph cut")
		save(filepath.Join(root, label+"-cut.json"), before)
		save(filepath.Join(root, label+"-decision.json"), decision)
		return before, map[string]any{"current": currentHash, "pending": pendingHash}
	}
	// crashAndRelease kills the real child, proves the durable cut survived the
	// kill, and drops the withheld bodies of the dead process.
	crashAndRelease := func(hashes map[string]any, label string) {
		stop(syscall.SIGKILL)
		require(hash(filepath.Join(checkpoint, "CURRENT")) == hashes["current"] &&
			hash(filepath.Join(checkpoint, "TIME_PENDING")) == hashes["pending"],
			label+": SIGKILL changed the durable alarm graph cut")
		if branch {
			primary.releaseHold(true)
		} else {
			secondary.releaseHold(true)
		}
	}
	committedAt := func(before graphCut, ingested uint64) graphCut {
		return waitGraphCut(checkpoint, func(c graphCut) bool {
			if c.Ingested != ingested {
				return false
			}
			for _, id := range sinkIDs {
				if c.Outputs[id] != before.Outputs[id]+uint64(arms) {
					return false
				}
			}
			return true
		}, uint16(22))
	}
	// noRepeat proves the recovered process really advanced a new durable cut
	// and still re-delivered no output and no new ordinal.
	noRepeat := func(label string, committed graphCut) {
		stop(syscall.SIGKILL)
		start()
		resume()
		restarted := waitGraphCut(checkpoint, func(c graphCut) bool { return c.Sequence > committed.Sequence }, uint16(22))
		time.Sleep(250 * time.Millisecond)
		for _, peer := range peers {
			require(peer.receivedRows() == consumed, label+": committed round was repeated after restart")
		}
		for _, id := range sinkIDs {
			require(restarted.Outputs[id] == committed.Outputs[id], label+": restart advanced a Sink ordinal")
		}
		require(alarmCommittedGeneration(checkpoint) == generation, label+": restart changed the state generation")
	}
	// acceptedLeg compares the accepted leg of an event with the accepted leg
	// of the same event one restart earlier.
	acceptedLeg := func(label string, first, second int) {
		rows := primary.snapshot().Rows
		require(len(rows) >= second+arms, label+": accepted alarm legs are missing")
		requireRowsReplay(label+"/accepted", rows[first:first+arms], rows[second:second+arms])
	}

	// A bootstrap cut proves the fixture starts with no ingested row and no
	// assigned Sink ordinal.  The durable generation is read once and must hold
	// for every output namespace and for every restart below.
	base := waitGraphCut(checkpoint, func(c graphCut) bool { return c.Ingested == 0 }, uint16(22))
	for _, id := range sinkIDs {
		require(base.Outputs[id] == 1, "alarm graph bootstrap Sink ordinal is not the first output")
	}
	require(primary.receivedRows() == 0, "alarm graph emitted output before the fixture input")
	generation = alarmCommittedGeneration(checkpoint)
	save(filepath.Join(root, "bootstrap-cut.json"), base)

	// An enter row must be durably committed as a pending latch before the
	// outage, and the outage must not spend any of its paused-time budget.
	appendAlarmSignal(file, true, false)
	pending := waitGraphCut(checkpoint, func(c graphCut) bool { return c.Ingested == 1 }, uint16(22))
	require(primary.receivedRows() == 0, "alarm activated before its paused-time deadline")
	save(filepath.Join(root, "pending-cut.json"), pending)
	stop(syscall.SIGKILL)
	save(filepath.Join(root, "stopped-cut.json"), currentGraphCut(checkpoint, uint16(22)))
	time.Sleep(1100 * time.Millisecond)
	withhold()
	start()
	resume()
	time.Sleep(150 * time.Millisecond)
	require(primary.receivedRows() == 0, "downtime advanced the paused alarm clock")

	// Round 1 (activate, withheld).  In partial-sinks the first leg already
	// accepts while the second leg withholds its ACK.
	waitRound("alarm activate round")
	activate := group("activate", round(primary), alarmGraphActivate, primarySink, 1)
	var withheldActivate []map[string]any
	if branch {
		require(primary.rowCount() == 0, "withheld Union Sink was counted as an accepted response")
	} else {
		// Received is recorded before the response is written, so wait for the
		// accepted row itself before asserting or slicing Rows.
		wait("accepted alarm activate leg", func() bool { return primary.rowCount() == arms })
		require(primary.rowCount() == arms, "accepted alarm Sink did not accept the activate round")
		require(secondary.rowCount() == 0, "withheld alarm Sink was counted as an accepted response")
		withheldActivate = round(secondary)
		group("activate/withheld", withheldActivate, alarmGraphActivate, secondarySink, 1)
		acceptedActivate := primary.snapshot().Rows[:arms]
		for i := range acceptedActivate {
			require(alarmOutputID(acceptedActivate[i]) != alarmOutputID(withheldActivate[i]),
				"required alarm Sinks shared one output ID")
			require(bytes.Equal(data(rowData(acceptedActivate[i])), data(rowData(withheldActivate[i]))),
				"required alarm Sinks disagreed on the activate payload")
		}
	}
	beforeActivate, activateHashes := heldCut("activate", 0)
	crashAndRelease(activateHashes, "activate")

	// Round 2 (activate replay): the withheld leg must come back byte-identical
	// and the round must then commit exactly `arms` ordinals on every Sink.
	start()
	resume()
	waitRound("alarm activate replay")
	requireAlarmReplay("activate", activate, group("activate/replay", round(primary), alarmGraphActivate, primarySink, 1))
	// The committed cut proves every required Sink ACKed, so Rows is already
	// updated and no extra wait is needed for the accepted legs.
	committed := committedAt(beforeActivate, 1)
	if branch {
		require(primary.rowCount() == arms, "branch-union lost the replayed activate round")
	} else {
		group("activate/withheld-replay", round(secondary), alarmGraphActivate, secondarySink, 1)
		requireRowsReplay("activate/withheld", withheldActivate, round(secondary))
		require(primary.rowCount() == 2*arms, "accepted alarm Sink lost the replayed activate round")
		require(secondary.rowCount() == arms, "withheld alarm Sink accepted its uncommitted activate round")
		acceptedLeg("activate", 0, arms)
	}
	save(filepath.Join(root, "committed-activate.json"), committed)
	noRepeat("activate", committed)

	// The clear row commits the Recovering latch first; only its 200 ms resolve
	// timer emits the resolve row.  A clear row that published the resolve in
	// the same cut would fail the next assertion.
	withhold()
	appendAlarmSignal(file, false, true)
	waitGraphCut(checkpoint, func(c graphCut) bool { return c.Ingested == 2 }, uint16(22))
	require(primary.receivedRows() == consumed, "resolve bypassed its delayed timer")

	// Round 3 (resolve, withheld) must keep the operator namespace, episode and
	// generation of the activate round.
	resolveFirst := 1 + uint64(arms)
	waitRound("alarm resolve round")
	resolve := group("resolve", round(primary), alarmGraphResolve, primarySink, resolveFirst)
	requireAlarmLifecycle(activate, resolve)
	var withheldResolve []map[string]any
	if branch {
		require(primary.rowCount() == arms, "withheld Union Sink was counted as an accepted response")
	} else {
		wait("accepted alarm resolve leg", func() bool { return primary.rowCount() == 3*arms })
		require(primary.rowCount() == 3*arms, "accepted alarm Sink did not accept the resolve round")
		require(secondary.rowCount() == arms, "withheld alarm Sink was counted as an accepted response")
		withheldResolve = round(secondary)
		group("resolve/withheld", withheldResolve, alarmGraphResolve, secondarySink, resolveFirst)
	}
	beforeResolve, resolveHashes := heldCut("resolve", 0)
	crashAndRelease(resolveHashes, "resolve")

	// Round 4 (resolve replay) followed by the committed-restart proof.
	start()
	resume()
	waitRound("alarm resolve replay")
	requireAlarmReplay("resolve", resolve, group("resolve/replay", round(primary), alarmGraphResolve, primarySink, resolveFirst))
	committedResolve := committedAt(beforeResolve, 2)
	if branch {
		require(primary.rowCount() == 2*arms, "branch-union lost the replayed resolve round")
	} else {
		group("resolve/withheld-replay", round(secondary), alarmGraphResolve, secondarySink, resolveFirst)
		requireRowsReplay("resolve/withheld", withheldResolve, round(secondary))
		require(primary.rowCount() == 4*arms, "accepted alarm Sink lost the replayed resolve round")
		require(secondary.rowCount() == 2*arms, "withheld alarm Sink accepted its uncommitted resolve round")
		acceptedLeg("resolve", 2*arms, 3*arms)
	}
	save(filepath.Join(root, "committed-resolve.json"), committedResolve)
	noRepeat("resolve", committedResolve)

	stop(syscall.SIGTERM)
	save(filepath.Join(root, "capture-primary.json"), primary.snapshot())
	if !branch {
		save(filepath.Join(root, "capture-secondary.json"), secondary.snapshot())
	}
	summary := map[string]any{
		"valid": true, "kind": kind, "real_sigkill": true,
		"pending_replay_identical": true, "committed_restart_no_repeat": true,
		"downtime_paused": true, "activate_resolve_episode_preserved": true,
		"partial_sink_flush_checked": !branch, "certified": false,
	}
	if branch {
		summary["operator_namespaces_distinct"] = true
	}
	save(filepath.Join(root, "summary.json"), summary)
	fmt.Println("ALARM_GRAPH_SCENARIO_OK", kind)
	return checkpoint
}

func runAlarmGraphMatrix(root, serverBin, oldServerBin string) {
	kinds := []string{"branch-union", "partial-sinks"}
	for _, kind := range kinds {
		runAlarmGraphProcess(filepath.Join(root, kind), serverBin, kind)
	}
	guards := map[string]any{}
	for _, kind := range kinds {
		source := filepath.Join(root, kind)
		// The high-version history is copied, never opened in place by the old
		// writer, and the catalog is created by the old binary itself so the
		// refusal can only come from the v22 checkpoint profile.
		guards[kind] = runProfileGuard(filepath.Join(root, "old-"+kind), oldServerBin,
			filepath.Join(source, "checkpoint"), filepath.Join(source, "a.ndjson"),
			"checkpoint source profile mismatch", "old v19 binary v22 alarm graph guard")
	}
	save(filepath.Join(root, "summary.json"), map[string]any{
		"valid": true, "process_scenarios": len(kinds), "crash_scenarios": len(kinds),
		"snapshot_versions": []int{22}, "old_profile_guards": guards,
		"exactly_once_claimed": false, "certified": false,
	})
	fmt.Println("ALARM_GRAPH_PROCESS_OK")
}
