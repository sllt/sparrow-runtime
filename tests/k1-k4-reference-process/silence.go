package main

// Real-process oracle for the bounded source-observed silence profile.
//
// Two outer profiles, one linear topology each:
//   File      v23, outer source identity observed-file-v1  (OFC1 cut + OFD1 log)
//   JetStream v24, outer source identity observed-jetstream-v1
//
// The OFC1 cut is a *committed observation* of the source prefix, not a health
// claim, and the OFD1 log is the exact fact that decision used. The legacy
// timed/alarm/graph gates are untouched: this file adds strict readers for
// versions 23 and 24 only.
import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const (
	silenceVersionFile      = 23
	silenceVersionJetStream = 24
	// Reviewed fixture: a 1 s window over a source observed with a 500 ms gap,
	// so the gap spans at least two 100 ms decisions and the window two gaps.
	silenceDurationMicros   = 1000000
	silenceGapMicros        = 500000
	silenceContractInterval = 100
	// The configured silence node and its bound state kind.
	silenceOperator     = 2
	silenceSlot         = 3
	silenceStateKind    = 12
	silenceFixtureKeys  = 16
	silenceGraceSlack   = 200 * time.Millisecond
	silenceCaseSIGKILLs = 5
)

func silenceVersion(jetstream bool) uint16 {
	if jetstream {
		return silenceVersionJetStream
	}
	return silenceVersionFile
}

func silenceKind(version uint16) string {
	if version == silenceVersionJetStream {
		return "observed-jetstream-v1"
	}
	return "observed-file-v1"
}

// silenceInnerKind is the source kind the OFC1 cut must carry for a profile.
func silenceInnerKind(version uint16) string {
	if version == silenceVersionJetStream {
		return "jetstream-v1"
	}
	return "file"
}

// silencePosition is the inner SourcePosition carried inside an OFC1 cut.
type silencePosition struct {
	Offset      uint64
	Records     uint64
	Kind        string
	Path        string
	Size        uint64
	Fingerprint uint64
}

// silenceCut is the committed OFC1 observation.
type silenceCut struct {
	ID         uint64
	Version    uint16
	Sequence   uint64
	Micros     int64
	Since      int64 // -1: unknown coverage
	LastFresh  int64 // -1: unknown coverage
	Ingested   uint64
	NextOutput uint64
	Generation string
	Source     silencePosition
}

// decodeSilenceCut reads the OFC1 payload: sequence, logical micros, coverage,
// then the existing raw SourcePosition encoding.
func decodeSilenceCut(raw []byte) silenceCut {
	require(len(raw) <= 30*1024, "observed cut bound")
	r := graphReader{raw: raw}
	require(string(r.take(4)) == "OFC1", "observed cut magic")
	cut := silenceCut{
		Sequence: r.u64(), Micros: int64(r.u64()),
		Since: int64(r.u64()), LastFresh: int64(r.u64()),
	}
	require(cut.Micros >= 0, "observed logical time is negative")
	coverage := (cut.Since == -1 && cut.LastFresh == -1) || (cut.Since >= 0 && cut.LastFresh >= 0)
	require(coverage, "observed coverage must be both absent or both present")
	if cut.Since >= 0 {
		require(cut.Since <= cut.LastFresh && cut.LastFresh <= cut.Micros,
			"observed coverage must satisfy since <= last_fresh <= micros")
	}
	cut.Source = silencePosition{
		Offset: r.u64(), Records: r.u64(), Kind: r.text(), Path: r.text(),
		Size: r.u64(), Fingerprint: r.u64(),
	}
	require(r.offset == len(raw), "trailing observed cut bytes")
	require(cut.Source.Kind != "" && cut.Source.Path != "", "observed inner source identity is empty")
	// Bootstrap carries no input, no coverage and no progress.
	if cut.Sequence == 0 {
		require(cut.Micros == 0 && cut.Since == -1 && cut.LastFresh == -1 &&
			cut.Source.Offset == 0 && cut.Source.Records == 0,
			"observed bootstrap cut must be empty")
	}
	return cut
}

func silenceReadString(raw []byte, offset *int) string {
	n, ok := readU32(raw, offset)
	require(ok && n <= 64*1024, "observed string bound")
	value, ok := readBytes(raw, offset, int(n))
	require(ok, "observed string truncated")
	return string(value)
}

func silenceCheckpointID(current string) uint64 {
	id, err := strconv.ParseUint(strings.TrimPrefix(strings.TrimSpace(string(current)), "chk-"), 10, 64)
	must(err)
	return id
}

func silenceNonZero16(raw []byte) bool {
	for _, byte := range raw {
		if byte != 0 {
			return true
		}
	}
	return false
}

// silenceStateFrame checks the single state frame of an observed profile: the
// configured operator/slot, the kind-12 silence tag and a declared entry count
// that the frame can actually hold.
func silenceStateFrame(frame []byte) uint32 {
	require(len(frame) >= 11, "observed state frame header truncated")
	operator := binary.LittleEndian.Uint32(frame[0:4])
	slot := binary.LittleEndian.Uint16(frame[4:6])
	kind := frame[6]
	entries := binary.LittleEndian.Uint32(frame[7:11])
	require(operator == silenceOperator && slot == silenceSlot && kind == silenceStateKind,
		fmt.Sprintf("observed state frame is operator=%d slot=%d kind=%d, want %d/%d/%d",
			operator, slot, kind, silenceOperator, silenceSlot, silenceStateKind))
	require(entries <= silenceFixtureKeys, fmt.Sprintf("observed state declares %d entries above max_keys", entries))
	// Every IoT entry carries at least a key count and a value count.
	require(uint64(len(frame)) >= 11+uint64(entries)*4,
		"observed state frame is too short for its declared entries")
	return entries
}

// currentSilenceCut parses an observed-time snapshot. Only versions 23 and 24
// are accepted here; the legacy timed reader keeps its own strict gates.
func currentSilenceCut(checkpoint string, requiredVersion uint16) silenceCut {
	require(requiredVersion == silenceVersionFile || requiredVersion == silenceVersionJetStream,
		"observed reader accepts only versions 23 and 24")
	current, err := os.ReadFile(filepath.Join(checkpoint, "CURRENT"))
	must(err)
	id := silenceCheckpointID(string(current))
	raw := snapshotPayload(checkpoint, id)
	require(len(raw) >= 38, "observed snapshot header")
	require(string(raw[:4]) == "SPV1", "observed snapshot magic")
	version := binary.LittleEndian.Uint16(raw[4:6])
	require(version == requiredVersion, fmt.Sprintf("observed snapshot version=%d want=%d", version, requiredVersion))
	offset := 6
	checkpointID, ok := readU64(raw, &offset)
	require(ok && checkpointID == id, "observed snapshot identity differs from CURRENT")
	ingested, ok := readU64(raw, &offset)
	require(ok, "observed snapshot ingested mirror truncated")
	outer := silencePosition{}
	outer.Offset, _ = readU64(raw, &offset)
	outer.Records, _ = readU64(raw, &offset)
	outer.Kind = silenceReadString(raw, &offset)
	outer.Path = silenceReadString(raw, &offset)
	outer.Size, _ = readU64(raw, &offset)
	outer.Fingerprint, _ = readU64(raw, &offset)
	require(outer.Kind == silenceKind(version),
		fmt.Sprintf("observed outer identity kind=%q want=%q", outer.Kind, silenceKind(version)))
	require(outer.Path != "", "observed outer identity path is empty")
	attempt, ok := readU64(raw, &offset)
	require(ok && attempt != 0, "observed snapshot lacks attempt identity")
	revision, ok := readU64(raw, &offset)
	require(ok && revision != 0, "observed snapshot lacks revision identity")
	generation, ok := readBytes(raw, &offset, 16)
	require(ok && silenceNonZero16(generation), "observed snapshot lacks a state generation")
	epoch, ok := readBytes(raw, &offset, 16)
	require(ok && bytes.Equal(epoch, generation), "observed output epoch differs from the state generation")
	next, ok := readU64(raw, &offset)
	require(ok && next > 0, "observed output cursor missing")
	n, ok := readU32(raw, &offset)
	require(ok && n <= 256*1024, "observed plan bound")
	plan, ok := readBytes(raw, &offset, int(n))
	require(ok && len(plan) >= 4 && string(plan[:4]) == "CPL1", "observed manifest version")
	states, ok := readU16(raw, &offset)
	require(ok && states == 1, "observed profile carries exactly one state")
	frameLength, ok := readU32(raw, &offset)
	require(ok && frameLength >= 11, "observed state frame length")
	frame, ok := readBytes(raw, &offset, int(frameLength))
	require(ok, "observed state frame truncated")
	require(offset == len(raw), "trailing observed snapshot bytes")
	silenceStateFrame(frame)

	decoded, err := hex.DecodeString(outer.Path)
	must(err)
	require(len(decoded) >= 20 && string(decoded[:4]) == "OFC1", "observed cut encoding")
	cut := decodeSilenceCut(decoded)
	require(cut.Source.Kind == silenceInnerKind(version),
		fmt.Sprintf("observed inner source kind=%q want=%q", cut.Source.Kind, silenceInnerKind(version)))
	require(cut.Source.Offset == outer.Offset && cut.Source.Records == outer.Records &&
		cut.Source.Size == outer.Size && cut.Source.Fingerprint == outer.Fingerprint,
		"observed inner cut position differs from the outer source position")
	cut.ID = id
	cut.Version = version
	cut.Ingested = ingested
	cut.NextOutput = next
	cut.Generation = hex.EncodeToString(generation)
	return cut
}

// sampleSilenceCut tolerates only ENOENT while a generation is being published.
func sampleSilenceCut(checkpoint string, requiredVersion uint16) (silenceCut, bool) {
	defer func() {
		if problem := recover(); problem != nil {
			if err, ok := problem.(error); ok && os.IsNotExist(err) {
				return
			}
			panic(problem)
		}
	}()
	return currentSilenceCut(checkpoint, requiredVersion), true
}

func waitSilenceCut(checkpoint string, condition func(silenceCut) bool, requiredVersion uint16) silenceCut {
	var cut silenceCut
	wait("committed observed cut", func() bool {
		if _, err := os.Stat(filepath.Join(checkpoint, "CURRENT")); err != nil {
			return false
		}
		sampled, ready := sampleSilenceCut(checkpoint, requiredVersion)
		if !ready || !condition(sampled) {
			return false
		}
		cut = sampled
		return true
	})
	return cut
}

// silenceJSON decodes decision JSON without losing 64-bit integers: a File
// fingerprint can exceed 2^53, so float64 decoding would reject a legal
// decision.
func silenceJSON(raw []byte, out *map[string]any) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	must(decoder.Decode(out))
	var trailing any
	require(decoder.Decode(&trailing) == io.EOF, "trailing observed decision JSON")
}

// silenceDecision reads the OFD1 log: magic, SHA-256 over the JSON, then JSON.
func silenceDecision(checkpoint string) map[string]any {
	raw, err := os.ReadFile(filepath.Join(checkpoint, "TIME_PENDING"))
	must(err)
	require(len(raw) >= 36 && len(raw) <= 128*1024 && string(raw[:4]) == "OFD1", "observed decision envelope")
	sum := sha256.Sum256(raw[36:])
	require(bytes.Equal(sum[:], raw[4:36]), "observed decision checksum")
	var decision map[string]any
	silenceJSON(raw[36:], &decision)
	return decision
}

// silenceGeneration reads the durable state generation marker (SG01 + 16 bytes).
func silenceGeneration(checkpoint string) string {
	raw, err := os.ReadFile(filepath.Join(checkpoint, "STATE_GENERATION"))
	must(err)
	require(len(raw) == 20 && string(raw[:4]) == "SG01", "state generation marker")
	require(silenceNonZero16(raw[4:]), "state generation unset")
	return hex.EncodeToString(raw[4:])
}

// silenceBytes32 reads a JSON byte array and refuses anything that is not
// exactly 32 legal, not-all-zero bytes.
func silenceBytes32(value any, label string) []byte {
	items, ok := value.([]any)
	require(ok && len(items) == 32, label+" must be a 32-byte array")
	out := make([]byte, 32)
	zero := true
	for index, item := range items {
		n, ok := silenceInt(item)
		require(ok && n >= 0 && n <= 255, fmt.Sprintf("%s has a non-byte element at %d", label, index))
		out[index] = byte(n)
		if n != 0 {
			zero = false
		}
	}
	require(!zero, label+" must not be all zero")
	return out
}

// silenceRequireDecision pins one pending OFD1 decision against the committed
// cut it is the immediate successor of.
//
// `dataRow` selects the two legal shapes: an input decision (row hash present,
// no fact, exactly one new record) or a coverage decision (fact present, no row
// hash, unchanged source prefix). The check re-derives the coverage rule of the
// runtime, so a decision that borrowed a fact it did not have is refused.
func silenceRequireDecision(decision map[string]any, before silenceCut, generation string, dataRow bool, expectIngested uint64) {
	require(generationText(decision["generation"]) == generation, "pending generation differs from SG01")
	silenceBytes32(decision["semantics"], "pending semantics")
	require(number(decision["sequence"]) == before.Sequence+1, "pending is not the immediate successor")
	micros, ok := silenceInt(decision["micros"])
	require(ok && micros >= before.Micros, "pending logical time went backwards")
	require(number(decision["ingested"]) == expectIngested,
		fmt.Sprintf("pending ingested=%v want=%d", decision["ingested"], expectIngested))
	position, ok := decision["position"].(map[string]any)
	require(ok, "pending position missing")
	require(fmt.Sprint(position["kind"]) == before.Source.Kind && fmt.Sprint(position["path"]) == before.Source.Path,
		"pending position identity changed")
	offset, records := number(position["offset"]), number(position["records"])
	jetstream := before.Source.Kind == "jetstream-v1"
	// The inner File identity is rebuilt from the live prefix, so an input
	// decision may legitimately carry a larger size/fingerprint; a coverage
	// decision probes the same prefix and must reproduce it exactly, and the
	// JetStream reader keeps one identity for the whole attempt.
	if jetstream || !dataRow {
		require(number(position["size"]) == before.Source.Size &&
			number(position["fingerprint"]) == before.Source.Fingerprint,
			"pending position size/fingerprint changed")
	}
	if dataRow {
		require(records == before.Source.Records+1, "an input decision must advance exactly one record")
		if jetstream {
			require(offset == before.Source.Offset+1, "a JetStream input decision must advance one sequence")
		} else {
			require(offset > before.Source.Offset, "a File input decision must advance the byte offset")
		}
	} else {
		require(offset == before.Source.Offset && records == before.Source.Records,
			"a coverage decision must not move the committed prefix")
	}
	require(decision["restart"] == false, "a decision inside a committed attempt is not a restart")
	fact, hasFact := decision["fact"].(map[string]any)
	if dataRow {
		require(!hasFact, "an input decision must not borrow a feed fact")
		silenceBytes32(decision["row_hash"], "pending row hash")
		return
	}
	require(decision["row_hash"] == nil, "a coverage decision carries no row hash")
	require(hasFact, "a coverage decision must record the feed fact it used")
	tag := number(fact["tag"])
	require(tag >= 1 && tag <= 6, fmt.Sprintf("unknown feed fact tag %d", tag))
	require(number(fact["head"]) >= before.Source.Offset, "feed fact head precedes the committed prefix")
	since, lastFresh := decision["since"], decision["last_fresh"]
	if tag != 1 {
		// An actual non-drained or unverified probe breaks coverage outright.
		require(since == nil && lastFresh == nil, "a non-drained or unverified probe must break coverage")
		return
	}
	// A caught-up probe is fresh at the decision time; its grace either
	// continues the previous one or restarts once the gap expired.
	require(since != nil && lastFresh != nil, "a caught-up decision must carry coverage")
	require(silenceInt2(lastFresh) == micros, "a caught-up observation is fresh at the decision time")
	expired := before.LastFresh < 0 || before.Since < 0 || micros-before.LastFresh > silenceGapMicros
	if expired {
		require(silenceInt2(since) == micros, "an expired grace must restart at this observation")
	} else {
		require(silenceInt2(since) == before.Since, "a continued observation must keep its grace start")
	}
}

func silenceInt2(value any) int64 {
	parsed, ok := silenceInt(value)
	require(ok, "expected an integer value")
	return parsed
}

func silenceInt(value any) (int64, bool) {
	switch value := value.(type) {
	case float64:
		return int64(value), value == float64(int64(value))
	case int64:
		return value, true
	case int:
		return int64(value), true
	case json.Number:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		return parsed, err == nil
	default:
		return 0, false
	}
}

func silenceTiming(registry []any) map[string]any {
	if registry == nil {
		registry = []any{}
	}
	return map[string]any{
		"kind": "silence", "clock": "paused",
		"duration_micros": silenceDurationMicros, "max_observation_gap_micros": silenceGapMicros,
		"registered_keys": registry,
	}
}

// silenceSpec is source -> silence -> required HTTP with nothing in between, so
// no projection or filter can hide an observation from the state.
func silenceSpec(file, sink, checkpoint string, brokerPort int, registry []any, jetstream bool, pipeline uint64) map[string]any {
	var source any = fileSource(file)
	if jetstream {
		source = jsSource(brokerPort, "silence")
	}
	spec := map[string]any{
		"version": 1, "stream": "telemetry", "source": source,
		"sink":           map[string]any{"kind": "http", "url": sink, "batch_rows": 1, "linger_ms": 0, "max_inflight": 1, "outbox_capacity": 8},
		"fail_on_decode": true, "recovery": "aligned",
		"checkpoint_dir": checkpoint,
		"checkpoint":     checkpointPolicy(float64(silenceContractInterval), true),
		"graph": map[string]any{"version": 1, "pipeline_id": pipeline, "revision_id": 1, "nodes": []any{
			map[string]any{"id": 1, "kind": "memory_source", "table": "telemetry", "out": []uint32{2}},
			map[string]any{"id": 2, "kind": "silence", "iot": map[string]any{
				"keys": []string{"device_id"}, "fields": []any{}, "emit_first": false, "ttl_micros": 0,
				"max_keys": silenceFixtureKeys, "invalid": "error", "timing": silenceTiming(registry)}, "out": []uint32{3}},
			map[string]any{"id": 3, "kind": "capture_sink", "name": "silence"}}},
	}
	if jetstream {
		spec["delivery"] = "checkpointed_at_least_once"
	}
	return spec
}

func appendSilenceRecord(path, key string, active bool) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	_, err = file.Write(append(data(map[string]any{"device_id": key, "active": active}), '\n'))
	must(err)
	must(file.Sync())
	must(file.Close())
}

// publishSilenceRecord uses the production client's wire form and fences on the
// broker's persisted message count instead of a reply subject.
func publishSilenceRecord(producer *nats, key string, active bool, total int) {
	body := data(map[string]any{"device_id": key, "active": active})
	_, err := fmt.Fprintf(producer.conn, "PUB input.rows %d\r\n%s\r\n", len(body), body)
	must(err)
	wait("JetStream persistence fence", func() bool {
		info := producer.request("$JS.API.STREAM.INFO.INPUT", []byte("{}"))
		return number(nested(info, "state", "messages")) >= uint64(total)
	})
}

// silenceRow is one fully validated transport wrapper plus the key-only payload:
// exactly one key and seven lifecycle columns, never a telemetry value.
type silenceRow struct {
	key        string
	event      string
	generation string
	operator   uint64
	episode    uint64
	micros     int64
	lastSeen   *int64
	neverSeen  bool
	id         string
	ordinal    uint64
	raw        map[string]any
}

func parseSilenceRow(row map[string]any, wantEvent, generation string, ordinal uint64) silenceRow {
	require(len(row) == 2, fmt.Sprintf("silence transport wrapper must be data+id, got %d fields", len(row)))
	payload, ok := row["data"].(map[string]any)
	require(ok, "silence row lacks a data object")
	require(len(payload) == 8, fmt.Sprintf("silence payload must be one key plus seven columns, got %d fields", len(payload)))
	key, ok := payload["device_id"].(string)
	require(ok && key != "", "silence key column is missing or empty")
	require(fmt.Sprint(payload["sparrow_silence_event"]) == wantEvent,
		fmt.Sprintf("silence event=%v want=%s", payload["sparrow_silence_event"], wantEvent))
	require(generationText(payload["sparrow_silence_generation"]) == generation,
		"silence generation column must equal the durable state generation")
	operator := number(payload["sparrow_silence_operator"])
	require(operator == silenceOperator, fmt.Sprintf("silence operator=%d want=%d", operator, silenceOperator))
	episode := number(payload["sparrow_silence_episode"])
	micros, timeOK := silenceInt(payload["sparrow_silence_time"])
	require(timeOK && micros > 0, "silence logical time is not an integer micros value")
	neverSeen, ok := payload["sparrow_silence_never_seen"].(bool)
	require(ok, "silence never_seen column is not a bool")
	var lastSeen *int64
	switch value := payload["sparrow_silence_last_seen"].(type) {
	case nil:
	case float64:
		at := int64(value)
		lastSeen = &at
	case int64:
		at := value
		lastSeen = &at
	case json.Number:
		parsed, err := strconv.ParseInt(string(value), 10, 64)
		must(err)
		lastSeen = &parsed
	default:
		panic("silence last_seen column is neither null nor an integer")
	}
	id, ok := row["id"].(string)
	require(ok && len(id) == 48 && id == strings.ToLower(id), "silence output ID is not lowercase 24-byte hex")
	decoded, err := hex.DecodeString(id)
	must(err)
	require(hex.EncodeToString(decoded[:16]) == generation, "silence output ID epoch must equal the state generation")
	got := binary.BigEndian.Uint64(decoded[16:])
	require(got == ordinal, fmt.Sprintf("silence output ordinal=%d want=%d", got, ordinal))
	return silenceRow{key: key, event: wantEvent, generation: generation, operator: operator, episode: episode,
		micros: micros, lastSeen: lastSeen, neverSeen: neverSeen, id: id, ordinal: got, raw: row}
}

func silenceStop(process **child, signal os.Signal) {
	if *process == nil {
		return
	}
	command := (*process).cmd
	(*process).stop(signal)
	if signal == syscall.SIGKILL {
		require(command.ProcessState != nil, "silence process lacks a wait status")
		status, ok := command.ProcessState.Sys().(syscall.WaitStatus)
		require(ok && status.Signaled() && status.Signal() == syscall.SIGKILL,
			"silence process did not actually die from SIGKILL")
	}
	*process = nil
}

// runSilenceProcess drives one real binary through the full recovery cycle for
// one transport and one key-source mode, returning the number of real SIGKILLs
// it performed. Evidence per step is written next to the checkpoint.
func runSilenceProcess(root, serverBin, natsBin, mode string, jetstream bool) int {
	require(mode == "observed" || mode == "registered", "unknown silence mode")
	must(os.Mkdir(root, 0700))
	version := silenceVersion(jetstream)
	file := filepath.Join(root, "input.ndjson")
	must(os.WriteFile(file, nil, 0600))
	checkpoint := filepath.Join(root, "checkpoint")
	capture := newCapture()
	defer capture.close()
	port := freePort()
	client := newAPI(port)
	var broker *child
	var producer *nats
	brokerPort := 0
	if jetstream {
		broker, producer, brokerPort = startBroker(root, natsBin)
		defer broker.stop(syscall.SIGTERM)
		defer producer.close()
	}
	var process *child
	defer func() { silenceStop(&process, syscall.SIGKILL) }()
	crashes := 0
	start := func() {
		process = spawnServer(serverBin, root, filepath.Join(root, "server.log"), port)
		waitHealth(client, "silence server")
	}
	resume := func() {
		client.ok("POST", "/v1/pipelines/silence/start", map[string]any{})
		waitRunning(client, "silence")
	}
	// Every crash is a real SIGKILL of the child, and the durable baseline is
	// re-read from disk afterwards: the live attempt may have committed more
	// decisions between an earlier read and the kill.
	crash := func() silenceCut {
		silenceStop(&process, syscall.SIGKILL)
		crashes++
		return currentSilenceCut(checkpoint, version)
	}
	// The registered mode proves a never-seen device from the static registry
	// alone; the observed mode proves it from a real record instead.
	var registry []any
	if mode == "registered" {
		registry = []any{[]any{"d"}}
	}
	pipeline := uint64(15001)
	if jetstream {
		pipeline = 15002
	}
	published := 0
	emit := func(active bool) {
		published++
		if jetstream {
			publishSilenceRecord(producer, "d", active, published)
			return
		}
		appendSilenceRecord(file, "d", active)
	}
	if mode == "observed" {
		emit(true)
	}
	wantIngested := uint64(0)
	if mode == "observed" {
		wantIngested = 1
	}
	// The required-ACK hold is armed before the first attempt, so the whole
	// first grace window is protected from the moment the state can decide.
	capture.setHold()
	start()
	configureTimed(client, capture, brokerPort)
	spec := silenceSpec(file, capture.url(), checkpoint, brokerPort, registry, jetstream, pipeline)
	save(filepath.Join(root, "spec.json"), spec)
	startPipeline(client, "silence", spec)

	// 1. A committed, freshly covered prefix with no event yet.
	healthy := waitSilenceCut(checkpoint, func(cut silenceCut) bool {
		return cut.Ingested >= wantIngested && cut.Since >= 0
	}, version)
	require(healthy.LastFresh >= healthy.Since, "fresh coverage must carry a last_fresh")
	require(healthy.Generation != "", "observed cut must carry the state generation")
	require(capture.receivedRows() == 0, "silence fired before its window")
	generation := healthy.Generation
	require(silenceGeneration(checkpoint) == generation, "CURRENT cut generation differs from SG01")
	save(filepath.Join(root, "healthy-cut.json"), healthy)

	// A real process outage must not spend any of the window, and the new grace
	// must start from a new observation rather than the stale committed cut.
	stopped := crash()
	require(stopped.Sequence >= healthy.Sequence, "the kill rolled the committed cut back")
	time.Sleep(1200 * time.Millisecond)
	start()
	resume()
	require(capture.receivedRows() == 0, "downtime produced an event")
	grace := waitSilenceCut(checkpoint, func(cut silenceCut) bool {
		return cut.Sequence > stopped.Sequence && cut.Since > stopped.Since
	}, version)
	require(grace.Generation == generation, "restart changed the state generation")
	// A fixed wait strictly inside the new 1 s grace: nothing may fire here.
	time.Sleep(silenceGraceSlack)
	require(capture.receivedRows() == 0, "silence fired inside the new grace")
	save(filepath.Join(root, "grace-cut.json"), grace)

	accepted := 0
	received := 0
	// Per-round inputs, declared before the closure that reads them: `dataRow`
	// selects the decision shape, `expectIngested` the expected input count.
	dataRow := false
	expectIngested := wantIngested
	verify := func(row silenceRow, event string) {
		require(row.key == "d", event+": silence key must be the fixture device")
		require(row.episode == 1, event+": the first lifecycle event opens episode 1")
		if event == "silent" {
			require(row.micros >= grace.Since+silenceDurationMicros,
				fmt.Sprintf("silence time %d precedes the grace deadline %d", row.micros, grace.Since+silenceDurationMicros))
			if mode == "registered" {
				require(row.lastSeen == nil && row.neverSeen,
					"a registered never-seen key must report last_seen=null and never_seen=true")
			} else {
				require(row.lastSeen != nil && !row.neverSeen, "an observed key must report a real last_seen")
				require(row.micros-*row.lastSeen >= silenceDurationMicros, "the window must follow the last record")
			}
			return
		}
		require(row.lastSeen != nil && !row.neverSeen, "resumed must report the record it just received")
		require(row.micros >= *row.lastSeen, "resumed time precedes its own record")
	}
	// heldRound completes one event: the body is already withheld by the caller,
	// then the uncommitted decision is killed, replayed byte-identically,
	// committed and finally shown not to repeat after another restart.
	heldRound := func(event string) silenceCut {
		wait("held "+event+" event", func() bool { return capture.receivedRows() >= received+1 })
		capture.waitHeld()
		received++
		require(capture.rowCount() == accepted, event+": withheld silence was counted as accepted")
		held := capture.snapshot().Received[received-1]
		verify(parseSilenceRow(held, event, generation, uint64(accepted+1)), event)
		for index := 0; index < received; index++ {
			require(fmt.Sprint(rowData(capture.snapshot().Received[index])["device_id"]) == "d",
				event+": an event for a key outside the registered or observed set")
		}
		before := currentSilenceCut(checkpoint, version)
		// A returning record may immediately follow the fresh coverage cut;
		// it need not wait for another idle checkpoint before resuming.
		require(before.Sequence >= grace.Sequence, event+": the pending decision lacks a committed predecessor")
		silenceRequireDecision(silenceDecision(checkpoint), before, generation, dataRow, expectIngested)
		save(filepath.Join(root, event+"-held-decision.json"), silenceDecision(checkpoint))
		currentHash := hash(filepath.Join(checkpoint, "CURRENT"))
		pendingHash := hash(filepath.Join(checkpoint, "TIME_PENDING"))
		time.Sleep(100 * time.Millisecond)
		require(hash(filepath.Join(checkpoint, "CURRENT")) == currentHash, event+": withheld ACK advanced CURRENT")
		stopped := crash()
		require(stopped.Sequence == before.Sequence && stopped.Since == before.Since,
			event+": the kill committed an unexpected decision")
		require(hash(filepath.Join(checkpoint, "CURRENT")) == currentHash &&
			hash(filepath.Join(checkpoint, "TIME_PENDING")) == pendingHash,
			event+": SIGKILL changed the durable observed decision")
		capture.releaseHold(true)
		start()
		resume()
		// The replay reuses the original fact: no new coverage is required.
		wait("replayed "+event+" event", func() bool { return capture.receivedRows() >= received+1 })
		replay := capture.snapshot().Received[received]
		require(bytes.Equal(data(held), data(replay)), event+": replayed silence changed its ID or payload")
		received++
		committed := waitSilenceCut(checkpoint, func(cut silenceCut) bool {
			return cut.Sequence > stopped.Sequence && cut.NextOutput > stopped.NextOutput
		}, version)
		accepted++
		require(hash(filepath.Join(checkpoint, "CURRENT")) != currentHash, event+": CURRENT did not advance after the ACK")
		require(capture.rowCount() == accepted, event+": accepted rows do not match the committed outputs")
		require(committed.ID > before.ID, event+": committed generation did not advance")
		save(filepath.Join(root, event+"-committed.json"), committed)
		// The next attempt must break coverage first (a restart is explicit), so
		// wait for a genuinely fresh post-restart decision before anything else.
		stopped = crash()
		start()
		resume()
		fresh := waitSilenceCut(checkpoint, func(cut silenceCut) bool {
			return cut.Sequence > stopped.Sequence && cut.Since > stopped.Since
		}, version)
		require(fresh.Generation == generation, event+": restart changed the state generation")
		require(capture.receivedRows() == received, event+": a committed event was repeated after restart")
		require(capture.rowCount() == accepted, event+": a committed event was re-accepted after restart")
		return fresh
	}
	// Round 1: the body of the coverage-authorized silent event is already held.
	grace = heldRound("silent")

	// Round 2: the same key's own record returns it, in episode 1. The hold is
	// armed before the input row, and the fresh post-restart decision above has
	// already committed, so this attempt is no longer restarting.
	capture.setHold()
	dataRow = true
	expectIngested = wantIngested + 1
	emit(false)
	heldRound("resumed")

	silenceStop(&process, syscall.SIGTERM)
	require(crashes == silenceCaseSIGKILLs, fmt.Sprintf("expected %d real SIGKILLs, counted %d", silenceCaseSIGKILLs, crashes))
	save(filepath.Join(root, "capture.json"), capture.snapshot())
	save(filepath.Join(root, "summary.json"), map[string]any{
		"valid": true, "transport": map[bool]string{true: "jetstream", false: "file"}[jetstream],
		"mode": mode, "snapshot_version": version, "state_kind": silenceStateKind, "crashes": crashes,
		"actual_sigkill": true, "source_health_replayed": true, "process_outage_only": true,
		"broker_outage_tested": false, "downtime_paused": true,
		"held_replay_identical": true, "committed_restart_no_repeat": true,
		"exactly_once_claimed": false, "certified": false,
	})
	fmt.Println("SILENCE_SCENARIO_OK", mode, map[bool]string{true: "jetstream", false: "file"}[jetstream])
	return crashes
}

// runSilenceMatrix runs the observed and registered scenarios for File, and for
// JetStream when the full matrix is requested, then proves the pre-silence
// binary refuses the new history through the shared profile guard.
func runSilenceMatrix(root, serverBin, oldServerBin, natsBin string, fileOnly bool) {
	type scenario struct {
		transport string
		mode      string
		jetstream bool
	}
	cases := []scenario{
		{transport: "file", mode: "observed"},
		{transport: "file", mode: "registered"},
	}
	transports := []string{"file"}
	if !fileOnly {
		cases = append(cases,
			scenario{transport: "jetstream", mode: "observed", jetstream: true},
			scenario{transport: "jetstream", mode: "registered", jetstream: true})
		transports = append(transports, "jetstream")
	}
	crashes := 0
	for _, item := range cases {
		crashes += runSilenceProcess(filepath.Join(root, item.transport+"-"+item.mode), serverBin, natsBin, item.mode, item.jetstream)
	}
	// One guard per stream type. The high-version history is copied, never
	// opened in place by the old writer, and the catalog is created by that old
	// binary itself, so the refusal can only come from the checkpoint profile.
	guards := map[string]any{}
	for _, transport := range transports {
		source := filepath.Join(root, transport+"-observed")
		guards[transport] = runProfileGuard(filepath.Join(root, "old-"+transport+"-guard"), oldServerBin,
			filepath.Join(source, "checkpoint"), filepath.Join(source, "input.ndjson"),
			"checkpoint source profile mismatch", "pre-silence binary observed-time guard")
	}
	versions := []int{silenceVersionFile}
	if !fileOnly {
		versions = append(versions, silenceVersionJetStream)
	}
	require(crashes == len(cases)*silenceCaseSIGKILLs, "matrix SIGKILL count differs from the per-case contract")
	save(filepath.Join(root, "summary.json"), map[string]any{
		"valid": true, "transport_cases": len(cases), "crash_scenarios": crashes,
		"snapshot_versions": versions, "file_only": fileOnly,
		"source_scope":   "linear File append_only / JetStream -> silence -> required HTTP",
		"actual_sigkill": true, "source_health_replayed": true,
		"process_outage_only": true, "broker_outage_tested": false,
		"old_profile_guards": guards, "exactly_once_claimed": false, "certified": false,
	})
	fmt.Println("SILENCE_PROCESS_OK")
}
