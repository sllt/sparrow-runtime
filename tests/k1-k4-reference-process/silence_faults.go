package main

// Real-process fault oracle for the bounded source-observed silence profile.
//
// Every fault is applied to this fixture's *own* resources: its own File, its
// own required HTTP capture, its own JetStream broker and its own consumer. No
// shared service, catalog, checkpoint or broker is modified, and no device may
// ever be judged silent while its source is unreadable, incomplete, backing up
// or waiting on a slow required sink. Waits use the logical committed cut or a
// generous lower bound, never a tight wall-clock upper bound.
import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// silenceFaultAttempt is one live silence fixture: its own root, checkpoint,
// capture and child process (plus its own broker for the JetStream transport).
type silenceFaultAttempt struct {
	root       string
	file       string
	checkpoint string
	capture    *capture
	client     api
	version    uint16
	jetstream  bool
	duration   int64
	gap        int64
	process    *child
	broker     *child
	producer   *nats
	brokerPort int
}

// silenceFaultSpec reuses the frozen silence spec builder and only widens the
// two reviewed durations, so fault cases can leave the first window long enough
// that the fault is applied before anything can fire.
func silenceFaultSpec(file, sink, checkpoint string, brokerPort int, registry []any, jetstream bool, pipeline uint64, duration, gap int64) map[string]any {
	require(duration > 0 && gap > 0 && gap*2 <= duration,
		"a fault fixture must keep duration >= 2 * gap")
	require(gap >= 2*silenceContractInterval*1000, "a fault fixture must keep gap >= two decisions")
	spec := silenceSpec(file, sink, checkpoint, brokerPort, registry, jetstream, pipeline)
	iot := spec["graph"].(map[string]any)["nodes"].([]any)[1].(map[string]any)["iot"].(map[string]any)
	timing := iot["timing"].(map[string]any)
	timing["duration_micros"] = duration
	timing["max_observation_gap_micros"] = gap
	return spec
}

// silenceFaultOpen starts one fault fixture, cleaning up everything it already
// allocated if the server, the admission or the start fails, so a broken
// fixture cannot leak a process, a broker or a listener.
//
// `prepare` runs after the server is healthy but *before* the pipeline starts,
// which is where a required-HTTP hold has to be armed so the very first event
// cannot slip out unheld.
func silenceFaultOpen(root, serverBin, natsBin string, registry []any, jetstream bool, pipeline uint64, duration, gap int64, prepare func(*silenceFaultAttempt)) *silenceFaultAttempt {
	must(os.Mkdir(root, 0700))
	attempt := &silenceFaultAttempt{
		root:       root,
		file:       filepath.Join(root, "input.ndjson"),
		checkpoint: filepath.Join(root, "checkpoint"),
		capture:    newCapture(),
		version:    silenceVersion(jetstream),
		jetstream:  jetstream,
		duration:   duration,
		gap:        gap,
	}
	started := false
	defer func() {
		if !started {
			attempt.close()
		}
	}()
	must(os.WriteFile(attempt.file, nil, 0600))
	port := freePort()
	attempt.client = newAPI(port)
	if jetstream {
		attempt.broker, attempt.producer, attempt.brokerPort = startBroker(root, natsBin)
	}
	attempt.process = spawnServer(serverBin, root, filepath.Join(root, "server.log"), port)
	waitHealth(attempt.client, "silence fault server")
	configureTimed(attempt.client, attempt.capture, attempt.brokerPort)
	if prepare != nil {
		prepare(attempt)
	}
	spec := silenceFaultSpec(attempt.file, attempt.capture.url(), attempt.checkpoint, attempt.brokerPort,
		registry, jetstream, pipeline, duration, gap)
	save(filepath.Join(root, "spec.json"), spec)
	startPipeline(attempt.client, "silence", spec)
	started = true
	return attempt
}

func (a *silenceFaultAttempt) close() {
	// Keep the transport evidence even if an oracle assertion panics.
	if a.capture != nil {
		save(filepath.Join(a.root, "capture-at-close.json"), a.capture.snapshot())
	}
	silenceStop(&a.process, syscall.SIGTERM)
	if a.producer != nil {
		a.producer.close()
	}
	silenceStop(&a.broker, syscall.SIGTERM)
	a.capture.close()
}

// resume restarts the job by hand after a failure, without changing the fixture.
func (a *silenceFaultAttempt) resume() {
	a.client.ok("POST", "/v1/pipelines/silence/start", map[string]any{})
	waitRunning(a.client, "silence")
}

func (a *silenceFaultAttempt) status() map[string]any {
	return statusOf(a.client, "silence")
}

func (a *silenceFaultAttempt) cut() silenceCut {
	return currentSilenceCut(a.checkpoint, a.version)
}

// failed waits for the attempt to stop serving and records how it failed. A
// missing error is itself a failure: `fmt.Sprint(nil)` must never pass as one.
func (a *silenceFaultAttempt) failed(label string) map[string]any {
	var status map[string]any
	wait(label+" failure", func() bool {
		status = a.status()
		return nested(status, "actual", "status") == "failed"
	})
	raw := nested(status, "actual", "last_error")
	require(raw != nil, label+": a failed attempt reported no error at all")
	message := strings.TrimSpace(fmt.Sprint(raw))
	require(message != "" && message != "<nil>", label+": a failed attempt reported an empty error")
	return status
}

// settle waits a lower bound longer than one full window, then reports whether
// the attempt is still serving. A retry that ignored the fault would have to
// become healthy and silence the registered key inside this window.
func (a *silenceFaultAttempt) settle(label string) string {
	time.Sleep(time.Duration(a.duration+a.gap)*time.Microsecond + 300*time.Millisecond)
	return fmt.Sprint(nested(a.status(), "actual", "status"))
}

func silenceFaultAppend(path string, bytes []byte) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	must(err)
	_, err = file.Write(bytes)
	must(err)
	must(file.Sync())
	must(file.Close())
}

// silenceFaultJournal saves exact journal bytes (or their absence) as evidence.
// Only a genuinely absent journal is "absent": any other I/O problem is an
// error, because a decision log that cannot be read is not evidence of silence.
func silenceFaultJournal(checkpoint string) map[string]any {
	path := filepath.Join(checkpoint, "TIME_PENDING")
	if _, err := os.Stat(path); err != nil {
		require(os.IsNotExist(err), fmt.Sprintf("observed journal probe failed: %v", err))
		return map[string]any{"present": false}
	}
	return map[string]any{"present": true, "sha256": hash(path)}
}

func silenceFaultRows(c *capture, event string) []map[string]any {
	rows := []map[string]any{}
	for _, row := range c.snapshot().Received {
		if fmt.Sprint(rowData(row)["sparrow_silence_event"]) == event {
			rows = append(rows, row)
		}
	}
	return rows
}

// silenceFaultSourceError recognises a source-side failure without pinning an
// exact message: the attempt must fail on its input, never on a decode policy
// or an output problem.
func silenceFaultSourceError(message string) bool {
	for _, token := range []string{"no such file", "identity", "truncat", "replaced", "observation", "source"} {
		if strings.Contains(message, token) {
			return true
		}
	}
	return false
}

// silenceFaultWaitSilent waits for at least one more accepted silence event and
// returns the row for `key`.
//
// Two keys can become due in the same committed cut, so a caller asking for the
// first event may legitimately already observe both bodies. Only the final
// settled count is exact; intermediate reads must not be.
func silenceFaultWaitSilent(attempt *silenceFaultAttempt, from int, grace silenceCut, key string, ordinal uint64) silenceRow {
	wait("silence fault event for "+key, func() bool { return attempt.capture.receivedRows() >= from+1 })
	rows := attempt.capture.snapshot().Received
	carried := -1
	for index := from; index < len(rows); index++ {
		if fmt.Sprint(rowData(rows[index])["device_id"]) == key {
			carried = index
			break
		}
	}
	require(carried >= 0, fmt.Sprintf("no silence event for %q among the bodies received so far", key))
	row := parseSilenceRow(rows[carried], "silent", grace.Generation, ordinal)
	require(row.key == key, "silence event for an unexpected device key")
	require(row.micros >= grace.Since+attempt.duration,
		fmt.Sprintf("silence time %d precedes the grace deadline %d", row.micros, grace.Since+attempt.duration))
	require(row.episode == 1, "the first silence event opens episode 1")
	return row
}

// silenceFaultBrokerUp is a bounded TCP readiness probe for our own broker.
func silenceFaultBrokerUp(port int) bool {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 200*time.Millisecond)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// silenceFaultStreamInfo asks this broker whether its stream is served again.
// It uses a fresh connection per probe so a transient API error cannot desync
// the long-lived fixture client.
func silenceFaultStreamInfo(port int, stream string) (map[string]any, bool) {
	conn, err := net.DialTimeout("tcp", fmt.Sprintf("127.0.0.1:%d", port), 300*time.Millisecond)
	if err != nil {
		return nil, false
	}
	defer conn.Close()
	must(conn.SetDeadline(time.Now().Add(2 * time.Second)))
	reader := bufio.NewReader(conn)
	inbox := fmt.Sprintf("_INBOX.silencefault.%d", os.Getpid())
	if _, err := fmt.Fprintf(conn,
		"CONNECT {\"verbose\":false,\"pedantic\":true}\r\nSUB %s 1\r\nUNSUB 1 1\r\nPUB $JS.API.STREAM.INFO.%s %s 2\r\n{}\r\n",
		inbox, stream, inbox); err != nil {
		return nil, false
	}
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, false
		}
		fields := strings.Fields(line)
		if len(fields) == 0 {
			continue
		}
		switch fields[0] {
		case "PING":
			if _, err := fmt.Fprint(conn, "PONG\r\n"); err != nil {
				return nil, false
			}
		case "MSG":
			if len(fields) < 4 {
				return nil, false
			}
			size, err := strconv.Atoi(fields[len(fields)-1])
			if err != nil || size < 0 || size > 1<<20 {
				return nil, false
			}
			body := make([]byte, size+2)
			if _, err := io.ReadFull(reader, body); err != nil {
				return nil, false
			}
			// Stream statistics carry 64-bit counters, so the probe decodes with
			// UseNumber exactly like the decision-log reader.
			decoder := json.NewDecoder(bytes.NewReader(body[:size]))
			decoder.UseNumber()
			var value map[string]any
			if decoder.Decode(&value) != nil {
				return nil, false
			}
			if problem, present := value["error"]; present && problem != nil {
				return nil, false
			}
			return value, true
		}
	}
}

// silenceFaultFilePartial proves that an incomplete record and the backlog
// behind it never silence a device, and that completing the record grants a
// brand new grace rather than continuing a stale one.
func silenceFaultFilePartial(root, serverBin string) map[string]any {
	duration, gap := int64(2000000), int64(1000000)
	attempt := silenceFaultOpen(root, serverBin, "", []any{[]any{"a"}}, false, 15101, duration, gap, nil)
	defer attempt.close()
	healthy := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Since >= 0 && cut.Ingested == 0
	}, attempt.version)
	require(attempt.capture.receivedRows() == 0, "silence fired before the fault fixture settled")
	require(attempt.cut().Generation == healthy.Generation, "the committed cut changed generation")

	// A half record for an actually received key: the source cannot drain, so
	// no device may be judged silent, not even the never-seen registered one.
	partial := []byte("{\"device_id\":\"b\",\"active\":true")
	silenceFaultAppend(attempt.file, partial)
	broken := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool { return cut.Since == -1 }, attempt.version)
	require(broken.Sequence > healthy.Sequence, "the partial record was not observed as undrainable")
	quiet := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Micros >= broken.Micros+duration+gap
	}, attempt.version)
	require(quiet.Since == -1, "an incomplete record must keep coverage broken")
	require(attempt.capture.receivedRows() == 0, "a partial record or its backlog produced a silence event")

	// Completing the record drains the source and grants a fresh full grace.
	silenceFaultAppend(attempt.file, []byte("}\n"))
	fresh := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Ingested == 1 && cut.Since >= 0
	}, attempt.version)
	require(fresh.Sequence > quiet.Sequence, "completing the record committed nothing new")
	require(fresh.Since > healthy.Since, "the completed record must not continue the broken grace")
	// Once the completed record drains the source, both keys are legitimately
	// due as the new grace elapses: the never-seen registered key and the key
	// that was actually received. Each must report its own truth, and a source
	// reconnect must never be reported as a device recovery.
	registered := silenceFaultWaitSilent(attempt, 0, fresh, "a", 1)
	require(registered.neverSeen && registered.lastSeen == nil,
		"the never-seen registered key must report last_seen=null and never_seen=true")
	observed := silenceFaultWaitSilent(attempt, 1, fresh, "b", 2)
	require(!observed.neverSeen && observed.lastSeen != nil,
		"the received key must report its real last_seen and never_seen=false")
	require(observed.micros >= *observed.lastSeen+attempt.duration,
		"the received key must wait its own full window from its record")
	require(observed.generation == registered.generation && observed.episode == registered.episode,
		"both silence events must share one generation and episode")
	require(observed.micros >= registered.micros, "silence events must follow their deadline order")
	require(len(silenceFaultRows(attempt.capture, "resumed")) == 0,
		"a completed record must never fabricate a resumed event")
	committed := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.NextOutput >= 3
	}, attempt.version)
	require(attempt.capture.receivedRows() == 2, "the fault produced a spurious or extra event")
	require(attempt.capture.rowCount() == 2, "the settled events were not all accepted")
	save(filepath.Join(root, "healthy-cut.json"), healthy)
	save(filepath.Join(root, "broken-cut.json"), broken)
	save(filepath.Join(root, "fresh-grace-cut.json"), fresh)
	save(filepath.Join(root, "committed-cut.json"), committed)
	save(filepath.Join(root, "capture.json"), attempt.capture.snapshot())
	return map[string]any{
		"valid": true, "duration_micros": duration, "gap_micros": gap,
		"partial_quiet_micros": quiet.Micros - broken.Micros, "fresh_since": fresh.Since,
		"registered_silent_micros": registered.micros, "observed_silent_micros": observed.micros,
		"observed_last_seen": *observed.lastSeen, "events": attempt.capture.receivedRows(),
		"journal": silenceFaultJournal(attempt.checkpoint),
	}
}

// silenceFaultFileOneLoss applies one File source-loss mutation to its own
// attempt and requires a source failure with no population silence at all.
func silenceFaultFileOneLoss(root, serverBin, mutation string) map[string]any {
	duration, gap := int64(2000000), int64(1000000)
	attempt := silenceFaultOpen(root, serverBin, "", []any{[]any{"registered"}}, false, 15102, duration, gap, nil)
	defer attempt.close()
	// A committed record gives the cut a real prefix, so a truncation below it
	// is detectable, and a healthy actor would silence the registered key.
	appendSilenceRecord(attempt.file, "b", true)
	healthy := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Ingested >= 1 && cut.Since >= 0
	}, attempt.version)
	require(attempt.capture.receivedRows() == 0, "silence fired before the source fault")
	switch mutation {
	case "delete":
		must(os.Remove(attempt.file))
	case "replace":
		replacement := attempt.file + ".replacement"
		must(os.WriteFile(replacement, nil, 0600))
		must(os.Rename(replacement, attempt.file))
	case "truncate":
		must(os.Truncate(attempt.file, 1))
	default:
		panic("unknown source-loss mutation " + mutation)
	}
	status := attempt.failed("source " + mutation)
	message := strings.ToLower(fmt.Sprint(nested(status, "actual", "last_error")))
	require(silenceFaultSourceError(message),
		fmt.Sprintf("%s was not reported as a source failure: %s", mutation, message))
	before := attempt.cut()
	settled := attempt.settle("source " + mutation)
	require(attempt.capture.receivedRows() == 0, mutation+": the fault produced a population silence")
	require(settled != "running", mutation+": the attempt kept serving with an unusable source")
	require(attempt.cut().Sequence == before.Sequence, mutation+": a failed attempt kept committing decisions")
	save(filepath.Join(root, "healthy-cut.json"), healthy)
	save(filepath.Join(root, "failure-status.json"), status)
	save(filepath.Join(root, "capture.json"), attempt.capture.snapshot())
	return map[string]any{
		"valid": true, "mutation": mutation, "error": message,
		"settled_status": settled, "silent_events": attempt.capture.receivedRows(),
		"journal": silenceFaultJournal(attempt.checkpoint),
	}
}

// silenceFaultFileSourceLoss covers deletion, replacement and truncation of the
// input path on three independent attempts.
func silenceFaultFileSourceLoss(root, serverBin string) map[string]any {
	// The parent root is this function's own; each mutation gets a child that
	// `silenceFaultOpen` creates itself.
	must(os.MkdirAll(root, 0700))
	results := map[string]any{}
	for _, mutation := range []string{"delete", "replace", "truncate"} {
		results[mutation] = silenceFaultFileOneLoss(filepath.Join(root, mutation), serverBin, mutation)
	}
	return map[string]any{"valid": true, "mutations": results}
}

// silenceFaultFileSlowSink proves that a required HTTP acknowledgement that
// outlives the observation gap cannot keep an expired grace alive. Withholding
// the ACK stops the actor from committing any further decision, so past the gap
// the old coverage may no longer authorize anything: a record that arrived
// during the stall has to wait for its own complete new grace.
//
// Data-only decisions carry no output, so a sink response delay cannot stall
// this profile; an unacknowledged required HTTP POST is the real slow-sink
// event and is what this case holds.
func silenceFaultFileSlowSink(root, serverBin string) map[string]any {
	duration, gap := int64(1000000), int64(500000)
	attempt := silenceFaultOpen(root, serverBin, "", []any{[]any{"a"}}, false, 15103, duration, gap,
		func(a *silenceFaultAttempt) {
			// Arm the required-HTTP hold before the pipeline exists, so the
			// registered key's first silence event cannot slip out unheld.
			a.capture.setHold()
		})
	defer attempt.close()
	grace := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Since >= 0 && cut.Ingested == 0
	}, attempt.version)
	require(attempt.capture.receivedRows() == 0, "silence fired before the fault fixture settled")

	// A real record arrives while the first grace is still running, well before
	// the never-seen registered key's own deadline, so the received key's raw
	// deadline is strictly later than the registered one's. The record is input
	// before the hold, not during the stall.
	midpoint := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Micros >= grace.Micros+duration/3
	}, attempt.version)
	silenceFaultAppend(attempt.file, []byte("{\"device_id\":\"b\",\"active\":true}\n"))
	ingested := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Ingested == 1 && cut.Since >= 0 && cut.Sequence > midpoint.Sequence
	}, attempt.version)
	require(attempt.capture.receivedRows() == 0, "the first silence event arrived before the hold was armed")

	// The registered key's own silence event is the held one; the received key
	// keeps waiting, because its own deadline is later. Only the registered key
	// is expected to have an event at this point.
	wait("held silence body", func() bool { return attempt.capture.receivedRows() >= 1 })
	attempt.capture.waitHeld()
	require(attempt.capture.rowCount() == 0, "a withheld silence was counted as accepted")
	held := attempt.capture.snapshot().Received[0]
	require(fmt.Sprint(rowData(held)["device_id"]) == "a",
		"the held event must be the registered never-seen key")
	before := attempt.cut()
	require(number(silenceDecision(attempt.checkpoint)["sequence"]) == before.Sequence+1,
		"the held event has no immediate successor decision")
	currentHash := hash(filepath.Join(attempt.checkpoint, "CURRENT"))
	pendingHash := hash(filepath.Join(attempt.checkpoint, "TIME_PENDING"))
	// Hold longer than the gap and longer than the received key's original
	// deadline, both measured from the decision that ingested it.
	time.Sleep(time.Duration(duration+gap)*time.Microsecond + 500*time.Millisecond)
	require(hash(filepath.Join(attempt.checkpoint, "CURRENT")) == currentHash,
		"a withheld required HTTP advanced CURRENT")
	require(hash(filepath.Join(attempt.checkpoint, "TIME_PENDING")) == pendingHash,
		"a withheld required HTTP changed the pending decision")
	require(attempt.cut().Sequence == before.Sequence, "the stalled attempt committed a decision")
	// The hold exceeds the HTTP request timeout, so retries are legitimate.
	// Every attempt must still be the very same event, never another device.
	for _, retry := range attempt.capture.snapshot().Received {
		require(bytes.Equal(data(retry), data(held)), "the stalled attempt produced another event or changed its identity")
	}
	require(attempt.capture.rowCount() == 0, "the withheld silence became accepted while stalled")

	// Releasing it commits the registered key's event and must force a brand new
	// grace: the stalled interval is credited to nobody, so the received key has
	// to wait its own full window from the new start instead of being emitted
	// for its pre-stall deadline. The registered key stays silent and is not
	// emitted again.
	attempt.capture.releaseHold(false)
	fresh := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Sequence > before.Sequence && cut.Since > before.Since
	}, attempt.version)
	// Polling may first see a later cut in the same new coverage interval;
	// the durable start, not catching one particular tick, is the contract.
	require(fresh.Since <= fresh.Micros, "coverage starts after its own committed cut")
	require(fresh.Since > before.Micros, "the new grace must start after the stalled interval")
	first := parseSilenceRow(held, "silent", fresh.Generation, 1)
	require(first.key == "a" && first.neverSeen && first.lastSeen == nil,
		"the held event must be the registered never-seen key")
	require(first.episode == 1, "the held event must open episode 1")
	var returned map[string]any
	wait("second device after the new full grace", func() bool {
		for _, row := range attempt.capture.snapshot().Received {
			if rowData(row)["device_id"] == "b" {
				returned = row
				return true
			}
		}
		return false
	})
	second := parseSilenceRow(returned, "silent", fresh.Generation, 2)
	require(second.micros >= fresh.Since+duration, "the second device used coverage from before the slow ACK")
	require(!second.neverSeen && second.lastSeen != nil, "the received key must report its real last_seen")
	require(*second.lastSeen >= midpoint.Micros && *second.lastSeen <= ingested.Micros,
		"the second device's last_seen is not its actual pre-stall input")
	require(second.generation == first.generation && second.episode == first.episode,
		"both silence events must share one generation and episode")
	require(len(silenceFaultRows(attempt.capture, "resumed")) == 0,
		"a stalled required HTTP must never fabricate a resumed event")
	committed := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.NextOutput >= 3
	}, attempt.version)
	captured := attempt.capture.snapshot()
	accepted := map[string]bool{}
	for _, row := range captured.Received {
		require(bytes.Equal(data(row), data(held)) || bytes.Equal(data(row), data(returned)),
			"the stalled sink produced an unknown event or changed a retry payload")
	}
	for _, row := range captured.Rows {
		accepted[fmt.Sprint(row["id"])] = true
	}
	require(len(accepted) == 2 && accepted[first.id] && accepted[second.id],
		"the two unique events were not both accepted")
	save(filepath.Join(root, "grace-cut.json"), grace)
	save(filepath.Join(root, "ingested-cut.json"), ingested)
	save(filepath.Join(root, "held-cut.json"), before)
	save(filepath.Join(root, "fresh-grace-cut.json"), fresh)
	save(filepath.Join(root, "committed-cut.json"), committed)
	save(filepath.Join(root, "capture.json"), attempt.capture.snapshot())
	return map[string]any{
		"valid": true, "duration_micros": duration, "gap_micros": gap,
		"held_wall_micros":          (duration + gap) + 500000,
		"stalled_committed_nothing": true, "fresh_since": fresh.Since,
		"registered_silent_micros": first.micros, "observed_silent_micros": second.micros,
		"observed_last_seen": *second.lastSeen, "unique_events": len(accepted),
		"transport_rows": captured.ReceivedRows, "identical_retries": captured.ReceivedRows - 2,
		"journal": silenceFaultJournal(attempt.checkpoint),
	}
}

// silenceFaultJetStreamOutage stops only this fixture's own broker, requires the
// job to fail without any population silence, then restores the same broker
// storage and starts the job again: the device needs a complete new grace and
// must never be reported as resumed by a reconnect.
func silenceFaultJetStreamOutage(root, serverBin, natsBin string) map[string]any {
	duration, gap := int64(3000000), int64(1000000)
	attempt := silenceFaultOpen(root, serverBin, natsBin, []any{[]any{"a"}}, true, 15104, duration, gap, nil)
	defer attempt.close()
	healthy := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Since >= 0 && cut.Ingested == 0
	}, attempt.version)
	require(attempt.capture.receivedRows() == 0, "silence fired before the broker outage")
	generation := healthy.Generation

	// Only this fixture's broker process dies; every other service is untouched.
	silenceStop(&attempt.broker, syscall.SIGKILL)
	brokerKilled := true
	if attempt.producer != nil {
		attempt.producer.close()
		attempt.producer = nil
	}
	status := attempt.failed("broker outage")
	message := strings.ToLower(fmt.Sprint(nested(status, "actual", "last_error")))
	require(strings.TrimSpace(message) != "", "a broker outage failed without an error")
	before := attempt.cut()
	settled := attempt.settle("broker outage")
	require(settled != "running", "the attempt kept serving without its broker")
	require(attempt.capture.receivedRows() == 0, "a broker outage produced a population silence")
	require(attempt.cut().Sequence == before.Sequence, "a failed attempt kept committing decisions")

	// Restore the very same broker storage on the same port and start by hand.
	attempt.broker = launch(natsBin, root, filepath.Join(root, "nats.log"), "-c", filepath.Join(root, "nats.conf"))
	wait("JetStream broker TCP", func() bool { return silenceFaultBrokerUp(attempt.brokerPort) })
	var stream map[string]any
	wait("JetStream store restored", func() bool {
		info, ready := silenceFaultStreamInfo(attempt.brokerPort, "INPUT")
		stream = info
		return ready
	})
	save(filepath.Join(root, "restored-stream.json"), stream)
	attempt.resume()
	restarted := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Sequence > before.Sequence && cut.Since >= 0 && cut.Since > healthy.Since
	}, attempt.version)
	require(restarted.Generation == generation, "the restored attempt changed the state generation")
	silent := silenceFaultWaitSilent(attempt, 0, restarted, "a", 1)
	require(silent.neverSeen && silent.lastSeen == nil,
		"the never-seen registered key must report last_seen=null and never_seen=true")
	require(len(silenceFaultRows(attempt.capture, "resumed")) == 0,
		"a source reconnect must never fabricate a resumed record")
	require(attempt.capture.receivedRows() == 1, "the outage or its recovery produced an extra event")
	save(filepath.Join(root, "healthy-cut.json"), healthy)
	save(filepath.Join(root, "failure-status.json"), status)
	save(filepath.Join(root, "restarted-cut.json"), restarted)
	save(filepath.Join(root, "capture.json"), attempt.capture.snapshot())
	return map[string]any{
		"valid": true, "duration_micros": duration, "gap_micros": gap,
		"own_broker_sigkill": brokerKilled, "settled_status": settled,
		"broker_error": message, "restart_since": restarted.Since, "silent_micros": silent.micros,
		"journal": silenceFaultJournal(attempt.checkpoint),
	}
}

// silenceFaultJetStreamConsumer removes this fixture's own consumer and requires
// the job to fail rather than silently judging devices from an broken reader.
// The shared broker service and other consumers are never modified.
func silenceFaultJetStreamConsumer(root, serverBin, natsBin string) map[string]any {
	duration, gap := int64(3000000), int64(1000000)
	attempt := silenceFaultOpen(root, serverBin, natsBin, []any{[]any{"a"}}, true, 15105, duration, gap, nil)
	defer attempt.close()
	healthy := waitSilenceCut(attempt.checkpoint, func(cut silenceCut) bool {
		return cut.Since >= 0 && cut.Ingested == 0
	}, attempt.version)
	generation := healthy.Generation

	name := readerName(attempt.producer)
	response := attempt.producer.request("$JS.API.CONSUMER.DELETE.INPUT."+name, []byte("{}"))
	require(response["success"] == true, fmt.Sprintf("consumer delete was not acknowledged: %v", response))
	status := attempt.failed("consumer removal")
	message := strings.ToLower(fmt.Sprint(nested(status, "actual", "last_error")))
	require(strings.TrimSpace(message) != "", "consumer removal failed without an error")
	before := attempt.cut()
	settled := attempt.settle("consumer removal")
	require(settled != "running", "the attempt kept serving without its own consumer")
	require(attempt.capture.receivedRows() == 0, "removing the job's own consumer produced a population silence")
	require(attempt.cut().Sequence == before.Sequence, "a failed attempt kept committing decisions")
	require(silenceGeneration(attempt.checkpoint) == generation, "the failure rotated the state generation")
	save(filepath.Join(root, "healthy-cut.json"), healthy)
	save(filepath.Join(root, "consumer-delete.json"), map[string]any{"consumer": name, "response": response})
	save(filepath.Join(root, "failure-status.json"), status)
	save(filepath.Join(root, "capture.json"), attempt.capture.snapshot())
	return map[string]any{
		"valid": true, "duration_micros": duration, "gap_micros": gap,
		"consumer": name, "settled_status": settled, "consumer_error": message,
		"shared_broker_touched": false, "journal": silenceFaultJournal(attempt.checkpoint),
	}
}

// runSilenceFaultMatrix runs the File source faults, the slow required sink, and
// (unless fileOnly) the two JetStream source faults that only touch this
// fixture's own broker and consumer.
func runSilenceFaultMatrix(root, serverBin, natsBin string, fileOnly bool) {
	cases := map[string]any{}
	cases["file-partial-and-complete"] = silenceFaultFilePartial(filepath.Join(root, "file-partial"), serverBin)
	cases["file-source-loss"] = silenceFaultFileSourceLoss(filepath.Join(root, "file-source-loss"), serverBin)
	cases["file-slow-required-http"] = silenceFaultFileSlowSink(filepath.Join(root, "file-slow-http"), serverBin)
	brokerOutage := false
	if !fileOnly {
		brokerOutageCase := silenceFaultJetStreamOutage(filepath.Join(root, "jetstream-broker-outage"), serverBin, natsBin)
		cases["jetstream-broker-outage"] = brokerOutageCase
		cases["jetstream-own-consumer-removed"] = silenceFaultJetStreamConsumer(filepath.Join(root, "jetstream-consumer"), serverBin, natsBin)
		brokerOutage = brokerOutageCase["valid"] == true && brokerOutageCase["own_broker_sigkill"] == true
	}
	valid := true
	for _, value := range cases {
		if value.(map[string]any)["valid"] != true {
			valid = false
		}
	}
	save(filepath.Join(root, "summary.json"), map[string]any{
		"valid": valid, "fault_cases": len(cases), "cases": cases, "file_only": fileOnly,
		"broker_outage_tested": brokerOutage, "shared_broker_touched": false,
		"sparrow_process_sigkill": false,
		// This file never replays an uncommitted decision, so it must not claim
		// that a source observation was replayed across a crash.
		"source_health_replayed": false, "soak": false,
		"exactly_once_claimed": false, "certified": false,
	})
	fmt.Println("SILENCE_FAULT_PROCESS_OK")
}
