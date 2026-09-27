package main

// Bounded, real-service capacity observations. A correct finite drain is NOT
// a sustained-rate pass. No engine budget, ACK policy or broker durability is
// weakened. Only explicitly owned processes and directories are touched.
import (
	"bufio"
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type capacityCase struct {
	Name       string  `json:"name"`
	Source     string  `json:"source"`
	Sink       string  `json:"sink"`
	Rows       int     `json:"rows"`
	Rate       int     `json:"rate"`
	Jobs       int     `json:"jobs"`
	Window     int     `json:"window"`
	Padding    int     `json:"padding"`
	DelayMS    int     `json:"delay_ms"`
	RecoveryMS int     `json:"recovery_ms"`
	IdleMS     int     `json:"idle_ms"`
	IdleCapMS  int     `json:"idle_cap_ms"`
	Reject     bool    `json:"expect_rejection"`
	P99MS      float64 `json:"p99_target_ms"`
}
type capacityPlan struct {
	Version int            `json:"version"`
	Cases   []capacityCase `json:"cases"`
}
type capacityOutput struct {
	Value  int64
	ID     string
	At     time.Time
	Device string
}
type capacityCapture struct {
	mu          sync.Mutex
	server      *http.Server
	listener    net.Listener
	rows        []capacityOutput
	max         int
	bad         int
	requests    int
	connections int
	delay       atomic.Int64
}

func capacityReceiver(max, delay int) *capacityCapture {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	c := &capacityCapture{listener: listener, max: max}
	c.delay.Store(int64(delay))
	c.server = &http.Server{ReadHeaderTimeout: 2 * time.Second, ConnState: func(_ net.Conn, s http.ConnState) {
		if s == http.StateNew {
			c.mu.Lock()
			c.connections++
			c.mu.Unlock()
		}
	}, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(io.LimitReader(r.Body, maxHTTPBody+1))
		_ = r.Body.Close()
		at := time.Now()
		var rows []map[string]any
		if err != nil || len(body) > maxHTTPBody || json.Unmarshal(body, &rows) != nil {
			c.mu.Lock()
			c.bad++
			c.mu.Unlock()
			w.WriteHeader(400)
			return
		}
		c.mu.Lock()
		c.requests++
		if len(c.rows)+len(rows) > c.max+128 {
			c.bad++
			c.mu.Unlock()
			w.WriteHeader(503)
			return
		}
		for _, row := range rows {
			value := liveData(row)
			field := "v"
			if _, ok := value["s"]; ok {
				field = "s"
			}
			n, ok := value[field].(float64)
			if !ok || n != float64(int64(n)) {
				c.bad++
			}
			id, _ := row["id"].(string)
			device, _ := value["device_id"].(string)
			c.rows = append(c.rows, capacityOutput{Value: int64(n), ID: id, At: at, Device: device})
		}
		c.mu.Unlock()
		if delay := c.delay.Load(); delay > 0 {
			select {
			case <-time.After(time.Duration(delay) * time.Millisecond):
			case <-r.Context().Done():
				return
			}
		}
		w.WriteHeader(204)
	})}
	go func() {
		if err := c.server.Serve(listener); err != nil && err != http.ErrServerClosed {
			panic(err)
		}
	}()
	return c
}
func (c *capacityCapture) count() int  { c.mu.Lock(); defer c.mu.Unlock(); return len(c.rows) }
func (c *capacityCapture) url() string { return "http://" + c.listener.Addr().String() + "/out" }

func capacityResources(pid int) map[string]any {
	result := map[string]any{}
	raw, err := os.ReadFile(fmt.Sprintf("/proc/%d/status", pid))
	if err != nil {
		return result
	}
	for _, line := range strings.Split(string(raw), "\n") {
		for _, key := range []string{"VmRSS:", "VmHWM:", "Threads:"} {
			if strings.HasPrefix(line, key) {
				parts := strings.Fields(line)
				if len(parts) > 1 {
					n, _ := strconv.ParseUint(parts[1], 10, 64)
					result[strings.TrimSuffix(key, ":")] = n
				}
			}
		}
	}
	raw, err = os.ReadFile(fmt.Sprintf("/proc/%d/stat", pid))
	if err == nil {
		end := bytes.LastIndexByte(raw, ')')
		if end >= 0 {
			parts := strings.Fields(string(raw[end+1:]))
			if len(parts) > 12 {
				u, _ := strconv.ParseUint(parts[11], 10, 64)
				s, _ := strconv.ParseUint(parts[12], 10, 64)
				result["cpu_ticks"] = u + s
			}
		}
	}
	return result
}
func runCapacityPlan(root, binary, natsBin, planPath, selected string) {
	bytes, err := os.ReadFile(planPath)
	must(err)
	require(len(bytes) <= 65536, "capacity plan bound")
	var plan capacityPlan
	decoder := json.NewDecoder(strings.NewReader(string(bytes)))
	decoder.DisallowUnknownFields()
	must(decoder.Decode(&plan))
	var trailing any
	require(decoder.Decode(&trailing) == io.EOF, "capacity plan trailing JSON")
	require(plan.Version == 1 && len(plan.Cases) > 0 && len(plan.Cases) <= 48, "capacity plan version/count")
	names := map[string]bool{}
	var chosen []capacityCase
	for _, c := range plan.Cases {
		require(c.Name != "" && !strings.ContainsAny(c.Name, "/\\.") && !names[c.Name], "capacity case names must be unique simple names")
		names[c.Name] = true
		require(c.Source == "jetstream" || c.Source == "file" || c.Source == "http_push", "capacity source")
		require(c.Sink == "http" || c.Sink == "file", "capacity sink")
		require(c.Source != "jetstream" || c.Sink == "http", "JetStream File Sink unsupported")
		require(c.Jobs >= 1 && c.Jobs <= 4 && c.Rows >= 1 && c.Rows <= 200000 && c.Rate >= 0 && c.Rate <= 20000 && c.Padding >= 0 && c.Padding <= 32768, "capacity case bounds")
		require((c.Window == 1 || c.Window == 3) && c.Rows%c.Window == 0 && c.DelayMS >= 0 && c.DelayMS <= 700 && c.IdleMS >= 0 && c.IdleMS <= 1500 && c.RecoveryMS >= 0 && c.RecoveryMS <= 10000, "capacity case timing/window")
		require(c.IdleCapMS == 0 || (c.Source == "jetstream" && c.IdleCapMS >= 5 && c.IdleCapMS <= 250), "capacity idle tuning")
		require(c.P99MS > 0 && c.P99MS <= 10000, "capacity latency target must be declared")
		require(!c.Reject || (c.Source == "jetstream" && c.Rows == 1 && c.Jobs == 1), "rejection oracle requires one JetStream row")
		require(c.Padding == 0 || c.Rows*c.Jobs <= 10000, "large-payload fixture must remain bounded")
		if selected == "" || selected == c.Name {
			chosen = append(chosen, c)
		}
	}
	require(len(chosen) > 0, "no matching capacity cases")
	root, err = filepath.Abs(root)
	must(err)
	must(os.Mkdir(root, 0700))
	self, err := os.Executable()
	must(err)
	save(filepath.Join(root, "plan.json"), plan)
	save(filepath.Join(root, "binaries.json"), map[string]any{"server_sha256": hash(binary), "nats_sha256": hash(natsBin), "driver_sha256": hash(self), "gate_replacement": false})
	summaries := []map[string]any{}
	for _, c := range chosen {
		summaries = append(summaries, runCapacityCase(filepath.Join(root, c.Name), binary, natsBin, c))
		save(filepath.Join(root, "results.json"), summaries)
	}
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "cases": len(chosen), "capacity_is_per_case": true, "soak": false, "certified": false})
	fmt.Println("CAPACITY_PROCESS_OK")
}
func runCapacityCase(root, binary, natsBin string, c capacityCase) map[string]any {
	must(os.Mkdir(root, 0700))
	save(filepath.Join(root, "case.json"), c)
	var broker *child
	var ctl *nats
	np := 0
	var nmu sync.Mutex
	if c.Source == "jetstream" {
		np = freePort()
		config := fmt.Sprintf("host: 127.0.0.1\nport: %d\nmax_payload: 65536\njetstream {\nstore_dir: %q\nmax_file_store: 512MB\nmax_memory_store: 16MB\nsync_interval: always\n}\n", np, filepath.Join(root, "broker-data"))
		must(os.WriteFile(filepath.Join(root, "nats.conf"), []byte(config), 0600))
		broker = launch(natsBin, root, filepath.Join(root, "broker.log"), "-c", filepath.Join(root, "nats.conf"))
		defer broker.stop(syscall.SIGTERM)
		ctl = dialNATS(np)
		defer ctl.close()
		ctl.request("$JS.API.STREAM.CREATE.KV_OWNERS", data(map[string]any{"name": "KV_OWNERS", "subjects": []string{"$KV.OWNERS.>"}, "storage": "file", "retention": "limits", "max_bytes": 1048576, "max_msg_size": 1024, "max_msgs_per_subject": 1, "num_replicas": 1, "discard": "new", "allow_direct": true, "allow_rollup_hdrs": true}))
	}
	apiPort := freePort()
	a := newAPI(apiPort)
	process := launch(binary, root, filepath.Join(root, "server.log"), "--bind", fmt.Sprintf("127.0.0.1:%d", apiPort), "--catalog", filepath.Join(root, "catalog.db"), "--max-jobs", strconv.Itoa(c.Jobs), "--safe-mode")
	defer process.stop(syscall.SIGKILL)
	waitHealth(a, "capacity server")
	if np > 0 {
		a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": np})
	}
	a.ok(http.MethodPut, "/v1/streams/capacity", map[string]any{"fields": []any{map[string]any{"name": "device_id", "type": "utf8", "nullable": false}, map[string]any{"name": "v", "type": "int64", "nullable": false}, map[string]any{"name": "pad", "type": "utf8", "nullable": false}}})
	type job struct {
		name, stream, input, output, push string
		capture                           *capacityCapture
		writer                            *nats
		sent                              atomic.Uint64
		writeTimes                        []time.Time
		publishStart, publishEnd          time.Time
		spec                              map[string]any
	}
	jobs := make([]*job, c.Jobs)
	for j := range jobs {
		v := &job{name: fmt.Sprintf("job%d", j), stream: fmt.Sprintf("INPUT%d", j), input: filepath.Join(root, fmt.Sprintf("input%d.ndjson", j)), output: filepath.Join(root, fmt.Sprintf("output%d", j)), writeTimes: make([]time.Time, c.Rows)}
		jobs[j] = v
		source := map[string]any{"kind": c.Source, "inbox_capacity": 16}
		sink := map[string]any{"kind": c.Sink}
		if c.Source == "jetstream" {
			ctl.request("$JS.API.STREAM.CREATE."+v.stream, data(map[string]any{"name": v.stream, "subjects": []string{"input." + v.name}, "storage": "file", "retention": "limits", "max_bytes": 128 * 1024 * 1024, "max_msg_size": 65536, "max_consumers": 32, "num_replicas": 1, "deny_delete": true, "deny_purge": true}))
			js := map[string]any{"servers": []string{fmt.Sprintf("nats://127.0.0.1:%d", np)}, "namespace": "capacity", "stream": v.stream, "consumer": v.name, "ownership_bucket": "OWNERS", "max_pending": 128, "pending_bytes": 262144, "pull_messages": 8, "pull_bytes": 73728}
			if c.IdleCapMS != 0 {
				js["idle_backoff_max_ms"] = c.IdleCapMS
			}
			source["jetstream"] = js
			v.writer = dialNATS(np)
			defer v.writer.close()
		} else if c.Source == "file" {
			source["path"] = v.input
			source["file_contract"] = "sealed"
			file, err := os.Create(v.input)
			must(err)
			buffer := bufio.NewWriter(file)
			for i := 0; i < c.Rows; i++ {
				_, err = buffer.Write(append(data(map[string]any{"device_id": v.name, "v": i + 1, "pad": strings.Repeat("x", c.Padding)}), '\n'))
				must(err)
			}
			must(buffer.Flush())
			must(file.Close())
		} else {
			p := freePort()
			source["bind"] = fmt.Sprintf("127.0.0.1:%d", p)
			source["path"] = "/push"
			v.push = fmt.Sprintf("http://127.0.0.1:%d/push", p)
			a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": p})
		}
		if c.Sink == "http" {
			v.capture = capacityReceiver(c.Rows/c.Window, c.DelayMS)
			defer v.capture.server.Close()
			sink["url"] = v.capture.url()
			sink["batch_rows"] = 64
			sink["batch_bytes"] = 262144
			sink["linger_ms"] = 5
			sink["max_inflight"] = 1
			a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": "127.0.0.1", "port": v.capture.listener.Addr().(*net.TCPAddr).Port})
		} else {
			must(os.Mkdir(v.output, 0700))
			sink["file"] = map[string]any{"directory": v.output, "segment_bytes": 1048576, "max_bytes": 16777216, "max_files": 32, "row_bytes": 65536, "sync_data": true}
		}
		sql := "SELECT device_id, v FROM capacity"
		if c.Window == 3 {
			sql = "SELECT SUM(v) AS s FROM capacity GROUP BY COUNT_WINDOW(3)"
		}
		spec := map[string]any{"version": 1, "stream": "capacity", "sql": sql, "source": source, "sink": sink, "recovery": "restart_fresh", "fail_on_decode": true}
		if c.Source == "jetstream" {
			spec["delivery"] = "checkpointed_at_least_once"
			spec["recovery"] = "aligned"
			spec["checkpoint_dir"] = filepath.Join(root, "checkpoint-"+v.name)
			spec["checkpoint"] = map[string]any{"interval_ms": 1000, "timeout_ms": 5000, "resume_latest": true}
		}
		v.spec = spec
		save(filepath.Join(root, v.name+"-spec.json"), spec)
		a.ok(http.MethodPost, "/v1/validate", spec)
		a.ok(http.MethodPut, "/v1/pipelines/"+v.name, spec)
	}
	begin := time.Now()
	for _, v := range jobs {
		a.ok(http.MethodPost, "/v1/pipelines/"+v.name+"/start", map[string]any{})
	}
	if c.Source != "file" {
		for _, v := range jobs {
			waitRunning(a, v.name)
			if c.Source == "jetstream" {
				wait("capacity bootstrap", func() bool { return number(nested(statusOf(a, v.name), "checkpoint", "last_success_id")) >= 1 })
				idleMax := c.IdleCapMS
				if idleMax == 0 {
					idleMax = 250
				}
				require(number(nested(statusOf(a, v.name), "effective", "jetstream_execution", "idle_backoff_ms", "maximum")) == uint64(idleMax), "effective idle diagnostic differs from accepted configuration")
			}
		}
	}
	idleBefore := mqttMetrics(a)
	beforeResources := capacityResources(process.cmd.Process.Pid)
	// Sample only owned processes and API status. Concurrent samples are
	// observational (not an atomic cross-system ACK/cut proof).
	stopSamples := make(chan struct{})
	samplesDone := make(chan struct{})
	sampleErrors := make(chan any, 1)
	var sampleRows []map[string]any
	go func() {
		defer close(samplesDone)
		defer func() {
			if err := recover(); err != nil {
				sampleErrors <- err
			}
		}()
		timer := time.NewTicker(200 * time.Millisecond)
		defer timer.Stop()
		for {
			select {
			case <-stopSamples:
				return
			case <-timer.C:
				sample := map[string]any{"elapsed_ms": time.Since(begin).Milliseconds(), "resources": capacityResources(process.cmd.Process.Pid)}
				values := []any{}
				for _, v := range jobs {
					status, code := a.call(http.MethodGet, "/v1/pipelines/"+v.name+"/status", nil)
					r := map[string]any{"name": v.name, "sent": v.sent.Load(), "status_code": code, "status": status}
					if v.capture != nil {
						r["arrived"] = v.capture.count()
					}
					if ctl != nil {
						info := func() map[string]any {
							nmu.Lock()
							defer nmu.Unlock()
							return ctl.request("$JS.API.STREAM.INFO."+v.stream, []byte("{}"))
						}()
						r["persisted"] = nested(info, "state", "messages")
					}
					values = append(values, r)
				}
				sample["jobs"] = values
				if len(sampleRows) < 1000 {
					sampleRows = append(sampleRows, sample)
				}
			}
		}
	}()
	var closeSamples sync.Once
	finishSamples := func() {
		closeSamples.Do(func() { close(stopSamples); <-samplesDone; save(filepath.Join(root, "samples.json"), sampleRows) })
		select {
		case err := <-sampleErrors:
			panic(err)
		default:
		}
	}
	defer finishSamples()
	if c.RecoveryMS > 0 {
		go func() {
			select {
			case <-time.After(time.Duration(c.RecoveryMS) * time.Millisecond):
				for _, v := range jobs {
					if v.capture != nil {
						v.capture.delay.Store(0)
					}
				}
			case <-stopSamples:
			}
		}()
	}
	publish := func(v *job) {
		v.publishStart = time.Now()
		defer func() { v.publishEnd = time.Now() }()
		if c.Source == "file" {
			v.sent.Store(uint64(c.Rows))
			return
		}
		client := &http.Client{Timeout: 5 * time.Second}
		defer client.CloseIdleConnections()
		for i := 0; i < c.Rows; i++ {
			if c.IdleMS > 0 {
				time.Sleep(time.Duration(c.IdleMS) * time.Millisecond)
			} else if c.Rate > 0 {
				if wait := time.Until(v.publishStart.Add(time.Duration(float64(i) / float64(c.Rate) * float64(time.Second)))); wait > 0 {
					time.Sleep(wait)
				}
			}
			body := data(map[string]any{"device_id": v.name, "v": i + 1, "pad": strings.Repeat("x", c.Padding)})
			v.writeTimes[i] = time.Now()
			if c.Source == "jetstream" {
				must(v.writer.conn.SetWriteDeadline(time.Now().Add(5 * time.Second)))
				_, err := fmt.Fprintf(v.writer.conn, "PUB input.%s %d\r\n%s\r\n", v.name, len(body), body)
				must(err)
			} else {
				response, err := client.Post(v.push, "application/json", bytes.NewReader(body))
				must(err)
				_, err = io.Copy(io.Discard, response.Body)
				must(err)
				must(response.Body.Close())
				require(response.StatusCode == 202, "HTTP push refused a paced input; keep failed evidence")
			}
			v.sent.Add(1)
		}
	}
	var publishers sync.WaitGroup
	publishErrors := make(chan any, len(jobs))
	for _, v := range jobs {
		publishers.Add(1)
		go func(v *job) {
			defer publishers.Done()
			defer func() {
				if err := recover(); err != nil {
					publishErrors <- err
				}
			}()
			publish(v)
		}(v)
	}
	publishers.Wait()
	close(publishErrors)
	for err := range publishErrors {
		panic(err) // unwind the owner goroutine too, stopping its server/broker
	}
	publishedAt := time.Now()
	if ctl != nil {
		for _, v := range jobs {
			wait("capacity persistence fence", func() bool {
				nmu.Lock()
				defer nmu.Unlock()
				info := ctl.request("$JS.API.STREAM.INFO."+v.stream, []byte("{}"))
				return number(nested(info, "state", "messages")) == uint64(c.Rows)
			})
		}
	}
	if c.Reject {
		status := waitActual(a, jobs[0].name, "failed")
		message, _ := nested(status, "actual", "last_error").(string)
		require(strings.Contains(message, "JetStream decode failed at source_sequence=1 (resource_exhausted)"), "payload rejection did not reach the expected decode quota")
		if _, diagnosticAPI := idleBefore["process_credits"]; diagnosticAPI {
			for _, key := range []string{"credit=reservation", "used=", "request=", "cap="} {
				require(strings.Contains(message, key), "payload rejection lost safe quota diagnostic: "+key)
			}
		}
		require(jobs[0].capture.count() == 0, "rejected payload emitted output")
		nmu.Lock()
		info := ctl.request("$JS.API.CONSUMER.LIST."+jobs[0].stream, []byte("{}"))
		nmu.Unlock()
		consumers, ok := info["consumers"].([]any)
		require(ok && len(consumers) == 1, "rejection consumer evidence missing")
		require(number(nested(consumers[0].(map[string]any), "ack_floor", "stream_seq")) == 0, "rejected input was ACKed")
		save(filepath.Join(root, "rejection-status.json"), status)
		save(filepath.Join(root, "consumer-rejection.json"), info)
		finishSamples()
		stopPipeline(a, jobs[0].name)
		stopped := mqttMetrics(a)
		verified := capacityStopped(stopped)
		save(filepath.Join(root, "metrics-stopped.json"), stopped)
		result := map[string]any{"name": c.Name, "valid": true, "expected_rejection": true, "output_rows": 0, "ack_floor": 0, "latency_pass": false, "credits_verified": verified, "certified": false}
		save(filepath.Join(root, "summary.json"), result)
		return result
	}
	deadline := time.Now().Add(60 * time.Second)
	for _, v := range jobs {
		for {
			status := statusOf(a, v.name)
			require(nested(status, "actual", "status") != "failed", fmt.Sprintf("capacity job failed: %v", status))
			done := v.capture != nil && v.capture.count() >= c.Rows/c.Window
			if c.Sink == "file" {
				done = nested(status, "actual", "status") == "completed"
			}
			if done {
				break
			}
			require(time.Now().Before(deadline), "capacity drain exceeded 60 seconds")
			time.Sleep(10 * time.Millisecond)
		}
	}
	outputDone := time.Now()
	finalAckDone := outputDone
	var results []map[string]any
	for _, v := range jobs {
		if c.Source == "jetstream" {
			for {
				_, code := a.call(http.MethodPost, "/v1/pipelines/"+v.name+"/checkpoint", map[string]any{})
				if code >= 200 && code < 300 {
					break
				}
				require(time.Now().Before(deadline), "capacity final checkpoint")
				time.Sleep(10 * time.Millisecond)
			}
			wait("capacity committed and ACKed", func() bool {
				s := statusOf(a, v.name)
				return number(nested(s, "checkpoint", "reliable_source", "committed_cut")) == uint64(c.Rows) && number(nested(s, "checkpoint", "reliable_source", "pending_messages")) == 0
			})
			nmu.Lock()
			info := ctl.request("$JS.API.CONSUMER.LIST."+v.stream, []byte("{}"))
			nmu.Unlock()
			consumers := info["consumers"].([]any)
			require(len(consumers) == 1, "capacity consumer count")
			require(number(nested(consumers[0].(map[string]any), "ack_floor", "stream_seq")) == uint64(c.Rows), "broker ACK floor differs from final durable cut")
			finalAckDone = time.Now()
			save(filepath.Join(root, v.name+"-consumer.json"), info)
		}
		var rows []capacityOutput
		if v.capture != nil {
			v.capture.mu.Lock()
			rows = append(rows, v.capture.rows...)
			bad := v.capture.bad
			v.capture.mu.Unlock()
			require(bad == 0, "capacity malformed capture")
		} else {
			_, rawRows := actionsSegments(v.output)
			for _, row := range rawRows {
				var value map[string]any
				must(json.Unmarshal(row, &value))
				field := "v"
				if c.Window == 3 {
					field = "s"
				}
				device, _ := value["device_id"].(string)
				rows = append(rows, capacityOutput{Value: int64(value[field].(float64)), Device: device})
			}
		}
		require(len(rows) == c.Rows/c.Window, "capacity output count mismatch")
		ids := map[string]bool{}
		latencies := make([]float64, 0, len(rows))
		for index, row := range rows {
			seq := (index + 1) * c.Window
			want := int64(seq)
			if c.Window == 3 {
				want = int64(3*seq - 3)
			}
			require(row.Value == want, "capacity independent numeric/order oracle")
			if c.Window == 1 {
				require(row.Device == v.name, "capacity source/job identity mixed")
			}
			if c.Source == "jetstream" {
				raw, err := hex.DecodeString(row.ID)
				require(err == nil && len(raw) == 24 && !ids[row.ID], "capacity invalid/duplicate reliable output ID")
				ids[row.ID] = true
			}
			if c.Source != "file" && v.capture != nil {
				latencies = append(latencies, float64(row.At.Sub(v.writeTimes[seq-1]).Microseconds())/1000)
			}
		}
		sort.Float64s(latencies)
		rank := func(p int) int { return (len(latencies)*p+99)/100 - 1 } // empirical nearest-rank, including max for small-N p99
		percentile := func(p int) any {
			if len(latencies) == 0 {
				return nil
			}
			return latencies[rank(p)]
		}
		rate := float64(c.Rows) / v.publishEnd.Sub(v.publishStart).Seconds()
		if c.Source == "file" {
			rate = 0
		} // no producer timing claim for preloaded input
		rateMet := c.Rate > 0 && rate >= float64(c.Rate)*.95
		latencyMet := len(latencies) > 0 && latencies[rank(99)] <= c.P99MS
		elapsed := outputDone.Sub(begin).Seconds()
		if c.Source == "file" && v.capture != nil {
			elapsed = rows[len(rows)-1].At.Sub(begin).Seconds()
		}
		if c.Source != "file" {
			elapsed = rows[len(rows)-1].At.Sub(v.publishStart).Seconds()
		}
		jobResult := map[string]any{"job": v.name, "rows": c.Rows, "outputs": len(rows), "actual_publish_rate": rate, "requested_rate": c.Rate, "offered_rate_met": rateMet, "p50_ms": percentile(50), "p95_ms": percentile(95), "p99_ms": percentile(99), "p99_target_ms": c.P99MS, "latency_target_met": latencyMet, "finite_input_rows_per_s": float64(c.Rows) / elapsed, "elapsed_s": elapsed, "sustained_target_met": rateMet && latencyMet && c.IdleMS == 0 && c.RecoveryMS == 0}
		if v.capture != nil {
			v.capture.mu.Lock()
			jobResult["http_requests"] = v.capture.requests
			jobResult["http_connections"] = v.capture.connections
			v.capture.mu.Unlock()
		}
		results = append(results, jobResult)
		save(filepath.Join(root, v.name+"-final-status.json"), statusOf(a, v.name))
	}
	finishSamples()
	save(filepath.Join(root, "metrics-after.json"), mqttMetrics(a))
	save(filepath.Join(root, "metrics-before.json"), idleBefore)
	beforeStop := capacityResources(process.cmd.Process.Pid)
	for _, v := range jobs {
		stopPipeline(a, v.name)
	}
	stopped := mqttMetrics(a)
	save(filepath.Join(root, "metrics-stopped.json"), stopped)
	verified := capacityStopped(stopped)
	result := map[string]any{"name": c.Name, "valid": true, "source": c.Source, "sink": c.Sink, "jobs": results, "scope": "finite_loopback_default_4MiB_no_TLS_no_soak; File excludes preload; paced includes client/broker costs; capture arrival precedes response delay", "drain_after_publish_ms": outputDone.Sub(publishedAt).Milliseconds(), "total_through_final_ack_ms": finalAckDone.Sub(begin).Milliseconds(), "total_through_stop_ms": time.Since(begin).Milliseconds(), "resources_before": beforeResources, "resources_end": beforeStop, "resources_stopped": capacityResources(process.cmd.Process.Pid), "samples": len(sampleRows), "certified": false}
	result["credits_verified"] = verified
	save(filepath.Join(root, "summary.json"), result)
	process.stop(syscall.SIGTERM)
	return result
}

func capacityStopped(stopped map[string]any) bool {
	observed, ok := nested(stopped, "observations", "jobs").([]any)
	require(ok && len(observed) == 0, "capacity stop left active observations")
	raw, exists := stopped["process_credits"]
	if !exists {
		return false
	} // baseline API predates this diagnostic
	credits, ok := raw.(map[string]any)
	require(ok, "invalid process credit diagnostics")
	for _, key := range []string{"physical_bytes", "live_handles", "reservation_bytes", "retention_bytes", "queue_bytes"} {
		value, ok := credits[key].(float64)
		require(ok && value == 0, "capacity stop left tracked credits: "+key)
	}
	return true
}
