package main

// The MQTT publisher below is a tiny independent wire fixture, not Sparrow's
// connector. Mosquitto is an isolated, pinned Docker fixture created by the
// validation script. Only that named fixture is stopped/restarted.
import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"syscall"
	"time"
)

func mqttString(text string) []byte {
	require(len(text) <= 65535, "MQTT string bound")
	result := make([]byte, 2, len(text)+2)
	binary.BigEndian.PutUint16(result, uint16(len(text)))
	return append(result, []byte(text)...)
}
func mqttPacket(conn net.Conn, header byte, payload []byte) {
	require(len(payload) < 65536, "MQTT fixture frame bound")
	frame := []byte{header}
	n := len(payload)
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			b |= 128
		}
		frame = append(frame, b)
		if n == 0 {
			break
		}
	}
	frame = append(frame, payload...)
	must(conn.SetWriteDeadline(time.Now().Add(2 * time.Second)))
	_, err := io.Copy(conn, bytes.NewReader(frame))
	must(err)
}
func mqttPublisher(address string) net.Conn {
	conn, err := net.DialTimeout("tcp", address, 2*time.Second)
	must(err)
	payload := append(mqttString("MQTT"), 4, 2, 0, 30)
	payload = append(payload, mqttString(fmt.Sprintf("live-oracle-%d", time.Now().UnixNano()))...)
	mqttPacket(conn, 0x10, payload)
	must(conn.SetReadDeadline(time.Now().Add(2 * time.Second)))
	response := make([]byte, 4)
	_, err = io.ReadFull(conn, response)
	must(err)
	require(bytes.Equal(response, []byte{0x20, 2, 0, 0}), "Mosquitto CONNACK")
	return conn
}
func mqttPublish(conn net.Conn, topic string, value []byte, retained bool) {
	header := byte(0x30)
	if retained {
		header |= 1
	}
	mqttPacket(conn, header, append(mqttString(topic), value...))
}
func liveData(row map[string]any) map[string]any {
	if inner, ok := row["data"].(map[string]any); ok {
		return inner
	}
	return row
}
func mqttMetrics(a api) map[string]any     { return a.ok(http.MethodGet, "/v1/metrics", nil) }
func mqttCounter(a api, key string) uint64 { return number(nested(mqttMetrics(a), "io", key)) }
func mqttDocker(root, container, action string) {
	command := exec.Command("sudo", "-n", "docker", action, container)
	output, err := command.CombinedOutput()
	must(os.WriteFile(filepath.Join(root, "broker-"+action+".log"), output, 0600))
	must(err)
}

func runMqttLiveProcess(root, binary, address, container string) {
	absolute, err := filepath.Abs(root)
	must(err)
	root = absolute
	must(os.Mkdir(root, 0700)) // refuse to overwrite any previous evidence
	self, err := os.Executable()
	must(err)
	save(filepath.Join(root, "binaries.json"), map[string]any{"server_sha256": hash(binary), "driver_sha256": hash(self), "broker_container": container})
	host, portText, err := net.SplitHostPort(address)
	must(err)
	port, err := strconv.Atoi(portText)
	must(err)
	c := newCapture()
	defer c.close()
	apiPort := freePort()
	a := newAPI(apiPort)
	var process *child
	defer func() {
		if process != nil {
			silenceStop(&process, syscall.SIGKILL)
		}
	}()
	start := func() {
		process = spawnServer(binary, root, filepath.Join(root, "server.log"), apiPort)
		waitHealth(a, "MQTT live server")
	}
	start()
	allowCaptures(a, c)
	a.ok(http.MethodPut, "/v1/allowlist", map[string]any{"host": host, "port": port})
	a.ok(http.MethodPut, "/v1/streams/telemetry", map[string]any{"fields": []any{
		map[string]any{"name": "device_id", "type": "utf8", "nullable": false},
		map[string]any{"name": "value", "type": "int64", "nullable": true},
	}})
	topic := fmt.Sprintf("sparrow/live-test/%d", time.Now().UnixNano())
	spec := map[string]any{"version": 1, "stream": "telemetry", "recovery": "restart_fresh", "fail_on_decode": false,
		"source": map[string]any{"kind": "mqtt", "host": host, "port": port, "topic": topic, "qos": 0, "clean_session": true, "inbox_capacity": 32, "inbox_bytes": 262144, "inbox_wait_ms": 5},
		"sink":   map[string]any{"kind": "http", "url": c.url(), "batch_rows": 1, "linger_ms": 0, "outbox_capacity": 4},
		"graph": map[string]any{"version": 1, "pipeline_id": 727, "revision_id": 1, "nodes": []any{
			map[string]any{"id": 1, "kind": "memory_source", "table": "telemetry", "out": []int{2}},
			map[string]any{"id": 2, "kind": "silence", "out": []int{3}, "iot": map[string]any{
				"keys": []string{"device_id"}, "fields": []string{}, "emit_first": false, "ttl_micros": 0, "max_keys": 128, "invalid": "error",
				"timing": map[string]any{"kind": "silence", "clock": "live", "duration_micros": 1600000, "max_observation_gap_micros": 800000, "registered_keys": [][]string{{"registered"}}},
			}}, map[string]any{"id": 3, "kind": "capture_sink", "name": "http"},
		}},
	}
	save(filepath.Join(root, "spec.json"), spec)
	publisher := mqttPublisher(address)
	// A pre-subscription retained value must never register a device.
	mqttPublish(publisher, topic, data(map[string]any{"device_id": "retained-only", "value": 1}), true)
	time.Sleep(100 * time.Millisecond)
	startPipeline(a, "live", spec)
	wait("live PINGRESP", func() bool { return mqttCounter(a, "mqtt_feed_probes") >= 2 })
	wait("retained ignored", func() bool { return mqttCounter(a, "mqtt_retained_ignored") >= 1 })
	mqttPublish(publisher, topic, data(map[string]any{"device_id": "seen", "value": 2}), false)
	wait("registered and seen silent", func() bool { return c.rowCount() == 2 })
	initial := c.snapshot().Rows
	for _, row := range initial {
		v := liveData(row)
		require(v["sparrow_silence_event"] == "silent" && v["device_id"] != "retained-only", "initial live event")
	}
	generation := liveData(initial[0])["sparrow_silence_generation"]
	mqttPublish(publisher, topic, data(map[string]any{"device_id": "seen", "value": 3}), false)
	wait("actual record resumes", func() bool { return c.rowCount() == 3 })
	require(liveData(c.snapshot().Rows[2])["sparrow_silence_event"] == "resumed", "actual record must resume")
	// QoS0 restart discards in-flight data. Do not claim broker replay.
	mqttDocker(root, container, "kill")
	must(publisher.Close())
	wait("broker disconnect observed", func() bool { return mqttCounter(a, "mqtt_reconnects") > 0 })
	time.Sleep(2 * time.Second)
	require(c.rowCount() == 3, "broker outage manufactured a silent event")
	reconnects := mqttCounter(a, "mqtt_reconnects")
	mqttDocker(root, container, "start")
	probes := mqttCounter(a, "mqtt_feed_probes")
	wait("new responsive session", func() bool { return mqttCounter(a, "mqtt_feed_probes") > probes })
	time.Sleep(600 * time.Millisecond)
	require(c.rowCount() == 3, "reconnect bypassed the complete grace")
	wait("silence after new grace", func() bool { return c.rowCount() == 4 })
	require(number(liveData(c.snapshot().Rows[3])["sparrow_silence_episode"]) == 2, "episode lost across reconnect")
	require(liveData(c.snapshot().Rows[3])["sparrow_silence_generation"] == generation, "connection restart changed pipeline generation")
	// Real process crash: the new attempt must NOT restore prior key history.
	silenceStop(&process, syscall.SIGKILL)
	start()
	a.ok(http.MethodPost, "/v1/pipelines/live/start", map[string]any{})
	waitRunning(a, "live")
	wait("restart probe", func() bool { return mqttCounter(a, "mqtt_feed_probes") >= 1 })
	time.Sleep(600 * time.Millisecond)
	require(c.rowCount() == 4, "crash restart skipped grace")
	wait("fresh registered state", func() bool { return c.rowCount() == 5 })
	last := liveData(c.snapshot().Rows[4])
	require(last["device_id"] == "registered" && last["sparrow_silence_generation"] != generation && last["sparrow_silence_last_seen"] == nil, "restart-fresh identity/state")
	a.rejected(http.MethodPost, "/v1/pipelines/live/checkpoint", map[string]any{})
	// Sustained incoming traffic past the broker's 45s keepalive cutoff. The
	// publisher itself sends packets every 20ms, so needs no separate pings.
	publisher = mqttPublisher(address)
	defer publisher.Close()
	before := mqttMetrics(a)
	beforeReconnects := number(nested(before, "io", "mqtt_reconnects"))
	started := time.Now()
	sent := 0
	for time.Since(started) < 75*time.Second {
		mqttPublish(publisher, topic, data(map[string]any{"device_id": "busy", "value": sent}), false)
		sent++
		time.Sleep(20 * time.Millisecond)
	}
	elapsed := time.Since(started)
	wait("sustained decode drain", func() bool {
		return mqttCounter(a, "mqtt_decoded") >= number(nested(before, "io", "mqtt_decoded"))+uint64(sent)
	})
	after := mqttMetrics(a)
	require(number(nested(after, "io", "mqtt_reconnects")) == beforeReconnects, "sustained inbound traffic starved keepalive")
	require(number(nested(after, "io", "mqtt_ping_timeouts")) == 0, "healthy Mosquitto ping deadline")
	require(number(nested(after, "io", "mqtt_decoded"))-number(nested(before, "io", "mqtt_decoded")) == uint64(sent), "local sustained fixture lost decoded input")
	require(number(nested(after, "io", "mqtt_dropped_full")) == 0 && number(nested(after, "io", "mqtt_dropped_budget")) == 0, "sustained fixture exceeded ingress")
	require(c.rowCount() == 5, "active device emitted false silence")
	wait("busy becomes silent", func() bool { return c.rowCount() == 6 })
	save(filepath.Join(root, "before-traffic.json"), before)
	save(filepath.Join(root, "after-traffic.json"), after)
	save(filepath.Join(root, "capture.json"), c.snapshot())
	save(filepath.Join(root, "status.json"), statusOf(a, "live"))
	stopPipeline(a, "live")
	silenceStop(&process, syscall.SIGTERM)
	save(filepath.Join(root, "summary.json"), map[string]any{"valid": true, "live_best_effort": true, "certified": false,
		"actual_sigkill": true, "crash_scenarios": 1, "broker_outage_tested": true, "broker_reconnects": reconnects,
		"retained_ignored": true, "restart_new_generation": true, "restart_full_grace": true, "checkpoint_rejected": true,
		"sustained_seconds": elapsed.Seconds(), "sustained_sent": sent, "sustained_reconnects": 0, "false_silence": 0, "durable_replay": false})
	fmt.Println("MQTT_LIVE_PROCESS_OK")
}
