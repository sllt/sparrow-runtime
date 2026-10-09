# NATS Core Source / Sink（`kind: "nats"`，Preview）

状态：2026-10-09 新增，`maturity: preview`，未生产认证。按
[CONNECTORS.md](CONNECTORS.md) 合同实现。NATS Core 是**纯 live、尽力而为**
的发布/订阅：没有持久化、没有 ACK、没有 replay。需要可靠输入请使用
[JetStream](JETSTREAM.md)（独立 profile，checkpoint + Explicit ACK）。

English summary: `kind: "nats"` subscribes to (source) or publishes to (sink) a
plain NATS Core subject. It is `live_best_effort`, at-most-once, has no replay
and no acknowledgement, and is refused in aligned/checkpoint/restore
pipelines. Messages published while the job is down or the client is
reconnecting are lost. Use JetStream when you need durability.

## Core 与 JetStream 对比

| | NATS Core（`nats`） | JetStream（`jetstream`） |
|---|---|---|
| cargo feature | `nats`（只带 `async-nats`） | `jetstream`（= `nats` + ring） |
| 角色 | Source + Sink | Source（输出走 required HTTP） |
| 交付语义 | `live_best_effort`，at-most-once | `checkpointed_at_least_once` |
| 确认 | 无（Source 不 ACK，Sink 不等 broker ACK） | durable publication 后 Explicit ACK |
| Replay / 恢复 | 不支持；`restart_fresh`，停机期间消息丢失 | consumer 位置随 checkpoint 恢复 |
| 允许的 recovery | 仅 `restart_fresh`；拒绝 `aligned`、`restore`、`checkpoint`、`checkpoint_dir` | 需要 `aligned` + checkpoint |
| 订阅 | subject（可含 `*` / 尾部 `>`），可选 queue group | stream + durable consumer + ownership bucket |
| 慢消费者 | SDK 订阅缓冲满即丢弃，计数 `nats_source_slow_consumer` | pull 批量受 credit 控制，不丢 |
| 断线 | 每次断线最多 `reconnect_attempts` 次；耗尽后 Source 可重试失败、Sink 退避重开 | 断线 = 协议连续性丢失，reader 永久 unhealthy |
| 服务器 | 任意 NATS 2.x；`max_payload` 不得超过 Source 的 `max_payload_bytes` | 固定 nats-server 2.14.6 profile |

## 配置

```json
{
  "version": 1,
  "stream": "telemetry",
  "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 30",
  "source": {
    "kind": "nats",
    "inbox_capacity": 16,
    "nats": {
      "servers": ["tls://nats.example.com:4222"],
      "subject": "plant7.*.telemetry",
      "queue_group": "sparrow-alerts",
      "token_secret": "env:NATS_TOKEN",
      "reconnect_attempts": 10,
      "connect_timeout_ms": 2000,
      "max_payload_bytes": 65536,
      "subscription_capacity": 16,
      "inbox_bytes": 262144
    }
  },
  "sink": {
    "kind": "nats",
    "outbox_capacity": 64,
    "nats": {
      "servers": ["tls://nats.example.com:4222"],
      "subject": "plant7.alerts.hot",
      "token_secret": "env:NATS_TOKEN",
      "publish_timeout_ms": 2000,
      "flush_timeout_ms": 2000
    }
  }
}
```

还需 `put_allow` 允许每个 `host:port`。除 `servers`、`subject` 外都可省略。

| 字段 | 默认 | 范围 / 说明 |
|---|---|---|
| `servers` | — | 1..4 个；仅 `nats://` / `tls://`；不允许 userinfo、路径、查询；每个端点经 `TargetPolicy` |
| `subject` | — | Source 可用 `*`（整段）与尾部 `>`；Sink 必须是字面 subject（不支持模板） |
| `queue_group` | 无 | Source 专用；`[A-Za-z0-9_.-]`，1..256 |
| `token_secret` | 无 | 仅 secret 引用；有 token 时必须 `tls://`；Debug/日志中显示 `<redacted>` |
| `reconnect_attempts` | 10 | 1..100，**每次断线**的上限；`0`（SDK 的“无限”）被拒绝 |
| `connect_timeout_ms` | 2000 | 100..30000 |
| `max_payload_bytes` | 65536 | 1 KiB..1 MiB；SDK 缓冲按 `capacity × (max_payload + 8 KiB) + 256 KiB` 计入 job reservation，须 ≤ reservation/2 |
| `subscription_capacity` / `client_capacity` | 16 | 1..256，SDK 订阅缓冲 / 发布通道 |
| `inbox_bytes` | 256 KiB | Source inbox 字节预算（P1-27 规则） |
| `publish_timeout_ms` / `flush_timeout_ms` | 2000 | 10..30000，Sink 单条发布 / 关闭时 flush 上限 |

## 行为

- **解码 / 编码**：Source 每条消息按 JSON 解码为一行（`sparrow-formats`），超过
  JSON 限制计 `dropped_oversize`，解码失败计 `dropped_bad`，`fail_on_decode` 时 job
  失败。Sink 每行编码为一个 JSON 对象消息，超过 min(`max_payload_bytes`、服务器
  `max_payload`、64 KiB) 计 `dropped_oversize`。
- **背压**：Source 解码后的行先记 Reservation 再进入有界 inbox；inbox 满时等待
  （计 `backpressure_waits`），此时 SDK 订阅缓冲继续接收，满后由 SDK 丢弃并上报
  slow consumer。内存始终有界；丢弃是**显式**的。
- **慢消费者计数**：`nats_source_slow_consumer` 统计 SDK 的 `SlowConsumer` 事件。
  SDK 事件通道本身有损，所以它是丢弃数的**下界**；`received`（已接收）和服务器
  侧统计可交叉比对。
- **重连**：指数退避 100 ms→2 s，每次断线最多 `reconnect_attempts` 次，成功后计数
  复位。`disconnects` / `reconnects` / `client_errors` 计数，健康状态在
  Reconnecting / Ready 间切换。耗尽后 Source 以可重试错误结束（由 supervisor 按
  restart 策略重启），Sink 关闭客户端后退避 100 ms→2 s 重开（计 `sessions`）。
  重连后 Source 再次检查服务器 `max_payload`。
- **Sink 确认**：`published` 表示已交给客户端，不是 broker 确认；只有 flush 成功
  才说明服务器已收到。发布超时/失败计 `failed`，该批次不回执。
- **关闭**：Sink 在 `flush_timeout_ms` 内先发布 outbox 中已排队的批次，剩余计
  `discarded_on_close`，然后 `flush`（`flushes` / `flush_failed`），再 drain 并等待
  SDK 真实退出后退还额度。Source 取消后 drain 订阅并同样等待退出。

## 指标

`/v1/metrics` 与 pipeline status（`nats_source` / `nats_sink` 对象）：

- Source：`nats_source_{received,rows,dropped_bad,dropped_oversize,dropped_budget,backpressure_waits,slow_consumer,disconnects,reconnects,client_errors,inbox_items,inbox_bytes}`
- Sink：`nats_sink_{published,failed,dropped_bad,dropped_oversize,discarded_on_close,flushes,flush_failed,disconnects,reconnects,client_errors,sessions}`

## 限制

- 仅 JSON；Sink 无 subject 模板、不支持 headers / reply / request-reply、不支持 action。
- TLS 使用 SDK 内置系统根证书并校验，不支持私有 CA / mTLS；TLS 路径未做真实证书测试。
- 只支持 token 认证（无 user/password、nkey、creds）。
- 服务器默认 `max_payload` 为 1 MiB；Source 必须把 `max_payload_bytes` 设到不小于它
  （例如 1 MiB + `subscription_capacity: 1`），否则连接被拒绝——这是为了让 SDK
  缓冲真实计入账本。
- 每个 NATS 客户端独立计入 job reservation（默认约 1.4 MiB）：单个 ≤ reservation/2，
  同一 pipeline（含 graph_io）所有 NATS 端点之和 ≤ reservation 的 3/4，超出在校验阶段
  返回 `BoundExceeded`；需调小 `subscription_capacity` / `client_capacity` /
  `max_payload_bytes`。校验按 compact 预算计算，更小的运行时预算仍可能在启动时拒绝。
- 不能用于 aligned / checkpoint pipeline（包括只有 Sink 是 NATS 的情况）。

## 测试

真实 broker 测试需要 `SPARROW_NATS_SERVER` 指向 nats-server 二进制：

```bash
SPARROW_NATS_SERVER=/path/to/nats-server \
  cargo test -p sparrow-connectors --features nats --lib nats -- --include-ignored
SPARROW_NATS_SERVER=/path/to/nats-server \
  cargo test -p sparrow-control --features nats --lib nats_ -- --include-ignored
```

覆盖：wildcard 订阅 + 解码/超限计数、`fail_on_decode`、queue group 分流、慢消费者
丢弃计数且内存有界、服务器重启后 Source/Sink 重连、重连耗尽后可重试失败、
服务器 `max_payload` 超界拒绝、Sink 停止时 flush 已排队批次、端到端
NATS → SQL 过滤 → NATS（独立订阅者校验）以及 graph_io 双 NATS 输入。
