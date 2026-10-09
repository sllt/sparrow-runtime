# ADR-005：JetStream Sink（PubAck 确认的发布）

日期：2026-10-09。状态：Preview，已实现，未生产认证。

English summary: `sink.kind = "jetstream"` publishes each output row to an
**existing** stream and counts a batch as delivered only after every row has a
server PubAck. Retries are bounded, so duplicates are possible
(at-least-once *into the stream*); an optional `msg_id_column` sets
`Nats-Msg-Id` so the server deduplicates within the stream's
`duplicate_window`. The sink never creates streams. It joins aligned
checkpoints only in the linear File profile, where the barrier already waits
for the sink outbox; other checkpoint profiles stay HTTP-only.

## 背景

NATS Core Sink（见 [NATS.md](../NATS.md)）是 at-most-once：发布即交给客户端。
需要输出侧持久化时，JetStream 的 PubAck 是 broker 给出的“已写入 stream”确认。

## 决策

1. **确认点 = PubAck**。每行一条消息；`max_inflight_acks`（1..=256，默认 8）个消息
   同时等待 PubAck（SDK `max_ack_inflight` + 背压，以及 `buffer_unordered` 两层上限）。
   一个批次只有当全部行都拿到 PubAck，才回执 outbox（`InflightCounter::ack`）。
2. **有界重试**。每次尝试的时限为 `2 × ack_timeout_ms`（send + PubAck），超时和
   传输类错误按 100 ms→2 s 退避重试至多 `max_retries` 次（≤20）。
   `MaxPayloadExceeded`、`WrongLast*`、非法 msg id、编码失败不重试。
3. **Fail closed**。任何一行最终无法确认 → `jetstream_sink_fatal`，取消 job，
   `join_job` 报 `JobFailed`（live 与 aligned 两个分支都检查，graph 端口同样）。
   不跳过、不静默丢弃“毒”行，与 ADR-004 的 fail/held 选择一致。
4. **语义：at-least-once 进入 stream**。重试时 broker 可能已经持久化但 ack 丢失，
   因此可能重复；`jetstream_sink_duplicates` 只统计服务器报告的去重命中。
5. **去重：仅 `msg_id_column`**。指定输出列（utf8/int64/uint64）作为 `Nats-Msg-Id`，
   服务器在 `duplicate_window` 内去重；要求 stream 的 `duplicate_window > 0`。
   **不提供内容哈希 id**：确定性内容哈希会把合法的相同行合并成一条（例如同一设备
   连续两次相同读数），这是数据丢失，而不是去重。空值行不带 header（计
   `msg_id_missing`）。
6. **不自动创建 stream**。启动时校验：stream 存在、未 sealed、不是 mirror、其
   subjects 绑定 sink 的字面 subject、（使用 msg id 时）duplicate_window > 0。
   stream 的保留、配额、副本归运维所有；sink 自建会产生无人负责的默认值。
   stream 不存在是不可重试错误；临时请求失败在 `max_retries` 内重试。
7. **Aligned checkpoint：仅线性 File profile**。现有 barrier 合同：kernel 把批次交给
   sink 前 `enqueue`，barrier 的 `wait_outbox` 等到 `pending()==0`，且标记后出现任何
   `fail()` 就拒绝提交。因为 sink 在全部 PubAck 后才 `ack()`，checkpoint 提交时
   barrier 之前的所有输出已在 stream 中 —— sink 是“提交前已刷新”的参与者，无需
   新协议。恢复从最后 checkpoint 重放 File 输入，重放部分会重复，`msg_id_column`
   可消除。其他 aligned profile（graph、IoT、引用表、JetStream source、paused/观测时间）
   依赖 HTTP 稳定输出 ID（`OutputSequence`），本批继续只接受 HTTP sink。
8. **内存**：SDK 缓冲（`client_capacity × (max_payload_bytes + 8 KiB) + 256 KiB`）加
   在途重试保留的 payload（`max_inflight_acks × (max_payload_bytes + 8 KiB)`）记入
   job reservation：单个 ≤ reservation/2，且与同 pipeline 的其他 NATS 端点合计
   ≤ 3/4。

## 顺序

`max_inflight_acks > 1` 时同一批次内消息并发发布，正常情况下按发送顺序入 stream，
但一旦某条重试，它会排在之后发送的消息后面。需要严格顺序时设
`max_inflight_acks: 1`（吞吐下降）。

## 后果与限制

- 只有 JSON、静态 subject；不支持 action、subject 模板、自定义 header。
- `delivery` 字段仍按 File profile 规则填写（`live_best_effort`）；可靠性来自
  `recovery: aligned` 与 PubAck，`check_delivery` 不为 File source 开放
  `checkpointed_at_least_once` 标签（后续可统一）。
- restart_fresh（非 aligned）时，job 被 kill 后 outbox 中未确认的行会丢失（上游
  不重放）；stop 时在 `flush_timeout_ms` 内确认已排队批次，剩余视为失败。
- 吞吐性能未测（10k/20k 基线延后）。
