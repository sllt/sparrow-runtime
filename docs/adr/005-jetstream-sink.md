# ADR-005：JetStream Sink（PubAck 确认的发布）

日期：2026-10-09。状态：Preview，已实现，未生产认证。

English summary: `sink.kind = "jetstream"` publishes each output row to an
**existing** stream and counts a batch as delivered only after every row has a
server PubAck. Retries are bounded, so duplicates are possible
(at-least-once *into the stream*); an optional `msg_id_column` sets
`Nats-Msg-Id` so the server deduplicates within the stream's
`duplicate_window`. The sink never creates streams. It joins aligned
checkpoints only in the independent linear File **v27** profile: the snapshot
binds the exact `JSI1` target and full computation, and restores never adopt
v3 or other source-only history. Aligned output requires File/Limits storage;
other checkpoint profiles stay HTTP-only. Validation remains Preview, not a
production or device-power-loss certification.

## 背景

NATS Core Sink（见 [NATS.md](../NATS.md)）是 at-most-once：发布即交给客户端。
需要输出侧持久化时，JetStream 的 PubAck 是 broker 给出的“已写入 stream”确认。

## 决策

1. **确认点 = PubAck**。每行一条消息；`max_inflight_acks`（1..=256，默认 8）个消息
   同时等待 PubAck（SDK `max_ack_inflight` + 背压，以及 `buffer_unordered` 两层上限）。
   每次发布指定 `Nats-Expected-Stream`，并核对 PubAck 的 stream。
   批次全部行拿到 PubAck 才回执 outbox（`InflightCounter::ack`）；v27 还必须
   在全部 PubAck 后成功复验目标 incarnation/配置。
2. **有界重试**。每次尝试的时限为 `2 × ack_timeout_ms`（send + PubAck），超时和
   传输类错误按 100 ms→2 s 退避重试至多 `max_retries` 次（≤20）。
   `MaxPayloadExceeded`、`WrongLast*`、wrong-stream、目标身份/配置变化、非法 msg id、
   编码失败不盲目重试；prepared 目标无法完成验证也不授权继续发布/提交。
3. **Fail closed**。任何一行最终无法确认 → `jetstream_sink_fatal`，取消 job，
   `join_job` 报 `JobFailed`（live 与 aligned 两个分支都检查，graph 端口同样）。
   不跳过、不静默丢弃“毒”行，与 ADR-004 的 fail/held 选择一致。
4. **语义：at-least-once 进入 stream**。重试时 broker 可能已经持久化但 ack 丢失，
   因此可能重复；`jetstream_sink_duplicates` 只统计服务器报告的去重命中。
5. **去重：仅 `msg_id_column`**。指定输出列（utf8/int64/uint64）作为 `Nats-Msg-Id`，
   服务器在 `duplicate_window` 内去重；要求 stream 的 `duplicate_window > 0`。
   调用方须保证 id 对合法输出唯一且重放稳定；复用 id 会合并不同的合法输出。
   **不提供内容哈希 id**：确定性内容哈希会把合法的相同行合并成一条（例如同一设备
   连续两次相同读数），这是数据丢失，而不是去重。空值行不带 header（计
   `msg_id_missing`）。
6. **不自动创建 stream**。启动时校验：stream 存在、未 sealed、不是 mirror、`no_ack=false`、其
   subjects 绑定 sink 的字面 subject、（使用 msg id 时）duplicate_window > 0。
   stream 的保留、配额、副本归运维所有；sink 自建会产生无人负责的默认值。
   stream 不存在是不可重试错误；临时请求失败在 `max_retries` 内重试。
   fresh PubAck-only 允许 Memory/其他 retention，但不承诺 broker 重启后保留；
   aligned 的 prepared probe 另外要求 `storage=File`、`retention=Limits`。
7. **Aligned checkpoint：独立线性 File v27 profile**。不能借用旧 v3/CPL1 的下游
   prefix 兼容授权输出恢复。v27 外层保存 File cut、原参与者 manifest 与独立 `JSI1`：
   canonical endpoints、token SecretRef（不保存 secret 值）、stream 的精确 created nanos、
   subject 和 msg-id 策略。恢复严格比较目标与**完整**计划，包括最后一个状态之后的
   Filter/Project、输出 schema 和 sink；任何变化都要求独立新历史，不自动 fork/迁移。
   v1..v26 的旧 codec/兼容规则不改；v27 与它们拒绝混用目录，HTTP↔JetStream、
   stream/subject/id 策略变化不能绕过校验。

   启动先在同一 SourceAdmission/MemoryOwner 上 probe 目标、取得 prepared session，
   再验证历史、打开/seek File 和激活状态；probe/恢复失败不推进 CURRENT，也不启动
   Source。运行复用这个 session，不另开连接/owner；失败清理显式 await close，SDK
   回调持有生命周期 guard，退出未完成时 slot/SDK 信用不能提前释放。

   kernel 把批次交给
   sink 前 `enqueue`，barrier 的 `wait_outbox` 等到 `pending()==0`，且标记后出现任何
   `fail()` 就拒绝提交。因为 sink 在全部 PubAck 后才 `ack()`，checkpoint 提交时
   barrier 之前的所有输出已得到 stream 接纳确认。每批前、全部 PubAck 后及每次 retry
   前取得实时 INFO，比较本 attempt 的完整起始配置和精确 incarnation；不靠 cached
   Ready。同名重建或配置变化 fail closed，不 ack receipt，不授权 CURRENT。
   恢复从最后 checkpoint 重放 File 输入，重放部分可能重复，msg id 只在去重窗口内有效。
   其他 aligned profile（graph、IoT、引用表、JetStream source、paused/观测时间）
   依赖 HTTP 稳定输出 ID（`OutputSequence`），本批继续只接受 HTTP sink。
8. **内存**：SDK 命令队列及 writer 批次缓冲（`(client_capacity + min(client_capacity, 16)) × (max_payload_bytes + 8 KiB) + 256 KiB`）加
   在途重试保留的 payload（`max_inflight_acks × (max_payload_bytes + 8 KiB)`）记入
   job reservation：单个 ≤ reservation/2，且与同 pipeline 的其他 NATS 端点合计
   ≤ 3/4。
   `client_capacity` 默认 4，独立于默认 8 个在途 PubAck；显式配置不暗中钳制，
   超出同 job 预算则拒绝。默认 Sink 预留 1,441,792 B（payload=64 KiB）。
   编码前另取同 owner 的短期 scratch 信用：`row.resident_bytes × 8 + 字段名 resident 合计 × 4 + 8 KiB`，
   覆盖 wide/Bytes/Dynamic 的临时 serde Value 与编码；返回时释放，预算不足直接失败。
   JSON writer 在缓冲增长前检查限额，包含 Expected-Stream 与 msg-id 的真实 header 字节。
   prepared identity/配置基线也记入同 owner，配置序列化上限 64 KiB。
   INFO 响应仍由 SDK 先解码，64 KiB 限制针对后续保留的配置基线；这些是保守信用额度，
   不是敌对 broker 或 SDK 临时分配的进程 RSS 硬上限。
9. **Stop 有界排空**。取消时建立一个 flush deadline，在途批次与队列共用，不给下一批
   或下一次 retry 重新发放预算。deadline 到期先 fail 未确认 receipt，再清理 SDK；
   已收到部分 PubAck 不等于整批成功，SDK 清理超时也不虚报退款。

## 顺序

`max_inflight_acks > 1` 时消息并发发布，不保证输入行的入 stream 顺序；重试也可能
重排。需要这个 Sink 自身的相对行顺序时设 `max_inflight_acks: 1`（吞吐下降），
不把其他生产者的 interleave 或重复视为全局顺序保证。

## 后果与限制

- 只有 JSON、静态 subject；不支持 action、subject 模板、自定义 header。
- `delivery` 字段仍按 File profile 规则填写（`live_best_effort`）；可靠性来自
  `recovery: aligned` 与 PubAck，`check_delivery` 不为 File source 开放
  `checkpointed_at_least_once` 标签（后续可统一）。
- restart_fresh（非 aligned）时，job 被 kill 后 outbox 中未确认的行会丢失（上游
  不重放）；stop 时在 `flush_timeout_ms` 内确认在途及已排队批次，剩余视为失败。
- broker 管理员是配置可信边界：INFO 检查不能原子禁止检查之间修改并恢复 stream；
  不支持通过删除/重建/回滚 broker 存储绕过 lineage。保留/淘汰、容量与副本仍归运维。
- File/PubAck 不是消费者业务事务确认，也不泛化为设备掉电/fsync、HA 或 exactly-once 承诺。
- 吞吐性能未测（10k/20k 基线延后）。
