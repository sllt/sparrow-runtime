# Local DataBus（`kind: "databus"`）

状态：Preview，已实现，未生产认证。决策见 [ADR-006](adr/006-local-databus.md)，
合同对照见 [CONNECTORS.md](CONNECTORS.md#附local-databus-sourcesinkkind-databus)。

English summary: an **in-process** named topic bus. A pipeline whose sink is
`databus` publishes its output rows to a topic, and pipelines whose source is
`databus` and that subscribe to a matching topic receive them, all inside the
same runtime. It works like eKuiper's memory source/sink. It is
`live_best_effort` / at-most-once: there is no replay, no ack, and no
checkpoint participation. Every subscriber has a bounded buffer that is charged
to its job's memory reservation. The overflow policy is explicit and each
policy has its own counter.

## 用法

```json
// 上游：任意 Source -> SQL -> databus Sink（字面 topic）
{"stream":"telemetry","sql":"SELECT device_id, temperature FROM telemetry",
 "source":{"kind":"mqtt", "...":"..."},
 "sink":{"kind":"databus","databus":{"topic":"plant.raw"}}}

// 下游：databus Source（可用通配）-> SQL -> 任意 Sink
{"stream":"telemetry","sql":"SELECT * FROM telemetry WHERE temperature > 30",
 "source":{"kind":"databus","databus":{"topic":"plant.>","overflow":"drop_oldest"}},
 "sink":{"kind":"http","url":"http://127.0.0.1:8080/alerts"}}
```

消息是一行 JSON 对象（上游输出 schema 编码）。下游按**自己的** `stream` schema 解码，
缺列 / 类型不符按 `fail_on_decode` 处理：默认计入 `databus_source_dropped_bad` 并丢弃，
开启时 job 失败。runtime 不检查上下游 schema 是否一致。

## Topic

- 由 `.` 分隔的 token，token 为 `[A-Za-z0-9_-]+`；最长 128 字节，最多 16 个 token。
- Sink（发布）只能用**字面** topic。
- Source（订阅）可用 `*`（匹配恰好一个 token）和末尾的 `>`（匹配一个或多个 token），
  与 NATS 的写法一致。
- 同一 pipeline 内，如果 Sink topic 与任一 databus Source 模式重叠（例如订阅 `a.>`
  又发布到 `a.b`），校验会拒绝这条自反馈环。**跨 pipeline 的环不检测**，例如 A→B→A，
  需要运维自行避免。

## Source 参数（`source.databus`）

| 字段 | 默认 | 范围 | 说明 |
|---|---|---|---|
| `topic` | 必填 | 见上 | 订阅模式 |
| `buffer_capacity` | 1024 | 1..=4096 | 订阅缓冲消息数上限 |
| `buffer_bytes` | 256 KiB | 1 KiB..=4 MiB | 订阅缓冲 payload 字节上限 |
| `overflow` | `drop_oldest` | `drop_oldest` / `drop_newest` / `block` | 缓冲满时的策略 |
| `block_timeout_ms` | 1000 | 1..=30000 | 仅 `block`：发布方等待单条消息的上限 |
| `inbox_bytes` | 256 KiB | ≤ job queue 预算、≤4 MiB | 进入内核前的 inbox 字节预算 |

`inbox_capacity`（顶层 source 字段，默认 16）仍是 inbox 的行数上限。

## Sink 参数（`sink.databus`）

| 字段 | 默认 | 范围 | 说明 |
|---|---|---|---|
| `topic` | 必填 | 字面 topic | 发布 topic |
| `flush_timeout_ms` | 1000 | 10..=30000 | 停止 / EOF 时把已排队批次送出的时限 |

## 内存

订阅缓冲的上限为 `buffer_bytes + buffer_capacity × 64 B`。订阅时把这一上限**一次性**
作为 Reservation credit 记入订阅方 job 的内存账本，取消订阅时退还。

- 单个订阅不能超过 job reservation 的 1/2。
- 一个 pipeline 的所有 databus 订阅，加上 NATS / JetStream SDK 缓冲，合计不能超过 3/4。
- 超出时，校验或启动返回 `BoundExceeded`。

消息正文是 `Arc<[u8]>`，扇出时共享，不复制。所以 N 个订阅者里只有缓冲上限计账，
没有 N 份拷贝。

## 溢出策略

| 策略 | 缓冲满时 | 计数（订阅方 Source） | 对发布方 |
|---|---|---|---|
| `drop_oldest`（默认） | 丢最旧一条，再接收新消息 | `databus_source_dropped_oldest` | 从不等待 |
| `drop_newest` | 拒收新消息 | `databus_source_dropped_newest` | 从不等待 |
| `block` | 发布方最多等 `block_timeout_ms`；仍满则只对这个订阅者丢弃该条 | `databus_source_block_timeouts`；发布方 `databus_sink_blocked_publishes` | 被拖慢 |

默认选 `drop_oldest` 有两个原因：

1. 实时监控场景更需要最新数据。
2. 它保证一个慢订阅者**永远不会**拖住上游 pipeline。

`block` 只在下游宁可让上游变慢、也要尽量不丢的时候使用。但它仍然是有界等待，不是无损。

单条消息超过某订阅者的 `buffer_bytes` 时，只对该订阅者丢弃，计入该订阅者的
`databus_source_dropped_oversize`。编码后超过 64 KiB 的行由 Sink 在发布前丢弃，计入
`databus_sink_dropped_oversize`，所在批次不回执。

**慢订阅者隔离**：发布方先把消息投给所有非 `block` 订阅者，再逐个等待 `block` 订阅者。
因此：

- 在 drop 策略下，一个慢订阅者只影响它自己的计数。
- 在 `block` 策略下，慢订阅者会拖慢发布方，从而拖慢发布方之后的消息。但同一条消息
  对其他订阅者的投递不受影响。

## 无订阅者 / 无发布者

- **没有匹配订阅者时**发布：行计入 `databus_sink_no_subscribers` 后丢弃，批次照常回执，
  不报错、不阻塞。总线不保留历史，后来的订阅者也看不到这些行。
- **没有发布者时**订阅：Source 正常运行（health `Ready`，详情 `databus_subscribed`），
  只是没有数据；发布者出现后，从那一刻起开始收到消息。
- 投递顺序：同一个发布者的消息在每个订阅者处保持顺序。多个发布者之间不保证全局顺序。

## 生命周期

- 订阅和发布注册都是 RAII 句柄：job 停止、失败、被新修订替换，或 runtime 关闭时自动注销。
  当一个 topic 的最后一个句柄消失，这个 topic 也随之消失，不会泄漏。
- 订阅者注销时，其缓冲中未消费的消息会被丢弃，计入 `databus_source_discarded_on_close`
  和 `databus_sink_discarded_on_close`。同时唤醒正在 `block` 等待它的发布方。
- 重启（stop → start 或修订切换）会**重新订阅**，从重新订阅那一刻起接收，**不补发**
  停机期间的消息。
- Sink 在停止或 EOF 时，于 `flush_timeout_ms` 内把已排队批次投给总线，剩余部分计入
  `databus_sink_discarded_on_close`。如果截止时间到达时某行正在等待 `block` 订阅者，这一行可能已经投给了非阻塞订阅者，
  但仍按"未完成"计数。
- 本 runtime 没有删除 pipeline 的 API。停止（desired=stopped）就是释放 topic 的操作。

## 语义与恢复

- Source 和 Sink 的 capability 都是 `live_best_effort` / `restart_fresh`，replay 为
  `unsupported`，即 at-most-once。
- Sink 的"完成"是指批次中每一行都已投给所有匹配订阅者的缓冲。这不代表下游已经处理，
  也不代表下游最终送达。
- 拒绝 `recovery: aligned`、`restore`、`checkpoint` / `checkpoint_dir`，以及持久化的
  `delivery` 声明，错误码为 `UnsupportedRestore`。databus Sink 即使挂在 File Source
  后面也会被拒绝。原因：内存总线没有可靠的重放点，上游 checkpoint 无法证明下游已经处理。
- Sink 的 publisher 注册失败（超过 runtime 上限）会把 job 判为失败（fail closed）：
  计 `databus_sink_fatal`，`join_job` 报 `JobFailed`。live 和 aligned 两个分支都检查。

## 上限（每个 runtime）

| 项 | 上限 |
|---|---|
| 订阅 | 256 |
| 发布者注册 | 256 |
| 单个订阅缓冲 | 4096 条 / 4 MiB |
| 单行 | ≤64 KiB（与其他 JSON Source 相同） |

## 指标

- `/v1/metrics` 提供 `databus_source_*` 和 `databus_sink_*`。
- pipeline status 中有 `databus_source` 和 `databus_sink` 对象。
- 计数范围是本次 attempt。
- `databus_sink_deliveries` 统计订阅缓冲接受的次数。扇出到 N 个订阅者时，每行计 N 次。
- 一个订阅者上有如下恒等式（不含关闭时的丢弃）：
  `deliveries + dropped_newest + block_timeouts + dropped_oversize = published`。
  `dropped_oldest` 是已经接受之后又被挤出的消息。

## 不在本次范围

- 宿主应用直接注入 frame/batch 的嵌入式入口（CONN-07 / EMB-01 的宿主 Handle 部分）。
- 跨进程总线。
- 持久化。
- 跨 pipeline 的环检测。
- Topic ACL。
