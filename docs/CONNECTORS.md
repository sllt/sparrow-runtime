# Connector 合同（新增内置 Source / Sink 必须满足）

状态：2026-10-09 起生效。本文从现有 MQTT / HTTP Push / HTTP Sink / HTTP Lookup /
JetStream 实现中**提炼**出新增内置 connector 必须遵守的规则，不新增运行时语义。
外部进程插件另见 [EXTENSIONS.md](EXTENSIONS.md)；JetStream 的可靠协议见
[JETSTREAM.md](JETSTREAM.md)。HTTP Poll Source（`http_poll`）是第一个按本合同
实现的 connector，文末给出它的对照；NATS Core Source / Sink（`nats`）是第二个，
见 [NATS.md](NATS.md)。

English summary: every new built-in connector must (1) declare its delivery /
replay / recovery semantics as a `ConnectorCapabilities` const and refuse
anything stronger, (2) size its inbox/outbox in bytes against the job memory
ledger (P1-27 rule), (3) apply bounded backpressure with counted drops/skips,
(4) take credentials only as secret refs and never weaken TLS, (5) pass every
network target through the deny-by-default `TargetPolicy`, (6) expose
`<kind>_*` diagnostics counters and observation health, (7) stop promptly and
release all credit on cancel, (8) validate config with `deny_unknown_fields`,
(9) ship the test matrix below, and (10) put heavy SDKs behind a cargo feature.

## 1. 声明交付 / 重放语义

- 在 `crates/sparrow-connectors/src/capabilities.rs` 增加一个
  `ConnectorCapabilities` 常量：`kind`、`replay`、`delivery`、`recovery`。
  **声明必须与实现一致，只能偏弱不能偏强。** 没有 broker 位置/确认协议的
  live 来源一律是 `LiveBestEffort` + `RestartFresh` + `ReplaySupport::Unsupported`
  （MQTT、HTTP Push、HTTP Poll、NATS Core）。
- 构造/绑定时调用 `refuse_durable_recovery(caps, restore)`，任何
  `RestoreClaim::Durable*` / checkpoint 恢复都返回 `UnsupportedRestore`，
  不得静默降级。
- 在 `sparrow_model::delivery::check_recovery_capabilities` 的 live 名单
  （`mqtt_like`）和 `validate.rs::replay_label_for_source` 中登记新 kind，
  使 `delivery` / `recovery` / `checkpoint_dir` / `restore` 的组合在 spec
  阶段就被拒绝，而不是运行时才失败。
- 在 `validate.rs::capabilities_json` 增加条目（roles、`maturity`、
  一句话合同），`/v1/capabilities` 是对外声明的唯一入口。新 connector 初始
  `maturity` 为 `preview`。
- 需要可靠语义（at-least-once、ACK、replay）的 connector 必须走 JetStream 那种
  独立 profile + ADR，不能在 live 路径上“顺便”加 ACK 或缓冲。

## 2. Inbox / outbox 字节预算（记入内存账本）

- Source 必须使用 byte-accounted ingress（同 MQTT 服务器路径）：
  `run_budgeted(tx: Sender<QueuedRow>, cancel, owner, max_row_bytes)`，由
  supervisor 通过 `JobRequest::with_budgeted_live_io` 接入。
  - 每行用 `QueuedRow::try_new(row, &queue_owner, &occupancy)` 申请 Queue
    credit；`queue_owner = MemoryOwner::child(owner, budget{queue_bytes = inbox_bytes}, label)`。
  - 等待入队期间，解码好的工作行持有 Reservation credit
    （`MemoryLease`, `CreditKind::Reservation`）；Queue credit 获得后才释放。
  - 单行上限取 `kernel.ingress_row_limit()`；超限行丢弃并计数，不截断。
- **P1-27 规则**：`inbox_bytes + QueuedRow::channel_budget(inbox_capacity)
  <= min(job queue budget, 4 MiB)`，在 spec 校验和 supervisor 启动前
  （用真实 `job_budget()`）各检查一次，失败为 `BoundExceeded`。
  `inbox_capacity` 是独立的条目上限（1..=4096）。默认 `inbox_bytes` 为 256 KiB。
- Connector 自身的大缓冲（响应体、批、解压缓冲）必须先向账本申请
  Reservation（`MemoryLease::grow_to`），**先记账再分配**，并有静态硬上限；
  上限还要小于 job reservation 预算（HTTP Poll：`max_response_bytes <= reservation/2`）。
- Sink outbox 同理：按字节和条目双重限界，`sink_outbox.*` 指标区分
  正常完成、失败与 `discarded_on_close`。

## 3. 背压与慢消费者

- 所有队列有界；不得引入无界 `Vec`/channel 作为“临时”缓冲。
- 必须在文档中写明满队列时的行为，三种已有模式任选其一并计数：
  - **等待**（MQTT：有界等待后丢弃；HTTP Poll：无限期但可取消的等待，且
    在等待期间不发起新请求）；
  - **拒绝上游**（HTTP Push：返回 429）；
  - **跳过**（HTTP Poll：错过的 tick 计入 `skipped_ticks`，不补发、不排队）。
- 轮询/拉取类 Source 同一时刻至多一个在途请求（不重叠），不得因下游慢而
  并发堆积请求。
- 计数器是诊断信息，不是投递回执。

## 4. 密钥与 TLS

- 凭据只能以 secret 引用出现（`*_secret` 字段，`env:NAME` 或 secrets store），
  spec 中不得有内联凭据字段；`deny_unknown_fields` 保证 `token` 之类会被拒绝。
- 解析后的值：HTTP 头设 `HeaderValue::set_sensitive(true)`；自定义 `Debug`
  脱敏；错误消息、诊断与日志不得回显密钥、完整 URL query 或响应体。
- 发送凭据的连接必须是 TLS（`https`），否则 `PolicyDenied`。
- HTTP 一律用 `tls::http_client`：webpki 根证书、校验证书、禁用重定向、禁用
  环境代理、无内置重试。**不提供 `skip_verify`**。私有 CA 需单独设计，不能通过
  关闭校验实现。
- 当前密钥在 bind 时解析一次；轮换需重启 pipeline（需在 connector 文档注明）。

## 5. 出站白名单 / 内网（SSRF）策略

- 每个网络目标（含重定向后的目标——因此禁用重定向）都必须通过
  `TargetPolicy::check_http_url` / `check_host_port`：host:port 默认拒绝，需
  `put_allow` 显式加入；link-local、云元数据、unspecified、multicast 地址永远拒绝。
- 校验在 spec `validate_io` 和 bind 时都执行；使用 secret 引用不豁免白名单。
- 已知限制：策略按 URL 主机名匹配，不对 DNS 解析后的 IP 再次校验
  （DNS rebinding 需要后续统一处理）。新 connector 不得自行实现另一套策略。

## 6. 指标 / 诊断命名

- 在 `diag.rs::IoDiagnostics` 中以 `<kind>_` 为前缀增加 `AtomicU64` 计数器，并
  同步到 `IoSnapshot` 及其 `add_assign`；`/v1/metrics` 的 `io` 字段和 pipeline
  status 中的 `<kind>` 对象由 `sparrow-server` 输出。
- 至少包含：请求/消息数、成功、失败（按原因拆分：超时、状态码、超大、解码）、
  入队行数、各类丢弃（`dropped_bad` / `dropped_oversize` / `dropped_budget`）、
  背压等待、在途 gauge 及 inbox occupancy（items / bytes）。
- 使用 `diag.observation` 报告 health（Healthy / Degraded / Unhealthy）、
  progress、lifecycle 和 latency（`HttpHeaders`、`HttpBody`、`Decode`、
  `SourceAdmission` 等已有种类）。
- 指标范围是“正在运行的 attempt”；不得把计数器当成持久回执。

## 7. 关闭

- 所有等待点（网络 I/O、退避 sleep、入队等待、Queue credit 重试）都必须
  `select!` 取消令牌，停止应当及时完成（测试中以秒级上限断言）。
- 关闭时释放所有 Reservation / Queue credit，`in_flight` gauge 归零；不得
  依赖 Drop 之外的清理路径。
- Source 返回错误时 supervisor 取消 job（restart 由既有退避策略处理）。

## 8. 配置校验

- spec 使用嵌套块 `source.<kind>`（同 `source.jetstream`），`#[serde(deny_unknown_fields)]`；
  tagged enum 也加 `deny_unknown_fields`。
- 该块只在对应 `kind` 下允许，且该 kind 下拒绝其他 connector 的扁平字段
  （`host`、`topic`、`tls`、`inbox_bytes` 等），避免“看似生效、实际忽略”。
- 所有数值有上下界（间隔、超时、头数量、URL/secret 长度、inbox）；
  越界为 `BoundExceeded` / `InvalidArgument`，在 `validate_io` 阶段报告。
- 序列化时省略默认值，保证 round-trip 稳定。

## 9. 必需测试

Connector crate（真实 loopback socket，不 mock 传输层）：
1. 配置边界与策略拒绝；凭据要求 https；预算检查。
2. 密钥不进入 `Debug` / 错误 / 诊断，头部为 sensitive。
3. 正常数据路径（每种格式）。
4. 超大输入（有/无长度提示）被拒绝且不增长内存。
5. 超时有界并计数。
6. 失败退避与恢复。
7. 慢消费者：不重叠、计数、内存不增长。
8. 阻塞在入队时停止：及时返回并释放 credit。
9. TLS（fixture CA）及默认信任拒绝自签证书。
10. `fail_on_decode` 行为。

Control crate：
11. 线性 pipeline 端到端（spec → validate → Supervisor → Kernel → sink）。
12. `graph_io` DAG 路径端到端。
13. spec 矩阵：缺块、混用字段、未知字段、内联凭据、durable/restore/checkpoint 声明被拒绝；
    `validate_io` 的 PolicyDenied / SecretMissing / BoundExceeded；`capabilities_json`。

合入前：`cargo test --workspace`、`cargo clippy --workspace --all-targets`
不新增警告、新文件通过 `rustfmt --check`；如有 feature，还需
`cargo build --features <f> --all-targets` 和 no-default 构建。

## 10. Feature flag 约定

- 只依赖已有依赖（reqwest、tokio、rustls）的 connector 内置，不加 feature
  （HTTP Poll）。
- 引入较重 SDK 或系统库的 connector（Kafka、PostgreSQL、Modbus 等）放在
  `sparrow-connectors` 的可选 feature 后（同 `jetstream` → `async-nats`），
  上层 crate 透传同名 feature；默认构建不包含。未启用时 spec 校验返回
  `FeatureUnavailable`，而不是未知 kind。
- 共用同一 SDK 的 connector 共享连接/认证/TLS 校验并用层级 feature：
  `nats`（NATS Core，只带 `async-nats`）是 `jetstream` 的子集，
  `jetstream = ["nats", ...]`；服务器白名单、subject 语法和 token 规则在
  `nats::common` 中只实现一次。

---

## 附：HTTP Poll Source（`kind: "http_poll"`）

周期性 GET 一个业务 API，把 JSON（对象或对象数组）或 NDJSON 解码为行。

```json
{
  "version": 1,
  "stream": "telemetry",
  "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 30",
  "source": {
    "kind": "http_poll",
    "inbox_capacity": 64,
    "http_poll": {
      "url": "https://api.example.com/v1/readings",
      "interval_ms": 5000,
      "timeout_ms": 3000,
      "backoff_max_ms": 60000,
      "auth": {"type": "bearer", "token_secret": "env:READINGS_TOKEN"},
      "headers": [
        {"name": "X-Tenant", "value": "plant-7"},
        {"name": "X-Api-Key", "value_secret": "readings.api_key"}
      ],
      "format": "json",
      "max_response_bytes": 262144,
      "conditional": true,
      "inbox_bytes": 262144
    }
  },
  "sink": {"kind": "http", "url": "https://sink.example.com/ingest"}
}
```

还需 `put_allow` 允许 `api.example.com:443`。

| 合同项 | HTTP Poll 的实现 |
|---|---|
| 语义 | `live_best_effort` / `restart_fresh` / replay `unsupported`；拒绝 restore、checkpoint、aligned |
| 调度 | 启动立即轮询一次，之后每 `interval_ms`（100 ms..24 h） |
| 背压 | 至多一个在途请求；上一响应的行全部处理完毕（入队或计数丢弃）前不发新请求；错过的 tick 计入 `http_poll_skipped_ticks` |
| 内存 | 响应体硬上限 `max_response_bytes`（默认 256 KiB，最大 1 MiB，≤ job reservation/2），先记 Reservation 再增长；`content-length` 超限直接拒绝；记录切分为惰性扫描，无索引放大；每条解码前按 `record_bytes * 64 + schema_fields * size_of(Scalar) * 2 + 4096` 预扣 Reservation，持续覆盖解码及入队等待，预算不足不解码并计入 `http_poll_dropped_budget` |
| 超时/重试 | 单请求 `timeout_ms`（10 ms..60 s）；失败退避从 interval 倍增至 `backoff_max_ms`（默认 max(interval, 60 s)，上限 max(interval, 1 h)），成功后复位；无请求内重试、不跟随重定向 |
| 条件请求 | `conditional: true` 时发送 `If-None-Match` / `If-Modified-Since`；304 计入 `http_poll_not_modified`；校验值与 `http_poll_ok` 只在完整入队后更新，解码/大小/预算丢行、失败或取消均不推进校验值；压力解除后同一版本仍可恢复，重试可能重复此前已入队的行 |
| 认证 | bearer / basic 仅 secret 引用；自定义头 `value` 或 `value_secret` 二选一；保留头（Authorization、Host、Content-Length 等）拒绝；需 https |
| 解码 | 复用 `decode_json_row`；记录字节上限在解码展开预算检查前拒绝，保留 `fail_on_decode` 语义；单条解码失败计数丢弃，`fail_on_decode` 时 job 失败；结构错误计入 `http_poll_bad_responses` |
| 指标 | `http_poll_{requests,ok,not_modified,failed,timeouts,status_errors,oversize,bad_responses,rows,dropped_bad,dropped_oversize,dropped_budget,skipped_ticks,backpressure_waits,inflight,inbox_items,inbox_bytes}` |

限制：仅 GET；不支持从包络字段（如 `{"data":[...]}`）提取记录；不按 DNS 解析
结果再次校验 IP；无私有 CA；数组中途出现结构错误时已入队的行保留；
密钥轮换需重启；持续 401 只退避不使 job 失败。

---

## 附：NATS Core Source / Sink（`kind: "nats"`，feature `nats`）

完整说明与 Core/JetStream 对照表见 [NATS.md](NATS.md)。

| 合同项 | NATS Core 的实现 |
|---|---|
| 语义 | Source / Sink 均为 `live_best_effort` / `restart_fresh` / replay `unsupported`，at-most-once、无 ACK；拒绝 restore、checkpoint、aligned（Sink 单独出现也拒绝） |
| 内存 | SDK 缓冲 `capacity × (max_payload_bytes + 8 KiB) + 256 KiB` 记入 job reservation（≤ reservation/2）；服务器 `max_payload` 大于配置时 Source 拒绝连接；inbox 按 `inbox_bytes` 计入 queue 账本 |
| 背压 | inbox 满时 Source 等待；SDK 订阅缓冲满后丢弃并计 `nats_source_slow_consumer`（下界）；Sink outbox 有界，发布超时计 `nats_sink_failed` |
| 重连 | 每次断线 ≤ `reconnect_attempts`（1..100，拒绝 0=无限），100 ms→2 s 退避；耗尽后 Source 可重试失败、Sink 退避重开 |
| 认证 | 仅 `token_secret`，必须 `tls://`；URL 不得含 userinfo；端点经 `TargetPolicy` |
| 关闭 | Sink 在 `flush_timeout_ms` 内发布已排队批次、flush，剩余计 `discarded_on_close`；等待 SDK 真实退出后退款 |
| 指标 | `nats_source_*` / `nats_sink_*`，pipeline status 中的 `nats_source` / `nats_sink` 对象 |
