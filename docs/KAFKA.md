# Kafka Source / Sink（`kind: "kafka"`，feature `kafka`）

Source 以消费组读取一个 topic，把每条记录的 value 解码为一行；Sink 把每行写成一条
记录。构建：`cargo build -p sparrow-server --features kafka`。默认构建不包含；未启用时
spec 校验返回 `FeatureUnavailable`。

## 客户端选择

使用 `rdkafka =0.39.0`（librdkafka 2.12.1，`rdkafka-sys` 从源码构建；features
`tokio` + `libz-static`，无 SSL / SASL / zstd）。构建需要 gcc、g++、make、perl。

没有选纯 Rust 客户端，原因（按固定版本源码 / README 核对）：

- `rskafka 0.6.0`：README 明确不支持 offset 跟踪、消费组和事务；协议实现中只有
  InitProducerId，没有幂等生产者的序号管理。本需求的核心（消费组、分区分配与
  rebalance、提交 offset、幂等 `acks=all`）都需要自己实现。
- `samsa 0.1.8`、`kafka 0.10`：不成熟或停更，没有增量（cooperative）rebalance。

librdkafka 提供消费组协议（cooperative-sticky）、rebalance 回调、带 metadata 的
提交、幂等生产者和投递报告，这些是本连接器的语义基础。代价是 C 依赖与工具链，
以及 librdkafka 内部内存不在 Sparrow 计账内（见“内存”）。

## 用法

```json
{
  "version": 1,
  "stream": "telemetry",
  "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 30",
  "source": {
    "kind": "kafka",
    "inbox_capacity": 64,
    "fail_on_decode": false,
    "kafka": {
      "brokers": ["10.0.0.5:9092", "10.0.0.6:9092"],
      "topic": "telemetry",
      "group_id": "sparrow-telemetry",
      "auto_offset_reset": "earliest",
      "max_message_bytes": 65536,
      "fetch_max_bytes": 262144,
      "prefetch_bytes": 524288,
      "commit_interval_ms": 1000,
      "session_timeout_ms": 10000,
      "max_poll_interval_ms": 300000,
      "stop_timeout_ms": 5000
    }
  },
  "sink": {
    "kind": "kafka",
    "outbox_capacity": 64,
    "kafka": {
      "brokers": ["10.0.0.5:9092"],
      "topic": "alerts",
      "key_column": "device_id",
      "idempotence": true,
      "max_in_flight": 128,
      "queue_bytes": 1048576,
      "linger_ms": 5,
      "delivery_timeout_ms": 30000,
      "max_message_bytes": 65536,
      "flush_timeout_ms": 5000
    }
  }
}
```

`brokers` 为 1..=8 个 `host:port`（IPv6 写 `[addr]:port`），不接受 scheme、userinfo、
逗号。只支持 plaintext。公共字段：`client_id`、`socket_timeout_ms`（1..=60 s）、
`request_timeout_ms`（1..=60 s，阻塞的 metadata / committed-offset 请求）、
`policy_check_interval_ms`（1..=600 s，默认 30 s）。

## Source 语义

作业合同是 `live_best_effort` / `restart_fresh`，replay `unsupported`：Sparrow 不做
checkpoint，拒绝 `restore`、`checkpoint`、`checkpoint_dir`、`aligned`。续读位置是
消费组在 Kafka 中的已提交 offset。

### 提交点

- 一个专用线程拥有 librdkafka consumer：poll、rebalance 回调、同步提交、metadata
  查询都在该线程，不占用 runtime worker。线程一次交出一条消息，等到异步侧给出
  结果后才再次 poll；因此 rebalance 回调执行时没有在途消息。
- 只有“已处理”的消息会推进 offset（`offset + 1`）：其行已进入 job inbox，或在
  skip 策略下作为毒消息被跳过。已交出但未进入 inbox 的消息（停止、失败）不提交，
  下次从它开始。
- 提交时机：每 `commit_interval_ms` 异步提交；分区被撤销前同步提交该分区；停止时
  同步提交。异步提交在 commit 回调确认前会被再次提交，失败不会被遗忘。
- 结果：进入 inbox 之前是至少一次（提交未落地时，例如分配丢失或进程崩溃，会重投）；
  进入 inbox 之后若进程崩溃，尚未写到 Sink 的行丢失（没有 Sparrow checkpoint）。
  没有实现“Sink 确认后才提交”的 checkpoint 模式：Sparrow 的对齐 checkpoint 只覆盖
  File / JetStream 的既有 profile，Kafka 不是参与方。

### 消费组与 rebalance

- `partition.assignment.strategy=cooperative-sticky`，自动提交关闭
  （`enable.auto.commit=false`、`enable.auto.offset.store=false`）。
- Revoke：同步提交被撤销分区的已处理 offset，再移除跟踪。如果 librdkafka 报告
  assignment lost（会话超时等），不提交（计 `kafka_source_lost`），这些分区的新
  拥有者从最后一次已落地的提交开始，可能重投。
- 下游背压期间线程不 poll，librdkafka 后台线程继续心跳；但 rebalance 要等下一次
  poll 才处理。背压持续超过 `max_poll_interval_ms` 时 consumer 离开组，分配丢失。

### 提交身份

每次提交的 metadata 为 `sparrow-kafka-v1:` + SHA-256（cluster id、topic、group、
topic 分区数、payload 格式及其选项）。分配分区时先读回已提交 offset：

- metadata 等于本身份：续读。
- 另一 Sparrow 身份（换了格式、protobuf descriptor、集群、分区数……）或没有身份
  （其他客户端写的）：Source 失败，`unsupported_restore`，不交出任何消息。
- 没有已提交 offset：按 `auto_offset_reset`。

绑定 cluster id 而不是 bootstrap 字符串：换一个 bootstrap 地址访问同一集群不应
拒绝，而同一地址指向新集群必须拒绝。topic 增加分区后旧提交被拒绝，需要新的
`group_id`。

### 起点

`auto_offset_reset` 必填，没有默认值：`earliest`、`latest`、`error`（分区没有
已提交 offset 或 offset 越界时 Source 以 `unsupported_restore` 失败）。

### 毒消息

以下消息是毒消息：value 超过 `max_message_bytes` 或格式上限、解码 scratch 超过
job 预算可能给出的额度、解码失败、行超过 inbox 上限。

- `fail_on_decode: false`：计数（`kafka_source_poison_skipped`，超限另计
  `kafka_source_oversize`，解码错误计 `decode_errors` 与格式计数器），视为已处理，
  提交越过它。
- `fail_on_decode: true`：Source 失败，不提交越过它；重启后再次遇到同一消息。

预算不足（payload 拷贝、解码 scratch、工作行、inbox 额度 / 槽位）一律等待
（可取消），不丢弃。value 为 null（tombstone）按空 payload 解码，通常是毒消息。

### 其他

- 启动时检查 topic 存在（否则 `invalid_argument`）并读取 cluster id。
- `isolation.level=read_committed`，`check.crcs=true`。
- 停止：取消在途 admission（该消息不提交），线程做最终同步提交并关闭 consumer。
  `stop_timeout_ms` 覆盖这两步；超时后线程被分离（计 `kafka_source_stop_timeouts`），
  可能在后台继续关闭，其静态预扣保留到线程真正结束。

## Sink 语义

- 每行一条记录。`key_column`（Utf8 / Bytes 列；NULL 为无 key）作为记录 key；
  其他类型的 key 行按坏行丢弃。分区由 librdkafka 默认分区器决定。
- 默认 `idempotence: true`：`enable.idempotence=true`、`acks=all`，librdkafka 在
  `delivery_timeout_ms`（`message.timeout.ms`）内重试，同一分区内不重复、不乱序。
  `idempotence: false` 时 `acks` 可为 `all` 或 `leader`，并设
  `message.send.max.retries=0`（重试可能重复 / 乱序，所以不重试）。幂等时
  `acks: leader` 被拒绝。
- 一个批次的每条记录投递报告都成功后才确认 outbox（batch receipt）。在途记录
  ≤ `max_in_flight`（也是 `queue.buffering.max.messages`），队列 ≤ `queue_bytes`；
  队列满时等待在途报告，不丢弃。
- 编码失败 / 超过 `max_message_bytes` 的行丢弃并计数，该批次 receipt 失败（与其他
  Sink 相同）。
- 任一投递报告失败（超时、broker 错误）或启动检查失败（广播地址被拒、topic 不存在）：
  Sink fail closed，health `Failed`，计 `kafka_sink_fatal`，之后每个批次的 receipt
  都失败，supervisor 让 job 失败。幂等生产者在 `message.timeout.ms` 超时后不保证
  无缺口，因此不继续写。
- 停止：`flush_timeout_ms` 覆盖当前批次剩余部分、已排队批次和未完成投递报告；
  到期后 purge 队列与在途请求，剩余计 `kafka_sink_discarded_on_close`。被 purge 的
  在途请求可能已被 broker 写入。之后关闭 producer（librdkafka 自身 flush 最多 500 ms，
  在阻塞线程上执行）。
- 作业重启后不去重：Sink 没有事务，上次未确认的批次可能重复写入。

## 内存

- Source 在生命周期内静态预扣 `prefetch_bytes + 2 × fetch_max_bytes + 256 KiB`
  （预取队列 `queued.max.messages.kbytes`、一个 fetch 响应及其解压副本、客户端固定
  缓冲），必须 ≤ job reservation 的 1/2；每条消息的 payload 拷贝在分配前记账，
  decode scratch 在解码前记账，行进入 inbox 按 `inbox_bytes` 计 Queue。
- Sink 静态预扣 `queue_bytes + 2 × max_message_bytes + 256 KiB`（≤ 1/2）；每行先按
  `encode_scratch` 记账，输出按增长记账（上限 `max_message_bytes`）。
- 所有 Kafka / NATS / DataBus / WebSocket 端点的静态预扣合计 ≤ 3/4（饱和算术）。
- librdkafka 的设置：`fetch.max.bytes = max.partition.fetch.bytes = fetch_max_bytes`、
  `receive.message.max.bytes = fetch_max_bytes + 512`（单个响应的硬上限）、
  `queued.max.messages.kbytes = prefetch_bytes / 1024`。
- 这些是配置上限，不是对 librdkafka 分配器的计量：metadata、请求缓冲、解压
  （gzip / snappy / lz4）等内部分配不在 Sparrow 计账内；预取队列按文档可被一个
  fetch 超出。zstd 未编译，zstd 压缩的 topic 无法消费。
- 比 `fetch_max_bytes` 大的 record batch：librdkafka 会尝试增大分区 fetch 尺寸，但
  响应受 `receive.message.max.bytes` 限制，失败的 fetch 计入 `kafka_source_errors`
  并重试，消费停滞，不会跳过。`fetch_max_bytes` 应不小于 topic 最大 batch。

## 白名单

- 每个 bootstrap `host:port` 经 `TargetPolicy` 检查。
- broker 在 metadata 中广播的地址在启动时与每 `policy_check_interval_ms` 检查一次；
  不在白名单时 Source 失败（`policy_denied`），Sink fail closed。
- 残余风险：rdkafka 0.39 没有连接前的地址钩子，librdkafka 可能在周期检查之前就连接
  了广播地址。网络层隔离仍是必要的。

## 指标

`kafka_source_*` / `kafka_sink_*`（`/metrics` 的 io 字段），pipeline status 中的
`kafka` 对象。主要计数器：`source_received`、`source_rows`、`source_poison_skipped`、
`source_oversize`、`source_budget_waits`、`source_backpressure_waits`、
`source_commits`、`source_commit_failed`、`source_assigned`、`source_revoked`、
`source_lost`、`source_errors`、`source_stop_timeouts`、`sink_acked`、`sink_failed`、
`sink_dropped_oversize`、`sink_dropped_bad`、`sink_queue_full_waits`、
`sink_discarded_on_close`、`sink_fatal`。

## 测试

单元测试覆盖配置上限（含 `usize::MAX`、饱和预扣）、身份敏感性、librdkafka 设置。
真实 broker 测试默认 `#[ignore]`，需要固定版本并校验 checksum 的 Kafka 4.3.1
（KRaft 单节点）与 Temurin 21 JRE：

```sh
eval "$(scripts/kafka-broker.sh)"
cargo test -p sparrow-connectors --features kafka -- --ignored --test-threads=1 kafka::
cargo test -p sparrow-control --features kafka -- --ignored --test-threads=1 kafka_tests::
```

每个测试在回环端口、临时目录启动独立 broker 子进程。覆盖：提交只含进入 inbox 的行
（背压下停止后续读）、重启从已提交 offset 续读、身份不符 / 外部提交 / 分区数变化被拒、
`earliest` / `latest` / `error`、毒消息 skip / fail、两个 consumer 间 rebalance 无丢失
无重复、三种格式经 Sink → Source 往返、`acks=all` 幂等与 `leader` 非幂等投递、
broker 重启后生产与消费继续、投递超时 fail closed、Source / Sink 停止截止时间、
广播地址不在白名单与 topic 不存在、控制面 SQL 过滤管道与重启续读。
