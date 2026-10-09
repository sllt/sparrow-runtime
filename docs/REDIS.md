# Redis Sink 与 Redis Lookup

Redis 以两种方式接入：

- **Sink**（`sink.kind: "redis"`）：每行执行一条命令，按批流水线发送。
- **外部 Lookup provider**（`external_lookups.<name>.redis`）：与 HTTP Lookup 共用同一个 runtime `ExternalLookup` 接口、Lookup 算子、缓存、选项和错误策略。

两者都无需 build feature。

客户端是仓库自带的最小 RESP2 实现（`crates/sparrow-connectors/src/redis/`），没有使用第三方 Redis crate。这样做有三个原因：

- 回复长度在读到头部时就检查。超过上限的 bulk 在缓冲之前就被拒绝，读缓冲固定为 `max_bulk + 4 KiB`。
- 没有隐式重连或隐式重发。是否重发由命令是否幂等显式决定。
- 读写缓冲都计入 job 内存额度。

**支持范围**

- 支持：Redis 6.2 / 7.x 单节点或主节点，RESP2 协议，ACL 用户。
- 不支持 Redis Cluster 和 Sentinel。收到 `MOVED` / `ASK` / `CROSSSLOT` / `CLUSTERDOWN` 时报错，不跟随重定向。
- 不支持 RESP3。

## 连接

```json
"url": "rediss://cache.example:6380/2",
"username_secret": "redis_user",
"password_secret": "redis_pw",
"ca_pem": "-----BEGIN CERTIFICATE-----\n...",
"connect_timeout_ms": 2000
```

**URL**

- 只接受 `redis://`（明文）和 `rediss://`（TLS）。
- 可选 `/db`，范围 0..=4095，默认 0。默认端口 6379。
- 拒绝 userinfo、query 和 fragment，凭据只能通过 secret 引用传入。

**认证**

- `password_secret` 单独使用时发送 `AUTH <password>`。
- 再加 `username_secret` 时发送 ACL 形式的 `AUTH <user> <password>`。只有 `username_secret` 而没有密码会被拒绝。
- 凭据只允许走 `rediss://`。在明文 `redis://` 上配置凭据会在校验阶段报 `PolicyDenied`。
- secret 不能为空且不超过 4 KiB，否则报 `SecretMissing`。

**TLS**

- 使用 rustls，证书校验始终开启，校验主机名。
- `ca_pem` 是 1..=16 张 PEM 证书（≤ 64 KiB），会替换内置根证书。

**白名单**：host:port 经过 `TargetPolicy` 检查，与其他出站连接相同。

**连接超时**：`connect_timeout_ms`（100..=60000，默认 2000）覆盖 TCP 建连、TLS 握手和流水线发送的 `AUTH` / `SELECT`。`AUTH` 被拒绝（`WRONGPASS` / `NOAUTH` / `NOPERM`）属于配置错误，不重试。

**诊断信息**：Debug 输出和错误信息中不包含主机、密码和用户名。

## Sink

```json
"sink": {"kind": "redis", "outbox_capacity": 16, "redis": {
  "url": "redis://127.0.0.1:6379",
  "command": "hset", "key": "device:{device_id}", "fields": ["temperature", "ts"],
  "pipeline_rows": 256, "pipeline_bytes": 262144, "flush_interval_ms": 10,
  "timeout_ms": 5000, "max_retries": 3, "retry_initial_ms": 100, "retry_max_ms": 5000,
  "flush_timeout_ms": 5000
}}
```

### 命令

| `command` | 每行发送 | 必填 / 可选 | 失败后重发 |
|---|---|---|---|
| `set` | `SET key value [PX ttl_ms]` | `key`；`value_column` 或整行 JSON；`ttl_ms` 1..=315360000000 | 无 TTL 才可重发；PX 重发会重置到期时间 |
| `hset` | `HSET key col val ...` 或 `HSET key field value` | `key`，加上 `fields`（1..=64 列，NULL 列跳过），或 `field` 模板加 `value_column` / JSON | 幂等，可重发 |
| `xadd` | `XADD key [MAXLEN =\|~ n] * col val ...` | `key`、`fields`；`maxlen` 1..=10^9；`approximate` 使用 `~` | **不幂等**，不重发 |
| `publish` | `PUBLISH channel value` | `channel`；`value_column` 或 JSON | **不幂等**，不重发 |
| `lpush` / `rpush` | `LPUSH/RPUSH key value` | `key`；`value_column` 或 JSON | **不幂等**，不重发 |

不属于所选命令的选项会被拒绝，例如 `xadd` 配 `ttl_ms`，或 `publish` 配 `key`。

**模板**：`key` / `channel` / `field` 是 1..=1024 字节的文本，用 `{列名}` 引用列（最多 16 个），`{{` / `}}` 转义花括号。

- 不允许两个占位符直接相邻。
- 不允许出现控制字符。
- 引用的列必须是 utf8 / bytes / int64 / uint64 / bool / timestamp 类型。float 列不能出现在模板中。
- 渲染结果超过 4096 字节，或者引用的列为 NULL 时，这一行计入 `redis_sink_dropped_bad`。

**值的文本形式**

| 类型 | 写入形式 |
|---|---|
| bool | `true` / `false` |
| timestamp | 十进制微秒 |
| float | 有限值的最短往返表示，例如 `0.5`、`1.0` |
| bytes | 原样写入 |
| JSON 值 | 与其他 Sink 相同的有界行编码器输出 |

`hset` / `xadd` 中所有字段列都为 NULL 的行同样计入 `dropped_bad`。

### 投递与重试

- **语义**：capability 为 `redis_sink`，`live_best_effort` / `restart_fresh`，replay `unsupported`。
  - 拒绝 restore、checkpoint 和 aligned，legacy Sink 和 graph Sink 都是如此。
  - checkpoint 不绑定目标与命令，而且 XADD / PUBLISH / PUSH 不幂等。
- **回执**：一批中每条命令都收到回复后，这一批才回执。
  - 期望的回复类型：`SET` → `+OK`；`HSET` / `PUSH` / `PUBLISH` → 整数；`XADD` → bulk id。
  - `PUBLISH` 返回 0（没有订阅者）仍算成功，另计入 `redis_sink_publish_no_receivers`。
  - 收到其他类型的回复视为协议错误，连接会被丢弃。
- **流水线**：同一时刻只有一个流水线在途。先完整写出，再按顺序读回复，依赖 Redis 自身缓冲回复。
  - 一个流水线受 `pipeline_rows`（1..=10000）、`pipeline_bytes`（1 KiB..=4 MiB）和 `flush_interval_ms`（1..=60000）约束。
  - outbox 有界。
- **错误回复**（如 `WRONGTYPE`、`OOM`）：该命令计入 `redis_sink_command_errors`，所在的批失败，不重试。同一流水线中其他命令的结果不受影响。
- **致命错误**：以下回复会停止 Sink 并使 job 失败（`redis_sink_fatal`）：
  - `NOAUTH` / `WRONGPASS` / `NOPERM`
  - `MOVED` / `ASK` / `CROSSSLOT` / `CLUSTERDOWN`
  - `READONLY`
  - 输入 schema 与命令不匹配（例如缺少列）
- **连接失败**：连接建立失败时，因为还没有发送任何命令，所有命令都可以按抖动指数退避重试，最多 `max_retries` 次（≤ 20）。
- **连接中断或超时**：已经发送后连接断开或超过 `timeout_ms`（100..=300000）时：
  - 无 TTL 的 `set` / `hset` 在新连接上从第一条未确认的命令开始重发。
  - 带 `ttl_ms` 的 `set`、`xadd` / `publish` / `lpush` / `rpush` **不会重发**。已发送但没有回复的命令计入 `redis_sink_unknown_outcome`（可能已经执行，也可能没有），这一批失败。
- **空闲连接检查**：发送前先检查空闲连接是否已被服务器关闭，若已关闭就重新建连，以减少不必要的 unknown outcome。这个检查不能保证发现所有断开的情况。
- **停止**：`flush_timeout_ms` 的截止时间同时覆盖在途流水线、重试等待和排队中的批次，不会每批重新计时。剩余的计入 `discarded_on_close`。

### 内存

- **预算估算**：`128 KiB 连接 + pipeline_bytes + pipeline_rows × (命令/回执槽 + 回执对象) + 当前批次回执 + 1 KiB`。JSON 值再加一份 `pipeline_bytes`，编译命令的工作集另计。
  - 必须 ≤ job reservation 的 1/2，计算使用检查过的算术。
  - 回执、编译命令和各缓冲均先预扣；命令/回执槽容量不超过 `pipeline_rows`。
  - 同一 child owner 对连接、映射、回执、流水线和编码 scratch 的实际合计占用施加半预算上限。
- **每行**：编码前先扣 scratch（JSON 行编码使用共享的 charged encoder）。
  - 额度不足时先 flush 再重试一次，仍然不足就计入 `dropped_budget`。
  - 单行命令超过 `pipeline_bytes` 时计入 `dropped_oversize`。
- **释放**：Sink 退出后，流水线和连接的额度全部归还。

### 指标

`redis_sink_*` 计数器：`commands_ok`、`pipelines`、`bytes_sent`、`command_errors`、`publish_no_receivers`、`dropped_bad`、`dropped_oversize`、`dropped_budget`、`retries`、`connects`、`connect_failures`、`unknown_outcome`、`discarded_on_close`、`fatal`。

这些计数出现在 `/metrics` 和 pipeline status 的 `redis_sink` 对象中。

## Lookup

```json
"external_lookups": {
  "limits": {
    "redis": {"url": "rediss://cache:6380", "password_secret": "redis_pw",
              "key": "limits:{site}:{device}", "format": "hash"},
    "fields": [{"name":"site","type":"utf8","nullable":false},
               {"name":"device","type":"int64","nullable":false},
               {"name":"threshold","type":"float64","nullable":true}],
    "keys": ["site", "device"],
    "options": {"max_inflight": 2, "batch_keys": 16, "timeout_ms": 200,
                "cache_ttl_ms": 1000, "cache_bytes": 65536,
                "cache_negative": true, "on_error": "fail"}
  }
}
```

每个外部 Lookup 必须声明且只能声明 `url`（HTTP）或 `redis` 中的一个。`header_secret` 只适用于 HTTP。

**key 模板**

- 必须恰好引用全部 key 列，不能引用非 key 列。
- 必须是单射的：只要某个 key 值包含下一段字面量的第一个字节，就会被拒绝（`InvalidArgument`，硬错误）。例如上面的模板中 `site` 不能包含 `:`。这样不同的 key 不会映射到同一个 Redis key。

**`format: "hash"`（默认）**

- 对非 key 字段发送 `HMGET key f1 f2 ...`，要求至少有一个非 key 字段。
- 全部为 nil 视为未命中。
- 部分为 nil 时：可空字段取 NULL；非空字段缺失属于协议错误（`CodecViolation`）。
- 文本按类型严格解析：
  - 整数为十进制，不允许前导 `+` 或空格，不允许溢出。
  - bool 接受 `true` / `false` / `1` / `0`。
  - float 必须是有限值。
  - timestamp 是十进制微秒。
  - bytes 原样保留。
  - utf8 必须是合法的 UTF-8。

**`format: "json"`**

- 发送 `GET key`，值必须是扁平 JSON 对象：不嵌套、不含数组、最多 64 个成员、≤ 64 KiB。
- 声明的字段按共享 JSON 行解码器读取，未声明的成员会被忽略。
- 对象中必须包含 key 列，且值必须与请求的 key 完全相等，否则 Lookup 算子报 `CodecViolation`。
- key 不存在时视为未命中。

**批量**

- `options.batch_keys`（1..=64，默认 1）大于 1 时，每次请求把最多这么多个未命中的 key 放进一条流水线，一次写出，按顺序读回。
- 同时最多有 `max_inflight` 个请求在途。`max_inflight` 也是连接池的大小（1..=16）。
- 只有 Redis provider 支持批量。HTTP provider 每个请求只发一个 key，`batch_keys > 1` 会在校验时被拒绝。

**超时与重连**

- `timeout_ms` 是整个请求的截止时间，包括等待连接池、建连、写入和读完所有回复。
- 复用的空闲连接如果在收到任何回复之前就断开，会在新连接上重发一次。这是只读命令，重发是安全的。其他情况下连接断开属于 transport 错误。
- 只有读到边界、没有剩余字节的连接才会放回连接池。

**错误分类**：`on_error` 只对 transport 类错误生效，其他错误都是硬错误。

| 情况 | 错误码 | 受 `on_error` 影响 |
|---|---|---|
| 超时、连接失败或断开、`LOADING` / `BUSY` / `READONLY` 等错误回复 | `JobFailed` | 是 |
| `WRONGTYPE` | `TypeMismatch` | 否 |
| `NOAUTH` / `WRONGPASS` / `NOPERM` | `PolicyDenied` | 否 |
| `MOVED` / `ASK` 等 Cluster 回复 | `FeatureUnavailable` | 否 |
| 解码失败、超过 64 KiB、key 不一致 | `CodecViolation` / `BoundExceeded` | 否 |

**内存**

- provider 声明的 scratch 为每个 key 384 KiB，包括请求、一次读缓冲（64 KiB + 行）、一次有界 flat JSON 解析和返回的行；hash 所有字段载荷合计超过 64 KiB 时，在继续复制/解码前拒绝。
- 算子另外为每个 key 计 64 KiB + 1 KiB。
- `max_inflight × batch_keys` 个 key 的窗口必须 ≤ job reservation 的 1/2。例如默认 4 MiB 预算下最多约 4 个 key。窗口超出时在构建时就被拒绝。
- TLS 和 socket 内部缓冲不计入额度（与 HTTP 相同）。

## 共享 Lookup 选项（HTTP 与 Redis）

`ExternalLookupOptions` 新增两个选项。省略时序列化结果与之前完全相同，已有的 HTTP 配置行为不变。

- `batch_keys`：见上文的批量说明。
- `cache_negative`（默认 `true`）：为 `false` 时只缓存命中结果，未命中每次都查询远端。

`on_error` 新增 `"drop"`：transport 类错误发生时丢弃对应的输入行，计入 `lookup_runtime` 的 `error_drops`。原有的 `"null"` 是把这些行的 Lookup 字段填为 NULL。批量请求失败时，`drop` 和 `null` 作用于这一批中的每一个 key。

错误不会进入缓存。

## 测试与边界回归

- 真实服务用例默认 ignored，缺少服务时不再假通过；配置 `SPARROW_REDIS_SERVER` 并传 `--include-ignored` 才执行。
- `websocket-contracts` 的 standalone / jetstream profile 分别验证 Redis 6.2.24 / 7.2.16（官方 SHA256 固定、TLS 构建），同一测试二进制重复两轮。
- 回归覆盖 PX 未知结果不重发、部分未请求回复污染连接、HMGET 累计大小、回执计费、停止期限与 RESP 行长度边界。
