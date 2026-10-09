# InfluxDB Sink（`sink.kind: "influxdb"`）

状态：Preview，已实现，未生产认证。合同对照见
[CONNECTORS.md](CONNECTORS.md#附influxdb-sinksinkkind-influxdb)。

English summary: writes output rows as InfluxDB **line protocol** to the
InfluxDB **v2** HTTP API (`POST <url>/api/v2/write?org=..&bucket=..&precision=..`)
over HTTPS with an API token taken from a secret reference. It is
`live_best_effort` / `restart_fresh`; checkpoints, restore and aligned recovery
are refused. A batch receipt is acknowledged only after HTTP 204 for every
request that carried its rows. Verified against a real InfluxDB OSS 2.9.1
binary locally (opt-in test); CI runs the in-process HTTPS mock only.

## 用法

```json
{"stream":"telemetry",
 "sql":"SELECT device_id, temperature, ts FROM telemetry",
 "source":{"kind":"mqtt", "...":"..."},
 "sink":{"kind":"influxdb","influxdb":{
   "url":"https://influx.example:8086",
   "org":"acme","bucket":"iot","token_secret":"influx-token",
   "measurement":"telemetry",
   "tags":["device_id"],
   "time_column":"ts","precision":"ms"}}}
```

每行输出变成一个 point：

```
telemetry,device_id=d\ 1 temperature=21.5 1700000000123
```

## 参数（`sink.influxdb`）

| 字段 | 默认 | 范围 | 说明 |
|---|---|---|---|
| `url` | 必填 | `https://` | 基础地址，可带路径前缀（如反向代理下的 `/influx`）；禁止 userinfo、query、fragment。实际写入 `<url>/api/v2/write` |
| `org` / `bucket` | 必填 | 1..=256 字节，无控制字符 | 作为 query 参数（URL 编码） |
| `token_secret` | 必填 | secret 引用 | 解析出的 token 以 `Authorization: Token <token>` 发送；token 不出现在日志 / Debug / status |
| `ca_pem` | 无 | ≤64 KiB PEM | 替换内置 webpki 根证书（私有 CA）；证书校验始终开启，没有 skip-verify |
| `measurement` | 二选一 | 名称规则见下 | 固定 measurement |
| `measurement_column` | 二选一 | `utf8` 列 | 每行的 measurement；不合法的值按行丢弃（`dropped_bad`） |
| `tags` | `[]` | ≤64，`utf8` 列 | tag 列；按 key 字节序输出；null / 空串不输出 |
| `fields` | 其余列 | ≤256 | field 列；默认是除 measurement 列、tag 列、时间列外的所有列 |
| `time_column` | 无 | `timestamp` 列 | 没有时 InfluxDB 用接收时间打点 |
| `precision` | `us` | `ns`/`us`/`ms`/`s` | 需要 `time_column`；`ms` / `s` 向下取整（负时间远离 0） |
| `batch_rows` | 5000 | 1..=100000 | 每个请求最多行数 |
| `batch_bytes` | 256 KiB | 1 KiB..=4 MiB | 每个请求未压缩 body 上限；单行超过即丢弃（`dropped_oversize`） |
| `flush_interval_ms` | 1000 | 10..=60000 | 首行进入缓冲后最多等待多久发送 |
| `gzip` | false | | `Content-Encoding: gzip` |
| `timeout_ms` | 10000 | 100..=300000 | 单次请求总超时 |
| `connect_timeout_ms` | 5000 | 100..=60000 | |
| `max_retries` | 5 | 0..=20 | |
| `retry_initial_ms` / `retry_max_ms` | 200 / 30000 | 10..=60000 / 100..=300000 | 指数退避 `initial × 2^(n-1)`，封顶 `retry_max_ms`，再在 `[d/2, d]` 内均匀抖动 |
| `flush_timeout_ms` | 5000 | 10..=60000 | 停止时的总截止时间（见“关闭”） |

顶层 `outbox_capacity` 仍是 outbox 批次上限。HTTP Sink 的顶层字段（`url`、`batch_rows`、
`linger_ms`、`action` 等）与 `sink.influxdb` 混用会被拒绝。

### 类型映射

| 列类型 | 作为 field | 作为 tag / measurement / 时间 |
|---|---|---|
| `bool` | `true` / `false` | — |
| `int64` | `123i` | — |
| `uint64` | `123u` | — |
| `float64` | 最短往返十进制（`1.0`、`1e300`）；NaN / ±Inf 按行丢弃 | — |
| `utf8` | `"..."`，转义 `"` 与 `\`；≤64 KiB | tag 值 / measurement |
| `timestamp` | — | 时间列 |

其他类型（`bytes`、`dynamic`、`array`、`struct`、`map`）作为 field 在校验时拒绝（`InvalidSchema`）。
校验在 validate 时针对 Sink 输入 schema 进行（列存在、类型匹配、名称规则）。

### 名称与转义

按 InfluxDB v2 line protocol：measurement 转义 `,` 与空格；tag key / tag value / field key
转义 `,`、`=`、空格。以下情况 InfluxDB 2.9.1 无法原样保存，Sink 拒绝而不是改写：

- 任何名称或值中的 `\n` / `\r`（2.9.1 会从字符串中静默删除 `\r`）；
- 名称或 tag 值以 `\` 结尾（会转义后面的分隔符，2.9.1 返回 400）；
- measurement 或 field key 中 `\` 紧跟 `,`、`=` 或空格：2.9.1 对这种 field key 返回 400，
  对这种 measurement **返回 204 但静默丢弃该 point**（本地实测）。tag key / tag value 中同样的
  序列可以原样往返，不受此限制；
- measurement 以 `#` 开头（整行被当作注释）、名称以 `_` 开头（保留前缀）、名称超过 256 字节、`time` 作为 tag / field
  key、同一列重复映射、空名称；
- 时间换算成纳秒后超出 `±9223372036854775806`。

固定名称在 validate 时拒绝（`InvalidArgument`）；来自行数据的值按行丢弃（`dropped_bad`），
该行所在批次的回执失败。

## 交付与回执

- 请求串行发送（同一时间最多一个在途请求，InfluxDB 建议串行写入以保持顺序）。
- 一个 outbox 批次只有在其所有行都成功编码、且所有携带其行的请求都得到 2xx（通常 204）后才回执成功；
  否则回执失败。多个批次可以合并进一个请求，一个批次也可以拆到多个请求。
- **live-only**：capability `influxdb_sink` 为 `live_best_effort` / `restart_fresh` / replay
  `unsupported`。restore、checkpoint、`checkpoint_dir`、aligned、非 live delivery 一律拒绝
  （`UnsupportedRestore`，含 graph 中的 InfluxDB Sink）。原因：checkpoint 尚未绑定写入目标
  （url / org / bucket / precision / measurement / tag / field 映射），无法在目标变化时拒绝恢复。

## 状态码

| 响应 | 行为 |
|---|---|
| 2xx | 成功；读取 ≤64 KiB 响应体后复用连接 |
| 400 | 请求被拒（InfluxDB 对格式错误的请求整体不写入），不重试，计 `rejected_requests` / `rejected_rows` |
| 413 | 请求过大，不重试，同上；应调小 `batch_bytes` |
| 422 | **部分写入**：能写的 point 已保存，其余被丢弃（例如字段类型冲突）。不重试，计 `partial_writes`，批次回执失败 |
| 401 / 403 / 404 | token 无效 / 无权限 / org 或 bucket 不存在：Sink 停止并使 job 失败（`fatal`，状态 `failed`，错误信息提示 InfluxDB Sink） |
| 429 / 503 | 仅有 `time_column` 时重试；无时间列则拒绝该请求并使批次回执失败。`Retry-After` 为秒数时用它代替退避（超过 `retry_max_ms` 时封顶并计 `retry_after_capped`）；HTTP-date 形式不解析，按退避 |
| 其他 4xx / 5xx（含 500） | 不重试，同 400 计数 |
| 传输错误 | 有 `time_column` 时重试（同一 point 覆盖写，幂等）；没有 `time_column` 时只重试连接建立失败（请求未发出），超时 / 连接中断不重试，因为服务端可能已写入，重发会以新的接收时间产生重复 point |

重试耗尽后批次回执失败，不会使 job 失败。schema 与映射不匹配（运行时）同样使 job 失败。

## 内存

每个请求的缓冲在每次容量增长前向 job reservation 申请额度（不足时先发送已缓冲的行再重试一次，
仍不足则该行计 `dropped_budget`），并持有到请求（含所有重试）结束。开启 gzip 时，压缩前先申请
压缩器 scratch（384 KiB，按 miniz_oxide 0.9.1 的 `CompressorOxide` 堆分配估算）与输出上界；
申请失败则以未压缩 body 发送并计 `gzip_fallbacks`。

峰值 `batch_bytes + batch_rows × (Arc 槽位 + 回执对象额度) + 当前批次回执额度 + 72 KiB`
（+ gzip 时 `gzip_bound(batch_bytes) + 384 KiB`）
必须 ≤ job reservation 的一半（compact 为 2 MiB），在 validate 与 bind 时用检查过的算术判断，
溢出视为超限，不做静默钳制。例如 compact 下 `batch_bytes = 1 MiB` 不开 gzip 可以，开 gzip 拒绝。
回执对象在创建前单独预扣；请求只持有已成功编码的行所属批次，槽位容量不超过 `batch_rows`。
坏记录不积累回执，前一个请求失败也不会错误地判定尚未加入该请求的下一批失败。

## 关闭

第一次取消开始一个 `flush_timeout_ms` 截止时间，由在途请求（含其重试与退避等待）、已排队批次与
最后的缓冲共用。到期未确认的行计 `discarded_on_close`，回执失败。被中止的在途请求可能已经被
服务端写入。EOF（上游结束）时没有截止时间，缓冲会全部发送。
编码循环也观察停止并定期让出执行权；已过期时，立即就绪的操作不能绕过期限。

## 指标

pipeline status 的 `influxdb_sink` 对象与 `/metrics` 中的 `influxdb_sink_*`：
`rows_written`、`requests_ok`、`bytes_sent`（实际发送的 body 字节，gzip 后）、`gzip_fallbacks`、
`retries`、`retry_after_waits`、`retry_after_capped`、`dropped_bad`、`dropped_oversize`、
`dropped_budget`、`rejected_requests`、`rejected_rows`、`partial_writes`、`discarded_on_close`、`fatal`。

## 测试范围

- CI：line protocol 编码单元测试（转义边界、类型、精度、取整、范围）；进程内 HTTPS mock 检查
  精确 body 字节、URL、header、gzip、批次边界、重试 / Retry-After、终止状态码、fatal、停止截止、
  预算拒绝；control 层 spec 矩阵、validate、Store → Supervisor → File → SQL → Sink 端到端与 401 失败。
- 真实服务测试：默认标记 ignored，不再未配置服务就显示通过；设置 `SPARROW_INFLUXD=<influxd 路径>`
  并传 `--include-ignored` 后运行真实 InfluxDB OSS 2.x（HTTPS，
  测试证书）：特殊字符 / 类型 / 精度往返（gzip）、422 部分写入、401 / 404 fatal。已用 2.9.1
  （linux amd64 发布包，sha256 `762e4fc825c4386e0c5138e7c3f91fc778081db2bada1ec47066e786bf55d9ff`）验证。
  `websocket-contracts` CI 的 standalone / jetstream profile 都下载并校验上述固定服务版本，
  使用同一批测试二进制重复两轮（含真实服务往返，非仅 mock）。

## 未承诺

- InfluxDB 1.x、3.x、Cloud 的兼容性未测试；只针对 v2 `/api/v2/write`。
- 不提供 at-least-once / exactly-once / checkpoint 恢复。
- 403、413、429、503 与 `Retry-After` 只用 mock 测试过（真实服务端测过 204、422、401、404）。
- 没有时间列的写入在超时或断连后不会重发，这些行可能丢失也可能已写入。
