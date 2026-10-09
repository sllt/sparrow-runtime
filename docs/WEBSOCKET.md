# WebSocket Source / Sink（`kind: "websocket"`，feature `websocket`）

WebSocket **客户端**：Source 连接一个 `ws://` / `wss://` 端点，把收到的消息解码为行；
Sink 连接一个端点，每行发送一条消息。两者都是 `live_best_effort` / at-most-once：
WebSocket 没有应用层确认，也没有历史可重放。

构建：`cargo build -p sparrow-server --features websocket`。默认构建不包含；未启用时
spec 校验返回 `FeatureUnavailable`。feature 只增加 `tokio-tungstenite`（仅 `handshake`，
不带它自己的 TLS/connect 栈）；TCP、TLS（rustls + webpki-roots）沿用已有依赖。

## 用法

```json
{
  "version": 1,
  "stream": "telemetry",
  "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 30",
  "source": {
    "kind": "websocket",
    "inbox_capacity": 64,
    "websocket": {
      "url": "wss://feed.example.com/v1/stream?site=7",
      "auth": {"type": "bearer", "token_secret": "env:FEED_TOKEN"},
      "headers": [{"name": "X-Tenant", "value": "plant-7"}],
      "subprotocols": ["telemetry.v1"],
      "ping_interval_ms": 10000,
      "idle_timeout_ms": 30000,
      "reconnect_max_ms": 5000,
      "reconnect_attempts": 10,
      "max_message_bytes": 65536,
      "framing": "message"
    }
  },
  "sink": {
    "kind": "websocket",
    "outbox_capacity": 64,
    "websocket": {
      "url": "wss://alerts.example.com/ingest",
      "headers": [{"name": "X-Api-Key", "value_secret": "alerts.api_key"}],
      "queue_capacity": 16,
      "overflow": "block",
      "send_timeout_ms": 5000,
      "flush_timeout_ms": 2000
    }
  }
}
```

还需 `put_allow` 允许 `feed.example.com:443` 与 `alerts.example.com:443`
（URL 未写端口时 `ws` 为 80、`wss` 为 443）。

## 只做客户端模式（决定）

不提供 listen / server 模式。现有 HTTP push 入口是自己实现的请求/响应服务器
（每个请求一条记录、有界请求体、立即回状态码），没有连接升级和长连接帧循环；
在其中加入 WebSocket 需要重做连接生命周期、每连接预算、背压和关闭语义，
不是“干净地挂上去”。需要被推送时，请让推送方使用 `http_push`，或在前面放一个
WebSocket 网关。listen 模式如需要，单独立项。

## 公共参数（`source.websocket` / `sink.websocket`）

| 参数 | 默认 | 范围 | 含义 |
|---|---|---|---|
| `url` | 必填 | `ws://` / `wss://`，≤ 4096 B | 不允许 userinfo（`user:pw@`）和 `#fragment`；查询串保留 |
| `auth` | 无 | `{"type":"bearer","token_secret":…}` / `{"type":"basic","username_secret":…,"password_secret":…}` | 只接受 secret 引用；需要 `wss://` |
| `headers` | `[]` | ≤ 16 个 | `{"name","value"}` 或 `{"name","value_secret"}` 二选一；`value_secret` 需要 `wss://`；保留头（`Host`、`Connection`、`Upgrade`、`Content-Length`、`Transfer-Encoding`、`Sec-WebSocket-*`）以及重复头（含设置了 `auth` 时的 `Authorization`）拒绝 |
| `subprotocols` | `[]` | ≤ 8 个不重复的 RFC 6455 token | 作为 `Sec-WebSocket-Protocol` 发送；服务器必须选中其中一个，否则握手失败 |
| `tls_ca_pem` | 无 | 内联 PEM，1..=16 张证书，≤ 64 KiB | **替换**内置 web PKI 根（私有 CA / 自签名）；证书与主机名校验始终开启，没有“跳过校验”选项；只能和 `wss://` 一起用 |
| `connect_timeout_ms` | 5000 | 100..=60000 | TCP + TLS + 升级握手的总时限；Source 的 Ping 发送也受它约束（Sink 的 Ping 受 `send_timeout_ms` 约束） |
| `ping_interval_ms` | 10000 | 100..=300000 | 每隔这么久发一个 Ping |
| `idle_timeout_ms` | 30000 | 200..=600000，且 > `ping_interval_ms` | 这么久收不到任何帧（含 Pong）即认为连接已死并重连 |
| `reconnect_max_ms` | 5000 | 100..=300000 | 重连退避上限 |
| `reconnect_attempts` | 10 | 1..=1000（拒绝 0 = 无限） | 一次断线内连续失败的次数上限；连接成功后清零 |
| `max_message_bytes` | 65536 | 1024..=1048576 | 单条消息 / 单帧上限，见“大小上限” |

Source 专有：`framing`（`message` 默认 / `ndjson`）、`binary_frames`（`drop` 默认 /
`decode`）、`inbox_bytes`（默认 256 KiB）。Source 级的 `format`、`fail_on_decode`、
`inbox_capacity` 照常使用。

Sink 专有：`frame`（`text` 默认 / `binary`）、`queue_capacity`（默认 16，1..=1024）、
`overflow`（`block` 默认 / `drop_newest`）、`send_timeout_ms`（默认 5000）、
`flush_timeout_ms`（默认 2000）；后两者范围 10..=60000。Sink 级的 `format`、
`outbox_capacity` 照常使用。

所有字段 `deny_unknown_fields`；`source.websocket` / `sink.websocket` 只能与
`kind: "websocket"` 同时出现，且不能混用 HTTP/MQTT/File/NATS/DataBus/plugin/action 字段。

## 安全

- **白名单 / SSRF**：主机和端口经 `TargetPolicy`（与 HTTP 相同的 deny-by-default 白名单，
  并拒绝 link-local / 云元数据 / 未指定地址）。与 HTTP 一样，不对 DNS 解析结果再次校验 IP。
- **凭据**：`auth` 和 `value_secret` 头只在 `wss://` 上允许，在 `bind` 时解析，
  缺失返回 `SecretMissing`。错误信息、`Debug` 输出和状态中**从不**出现 URL、
  头的值或凭据；握手被拒只报告 `HTTP <status>`（例如 401）。
- **TLS**：rustls，强制证书校验；`tls_ca_pem` 只是换一组信任根。

## 心跳与重连

- 每个连接按 `ping_interval_ms` 发 Ping，计 `*_pings_sent`。
- 等待读的时候超过 `idle_timeout_ms` 没收到任何帧 → 计 `*_heartbeat_timeouts`，
  断开并重连。Source 因自身 inbox 满而阻塞的时间**不算**空闲（不会因为下游慢就被误判断线）。
- 断线（对端 Close、读写错误、心跳超时、超大消息）计 `*_disconnects`，
  然后按指数退避重连：从 100 ms 起翻倍，封顶 `reconnect_max_ms`，带随机抖动
  （实际等待在 [d/2, d] 内均匀取值）。断线后的**第一次**重连前也会退避，
  所以频繁接受后立即断开的服务器不会被打成忙循环。
- 一次断线内连续失败 `reconnect_attempts` 次后：
  - Source 以可重试的 `JobFailed` 结束（由 supervisor 按 restart 策略处理）；
  - Sink 记 `websocket_sink_fatal`、取消 job，job 以可重试的失败结束（fail closed，
    不会悄悄吞掉数据）。
- 重连后 Source 从**当前**开始接收；断线期间服务器发出的消息丢失（at-most-once）。

## 大小上限

`max_message_bytes` 同时设置 tungstenite 的 `max_frame_size` 与 `max_message_size`
（已对照锁定的 tungstenite 0.30 源码 `FrameCodec::read_frame` / `IncompleteMessage::extend`）：

- 单帧超限：读到帧头即拒绝，不为载荷预留缓冲。
- 分片消息超限：每追加一个分片前按累计长度检查，超限即拒绝；被拒的那个分片本身
  （≤ `max_message_bytes`）已经读入。

Source 遇到超大消息计 `websocket_source_dropped_oversize`，并且由于协议层已无法继续该连接，
断开后重连。解码前每条记录还按长度受格式的解码上限约束（JSON 64 KiB、CSV
`max_record_bytes`），超过的记录不解码、同样计入 `dropped_oversize`（连接保留）。
Sink 编码时输出有上限：超过 `max_message_bytes` 的行在增长越界前停止编码，计
`websocket_sink_dropped_oversize` 并丢弃。

## 帧与格式

| 方向 | 帧 | `json` | `csv` |
|---|---|---|---|
| Source | 文本帧，`framing: message` | 一条消息 = 一个 JSON 对象 | 一条消息 = （表头 +）一条记录，同 MQTT |
| Source | 文本帧，`framing: ndjson` | 一条消息 = 多行 NDJSON，空行跳过（只由 JSON 空白：空格、`\t`、`\r`、`\n` 组成的行；含 `\f` / `\v` 的行按记录解码） | **拒绝** |
| Source | 二进制帧 | `binary_frames: drop`（默认）计 `dropped_binary`；`decode` 时按 JSON 解码 | **拒绝** `decode`；只能 `drop` |
| Sink | `frame: text`（默认） | 每行一个 JSON 文本帧 | 每行一条（表头 +）记录，文本帧 |
| Sink | `frame: binary` | 每行一个 JSON 二进制帧 | **拒绝** |

CSV 只走文本帧；CSV 选项和 schema 检查在 spec 校验阶段完成。解码失败计
`websocket_source_dropped_bad`（及 `csv_*` 细分），`fail_on_decode: true` 时 job 失败。
Sink 中 `frame: text` 但编码结果不是合法 UTF-8 的行计 `dropped_bad`。

## Source：入口预算

与 NATS/HTTP Poll 相同的预算式入口：每行先在 inbox 中按 `inbox_bytes`
取得 Queue 额度再入队；额度/容量满时等待（计 `websocket_source_backpressure_waits`），
等待期间不读 socket，压力通过 TCP 窗口传回服务器；单行大于整个预算时计
`dropped_budget`。inbox 占用见 `websocket_source_inbox_items` / `inbox_bytes`。

解码前，先按长度拒绝，再把该格式的解码工作集估算（`decode_scratch`，JSON 与 CSV
各自的估算）记入 job reservation，持有到该行入队或丢弃；额度不足时该记录不解析，
计 `dropped_budget`（有测试：额度不足的畸形记录不计 `dropped_bad`）。

## Sink：发送队列、溢出、停止

- 结构：pump（从 outbox 取批次、编码、放入有界队列）与 writer（持有连接、
  发送、心跳、重连）并行运行，共享一个 `queue_capacity` 条消息的队列。
- 批次中的每一行都放入队列（或计数丢弃）后才回执 outbox——“完成”是“交给了发送队列”，
  不是对端收到。
- 队列满：`block` 等待（计 `backpressure_waits`，压力传回 outbox / SQL）；
  `drop_newest` 丢掉新行并计 `dropped_overflow`。
- 单条发送超过 `send_timeout_ms`（对端不读、TCP 窗口塞满）计 `send_timeouts`，
  断开并重连；该消息丢失。发送错误计 `send_failed`。
- 服务器发给 Sink 的数据帧计 `ignored_frames` 并丢弃。
- 编码前先把编码临时额度记入 job reservation，输出每次扩容也先记账，并且有
  `max_message_bytes` 上限；额度不足的行不编码，计 `dropped_budget`，所在批次不回执。
  该额度持有到消息进入（已预扣的）发送队列为止。
- 停止：在 `flush_timeout_ms` 内把 outbox 和队列中剩余的行发完，然后发 Close 帧
  （计 `closes`，失败计 `close_failed`）；超时后剩余的计 `discarded_on_close`。
  停止发生在某次被阻塞的发送中时，该次发送也只等到 flush 截止时间。

## 内存

- 每个连接的读方向：128 KiB 固定开销 + 2 × `max_message_bytes`（读缓冲中的一帧载荷与
  一条在组装的分片消息；依据 tungstenite 0.30 的读路径）。Source 只发 Ping，按此记账。
- Sink 另加 2 × `max_message_bytes`（正在发送的消息，以及 tungstenite 把它复制进写缓冲
  后的帧）和 `queue_capacity × max_message_bytes`。默认 128 KiB + (2 + 2 + 16) × 64 KiB
  ≈ 1.4 MiB。单个端点必须 ≤ job reservation 的一半。
- 已编码但尚未进入队列的行由它的编码额度单独记账（见上）。
- 这些是账本估算，不是 RSS 上限（TLS、内核 socket 缓冲等不在内）。所有算术饱和，
  超大配置值在校验时以 `BoundExceeded` 拒绝（有测试）。
- 在 `bind` 时记入 job reservation；pipeline 中所有 WebSocket / NATS / JetStream /
  DataBus 端点合计 ≤ reservation 的 3/4，超出在校验时返回 `BoundExceeded`。
- Source 的 inbox 按 `inbox_bytes` 计入 queue 账本。

## 语义与恢复

capability `websocket` / `websocket_sink`：delivery `live_best_effort`、recovery
`restart_fresh`、replay `unsupported`。spec 中的 `aligned`、`checkpoint`、
`checkpoint_dir`、`restore` 或其他 delivery 一律以 `UnsupportedRestore` 拒绝
（Sink 搭配其他 Source 时也一样）。没有应用层确认：已发送 = 写进 socket，
不代表对端处理成功；断线、超时、停止时在途或排队的数据可能丢失，并在计数器中体现。

## 指标

pipeline status 中的 `websocket_source` / `websocket_sink` 对象，`/metrics` 的 io 字段：

- Source：`websocket_source_{received,rows,dropped_bad,dropped_oversize,dropped_binary,dropped_budget,backpressure_waits,connects,reconnects,disconnects,connect_failures,heartbeat_timeouts,pings_sent,inbox_items,inbox_bytes}`
- Sink：`websocket_sink_{sent,dropped_bad,dropped_oversize,dropped_overflow,dropped_budget,backpressure_waits,send_failed,send_timeouts,discarded_on_close,connects,reconnects,disconnects,connect_failures,heartbeat_timeouts,pings_sent,ignored_frames,closes,close_failed,fatal,queue_items}`

健康状态：断线时为 `Reconnecting`（原因如 `websocket_heartbeat_timeout`），
重连耗尽或 `fail_on_decode` 时为 `Failed`。

## 不在本次范围

- listen / server 模式（见上文）。
- permessage-deflate 压缩、分片发送、客户端证书（mTLS）、HTTP 代理。
- 连接时动态生成的认证（签名 URL、刷新 token）；密钥轮换需重启 job。
- 订阅握手（连接后先发一条订阅消息）；需要时可作为后续选项加入。
