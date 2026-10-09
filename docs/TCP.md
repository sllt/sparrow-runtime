# TCP Source / Sink（`kind: "tcp"`）

TCP **客户端**：Source 连接一个 `host:port`，按分帧把字节流切成记录并解码为行；
Sink 连接一个端点，每行写一条带分帧的记录。两者都是 `live_best_effort` / at-most-once：
裸 TCP 没有应用层确认，也没有历史可重放。

始终构建，不需要 cargo feature：只用已有的 tokio TCP、rustls（与 MQTT/WebSocket 共用）
和 socket2（TCP keepalive）。可选 TLS。

## 用法

```json
{
  "version": 1,
  "stream": "telemetry",
  "sql": "SELECT device_id, temperature FROM telemetry WHERE temperature > 30",
  "source": {
    "kind": "tcp",
    "inbox_capacity": 64,
    "tcp": {
      "host": "gateway.plant.local",
      "port": 7000,
      "framing": "lines",
      "max_frame_bytes": 16384,
      "oversize": "resync",
      "idle_timeout_ms": 60000,
      "keepalive_ms": 30000
    }
  },
  "sink": {
    "kind": "tcp",
    "outbox_capacity": 64,
    "tcp": {
      "host": "collector.example.com",
      "port": 9443,
      "tls": true,
      "framing": "length_prefixed",
      "length_bytes": 4,
      "queue_capacity": 16,
      "overflow": "block",
      "send_timeout_ms": 5000,
      "flush_timeout_ms": 2000
    }
  }
}
```

还需 `put_allow` 允许 `gateway.plant.local:7000` 与 `collector.example.com:9443`
（端口必填，没有默认值）。

## 只做客户端模式（决定）

不提供 listen / server 模式。被动接收意味着一个 job 同时持有多条对端发起的连接，
要做好需要：每连接的 inbox 公平性与预算（一条快连接不能饿死其他连接或吃光 job 的 queue
额度）、`max_connections` 上限与超限时的拒绝策略、绑定地址策略（默认不应监听 0.0.0.0）、
按对端地址的白名单，以及每连接的空闲 / 慢读处理。这些都不是在客户端代码上“顺手加上”的，
listen 模式单独立项。现在需要被推送时，可让推送方使用 `http_push`，或在前面放一个
监听端口的转发进程（例如 socat / vector），由 Sparrow 作为客户端去连它。

## 公共参数（`source.tcp` / `sink.tcp`）

| 参数 | 默认 | 范围 | 含义 |
|---|---|---|---|
| `host` | 必填 | DNS 名或 IP 字面量，≤ 253 B | 不带方括号、scheme、路径；IPv6 直接写地址 |
| `port` | 必填 | 1..=65535 | |
| `tls` | `false` | | rustls，强制证书与主机名校验，没有“跳过校验”选项 |
| `tls_ca_pem` | 无 | 内联 PEM，1..=16 张证书，≤ 64 KiB | **替换**内置 web PKI 根（私有 CA / 自签名）；需要 `tls: true` |
| `connect_timeout_ms` | 5000 | 100..=60000 | TCP 连接 + TLS 握手的总时限（一个截止时间，两步共用） |
| `keepalive_ms` | 无（关闭） | 1000..=7200000 | 开启 TCP keepalive，空闲这么久后开始探测（探测间隔取 min(该值, 75 s)） |
| `reconnect_max_ms` | 5000 | 100..=300000 | 重连退避上限 |
| `reconnect_attempts` | 10 | 1..=1000（拒绝 0 = 无限） | 一次断线内连续失败的次数上限；连接成功后清零 |
| `framing` | `lines` | `lines` / `length_prefixed` | 见“分帧” |
| `length_bytes` | 4 | 2 / 4 | 长度前缀宽度（大端），只能与 `length_prefixed` 一起写 |
| `max_frame_bytes` | 65536（`length_prefixed` + `length_bytes: 2` 时 65535） | 16..=65536 | 单条记录上限（行不含换行符 / 帧不含前缀）；`length_bytes: 2` 时显式写大于 65535 的值以 `BoundExceeded` **拒绝**，不会被静默截到 65535 |

Source 专有：`oversize`（`resync` 默认 / `disconnect`）、`idle_timeout_ms`
（默认 60000，100..=86400000）、`inbox_bytes`（默认 256 KiB）。Source 级的 `format`、
`csv`、`fail_on_decode`、`inbox_capacity` 照常使用。

Sink 专有：`queue_capacity`（默认 16，1..=1024）、`overflow`（`block` 默认 /
`drop_newest`）、`send_timeout_ms`（默认 5000）、`flush_timeout_ms`（默认 2000）；
后两者范围 10..=60000。Sink 级的 `format`、`csv`、`outbox_capacity` 照常使用。

所有字段 `deny_unknown_fields`；`source.tcp` / `sink.tcp` 只能与 `kind: "tcp"` 同时出现，
且不能混用 HTTP/MQTT/File/NATS/DataBus/WebSocket/plugin/action 字段（例如顶层
`host` / `port` / `tls` 会被拒绝，TCP 的这些参数只写在 `tcp` 块里）。

## 分帧

### `lines`

- 一条记录 = 以 `\n` 结尾的一行；行尾的 `\r` 一并去掉（接受 `\r\n`）。
- 空行跳过，不计数；JSON 另外跳过只含 JSON 空白（空格、`\t`、`\r`）的行。CSV 的空格、
  Tab 和去掉一次 CRLF 后剩余的 CR 必须交给 CSV 解码器，不可当空行丢弃。其他字节（如 `\x0b`、
  `\x0c`、Unicode 空白）不算空白，按记录解码（通常计 `dropped_bad`）。
- 长度检查在解码之前：缓冲区中超过 `max_frame_bytes` 仍没有换行即判为超长，
  不会继续缓冲（读缓冲是固定大小的，不会增长）。
- 超长处理：`resync` 计 `dropped_oversize`，丢弃到下一个 `\n` 后继续；
  `disconnect` 计 `dropped_oversize` 并断开重连。
- 断线 / EOF 时没有换行结尾的残余记录计 `dropped_partial`，**不解码**。

### `length_prefixed`

- 一条记录 = 大端无符号长度（`length_bytes` 2 或 4，默认 4）+ 该长度的负载。
- 长度在读到前缀时即检查：超过 `max_frame_bytes` 的帧**在分配或缓冲负载之前**拒绝，
  计 `dropped_oversize`。`resync` 按声明长度跳过（只计数，不缓冲）；`disconnect` 断开重连
  （对端声明 4 GiB 也不会分配内存）。
- 长度为 0 的帧按坏记录计 `dropped_bad`。
- 断线时被截断的帧计 `dropped_partial`。

读取对任意切分都正确：逐字节到达的记录、一次写入里多条记录、跨读的前缀都有测试覆盖。

### Sink

分帧由 Sink 自己添加：

- `lines`：编码结果去掉末尾的 `\n` / `\r` 后加一个 `\n`。编码结果中间含 `\n`
  或以 `\r` 结尾（去掉 `\n` 后）的行计 `dropped_bad` 并丢弃——否则会在对端被切成两条。
- `length_prefixed`：大端长度 + 负载；超过 `max_frame_bytes` 的行计 `dropped_oversize`
  并丢弃（`max_frame_bytes` 不会超过前缀可表达的上限，见上表）。
- 编码前先把编码器工作集记入 job reservation，输出缓冲每次扩容也先记账，输出在
  `max_frame_bytes`（行模式另加换行符）处截停，不会先完整编码再检查长度；额度不足的行计
  `tcp_sink_dropped_budget` 并丢弃，该批次不回执。这份额度一直持有到帧放进发送队列
  （队列槽位本身在静态 reservation 里）。
- CSV over `lines` 的表头：长度先不分配地算出，超过 `max_frame_bytes` 时整批计
  `dropped_oversize`、不发送、不回执（没有表头的 CSV 文档无法使用）。表头与该连接的第一条
  记录在同一次有界发送中先后写出，不拼接复制。
  表头上限不含末尾 LF，输出容量也有界；含 CR/LF 的列名拒绝，避免拆成多行。

## 格式

| 方向 | 分帧 | `json` | `csv` |
|---|---|---|---|
| Source | `lines` | 一行 = 一个 JSON 对象（NDJSON） | 每个连接是一份 CSV 文档：`header: true` 时连接后的第一行是表头，之后每行一条记录；`header: false` 按列位置；**拒绝** `csv.multiline` |
| Source | `length_prefixed` | 一帧 = 一个 JSON 对象 | 一帧 = （表头 +）一条记录，同 MQTT |
| Sink | `lines` | 每行一个 JSON 对象 | 每个连接先写一次表头（`header: true`），之后每行一条记录；重连后在新连接上重新写表头 |
| Sink | `length_prefixed` | 每帧一个 JSON 对象 | 每帧 = （表头 +）一条记录，同 MQTT |

Source 中表头无效（与 schema 不符等）计 `dropped_bad`（及 `csv_*` 细分），并断开重连，
以便在新连接上拿到新的表头。解码失败计 `tcp_source_dropped_bad`（及 `csv_*` 细分），
`fail_on_decode: true` 时 job 失败。所有记录另受 64 KiB 解码上限约束。

解码顺序：先按长度（不分配）检查格式上限（JSON 为 `max_bytes`，CSV 另受
`max_record_bytes` 约束），再把该格式的解码工作集记入 job reservation，然后才解析
（表头和记录都一样）。额度不足的记录计 `tcp_source_dropped_budget`、不解析；
如果是 CSV over `lines` 的表头拿不到额度，则断开重连（原因 `tcp_csv_header_budget`），
避免把下一行误当成表头。
表头超过分帧或 CSV 格式上限时同样断开重连，即使配置了 `resync`；该连接剩余数据不再解析。

## 空闲、keepalive 与重连

- Source 在等待读取时超过 `idle_timeout_ms` 没有收到任何字节 → 计 `tcp_source_idle_timeouts`，
  断开并重连。Source 因自身 inbox 满而阻塞的时间**不算**空闲。
- `keepalive_ms` 打开内核 TCP keepalive，用于在没有应用数据时发现半开连接（Sink 尤其需要）。
- 断线（EOF、读写错误、空闲超时、`disconnect` 策略）计 `*_disconnects`，然后按指数退避
  重连：从 100 ms 起翻倍，封顶 `reconnect_max_ms`，带随机抖动（[d/2, d] 内均匀取值）。
  断线后的**第一次**重连前也会退避，所以接受后立即断开的服务器不会被打成忙循环。
- 一次断线内连续失败 `reconnect_attempts` 次后：
  - Source 以可重试的 `JobFailed` 结束；
  - Sink 记 `tcp_sink_fatal`、取消 job，job 以可重试的失败结束（fail closed）。
- 错误与状态中只出现固定原因（如 “TCP connect failed”、“TCP TLS handshake failed
  (certificate verification is mandatory)”、“TCP connect timed out”），不出现主机、端口或证书内容。

## 安全

- **白名单 / SSRF**：`host:port` 经 `TargetPolicy`（与 HTTP 相同的 deny-by-default 白名单，
  并拒绝 link-local / 云元数据 / 未指定地址），拒绝时的错误不回显目标。与 HTTP 一样，
  不对 DNS 解析结果再次校验 IP。
- **TLS**：可选；开启后强制校验，`tls_ca_pem` 只是换一组信任根。没有客户端证书（mTLS）。
- 没有凭据字段：裸 TCP 协议若需要认证，由对端协议层负责，不在本连接器内。

## Source：入口预算

与 NATS / WebSocket 相同的预算式入口：每行先在 inbox 中按 `inbox_bytes` 取得 Queue 额度
再入队；额度/容量满时等待（计 `tcp_source_backpressure_waits`），等待期间不读 socket，
压力通过 TCP 窗口传回对端；单行大于整个预算时计 `dropped_budget`。inbox 占用见
`tcp_source_inbox_items` / `tcp_source_inbox_bytes`。

## Sink：发送队列、部分写、停止

- 结构：pump（从 outbox 取批次、编码、加分帧、放入有界队列）与 writer（持有连接、写、
  重连）并行运行，共享 `queue_capacity` 帧的队列。
- 只有批次中每一行都成功入队才回执 outbox；任一行丢弃则该批次失败——“完成”是“交给发送队列”，不是对端收到。
- 读取和发送交替优先；部分写阻塞期间仍读取并丢弃对端数据，避免双向写入相互阻塞。
- 首次观察到停止时固定一个 deadline，pump、writer、在途发送和排空共用，不逐行重置。
  期限到达后即使队列/写 socket 立即就绪，也不得继续接收或发送。
- 队列满：`block` 等待（计 `backpressure_waits`，压力传回 outbox / SQL）；`drop_newest`
  丢掉新行并计 `dropped_overflow`。
- 一帧的写入（含所有部分写和 flush）超过 `send_timeout_ms`（对端不读、窗口塞满）计
  `send_timeouts` 并**断开**：半帧已经写进流里，连接不能再用，重连后从下一帧开始。
  该帧丢失。写错误计 `send_failed`。
- 对端发给 Sink 的字节被读出并丢弃，计 `ignored_bytes`；对端 EOF 视为断线。
- 停止：在 `flush_timeout_ms` 内把 outbox 和队列中剩余的帧写完，然后关闭写方向
  （`shutdown`，计 `closes`，失败计 `close_failed`）；超时后剩余的计 `discarded_on_close`。

## 内存

- 每个连接：64 KiB 固定开销 + 固定读缓冲（`max_frame_bytes` + 前缀 + 16 KiB）
  + 一帧（`max_frame_bytes` + 前缀），必须 ≤ job reservation 的一半。默认约 208 KiB。
- Sink 另加 `(queue_capacity + 1) × (max_frame_bytes + 前缀)`（满队列 + writer 正在写的一帧），
  CSV over `lines` 再加一帧大小的表头槽位。默认 JSON：17 × 64 KiB + 连接 ≈ 1.27 MiB。
  正在编码的那一帧不在静态额度里，而是按行单独记账（见上文 Sink）。
- 所有额度计算都是饱和运算（`usize::MAX` 有测试），溢出只会导致校验拒绝，不会回绕。
- 在 `bind` / 运行时记入 job reservation；pipeline 中所有 TCP / WebSocket / NATS /
  JetStream / DataBus 端点合计 ≤ reservation 的 3/4，超出在校验时返回 `BoundExceeded`。
- Source 的 inbox 按 `inbox_bytes` 计入 queue 账本。

## 语义与恢复

capability `tcp` / `tcp_sink`：delivery `live_best_effort`、recovery `restart_fresh`、
replay `unsupported`。spec 中的 `aligned`、`checkpoint`、`checkpoint_dir`、`restore`
或其他 delivery 一律以 `UnsupportedRestore` 拒绝（Sink 搭配其他 Source 时也一样）。
没有应用层确认：已发送 = 写进 socket，不代表对端处理成功；断线、超时、停止时在途或
排队的数据可能丢失，并在计数器中体现。重连后 Source 从**当前**开始读。

## 指标

pipeline status 中的 `tcp_source` / `tcp_sink` 对象，`/metrics` 的 io 字段：

- Source：`tcp_source_{received,rows,bytes_read,dropped_bad,dropped_oversize,dropped_partial,dropped_budget,backpressure_waits,connects,reconnects,disconnects,connect_failures,idle_timeouts,inbox_items,inbox_bytes}`
- Sink：`tcp_sink_{sent,bytes_written,dropped_bad,dropped_oversize,dropped_budget,dropped_overflow,backpressure_waits,send_failed,send_timeouts,discarded_on_close,connects,reconnects,disconnects,connect_failures,ignored_bytes,closes,close_failed,fatal,queue_items}`

健康状态：断线时为 `Reconnecting`，重连耗尽或 `fail_on_decode` 时为 `Failed`。

## 不在本次范围

- listen / server 模式（见上文）。
- 其他分帧：变长整数前缀、自定义分隔符、STX/ETX、固定长度记录、多行 CSV 记录跨行。
- 客户端证书（mTLS）、代理、连接后先发的握手 / 订阅报文。
- UDP。
