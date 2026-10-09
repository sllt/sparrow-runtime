# 负载格式（`format`：`json` / `csv` / `protobuf`）

字节型 Source / Sink 用 `source.format` / `sink.format` 选择行的编解码：

- `"json"`：默认值，行为与以前完全相同。未写 `format` 的已存 spec 不变。
- `"csv"`：由 `source.csv` / `sink.csv` 配置。
- `"protobuf"`：由 `source.protobuf` / `sink.protobuf` 配置，见文末 [Protobuf](#protobuf) 一节。

两者都由 `sparrow-formats` 实现。CSV 先做严格的结构预扫描，再用 `csv` crate（`=1.4.0`）去转义，不自己手写解析器。capabilities JSON 的 `formats` 键列出受支持的组合。

## 校验（全部在 spec 校验 / 启动前拒绝）

- `format` 只能是 `json`、`csv` 或 `protobuf`。其他值返回 `invalid_argument`。
- 有 `csv` 块但没有 `format: "csv"` 时拒绝。
- `csv` 块是 `deny_unknown_fields`，拼错的选项会被拒绝。
- Sink 不接受只用于解码的选项：`trim`、`multiline`、`columns`、`missing_columns`、`extra_columns`、`max_record_bytes`、`max_fields`。
- 不支持 CSV 的 kind 返回 `feature_unavailable`，见下面的矩阵。
- 下列 schema 与选项的组合在启动前拒绝：
  - 必填字段找不到列；
  - 只有一个可空列且 `null_value` 为空（否则 NULL 行会变成空行）；
  - `quote` 与 `delimiter` 相同；
  - `header: true` 时又写了 `columns`。

## 选项

| 选项 | 默认 | 方向 | 含义 |
|---|---|---|---|
| `delimiter` | `","` | 双向 | 一个 ASCII 字节：tab，或除引号外的标点 |
| `quote` | `"\""` | 双向 | 一个 ASCII 标点字节，不能是 tab；在引号字段中把它写两次表示字面引号 |
| `header` | `true` | 双向 | 解码：首条记录是列名。编码：每条消息 / 请求体 / 文件段先写一行表头 |
| `columns` | 无 | 解码 | 无表头时按顺序给出列名；不写则按 schema 字段顺序（位置模式） |
| `null_value` | `""` | 双向 | 不带引号且等于它的字段是 NULL；带引号的字段永远不是 NULL；长度 ≤ 64 B |
| `trim` | `false` | 解码 | 去掉*未加引号*字段两侧的空格 / tab；其他 ASCII 空白（如 `\x0b`、`\x0c`）是数据，保留 |
| `multiline` | `false` | 解码 | 允许引号字段内换行，见下文 File 一节 |
| `missing_columns` | `error` | 解码 | `null`：缺列的可空字段解为 NULL，必填字段仍然报错 |
| `extra_columns` | `ignore` | 解码 | `error`：表头中有 schema 之外的列时拒绝 |
| `max_record_bytes` | 65536 | 解码 | 每条记录的字节数，范围 1..=65536 |
| `max_fields` | 256 | 解码 | 每条记录或表头的字段数，范围 1..=1024 |

## 语法（严格）

- 记录以 `\n` 或 `\r\n` 结束，只剥掉一组末尾终止符；裸 `\r` 不会被当作终止符宽松剥掉，出现在引号外即 malformed。空行跳过。
- 引号只能出现在字段开头。引号字段在结束引号之后只能紧跟分隔符或行尾。字段中间出现裸引号、引号后面还有字符、引号未闭合，都属于 malformed。
- UTF-8 BOM 只在文档开头 / 文件开头（偏移 0）接受，其他位置的 BOM 原样作为数据。
- 列名不能重复，长度 ≤ 256 B。
- 字段数与表头不一致的记录是 malformed（不会补齐，也不会截断）。

## 类型

| Schema 类型 | 解码 | 编码 |
|---|---|---|
| Bool | `true` / `false`（不区分大小写） | `true` / `false` |
| Int64、UInt64 | 十进制，越界报错 | 十进制 |
| TimestampMicrosUTC | 整数微秒 | 整数微秒 |
| Float64 | 必须是有限值 | Rust 最短表示；NaN / ∞ 写作 NULL（与 JSON 相同） |
| Utf8 | 必须是合法 UTF-8 | 只在必要时加引号 |
| Bytes | 标准 Base64（带 padding） | 标准 Base64 |
| Dynamic / 嵌套类型 | 字段文本按严格 JSON 解析 | JSON 文本 |

编码只在以下情况给字段加引号：

- 非 NULL 空单元格总是加引号，避免变成被跳过的空记录或误解为 NULL；字段文本等于 `null_value` 也加引号；
- 含有分隔符、引号、`\r` 或 `\n`；
- 首尾有空格或 tab；
- 以 BOM 开头。

这些转义规则适用于所有非 NULL 标量文本，包括数字、bool 和 Base64，不只 Utf8；使用自定义分隔符、引号或 NULL 标记时也一样。

编码再解码可以完整还原（有单元测试覆盖）。

## 消息与文档

| kind | Source | Sink | 单位 |
|---|---|---|---|
| `mqtt` | ✓ | ✓ | 每条消息一条记录，`header: true` 时前面带表头 |
| `nats` | ✓ | ✓ | 同上 |
| `jetstream` | ✓ | ✓ | 同上 |
| `http_push` | ✓ | — | 每个请求一条记录 |
| `websocket` | ✓ | ✓ | 每条文本帧一条记录（同 MQTT）；`framing: ndjson`、`binary_frames: decode` 与 Sink `frame: binary` 都拒绝 CSV |
| `tcp`（`lines`） | ✓ | ✓ | 每个连接是一份文档：`header: true` 时连接后第一行是表头（Sink 每个新连接重写表头），之后一行一条记录；`multiline` 拒绝 |
| `tcp`（`length_prefixed`） | ✓ | ✓ | 每帧一条记录，`header: true` 时前面带表头（同 MQTT） |
| `http` | — | ✓ | 一个请求体 = 表头 + 多条记录，`Content-Type: text/csv; charset=utf-8` |
| `http_poll` | ✓ | — | 每个响应是一份文档：表头 + 多条记录，请求带 `Accept: text/csv` |
| `file` / `file_replay` / `replay` | ✓ | ✓（`file`） | 文件或段文件是一份文档 |
| `databus` | ✗ | ✗ | 进程内传递行，JSON 字节只是内部传输细节，CSV 没有意义 |
| `log`、plugin | — | ✗ | 不是字节型端点 |

在 HTTP Poll 中，响应体为空或只有空行时视为没有行，即使 `header: true` 也一样。`http_poll.format`（JSON 文档 / NDJSON 分帧）只适用于 JSON；与 `format: "csv"` 一起设置会被拒绝。

## File Source

- 第一条非空记录是表头，读取后消费掉，永远不会作为行返回。偏移 0 处的 BOM 会被去掉。
- 表头错误是 fatal（`invalid_schema`），包括表头超过 `max_record_bytes`。坏表头下没有任何行可信。
- **恢复 / seek。** checkpoint 只记录行的结束偏移，因此恢复位置不会落在表头中间，也不会落在记录中间。seek 时先从文件开头重新读取表头，重建列映射，再定位：
  - 非 `multiline`：每条记录就是一行，只要偏移在行边界上就是记录边界。
  - `multiline`：引号内允许换行，单看偏移无法判断是否在记录中间。所以恢复时会从文件开头重新分帧整个前缀；如果偏移落在引号记录内部，则拒绝（fatal），不会猜。有测试对每个切点验证恢复的精确性。
- **`multiline` 分帧。** 丢弃超长记录时仍然保持引号奇偶状态，所以超长的多行记录会被整条丢掉。孤立的引号可能把后面几条记录合并，直到引号闭合；合并出的记录随后被严格扫描判为 malformed 并丢弃 / 失败。在 `multiline` 下，未闭合的引号会把文件剩余部分当作一条记录（通常超长，或者在 EOF 处 malformed）。如果数据里不会有引号内换行，请保持 `multiline: false`，这样坏行只影响它自己这一行。
- 文件只能追加。表头在两次写入之间被拆开时，会等到完整一行再处理（有测试覆盖）。
- **checkpoint 身份绑定格式。** CSV 读取时，checkpoint 中的文件指纹会混入全部解码选项的规范编码（分隔符、引号、`header`、`columns`、`null_value`、`trim`、`multiline`、`missing_columns`、`extra_columns` 以及生效的 `max_record_bytes` / `max_fields`）。在 NDJSON 下取的 cut 不能被 CSV 读取器恢复，反之亦然；CSV 选项变化后恢复也会被拒绝（`unsupported_restore`，错误信息包含 "payload format / CSV options"）。NDJSON 的指纹不变，已有的 JSON checkpoint 仍可恢复。编码使用的是编译后的生效值，所以显式写出默认值（如 `delimiter: ","`、`max_fields: 256`、`max_record_bytes: 65536`、`missing_columns: error`）与省略等价，可以互相恢复（有测试）。唯一不做归一化的是无表头时的 `columns`：显式列出与 schema 顺序相同的列名，和省略 `columns`（按 schema 位置）语义相同，但指纹不同，恢复会被拒绝。

- **JetStream Source 同样绑定。** JetStream cut 的身份指纹（`SourceIdentity.fingerprint`）在 JSON 下保持为 0（与已有 checkpoint 完全一致，布局不变），CSV 下为上述规范编码的非零哈希。恢复时严格比较，双向拒绝：JSON cut 不能被 CSV 读取器恢复，CSV cut 不能被 JSON 读取器或 CSV 选项不同的读取器恢复（`unsupported_restore`，"payload format / CSV options"）。真实 broker 测试覆盖 JSON→CSV、CSV→JSON、改 `trim` / `null_value` / `max_record_bytes` 均被拒绝，以及同一选项（显式默认值写法）从 cut 之后精确续读。

## File Sink

- 目录标记文件 `FORMAT` 为 `SPARROW_CSV_SINK_V1` 加一行编码选项的 JSON（`delimiter`、`quote`、`header`、`null_value`），与 NDJSON 目录互不兼容。这四项总是写出，Sink 也不接受解码选项，因此显式写默认值与省略得到相同的标记。段文件命名为 `part-N.csv`。
- 用不同的 CSV 选项重新打开已有目录（无论是否 aligned 恢复）会被拒绝（`policy_denied`，"different CSV options"），不会在同一目录中混写两种方言。
- 每个新段文件开头写一次表头，表头计入段字节数和配额。表头加一行必须能放进一个段，否则拒绝。
- 表头的驻留信用随 `Writer.header` 跨 batch 保留，直到表头替换或 Writer 关闭才释放，不在单批编码结束时提前退款。
- schema 表头变化时轮转到新段。
- CSV File Sink 不接受 `action`。

## HTTP Sink

- 多个批次合并成一个请求时，保留第一个请求体的表头，后续请求体的表头会被去掉（按编码后表头的长度去除，即使列名中有引号或换行也正确）。JSON 与 CSV 的投递不会合并。
- 不支持 `action.body` 和 `single`。按行发送的 query action 每次 POST 表头 + 一条记录。
- **可靠性：** CSV 请求体无法携带输出身份（`output_sequence`），所以以下情况返回 `unsupported_restore`：
  - Source 是 JetStream；
  - `recovery: aligned`。

  HTTP 需要 checkpointed 投递时请用 `format: "json"`；此限制不适用于下面的 JetStream Sink v28。

## JetStream Sink

- 一行一条消息；`header: true` 时每条消息都带自己的表头，PubAck/outbox 确认语义与 JSON 相同。
- 独立线性 File → JetStream 的 aligned 输出用 JSON **v27/JSI1** 或 CSV **v28/JSI2**。CSV 目标身份绑定编译后的 `delimiter`、`quote`、`header`、`null_value` 四项，显式默认值与省略等价。
- JSON↔CSV、任一编码选项、目标或完整输出计算语义变化都拒绝继承历史（`unsupported_restore`）。v27/v28 使用独立目录，不自动迁移；拒绝发生在 File seek/状态激活/发布之前，不推进 `CURRENT` 或 `STATE_GENERATION`。
- 仍要求可写 File/Limits stream、全部前置 PubAck 和目标复验；不开放其他 source-only/graph/IoT checkpoint profile，亦不承诺 exactly-once。

## 坏数据与计数

- 坏记录走已有的 `fail_on_decode` / `dropped_bad` 路径：可以丢弃并计数，也可以让 job 失败。
- 各 kind 原有的 decode 计数器之外，全局 IoDiagnostics（`/metrics` 与 pipeline status）还会按原因分开计数：

| 计数器 | 原因 |
|---|---|
| `csv_malformed` | 结构错误（`CodecViolation`） |
| `csv_oversize` | 超过 `max_record_bytes` / `max_fields` |
| `csv_type_errors` | 字段不符合 schema 类型 / 非空约束 |
| `csv_header_errors` | 表头缺列、多列、重复列 |
| `csv_encode_errors` | 编码失败，例如单条记录超过 Sink 上限（内存额度不足不计入这里，计入各 kind 的 budget / failed 计数） |

## 内存

顺序始终是：先按字节长度拒绝（不分配），再把解码 / 编码的工作内存记到 job 的 reservation，最后才解码 / 编码。

- **CSV 解码估算**（`CsvFormat::decode_scratch`，饱和运算）：
  - 每个线路字节记 4 B（去转义副本、`csv` crate 记录缓冲、表头列名、Utf8/Bytes 值各至多一份）；
  - schema 中有 Dynamic / 嵌套列时，这些单元格要按 JSON 解析，因此按 JSON 的系数记 64 B/字节；
  - 每个字段记扫描 / 切分 / 边界向量（按 `Vec` 翻倍增长记两倍），字段数以 `max_fields` 为上限；
  - 每个 schema 字段记映射、Row 以及位置模式下的列名；
  - 另加 8 KiB 读缓冲和 4 KiB 余量。
  - 对扁平 schema，这个估算明显小于 JSON 的 64×，但不是实测峰值。
- **JSON** 沿用 NATS / HTTP Poll 的估算（`len·64 + 字段数·2·size_of(Scalar) + 4 KiB`）。
- **编码**：先记格式自己的编码 scratch（CSV：`resident·2`，有 Dynamic 时 `·8`，加每列 64 B 与 4 KiB），之后输出缓冲每次扩容都先记账；输出有硬上限。
- 解码前记账的连接器：NATS、JetStream、HTTP Poll（表头与每条记录分别记账；任何一条因额度丢弃都会让该响应不完整，不推进 ETag / Last-Modified）、MQTT（仅 budgeted ingress）、File（`poll_admitted`）。
- 编码前记账且有上限的连接器：NATS、JetStream、MQTT（含 action）、HTTP、File Sink（CSV 表头也先记账）。

## Protobuf

`format: "protobuf"` 按用户提供的 schema 把一条 protobuf 消息与一行互转。没有单独的 cargo feature：依赖为 `prost-reflect =0.16.5`（关闭默认 feature）与 `prost =0.14.4`，release 二进制增量见 PR 说明。

### 选项（`source.protobuf` / `sink.protobuf`，`deny_unknown_fields`）

| 选项 | 默认 | 方向 | 含义 |
|---|---|---|---|
| `descriptor_set` | 必填 | 双向 | 标准 Base64（带 padding）编码的 `FileDescriptorSet`，须包含消息类型及全部 import（`protoc --include_imports --descriptor_set_out=…`）；解码后 ≤ 48 KiB |
| `message` | 必填 | 双向 | 完整消息名，如 `telemetry.v1.Reading` |
| `fields` | `{}` | 双向 | schema 列 → 点分字段路径（如 `location.lat`）；未列出的列映射到同名的顶层字段 |
| `unknown_fields` | `ignore` | 解码 | `error`：消息含 descriptor 未声明的字段时拒绝该消息 |
| `max_message_bytes` | 65536 | 解码 | 单条消息字节数，1..=65536 |
| `max_depth` | 32 | 解码 | 消息嵌套深度（顶层 = 1），1..=100 |

- 不在运行时编译 `.proto`：只接受预编译的 `FileDescriptorSet`，校验时由 `prost-reflect` 解析并解析出消息类型。
- 只接受 `syntax` 为空（proto2）、`proto2`、`proto3` 的文件；`editions` 及其他值返回 `invalid_schema`。
- Sink 不接受 `unknown_fields`、`max_message_bytes`、`max_depth`（`invalid_argument`）。
- 有 `protobuf` 块但 `format` 不是 `protobuf`，或 `format: "protobuf"` 但缺少 `protobuf` 块，都会被拒绝。

### 字段映射（启动前校验）

- 路径只能穿过单值（非 repeated、非 group）的消息字段。
- `repeated`、`map`、group 字段不能映射，也不能作为路径中间节点；消息类型的叶子只接受 `google.protobuf.Timestamp`。没有 JSON 列或扁平化 repeated 的模式。
- 两列映射到同一路径、或一列路径是另一列路径的前缀，都会被拒绝；`fields` 的键必须是 schema 列。
- proto2 `required` 字段在 Sink 上必须被一个非空列映射。解码**不**检查 `required` 是否出现（缺失时按下文缺省规则处理）。

### 类型

| protobuf | Schema 类型 |
|---|---|
| `int32` / `sint32` / `sfixed32` | Int64 |
| `int64` / `sint64` / `sfixed64` | Int64、TimestampMicrosUTC（整数微秒） |
| `uint32` / `fixed32` | Int64、UInt64 |
| `uint64` / `fixed64` | UInt64 |
| `float` / `double` | Float64 |
| `bool` | Bool |
| `string` | Utf8（必须是合法 UTF-8） |
| `bytes` | Bytes |
| enum | Int64（数值）或 Utf8（值名） |
| `google.protobuf.Timestamp` | TimestampMicrosUTC |

- 其他组合在启动前拒绝（`invalid_schema`）。
- 编码不做截断或钳位：超出字段范围的值（如 Int64 写入 `int32`、负数写入 `uint32`、超出 `float` 范围的有限值）是 `type_mismatch`。写入 `float` 时 Float64 舍入到最近的 f32。
- 解码：NaN / ±∞ 是类型错误。编码：NaN / ±∞ 在有 presence 的字段上与 NULL 同样省略，在隐式 presence 字段上是错误。
- enum：解为 Utf8 时，未声明的数值是类型错误；解为 Int64 时 open enum（proto3）保留原数值，closed enum（proto2）的未声明数值是类型错误。编码时未知的值名是错误，closed enum 的未声明数值也是错误。
- Timestamp：范围 0001-01-01..=9999-12-31（UTC），`nanos` 必须在 0..=999 999 999；解码时不是整微秒（亚微秒精度）的值是类型错误，不会截断。

### presence 与缺省值

| 情况 | 解码 | 编码 |
|---|---|---|
| 路径上的祖先消息缺失 | NULL | 消息仅在其下至少一个叶子非 NULL 时写出 |
| 有 presence 的字段（proto2 optional、proto3 `optional`、oneof 成员、消息）缺失 | NULL | NULL 不写该字段 |
| 隐式 presence 字段（proto3 普通标量）缺失 | 该类型的缺省值（0、`""`、`false`、空 bytes、enum 0） | 等于缺省值时不写（浮点按位比较，`-0.0` 会写出）；列必须非空，可空列映射到隐式字段在启动前拒绝 |

- NULL 解到非空列是类型错误。
- 同一 oneof 的多个成员同时非 NULL 时，编码报错（`type_mismatch`）。
- 解码遵循 protobuf 合并语义：标量后者覆盖前者，重复出现的单值消息合并，oneof 成员互相清除。拼接两条消息等同于合并（有测试）。
- 编码按字段号顺序写出，与 `protoc --encode` 对同一值的输出逐字节相同（golden 测试，protoc 36.2）。

### 解码校验

解码不构造 `DynamicMessage`，而是对线路字节做一次校验遍历：

- 每个 key、varint、长度和 group 都会检查：截断、wire type 6/7、字段号 0、group 不平衡都属于 malformed（`codec_violation`）。
- 已声明字段必须使用声明的 wire type（可 packed 的 repeated 标量两种编码都接受）；已声明的 `string` 必须是 UTF-8；已声明的消息、group、map entry 都会递归检查，即使没有被映射。
- 未声明字段：`ignore` 时跳过；未知的长度前缀字段不解析内容，未知 group 按结构跳过并计入深度。
- 先按长度拒绝（超过 `max_message_bytes` → `max_record_size`，不分配），超过 `max_depth` → `bound_exceeded`。

### 消息与文档

| kind | Source | Sink | 单位 |
|---|---|---|---|
| `mqtt` / `nats` / `jetstream` | ✓ | ✓ | 一条消息 = 一条 protobuf 消息（无长度前缀） |
| `http_push` | ✓ | — | 一个请求体 = 一条消息 |
| `websocket` | ✓ | ✓ | 一帧 = 一条消息；Source 必须 `framing: message` 且 `binary_frames: decode`；文本帧不解码，按坏记录计数（`dropped_bad`、`protobuf_malformed`，`fail_on_decode` 时失败）；Sink 必须 `frame: binary` |
| `http` | — | ✓ | 请求体 = 长度前缀消息流（每条前面一个 varint 长度，即 `writeDelimitedTo` 格式），`Content-Type: application/x-protobuf`；多个批次合并时直接拼接 |
| `http_poll` | ✓ | — | 响应 = 长度前缀消息流，请求带 `Accept: application/x-protobuf`；`http_poll.format` 必须为空；长度前缀本身损坏或截断时整个响应视为坏响应，单条坏消息按坏记录处理 |
| `file` / `file_replay` / `replay` | ✗ | ✗ | 拒绝（`feature_unavailable`）：没有带恢复语义的长度前缀文件格式 |
| `databus` / `log` / plugin | ✗ | ✗ | 不是字节型端点 |

- HTTP Sink 不支持 `action.body` 与 `single`；与 CSV 相同，不能与 JetStream 源或 `recovery: aligned` 一起使用（`unsupported_restore`）。
- JetStream Sink 的 aligned 输出（v27/v28）没有 protobuf 身份，`format: "protobuf"` 与 aligned 一起使用时拒绝；非 aligned 投递正常。

### checkpoint 身份

格式身份（File 指纹 / JetStream Source cut 指纹的输入）为 `sparrow-protobuf-v1` + 方向 + descriptor 原始字节 + `message` + 映射（与同名缺省等价的条目去掉，因此显式写出同名映射与省略等价）+ `unknown_fields` + 生效的 `max_message_bytes` / `max_depth`。descriptor、消息类型、映射或任一解码选项变化后恢复会被拒绝（`unsupported_restore`），JSON / CSV 与 protobuf 之间也互相拒绝（真实 broker 测试）。descriptor 只按字节比较：内容等价但序列化不同的 descriptor 也视为变化。

### 计数器

| 计数器 | 原因 |
|---|---|
| `protobuf_malformed` | 线路结构错误（`codec_violation`） |
| `protobuf_oversize` | 超过 `max_message_bytes` 或 `max_depth`（在连接器自身长度检查之前已被拒绝的，计入该连接器的 `dropped_oversize`） |
| `protobuf_type_errors` | 值不符合列类型 / 非空约束 / 范围 / enum |
| `protobuf_unknown_fields` | `unknown_fields: error` 下出现未声明字段 |
| `protobuf_encode_errors` | 编码失败（内存额度不足不计入这里） |

### 内存

- 解码 scratch（`ProtobufFormat::decode_scratch`，饱和运算）：线路字节数 + 每列状态 + 按列数及映射路径节点计算的冷计划额度 + 4 KiB。enum 名称来自 descriptor，不受线路长度约束，其展开计入计划额度。计划不复制每个祖先的完整子树，也不为 oneof 每个成员复制映射列表；分配器测试覆盖 7 种 64 KiB 输入、80 层映射及 400 成员 oneof。
- 配置元数据与逐记录 scratch 分开限定，不声称全部配置常驻内存都计入 Job ledger：descriptor 最多 48 KiB、32 文件、2048 个被检查的符号、32 层定义；单名称 256B、全名 1024B、累计展开全名 512 KiB，在链接前拒绝越界。映射最多 64 列、256 个消息节点，列名最多 256B；带点列名必须显式配置字段路径。缓存仅保留最近的一个 schema 计划。
- `google.protobuf.Timestamp` 必须是规范 proto3 seconds/int64、nanos/int32 形状，不能仅凭名称进入快速解码。proto2 closed enum 的未知数值也会在未映射字段和 packed 字段上拒绝，不能静默清除映射的 oneof 成员。
- 编码：先算出精确输出长度，超过上限直接拒绝，再按 `encode_scratch`（每列状态 + 计划 + 4 KiB）与精确输出长度一次记账，`try_reserve_exact` 分配。
- 先按长度拒绝、再记账、最后解码 / 编码的连接器与 CSV 相同（NATS、JetStream、HTTP Poll、MQTT budgeted ingress、WebSocket；HTTP、NATS、JetStream、MQTT、WebSocket Sink）。HTTP Push 在解码前按 `decode_scratch` 记账并持有到行交给 inbox；额度不足时回 503、计 `http_dropped`，不解码（JSON / CSV 请求体仍不记账，与以前相同）。
