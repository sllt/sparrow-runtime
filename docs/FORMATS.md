# 负载格式（`format`：`json` / `csv`）

字节型 Source / Sink 用 `source.format` / `sink.format` 选择行的编解码：

- `"json"`：默认值，行为与以前完全相同。未写 `format` 的已存 spec 不变。
- `"csv"`：由 `source.csv` / `sink.csv` 配置。

两者都由 `sparrow-formats` 实现。CSV 先做严格的结构预扫描，再用 `csv` crate（`=1.4.0`）去转义，不自己手写解析器。capabilities JSON 的 `formats` 键列出受支持的组合。

## 校验（全部在 spec 校验 / 启动前拒绝）

- `format` 只能是 `json` 或 `csv`。其他值返回 `invalid_argument`。
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

- 记录以 `\n` 结束，前面的 `\r` 会被去掉（CRLF）。空行跳过。
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

- 字段文本等于 `null_value`（因此空字符串在默认配置下写成 `""`，与 NULL 区分开）；
- 含有分隔符、引号、`\r` 或 `\n`；
- 首尾有空格或 tab；
- 以 BOM 开头。

编码再解码可以完整还原（有单元测试覆盖）。

## 消息与文档

| kind | Source | Sink | 单位 |
|---|---|---|---|
| `mqtt` | ✓ | ✓ | 每条消息一条记录，`header: true` 时前面带表头 |
| `nats` | ✓ | ✓ | 同上 |
| `jetstream` | ✓ | ✓ | 同上 |
| `http_push` | ✓ | — | 每个请求一条记录 |
| `websocket` | ✓ | ✓ | 每条文本帧一条记录（同 MQTT）；`framing: ndjson`、`binary_frames: decode` 与 Sink `frame: binary` 都拒绝 CSV |
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
- schema 表头变化时轮转到新段。
- CSV File Sink 不接受 `action`。

## HTTP Sink

- 多个批次合并成一个请求时，保留第一个请求体的表头，后续请求体的表头会被去掉（按编码后表头的长度去除，即使列名中有引号或换行也正确）。JSON 与 CSV 的投递不会合并。
- 不支持 `action.body` 和 `single`。按行发送的 query action 每次 POST 表头 + 一条记录。
- **可靠性：** CSV 请求体无法携带输出身份（`output_sequence`），所以以下情况返回 `unsupported_restore`：
  - Source 是 JetStream；
  - `recovery: aligned`。

  需要 checkpointed 投递时请用 `format: "json"`。

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
