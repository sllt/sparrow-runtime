# Action/Sink 与纯函数首批

后端开发 Preview；不是 eKuiper 配置兼容、可靠交付或生产认证。Action 是输出整形，不是脚本/独立执行引擎。省略 `sink.action` 保留原编码路径；旧 CAST、ASCII lower/upper、eager 求值和语义版本不变。

## 输出映射

HTTP、MQTT、Log、File 都支持：

```json
{"action":{"version":1,"body":{"device":{"$field":"device_id"},"value":{"$field":"temperature"},"unit":"C"}}}
```

- 引用**最终 Sink 输入 schema**。未知字段在配置验证时拒绝。
- `{"$field":"name"}` 保留类型（UInt64 全范围、NULL、对象/数组）；Bytes Base64，timestamp 整数微秒；NaN/Infinity 拒绝。
- `{"$literal":...}` 将子树作为原样 JSON，转义保留字。不写 `body` 输出全行，显式 `body:null` 输出 JSON null。
- 模板深度≤8、节点≤256、输入字段≤128；最终字节另受 Sink 配置及 Job 预算约束。没有脚本、环境变量/secret 插值、随机值或网络函数。
- 最终展开遍历另限深度16、节点16384；深模板嵌套Dynamic输入仍可能触发此界，单独合法不表示任意组合都可接纳。
- Action/File 当前只允许 `restart_fresh`，拒绝 aligned、restore、checkpoint/checkpoint_dir，不隐式继承旧可靠输出 ID。
- 因此不能把新模板直接接入当前要求 aligned 的 Alarm/可靠 Silence/Resample profile；其原 HTTP envelope 保持不变。普通阈值规则可用本批 Webhook，持久告警的模板化输出需另立可靠输出协议，不偷偷降级。

### HTTP / Webhook

```json
{"kind":"http","url":"https://example.invalid/webhook?fixed=1","batch_rows":1,"max_inflight":1,"linger_ms":0,
 "action":{"single":true,"body":{"value":{"$field":"value"}},"query":{"device":["edge-",{"$field":"id"}]}}}
```

- `single:true` 每行一个 JSON value，否则是 JSON array。仅 body 映射仍支持原合批/linger/并发；有 single 或 query 时必须 `batch_rows=1,max_inflight=1,linger_ms=0`，多行上游 batch 也逐行串行发送。
- 若实际收到空batch，逐行模式不发HTTP请求但结算空batch；array路径保留原`[]`编码/合批行为。MQTT/Log/File不产生数据行。Filter完全抑制输出时没有batch到Sink，不能据此期待一个空通知。
- 动态部分仅 query 值，不改 scheme/host/port/path/header。URL 编码后再次检查 allowlist；userinfo、fragment、动态 key 与固定 query 重名拒绝。
- ≤16个 query 参数，key≤64 B。文字模板1～32段、literal合计≤1024 B，展开值≤1024 B，URL≤4096 B；NULL/structured/nonfinite 目标值拒绝。
- URL/body 每行渲染一次，再复用原重试、响应体消费和连接池。重试可能重复；原 batch 仅完成一次 receipt，前几行已发送、后续失败不回滚远端副作用。
- live best-effort 不升级：编码/交付失败有计数，图 required 输出可失败整个 Job；HTTP 2xx 不等于业务幂等或事务提交。

### MQTT

```json
{"action":{"topic":["site/",{"$field":"device_id"},"/temperature"],"body":{"value":{"$field":"temperature"}}}}
```

变量仅占一个 topic level，拒绝 `/ + # NUL`；literal 可引入 `/`，不能带 wildcard/NUL。展开 topic 非空且≤1024 B。固定 broker 仍经过 allowlist；QoS0、clean session，无持久 outbox/消费方 ACK。payload≤64 KiB，编码副本提前计入预算，额度不足拒绝。

### 多动作与 Log

复用 [DAG](DAG.md) 的 `branch` + `graph_io.sinks`，每个 Sink 独立 Action。required 反压/报告失败，显式 best-effort 可丢弃/脱离；没有跨 Sink 原子事务。

生产 Supervisor 的 Log 写 stderr，不保留无人消费的第二份 ring。嵌入 `LogSink::new(diag,max_lines)` 仍是调用方显式调试捕获：按行数而非 Job 字节预算限额，不作为生产存储。stderr 阻塞不是端到端有界延迟保证。

## 有界 NDJSON File Sink（Linux）

先创建专用输出目录（建议 `install -d -m 700`），必须在 `SPARROW_DATA_ROOTS` 内，不能是 symlink 或 group/world writable；不能混放输入、checkpoint、用户文件。

| 配置 | 范围 |
|---|---|
| `segment_bytes` | 1 KiB～64 MiB |
| `max_bytes` | ≥segment_bytes，≤1 TiB；累计现有数据段字节 |
| `max_files` | 1～1024；累计数据段，不含 marker/lock |
| `row_bytes` | 2 B～1 MiB；JSON加换行须放入一个段 |
| `sync_data` | 默认 false；true 时每批同步文件及新段目录项 |

- 持有目录 FD，经 `/proc/self/fd` 锚定后续操作；目录重命名/替换不会把 writer 引到新路径。需要 procfs 及支持本协议锁/同步的文件系统。
- 永久 `WRITER_LOCK` 是协作式单 writer 锁，不要在线删除。`FORMAT` 必须匹配；foreign、不完整 marker、非普通文件、symlink、hardlink 拒绝。仍要求同用户进程不恶意改专用目录，不是针对目录所有者攻击的事务文件系统。
- `part-00000000000000000001.ndjson` 起新段，create_new、0600、不覆盖、不 append 旧段、不跨段写行。达到总容量/文件数明确失败，**不自动删旧数据**。
- 每批 flush，可选 sync_data；receipt 在其后完成。written/bytes 是完整行写调用成功数，最终 sync 失败仍可能非零，**不是可恢复提交水位**。
- 重启扫描有界历史，只接受换行结尾（空段允许）。坏尾拒绝并保留现场，离线检查修复，不自动 truncate/补写。不会重新解析每条历史业务记录。
- restart_fresh 重读 Source 可能重复，写入新段；没有 source replay/checkpoint 关联、幂等键或 exactly-once。
- 一个 blocking OS 工作项，取消等待已开始的写收尾，不遗留 detached writer。批内部分成功不回滚；文件系统无限挂起时无法保证强制有界停止。
- 指标 `io.file_written/file_bytes/file_segments/file_syncs/file_failed`。open/quota/write 错误令线性 File Job 或 required 图失败，optional 图隔离。不宣称断电、远程文件系统或存储控制器认证。

## 18个新增纯函数

SQL 与 Graph `{"k":"call","name":...,"args":[...]}` 共用 binder/evaluator、类型/nullable 及预算；仍 eager 从左到右，NULL 不跳过其他参数错误。

| 类别 | 名称 / 语义 |
|---|---|
| 字符串 | concat（2～16参数）；substring(text,start,count)（Unicode scalar，1-based 正start，非负count）；replace（非空 literal search）；contains/starts_with/ends_with（literal，无regex）；trim（Unicode whitespace） |
| 数值 | round/floor/ceil 一元；整数保持类型；float有限，round ties away from zero |
| 转换 | to_int64 严格范围、float向零截断不饱和；to_float64 有限、整数转换可能损精度；to_string 有限primitive，timestamp→整数微秒文本 |
| JSON | json_get(json_text,pointer)：严格JSON、拒重复key、RFC6901，missing/JSON null→SQL NULL；json_object(k,v,...)：1～8对，拒重复key，NULL key→NULL、NULL value保留；json_stringify：SQL NULL→文本null，Bytes Base64 |
| 时间 | parse_timestamp：已知偏移RFC3339→UTC微秒；format_timestamp：timestamp/int64微秒→UTC RFC3339固定6位小数 |

时间限制UTC年1～9999，拒leap second、未知偏移-00:00、非零亚微秒（多余零允许）；无now/locale/timezone database。

新函数每参数≤64 KiB文本/Bytes或Dynamic resident，输出≤64 KiB文本/JSON Dynamic resident；JSON输入深度≤8、pointer≤1024 B。先预约峰值scratch再执行。**硬上限不等于每个预算都能用满**，保守JSON估算可能先耗尽默认4 MiB预约。work units 仍按表达式步骤，不宣称逐字符抢占。

新逐行 HTTP 另预留32 KiB scratch，File/Log Action 为8 KiB；MQTT 同时计入 payload、编码中间帧、最终帧及有界目标文本空间。低额度嵌入调用可能在小 payload 时也拒绝；这是预付配额，不等同于每行实际常驻内存都达到此值。

支持三参数SUBSTRING和`SUBSTRING(x FROM start FOR count)`；不支持省略count、TRIM方向/自定义字符、FLOOR/CEIL scale。新增普通函数拒DISTINCT/ORDER BY/FILTER/OVER/named args等修饰，不静默忽略。

新增函数名称进入原canonical表达式身份，旧函数版本不变；旧二进制不识别新函数/Action/File配置时会拒绝，不支持自动降级。回退需保留匹配的旧配置/catalog备份，不能仅替换二进制并假设新规则仍可运行。

## 模板与验证

`deploy/stream-actions.json`、`deploy/pipeline-actions-{http,mqtt,file}.json`：先建stream，配置实际input/target/data roots/allowlist；示例loopback不是自动放行。capability是静态清单，最终以`/v1/validate`为准。

`scripts/production-actions-validate.sh` 使用固定清单/冻结二进制，重复测试并以独立Go wire/disk oracle 验证无demo正式包、File重启/配额/坏尾、HTTP重试/连接复用及真实Mosquitto输出。匹配结果见[验收记录](PRODUCTION.md#actions-validation)；长稳、介质故障及未声明可靠组合不能由本批短测推导。
