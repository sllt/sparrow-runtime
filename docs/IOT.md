# K4：变化检测与 Deadband

状态：2026-09-17，K4 首批功能、全局 Review 修复及下方限定矩阵复验已完成。仍是 **Preview，未 commit/push/tag**；不是任意组合或全场景生产认证。

## 范围

本批实现 IOT-01 变化检测、IOT-02 Deadband，以及相应状态、资源、恢复、配置、诊断和模板。复用现有 GraphSpec → BoundPlan → PhysicalPlan → Kernel → Connector 路径，不另造执行引擎。

- 新 Graph 节点：`change_detect`、`deadband`，一入一出，输出保留完整原行、schema 和非聚合来源 metadata。
- 可用于线性计划或已有 DAG；不合并不同 key 的状态，不承诺跨输入的全局排序。
- 本批不增加同名 SQL 函数／SQL 语法；配置入口是现有 PipelineSpec 的 Graph。已有 SQL 阈值、窗口统计和 Lookup 继续复用。
- 不做 Web 前端；不包含 Hysteresis、HoldFor、Debounce、离线检测或完整告警生命周期。

## 公共配置

每个节点的 `iot` 配置显式声明以下字段：

```json
{
  "keys": ["device_id"],
  "fields": ["temperature"],
  "emit_first": true,
  "ttl_micros": 0,
  "max_keys": 1024,
  "invalid": "error"
}
```

| 字段 | 合同 |
|---|---|
| `keys` | 1～16 个不重复、非空、已声明且 non-null 的字段；Bool/Int64/UInt64/Utf8/Bytes/Timestamp。拒绝 Float key、Dynamic、嵌套类型 |
| `fields` | 变化检测观察 1～16 个不重复字段；支持 Bool/Int64/UInt64/Float64/Utf8/Bytes/Timestamp。Deadband 必须恰好一个 Int64/UInt64/Float64 字段 |
| `emit_first` | 每 key 首个有效值是否输出；为 false 时仍建立参考值，不是忽略这条状态观察 |
| `ttl_micros` | 0 明确禁用时间淘汰，仍受 key/bytes 上限约束；正值为本次运行的 processing-time 空闲 TTL，不是设备时间或事件时间 |
| `max_keys` | 正数上限，还受 Job 的 max_state_keys、内存和进程总准入限制；到限明确失败，不偷偷淘汰仍有效的 key |
| `invalid` | `error`：无效观察值使 attempt 失败；`ignore`：整行不输出、不更新参考值、不刷新 TTL，并计数。自然 TTL 仍独立到期 |

NULL、缺失后解码为 NULL 的值、NaN/Infinity 属于无效观察值。Source 自身的解码错误仍按既有 decode 策略处理，不能误当已经进入 IoT 算子的 `ignore`。key 的缺失／NULL／错误类型不属于可忽略的传感器值。

## 变化检测

`kind: "change_detect"`，不允许附带 `deadband` 配置。任意一个观察字段与该 key 上个有效输入不同，就输出当前完整行；相同则抑制。比较保留声明类型，整数不先转换成浮点；有效 Float 的 `+0` / `-0` 相等。只观察指定 `fields`，其他业务列改变不会独自触发输出。

## Deadband

`kind: "deadband"`，在公共配置内增加：

```json
"deadband": {
  "mode": "absolute",
  "baseline": "last_output",
  "threshold": 2.0
}
```

- `absolute`：只有 `abs(current - reference) > threshold` 才输出；**等于阈值不输出**。
- `relative`：只有绝对变化超过 `abs(reference) × threshold` 才输出；0.05 表示 5%，不是 5。零参考值下，任何非零变化满足相对变化条件。
- `last_input`：每个有效输入都更新参考值，含被抑制的输入。
- `last_output`：只有被选中输出的输入更新参考值；首个有效值始终负责初始化，即使 `emit_first=false`。
- threshold 必须有限且非负；不接受隐式类型转换、无限阈值或负值。

例：输入 `10, 11, 12, 13`，绝对阈值 2、输出首值。`last_input` 只输出 10；`last_output` 输出 10、13。后一种不会把逐步漂移永久吞掉。

## 状态、TTL 与取消

状态仅保存 detached 的 key 和观察值，不长期引用完整输入 batch。候选替换、变量长度值、索引与输出同时存活时先做预算准入；失败不能留下半更新的 key 状态。max_keys 不代表任意长字符串都能接纳。

HashMap bucket 容量按保守高水位单独计费：部分 key 到期后容量信用仍保留，空表或 cleanup 实际释放容器后才退还。正 TTL 的每 key 到期项同时受 `max_timers` 限制；底层复用一个唤醒 timer，不代表可以绕过逻辑待到期项上限。

正 TTL 在有效输入时刷新。到期后相同值再次到来按新 key 的首值策略处理；TTL 清理有定时触发，不依赖下一条业务消息。实时运行以单调计时，测试使用虚拟时钟；它不利用上游 watermark，也不能用墙钟回拨延长本应结束的等待。停止、失败和取消都释放状态、索引、timer、输出与持有信用。

## 恢复与兼容

K4 使用独立 **v6 snapshot profile**，IoT participant 使用独立 codec/slot，不伪装窗口状态。v6 与 File/v3、JetStream/v4、DAG/v5 必须不同目录，旧 binary 不自动迁移新状态。

声明的初始 aligned 矩阵：

- File 输入，IoT `ttl_micros=0`；线性计划最多两个 Count/IoT 状态参与者。
- 既有 required File→HTTP DAG 可组合 Count/IoT 状态，仍受图级参与者与总预算限制。
- 不开放正 TTL、ET/PT、Lookup、Dedup、Source time、side output、有损图或 JetStream 与 IoT 的 aligned 组合；不自动降级为 fresh。

所有状态和 required 输出在同一 checkpoint 切点冻结／flush。恢复前验证完整计算语义、实例、codec、schema 与源身份；修改字段、阈值、首值策略等不能复用旧状态。IoT profile 不使用旧线性计划的下游语义放宽。

`restart_fresh`／显式 reset 会丢失前值连续性，重新执行首值策略；不能把它当作业务恢复事件。这里是值过滤，不输出稳定告警 episode，也不声称外部 HTTP exactly-once。未提交的输出在故障重放中可能重复。

## 诊断与验收

`iot_input_rows`、`iot_emitted_rows`、`iot_filtered_rows`、`iot_invalid_rows`、`iot_expired_keys` 为算子处理累计计数；`iot_state_keys` / `iot_state_bytes` 为活跃 IoT 实例汇总。一个输入经过多个 IoT 节点会被各节点分别计数，不能当作 Source 行数或外部送达证明。`invalid_rows` 是被过滤行中的无效值子集。

上述字段同时进入 metrics JSON 和结构化运行日志。旧 `JobStats.state_keys/state_bytes` 仍是窗口口径，不能用它们替代 IoT 专项指标；内存总准入仍统一由 Job/Process owner 负责。

匹配源码的测试／故障／性能证据和全局 Review 结论见下方验收记录。

## 可运行模板与操作

以下模板是 K4 的最小可复现场景，不表示端口、目录或 HTTP 目标已经在
机器上自动创建。先准备一个能返回 HTTP 2xx 的本机 fixture，再使用模板；
`18081`、`18082` 只是模板默认端口，不能当作“开箱即用”的服务。

### 1. 准备 schema、数据目录和 HTTP fixture

先注册 [`stream-k4-telemetry.json`](../deploy/stream-k4-telemetry.json)：

```sh
export SPARROW_URL=http://127.0.0.1:43180
# 由 secret manager 或受控环境注入；不要写入命令行参数或 Git。
export SPARROW_TOKEN='从受控环境注入的随机管理 token'

sparrowctl put-stream telemetry deploy/stream-k4-telemetry.json
```

这个 schema 的 `device_id` 是非空 key，`temperature` 是可为 NULL 的
`float64`；因此 change 模板可以演示 `invalid=ignore`，而 deadband 模板的
`invalid=error` 仍只接收有效数值。

服务进程必须设置受控的数据根目录。例如生产服务使用：

```sh
export SPARROW_DATA_ROOTS=/var/lib/sparrow/data:/var/lib/sparrow/checkpoints
```

把 fixture 复制到 `/var/lib/sparrow/data/`，并保证运行用户可读；不要把
`/tmp` 或整个用户目录加入 allowlist。开发环境也应使用明确目录，例如
`$PWD/deploy:/tmp/sparrow`，同时把模板中的绝对路径改为该目录下的副本。

HTTP 输出端口需要由操作者自备 2xx fixture 并加入目标 allowlist。当前
`sparrowctl` 没有 allowlist 子命令，使用现有认证 API 配置（或由部署初始化
脚本写入 catalog）：

```sh
curl --fail --silent --show-error \
  -X PUT "$SPARROW_URL/v1/allowlist" \
  -H "Authorization: Bearer $SPARROW_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"host":"127.0.0.1","port":18081}'
curl --fail --silent --show-error \
  -X PUT "$SPARROW_URL/v1/allowlist" \
  -H "Authorization: Bearer $SPARROW_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"host":"127.0.0.1","port":18082}'
```

若修改模板中的 URL，必须把修改后的 host/port 加入 allowlist；不要使用
`skip_verify` 或未授权的旁路代理。管理 token 使用 `SPARROW_TOKEN`；启用
safe-mode 时还应设置持久的 `SPARROW_SECRETS_KEY_FILE`（32 字节原始密钥或
64 位十六进制）及 `SPARROW_REQUIRE_SECRETS_KEY=1`。K4 模板本身不包含
`SecretRef`，外部认证信息不要写进 JSON。

### 2. 变化检测：aligned v6

模板：[`pipeline-k4-change.json`](../deploy/pipeline-k4-change.json)，输入和
golden：[`k4-change.ndjson`](../deploy/k4-change.ndjson)、
[`k4-change.expected.json`](../deploy/k4-change.expected.json)。6 条输入中有 1 条
NULL 行被 `invalid=ignore` 丢弃，不更新 edge-b 的参考值；最终输出 3 行：

```json
[
  {"device_id":"edge-a","temperature":20.0},
  {"device_id":"edge-b","temperature":5.0},
  {"device_id":"edge-a","temperature":21.5}
]
```

实际 CLI 命令如下，`start` 只提交 desired 状态，必须继续检查 status：

```sh
sparrowctl validate deploy/pipeline-k4-change.json
sparrowctl explain deploy/pipeline-k4-change.json
sparrowctl put-pipeline change deploy/pipeline-k4-change.json
sparrowctl start change
sparrowctl status change
sparrowctl diagnose change --output change-diagnostic.json
sparrowctl checkpoint change
sparrowctl checkpoints change
```

模板使用 `ttl_micros=0` 和 `recovery=aligned`，checkpoint profile 是 v6。
File 使用 `append_only`，读完这份 fixture 后等待追加，保留周期／手动
checkpoint 入口；不要为这个恢复演练换成读完即结束的 `sealed`。
确认 checkpoint 已提交后，异常重启或 `kill` 演练结束，再执行
`sparrowctl start change`；`resume_latest=true` 会从兼容的 v6 CURRENT 恢复。
也可以先用 `sparrowctl checkpoints change` 查看数字 ID，再用
`sparrowctl restore change --snapshot-id N` 请求指定恢复点。恢复请求是新的
catalog revision，实际结果仍以 status 的 revision/attempt、checkpoint 和
诊断为准。

不要直接复用旧 v3/v4/v5 目录。需要显式 fresh/reset 时，复制模板并使用新
`checkpoint_dir`（或新 pipeline 名称），去掉固定 restore 后再用
`sparrowctl put-pipeline NAME FILE --if-match ETAG` 更新；不要删除仍需回退的
旧目录来“解锁”。fresh 会丢失前值连续性，status/explain 的 effective IoT
字段会标明这一点。

### 3. Deadband：restart_fresh

模板：[`pipeline-k4-deadband.json`](../deploy/pipeline-k4-deadband.json)，输入和
golden：[`k4-deadband.ndjson`](../deploy/k4-deadband.ndjson)、
[`k4-deadband.expected.json`](../deploy/k4-deadband.expected.json)。配置是
`absolute + last_output + threshold=1.0`；5 条输入输出 2 行（20.0、21.6），
等于或小于阈值的变化被抑制：

```sh
sparrowctl validate deploy/pipeline-k4-deadband.json
sparrowctl explain deploy/pipeline-k4-deadband.json
sparrowctl put-pipeline deadband deploy/pipeline-k4-deadband.json
sparrowctl start deadband
sparrowctl status deadband
sparrowctl diagnose deadband --output deadband-diagnostic.json
```

这是 `restart_fresh` 模板，未声明 aligned checkpoint。要演示 fresh reset，
先执行 `sparrowctl stop deadband`，确认 status 已停止，再执行
`sparrowctl start deadband`；文件从头读取，前值状态重新建立，第二轮输出会
再次得到 20.0、21.6。它不是告警恢复事件，也不是外部 HTTP 去重证明。

### 4. 有界失败演示

复制一份模板，把 IoT 配置的 `max_keys` 改成 `1`，再输入两个不同的
`device_id`。`validate` 可通过 schema/参数检查，但第二个 key 在运行时会
得到有界的 state quota failure；检查 `status`/`diagnose`，应看到失败而不是
隐式淘汰旧 key。修复配置或扩大受控预算后，用新 revision 重启；不要通过
无限增大队列或关闭 owner 计费来绕过限制。

### 5. 复用既有模板（TPL-02）

K4 不重建已有能力；下面只说明仓库中现有示例实际做什么，以及它们能否沿用
当前 Server 的控制面。TPL-02 不是新的 IoT 状态或恢复承诺。

#### 5.1 `pipeline-k1-zero.json`：线性 SQL 过滤

文件中的 SQL 是：

```sql
SELECT device_id, v FROM sensors WHERE v > 100
```

它是无状态的逐行过滤：输入

```json
{"device_id":"d1","v":50}
{"device_id":"d1","v":200}
{"device_id":"d2","v":101}
```

只会向 `log` sink 输出后两行（日志前缀通常为
`sparrow-log `）。它没有 Count/ET/PT 窗口、没有 watermark，也没有 IoT
前值；`v` 不是 IoT Deadband 的阈值配置。

模板使用 `source.file_contract=append_only`、`recovery=aligned`、显式
`checkpoint_dir` 和周期 checkpoint。实际操作前，应把模板的绝对路径改到
已配置的 `SPARROW_DATA_ROOTS`，注册与 SQL 一致的 schema（例如
`device_id` 非空 `utf8`、`v` 非空 `int64`），然后执行：

```sh
sparrowctl put-stream sensors /path/to/sensors-stream.json
sparrowctl validate /path/to/pipeline-k1-zero.json
sparrowctl explain /path/to/pipeline-k1-zero.json
sparrowctl put-pipeline zero /path/to/pipeline-k1-zero.json
sparrowctl start zero
sparrowctl status zero
sparrowctl checkpoint zero
sparrowctl checkpoints zero
```

`append_only` 到达 EOF 后会等待追加，不代表作业失败或已经完成；有限文件若要
正常结束，应明确改成 `file_contract=sealed`，并在结束前完成需要的 checkpoint。
这个过滤计划走 File/v3 的 zero-state aligned profile：切点仍需 Source 和
required Sink 的真实 ACK，不能因为没有 state 就跳过 barrier。恢复会从兼容
checkpoint 的 source cut 继续，而不是静默从文件头重放。

将同一配置改为普通 `restart_fresh` 时，不创建持久 checkpoint/generation，
每次启动都会从文件头重新执行，外部日志可能重复。aligned 模式即使当前没有
可恢复快照，fresh/reset 也会在受控 checkpoint 目录激活新的
`STATE_GENERATION`；它表示新的 lineage，不保留旧 state 连续性。v3/v4/v5
目录或损坏/缺失 CURRENT 不会被改成 fresh 绕过。

#### 5.2 `pipeline-k1-two-count.json`：线性 GraphSpec 的 Count 窗口

该文件用 GraphSpec 表达一条**线性**链路，因此 physicalize 后仍是线性
File/v3 计划，不需要 `graph_io`。它先按 `device_id` 每 3 行计算 `SUM(v)`，
再把这些 `s` 结果每 2 个计算一次 `SUM(s)`，最终列名为 `total`。例如单个设备
输入 `1..12` 时，第一层输出 `6,15,24,33`，第二层 `total` 输出 `21,57`；
输入只有 `1..7` 时先输出 `21`，第 7 行留在第一层 Count state 中等待后续两行，
第二层已经消费两个 `s` 并完成一次输出。日志行还会带有 Count 窗口的
`count_start/count_end`，它们是 0-based 的到达序号，不是时间戳。

Count 窗口按记录数关闭，不依赖设备时间、wall clock 或 watermark；未满窗口在
checkpoint 中保留，故恢复后不能把它当作 fresh 重跑。该模板的 `source` 是
`append_only` File、`sink` 是 `log`，所以不需要 HTTP fixture；EOF 后默认等待
追加。要让有限输入正常结束，可明确改成 `file_contract=sealed`。

实际操作与 zero-state 模板相同，先准备同样的 `sensors` schema 并把绝对路径改到
`SPARROW_DATA_ROOTS` 下：

```sh
sparrowctl put-stream sensors /path/to/sensors-stream.json
sparrowctl validate /path/to/pipeline-k1-two-count.json
sparrowctl explain /path/to/pipeline-k1-two-count.json
sparrowctl put-pipeline two-count /path/to/pipeline-k1-two-count.json
sparrowctl start two-count
sparrowctl status two-count
sparrowctl checkpoint two-count
sparrowctl checkpoints two-count
```

这是 zero/双 Count 的同一 File/v3 aligned 矩阵：Source、两个 state participant
和 required Sink 都必须在同一切点完成；恢复保留未满窗口和 source cut。改成普通
`restart_fresh` 时不持久化 checkpoint/generation，每次从文件头重新计算；aligned
fresh/reset 则在受控 checkpoint 目录建立新的 `STATE_GENERATION`，不延续旧窗口
状态。Lookup、Dedup、ET/PT 或带分支/多源的真正 DAG 不由这个线性 fixture 代表，
应按各自的支持矩阵单独 validate。

#### 5.3 `v02_static_table`：静态 ReferenceTable

这是 `crates/sparrow-cli/src/bin/v02_static_table.rs` 的 embedding 示例，
不是 SQL 或 Server API。它建立 `sites` 的两个内存快照：v1 将 `a` 映射到
`west`，v2 映射到 `east`；分别用两个新 Job 运行同一条
`device_id=a, temperature=21.0` 输入，预期输出：

```text
job1 (running keeps v1) site=west
job2 (new job uses v2) site=east
v02_static_table: ok
```

可用 examples feature 运行（这只是开发/embedding 命令，不是生产 Server
构建的一部分）：

```sh
cargo run --locked -p sparrow-cli --no-default-features --features examples --bin v02_static_table
```

静态快照在 Job 建立时绑定；运行中的 Job 不会因为另一个新快照自动切换。表名、
key/schema、keep 字段或快照 CRC 不匹配会拒绝构造/运行；示例没有 checkpoint、
`STATE_GENERATION` 或恢复连续性，也没有时间语义。当前 Server 没有参考表发布和
pipeline 绑定工作流，不能只把这个 binary 的 `ReferenceTable` 当作 Server
可运行配置。

#### 5.4 `v03_versioned_lookup`：按事件时间 as-of 的版本表

这是 `crates/sparrow-cli/src/bin/v03_versioned_lookup.rs` 的 embedding 示例。
`sites` v1 从 `valid_from=0` 起为 `west`，v2 从 5 秒起为 `east`；输入的
`ts=1_000_000` 和 `ts=6_000_000` 分别命中：

```text
as-of t=1s site=west
as-of t=6s site=east
v03_versioned_lookup: ok
```

运行命令：

```sh
cargo run --locked -p sparrow-cli --no-default-features --features examples --bin v03_versioned_lookup
```

这里的 `ts` 只用于 `FOR SYSTEM_TIME AS OF` 等价的版本选择，不是 IoT TTL、
watermark 或窗口结束时间。版本表必须有有效区间和可解析的 event-time；缺失或
错误类型会在 lookup 时失败。该示例同样是内存 Job，默认 fresh，未接入
Server 的表发布、依赖 pin 或 checkpoint；当前 aligned checkpoint matrix
明确拒绝 Lookup，因此不能宣称恢复时自动保留外部表版本。

参考表发布、依赖 pin、外部 Lookup 的恢复与运营属于后续 TAB-01～03；K4 只复用
这些已有能力，不扩大其 Server 或生产保证。

本页命令依赖已构建的 `sparrow-server`、`sparrowctl`，以及操作者提供的
HTTP 2xx fixture。K4 不提供 Web 页面，不自动启动目标端口，不自动写入
allowlist，也不将上述本地演示升级为生产认证。

<a id="k4-validation"></a>
## K4 全局 Review 与匹配验收（2026-09-17）

基线 HEAD 为 `d52b15c`，工作区包含未提交的 K3/K4。测试在
`box@100.64.0.16` 完成，本机未编译；产物根为
`/workspace/bench-compare/k4-artifacts-20260917/`，源码副本为
`/workspace/bench-compare/k4-source-20260917/`。未修改既有 Mosquitto 服务，
只使用独立目录、子进程和 loopback fixture。

### Review 后落地的修复

- **数值与状态原子性**：整数相对阈值移位溢出、浮点极端差值溢出；输出准入先于基准更新，候选/旧值复制先取得预算。新 key 的 table/index/state 准入失败回滚容量信用。
- **资源与时间**：per-key TTL 受逻辑 timer 上限约束；bucket 高水位持续计费，空表/cleanup 真正释放；恢复使用独立 replacement map，失败不改旧状态。输入结构 schema 与 owner 在状态变更前校验。
- **恢复边界**：IoT 独立 codec，拒绝空 key、错误 arity/profile 和 RCP2 宽松前缀；v5/v6 encode/decode 拓扑检查一致；无法分类的残缺历史 fail-closed，保留原目录。
- **图执行**：aligned 图只接收有序 events 输入；Union 的关闭输入不能替代 barrier ACK，active alignment 中关闭会失败，普通逻辑 EOF 不伪造切点。
- **控制面**：使用每个物理 Source 的 schema 校验 connector，Source/Sink 分开校验；HTTP-only/目录要求仅收窄新增 IoT aligned，不破坏旧线性 Count/Log。多源 replay、fresh 连续性、能力类型清单和有限错误归因与实际执行一致。
- **交付模板**：测试实际加载两份 K4 JSON、NDJSON 和完整 golden；change 使用 append_only，避免 sealed 读完即结束导致后续 checkpoint 演练失效。

### 测试结果

- `frozen-v5`：reliable/default-members **630 通过、0 失败、16 ignored**；独立 no-demo 生产入口 **36 通过、0 失败**。两种生产包均构建通过，默认包不带 NATS SDK。
- `validation-v5c`：**50 项 K4 × 20 轮 = 1000 次通过**。覆盖计划、算子、Kernel、codec/store、控制面、API、两个实际模板；精确清单为 `expected-tests-v5b.txt`。缺 crate、缺测试、改名、多余、重复、ignored 六个反例均在执行 driver 前拒绝，完整清单通过。
- 两种算子的真实进程 oracle 均通过：周期 v6 提交、SIGKILL 后未提交后缀重放、恢复后的等值抑制、HTTP 响应被阻塞时不能发布 checkpoint、CURRENT.tmp 提交失败不移动 CURRENT、语义变化拒绝旧状态、显式 fresh 新 generation、旧 K3 binary 拒绝 v6 且不改历史。
- `k3-regression-v5`：**24 项 × 20 轮 = 480 次通过**，另有零/双状态多源多 Sink 进程故障回归。
- 默认生产/K1 smoke、**32 项 K2（含独立真实 NATS）**及 K2 零/双 Count 进程故障回归通过。
- Go driver `vet/build` 通过。Clippy 正常命令退出 0，但仍有 **87 条 lint 提示**；`-D warnings` 未通过，不能称为零告警。后续清理见 [OPT-009](OPTIMIZATION_BACKLOG.md#opt-009)。

### 旧 File 路径性能回归

预先声明三组完整 ABBA，三组单独及全部样本合并均通过；没有编译与测量并行，
没有丢弃慢样本。fresh 为 K3 v14 → K4 v5，periodic 为同一 K4 binary 的周期
on/off（100 ms checkpoint、20 ms 慢 HTTP、原批处理参数）。fresh 每组形状合并
36 个测量试次，periodic 24 个；warmup 同样核对正确性。

| 形状 | 吞吐中位数比值 | peak RSS 差值 | checkpoint |
|---|---:|---:|---|
| fresh 零状态，K3 → K4 | 0.989576 | +72 KiB | 不适用 |
| fresh 双 Count，K3 → K4 | 1.006209 | +312 KiB | 不适用 |
| K4 零状态，周期 on/off | 1.004484 | −44 KiB | 12 个 on 试次各成功 18 次，失败 0 |
| K4 双 Count，周期 on/off | 1.005210 | −136 KiB | 12 个 on 试次各成功 12 次，失败 0 |

原门槛仍为 fresh ≥0.97、periodic ≥0.90、RSS 增量 ≤2048 KiB。全部试次输出
无 missing/duplicate/invalid，各组 normalized hash 一致。结果不表示每个指标
都提升，也不是远端 RTT/p99 或 eKuiper 新对照；本轮没有作这些推论。
完整脚本为 `sparrow-k4-regression-v5c.sh`，最终 `regression-v5c.exit=0`；
原始试次与四份合并 JSON 位于 `file-performance-v5-*`。

### 构建指纹与失败记录

```text
default server: 747580ab99f252cccf4ce99bd3888678a5a06a6f00b909415c72e089ff614cf7
JetStream server: 23f7b73ac0ab12626c7b3ca5e8b650faeb822c3c0895198bfb126d65c9f36a2d
K4 Go driver v2: 96e74b176264de4dcf4d8c449a80b162e01f8641defe66cdd974851076f5af75
50-test manifest: 3dd5a5a4bd4a8aa046864023921e05645102fe1c821c56cd028dd0d97c8f5d1f
v5 build source manifest: d662dcee748d9716d99c889cc43571a439f6c31c5ccee8766ede2ef1ae5859d4
```

产物不覆盖：早期编译错误、两处 fixture 合同错误、跨源全局顺序误断言、
49/50 精确清单拒绝，以及旧 binary 仅在 status 而非 stderr 报错导致的 driver
失败均保留。修正的是测试 oracle/清单时，没有改运行语义来迁就测试。
v5 冻结后只更新两项**不参与 Rust 编译**的验证输入：显式测试清单和 Go driver；
`build-to-validation-source-v2.diff` 单独记录差异，最终源码逐文件校验见
`local-remote-source-verify-v2.log`。包内源码指纹保持构建时原值，未伪造重编译。
本页和 TODO 的最终文档更新不改变已测试 binary，候选包中保留当时的开发说明。

**放行边界不变**：K4 首批功能完成不等于稳定发行或全部设计完成。TTL aligned、
IoT+JetStream、Hysteresis/HoldFor/完整告警生命周期、Server 表发布和 Web/K5
未开放；目标设备、TLS/WAN、24/72 h 长稳、掉电/介质故障仍需对应部署验收。
