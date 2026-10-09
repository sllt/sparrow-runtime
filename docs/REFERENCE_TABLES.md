# 不可变参考表、动态更新与外部 Lookup

## 第9批：动态表与 HTTP Lookup（2026-10-05 Preview）

以下是新增的明确选项，不改变旧的静态revision绑定与v8～v11恢复合同。是否验证通过及匹配构建以 [PRODUCTION](PRODUCTION.md) 的本批证据为准；后文B1/B2历史成绩不能替代本批结果。

### 增量发布及回退

表仍保存不可变revision。初始schema/主键通过 `put-table` 发布；此后 `POST /v1/tables/{name}/mutate` 接受：

```json
{"expected_revision":1,"operations":[
  {"op":"upsert","row":["a",20]},
  {"op":"delete","key":["b"]}
]}
```

`row`是完整表字段顺序，`key`按表的keys顺序。一次1～256操作、完整JSON≤64KiB；同批重复主键、缺失delete、NULL/错误key、非法row或最终容量超限导致整批回滚。更新保持原row顺序，新key按操作顺序追加。成功只生成一个新revision；重试旧expected_revision会CAS失败，响应丢失时先查看head再决定后续操作，不隐式重复执行。

`POST /v1/tables/{name}/rollback`：`{"expected_revision":2,"target_revision":1}` 将保留的历史内容发布为revision3，**不倒退head**；target必须还存在，schema/keys必须与当前相同。`GET /v1/tables/{name}/revisions`只返回有限metadata。原128版本/名称、8MiB/名称、32MiB目录逻辑配额不放宽；没有自动删除仍被历史管道引用的数据。GC可回收非latest且无持久引用的版本。SQLite提交失败执行ROLLBACK，不保留半提交head。

CLI：`tables`、`table NAME [REVISION]`、`table-revisions NAME`、`put-table NAME JSON`（含expected_revision/table）、`mutate-table NAME JSON`、`rollback-table NAME EXPECTED TARGET`、`gc-table NAME`。API沿用Bearer、drain/body/并发限制、CAS与不含业务值的审计错误。

### 显式在线跟随

```json
"reference_tables": {
  "limits": {"revision":1,"sha256":"APPROVED_INITIAL_SHA256","follow_latest":true}
}
```

不写 `follow_latest` 时仍为false，旧JSON身份不变，发布新表不影响旧静态Job。开启后仍需初始revision/hash，它是持久baseline pin和schema/key约束。启动先验证baseline，再在任何来源I/O前解析兼容的最新内容；运行时由Supervisor约200ms的converge轮询metadata，仅变化时解码并构造新的Job-owned snapshot。切换发生在**每个Lookup算子处理下一批时**，在途批保持旧Arc；不承诺多个算子、多个表或一整条规则同时观察同一版本，也不是ET as-of重放。

head倒退、缺失、加载到的内容损坏、schema/key改变或构建预算不足都会使Job失败并取消；没有无限沿用旧值的stale fallback。head未变化时不重复解码或重算内容摘要，因此不承诺持续检测绕过API的磁盘篡改。旧snapshot只随在途reader保留，其retention credit直到最后reader释放才归还；替换必须容得下构建峰值和新旧重叠。历史baseline仍由管道revision保守pin；运行时观察过的其他版本不构成checkpoint pin，GC后不承诺历史重现。

### 外部 HTTP Lookup

复用SQL静态表JOIN的Lookup路径或Graph的 `kind:"lookup"`，无需新造SQL函数。通过 `external_lookups` 声明远端schema/keys及连接：

```json
"external_lookups": {
  "limits": {
    "url":"https://business.example/lookup",
    "header_secret":"lookup_token",
    "fields":[{"name":"device_id","type":"utf8","nullable":false},
              {"name":"threshold","type":"int64","nullable":false}],
    "keys":["device_id"],
    "options":{"max_inflight":4,"timeout_ms":1000,"cache_ttl_ms":1000,
               "cache_bytes":65536,"on_error":"fail"}
  }
}
```

URL固定，不从输入拼地址；需原有TargetPolicy allowlist，凭据通过命名Secret显式解析且只允许HTTPS。禁止userinfo/fragment、重复query key、重定向、环境proxy和隐式/应用重试。共用HTTP Client的隐式协议重试也关闭，原HttpSink显式max_retries策略不变。TLS校验仍启用，没有skip_verify。

Wire为 `POST {"keys":{"device_id":"a"}}`；仅 `200 {"row":{"device_id":"a","threshold":20}}` 或 `200 {"row":null}`。必须返回完整声明字段，拒绝未知/重复字段、错误类型、非有限浮点、错主键、超限数据；204/404不是隐式miss。最多16列、8个非NULL稳定主键列，不接受Float key/Dynamic/Array；Bytes为base64，Int64/UInt64/TimestampMicrosUTC精确保留。流的NULL key直接产生miss，不请求远端。请求/响应wire和返回row resident各≤64KiB。

每个**物理Lookup算子**最多1～16并发（默认4），timeout10～5000ms，按输入顺序产出，控制消息不能超越请求；整批完成类型/key验证及输出计费后才发布。不是整个服务或同名provider所有引用合计的并发上限。每请求窗口先取应用scratch credit（HTTP provider声明512KiB，runtime另计key/返回row/输出等），额度不足在请求前拒绝，不自动扩大Job预算。Client/pool/TLS内部metadata不冒充全部纳入应用frame额度或进程RSS硬上限。

每算子cache最多1MiB、1024项、TTL≤60s（默认64KiB/1s），typed key、正负缓存、单调时钟过期和有界淘汰；TTL=0或cache_bytes<256禁用。缓存按算子独立，任务重启清空；TTL内业务变化可能暂不可见，不当历史快照。错误不进cache。默认 `on_error:"fail"`；显式 `"null"` 只将transport/status/timeout错误降为NULL，不吞类型/key/协议/策略/预算错误，也不把cancel当成功。超限或未读完的响应不复用连接；有界响应体读完后HTTP连接可复用，业务JSON校验失败仍使本次Lookup失败。失败/取消会终止并join在途任务，未join响应仍持有内存credit。

### 恢复、诊断与边界

`follow_latest`或external绑定只接受 **live_best_effort + restart_fresh**，拒绝任何restore/checkpoint配置、managed temporal或专用paused-time/feed-observation profile；有限查询不接外部I/O。失败或Server重启后需要显式start，不自动用今天的数据重放旧来源。不是aligned/exactly-once、CDC消费位置提交或历史请求结果日志；远端API必须是只读查询，不能把有副作用的POST塞进Lookup。

`status.lookup_runtime`区分实际attempt、live表observed_revision/SHA/failed与external请求、miss、NULL key、缓存、超时/失败、inflight/peak等计数。原 `reference_tables.stored_latest/running_actual.bindings` 是管道声明，不再被误读为热跟随当前内容。指标不包含行或Secret；同provider多算子计数合并，瞬时inflight可能超过单算子配置。

`timeouts`精确计数runtime外层请求deadline触发；provider自身的连接/读超时可能先返回，计入`failures`，不保证同时增加`timeouts`。判断超时策略是否执行不能只看该子计数；所有这些错误都按同一个`on_error`合同处理。

catalog仍为v4，新增选项省略时不改变旧静态表示及checkpoint codec。旧二进制不认识新配置，应保留配套catalog/配置备份再回退；不能把follow_latest改成静态、或把当下远端返回当旧快照以绕过恢复校验。下一步批量CDC接入、外部数据库专用provider、持久响应日志/恢复、目标容量与长期soak均需独立验证，不由本批Preview泛化。

---

## 以下为静态参考表历史合同与验收

状态：核心增强 B1 和 B2-A 均已在下述限定范围完成实现、交叉 Review、匹配构建、专项/真实进程与相关全局回归；各自三组 File ABBA 的全样本合并通过原门槛，单组失败样本保留。B2-A 有独立证据，不继承 B1 测试结论。未 commit/push/tag，不是生产放行声明。

## 本批边界

**K1～K4 本轮扩展（Preview）**：现有 v8 的静态无状态 File 合同不变；新增 v9（File＋1～2 Count/IoT 状态）、v10（JetStream＋0～2 Count/IoT 状态和稳定输出 cursor）、v11（required File→HTTP DAG＋最多16状态），均带精确静态依赖和完整计算语义。IoT TTL=0，可包含新迟滞；不开放 temporal/ET/PT/Dedup/side-output/lossy 图。新 profile 必须独立目录。本轮匹配功能/进程回归已通过，证据及性能分别见 [组合验收](PRODUCTION.md#k1-k4-reference-validation)，不借用下面 B1/B2-A 的旧成绩。

复用现有 Lookup，补齐 Server 可操作的表发布、固定版本绑定、运行与回收流程。第一批只开放 **线性 `restart_fresh` + 静态不可变参考表**，没有前端，也不将外部实时查询混入历史重放。

- 每次发布生成不可变 revision 和 SHA-256，内容覆盖表名、版本、schema、主键约定和全部行。发布以 CAS 原子推进 latest；不会热替换运行 Job 的表。
- Pipeline revision 显式携带 `reference_tables: {name: {revision, sha256}}`。缺失版本、摘要不符、无用绑定、schema/key 不兼容和未绑定 Lookup 均拒绝；不默默使用 latest。
- Job 启动前解析确切版本，表常驻内存归该 Job owner 计费。参考表发布成功或失败都不原地替换运行 Job 的表；`restart_fresh` 重启后重新处理来源仍使用原绑定版本，而不是最新表。显式切换 pipeline revision 沿用 stop/join 后 start；后期准入/连接失败会令新 attempt 失败，不承诺自动无缝回退旧 Job。
- 全部持久 pipeline revisions 都是保守 pin，即使当前未运行、不是 latest，也可能再次启动。GC 仅删除非 latest 且没有此类引用的版本；检查和删除与发布配置使用相同 catalog 事务边界。
- 暂无 pipeline revision 删除工作流，因此被历史配置引用的表不会自动释放。返回 pin 原因，不建议手工删除 SQLite 行绕过。
- 每表/每次发布/总目录容量均有硬限制；重复主键、错误行宽/类型、NULL 主键、非确定性 key 等明确拒绝，不覆盖、不猜测转换。

## 不等于 checkpoint 恢复

B1 的引用 pin **不是** checkpoint dependency pin。现有 v3/v4/v5/v6/v7 均不因该批次自动支持 Lookup aligned。

后续可恢复子集必须同批具备：完整 schema/content 摘要与来源切点进入版本化 checkpoint、启动前精确 resolve、未提交 checkpoint 与已保留 checkpoint 的依赖 pin、GC 及进程故障验证。缺失任一依赖必须失败，不能换成今天的表。Temporal/as-of 表还需要冻结所依赖的版本时间线，不仅是一个 latest revision。

### B2-A：静态 File aligned 合同

独立开放的首个组合是 **File → stateless Transform/static Lookup → required HTTP**，1～8 个精确依赖，显式 `checkpoint_dir`。它不是任意 Lookup aligned：Count、IoT、ET/PT、Dedup、DAG、JetStream 和 temporal/as-of 组合继续拒绝。

- 使用独立 **snapshot v8 / plan CPL3**。没有重用曾废弃的 CPL2，也没有修改 v3～v7/CPL1 的编码。不同 profile 必须使用不同目录；旧 binary 或无 reference profile 不得把 v8 当普通损坏后回退到旧快照。
- 依赖记录表名、revision、canonical SHA-256 和 runtime CRC32；表 schema/key/内容由两个各自明确的编码校验，SHA 与 CRC 不能互相代替。依赖不是可变状态参与者，checkpoint 不复制全部表行。
- 恢复先从 catalog 解析精确表并按 Job owner 预算构造，再校验 manifest 与完整计算语义，最后定位 Source cut、开放来源。任何缺表、坏摘要、schema/key/CRC 变化都失败；不能切到 latest 或自动 fresh。
- 自动恢复必须显式配置 `checkpoint.resume_latest:true`，或使用既有显式 checkpoint restore。新 v8 目录已有 CURRENT、损坏 CURRENT 或 generation 历史时，省略恢复选择也不能绕过兼容校验从头启动；fresh 重放需新目录。首次真正空目录仍允许只做手动 checkpoint。
- 新 profile 使用完整计算语义，**不应用**旧零状态路径的 downstream-prefix 放宽。改变 Lookup、过滤/投影或表 revision 都需要原兼容配置或显式新目录重放。
- 保留的历史 pipeline revisions 对其表作永久保守 pin，覆盖未提交及保留 checkpoint 所需的依赖；GC 不会删除这些版本。没有独立解除 checkpoint pin 或删除 pipeline 历史的接口。迁移/备份必须包含 catalog、所需输入历史和 checkpoint，单独复制 v8 文件不包含表数据。
- CURRENT 只有 Source cut 和 required HTTP flush 屏障完成后才推进。HTTP 响应不确定或 CURRENT 提交失败可导致旧点之后的输出重复；**File profile 不提供 JetStream 的稳定业务输出 ID，也不承诺 exactly-once**，接收端应按业务需要处理重复。

模板 `deploy/pipeline-reference-lookup-aligned.json` 使用独立 v8 目录。先发布 `reference-table-limits.json`，将返回的 revision/SHA 一起替换占位符；输入与独立 golden 复用 B1 文件。发布新表不会改变运行绑定或已有 checkpoint，不能通过改 catalog 行或版本号强制兼容。

### 2026-09-17 B2-A 验证证据

当前候选 `package-v4-default` / `package-v4-jetstream`，服务器证据目录 `/workspace/bench-compare/core-b2-artifacts-20260917/`。最终 `regression-v4c.exit=0`：功能、相关全局回归与预先安排的三组 File ABBA 全样本合并通过原门槛；不是生产放行。

- reliable/default-members **700 passed、17 ignored**；独立 no-demo **44 passed**。独立清单 **25 × 20 = 500**：Plan 10、Runtime 8、Control 7；没有以零测试或 ignored 代替通过。门禁的 9 类 shell fake inventory/summary 反例通过。
- `validation-v4c/process` 使用独立 Go v6 oracle，真实 File/HTTP/Server：v8/CPL3 精确表名/revision/SHA/CRC、固定 r1、周期 Source cut、HTTP body 收到但未响应时 CURRENT 不提前、SIGKILL 后只重放未提交后缀、CURRENT.tmp 真实 I/O 故障与精确旧点恢复、改绑定拒绝、API 缺依赖拒绝、GC 保留依赖、旧 B1 和新无引用 profile 双向拒绝 v8。对拒绝路径校验完整历史/CURRENT/HTTP 输出不变，不以泛化错误匹配代替预期分支。
- 直接修改真实 SQLite table payload 的 Rust 用例证明 exact resolve/start 失败且 CURRENT/输出不变。另有空目录手动 checkpoint → 不选择恢复时拒绝旧历史 → 显式 `resume_latest` 恢复的完整流程。表 foreign owner、未 verified 身份、坏 CRC、key/output schema 不符在 Kernel 激活前拒绝。
- A **8×20**/5 种真实进程、B1 **38×20**/真实进程、K4 **50×20**/两个进程、K3 **24×20**/进程、K2 **32 项**/真实 broker 进程、default 与 K1 smoke（含旧 codec/旧 catalog 回退）通过。不是 TLS/WAN、24/72 h 或掉电认证。
- Clippy 普通模式退出 0，**97 条 warning**，并非 `-D warnings`。Go vet/build 通过。未在本机运行 Cargo；Rust 不因后续 Go fixture 修正反复重编译。

性能对照仍使用原 **K4 v5**，没有换成较慢的中间候选。fresh 零/双 Count 合并比值 **1.004972 / 0.990004**（各 36 个实测样本，门槛 ≥0.97，RSS +340/+716 KiB）；100 ms checkpoint/20 ms 应用响应等待的周期零/双 Count 比值 **0.999167 / 1.008431**（各 24 个样本，门槛 ≥0.90，RSS +52/+660 KiB），RSS 原门槛均为 +2048 KiB。输出完整、无缺失/重复/非法行且归一化 hash 一致；周期 checkpoint 成功次数为正、失败 0。**第 1 组 fresh 零状态 0.938404、第 2 组 fresh 双 Count 0.965253 单组未过**，第 3 组通过；全部原始样本保留，未删样本或修改门槛。不能据此声称已定位历史单组波动的根因。

```text
base commit       1dd17c8186b5f52f2cea85a5fae1a94d79722b22
source manifest   cbf8c6a4601a24230639ccd8cd91ec64aa2a60e031de1105c5a04db19d8042f9
default server    76918cf526de7752de4f8518987496447dc6de4fcda0eca6063f76f368c56702
JetStream server  c5a1deed5291f49b5ed0bb695d1c2fff2076275d6124654b165eba1045ce3fbf
Go B2 v6 oracle   457bfb8c57ca8a26e94e11fda95ce97ee6a698e404f4f1ec9d21019a8721aa55
```

保留的失败及修正：v1 漏补一个旧测试的 `CheckpointPlan.reference_tables` 字段；v2 两个新控制面夹具漏设默认关闭的 `resume_latest`；v3 新 status 文案漏了旧 R11 要求保留的 `plain_CP01` 描述，补回真实旧合同而未删除断言。v4 Rust 全部通过。`regression-v4` 的 Go periodic oracle 错把 `storage` 嵌套两次，实际已提交 20 个 checkpoint；Go v5 修正后，`regression-v4b` 又暴露 HTTP hold 夹具把 discard 标记带到重启后的正常请求，Go v6 将 discard 限制到被 hold 的请求，并保存可序列化的响应证据。v4/v4b 失败目录原样保留；最终功能证据来自 v4c。Go v6 源码/二进制独立归档，v4 build 的原源码清单没有伪装包含这些后续夹具改动。

复用入口（不编译）：

```sh
bash scripts/production-core-b2-validate.sh NEW_EVIDENCE PACKAGE FROZEN_TESTS CORE_B2_DRIVER OLD_B1_SERVER 20
```

## 操作入口与模板

当前实现中的 HTTP 入口均复用管理面 Bearer 鉴权与 drain/并发/请求大小限制：

| 操作 | 入口 |
|---|---|
| 发布并原子推进 latest | `PUT /v1/tables/{name}`，`{"expected_revision":0,"table":{...}}`；0 仅用于首次发布，更新传当前 revision |
| 表列表（不返回 rows） | `GET /v1/tables` |
| 查看当前内容或确切版本 | `GET /v1/tables/{name}`、`GET /v1/tables/{name}/revisions/{revision}` |
| 查看依赖与保守 pin | `GET /v1/tables/{name}/dependencies` |
| 回收无引用旧版本 | `POST /v1/tables/{name}/gc`，body 为 `{}` |

先发布 `deploy/reference-table-limits.json`。把返回的 `revision` 和 `sha256` **一起**填入 `deploy/pipeline-reference-lookup.json`；模板的全零摘要只是不可执行占位符，不能直接启动。输入 schema 为 `device_id: utf8, non-null`、`v: int64, non-null`。配置来源文件、数据根目录和 HTTP allowlist 后，先调用既有 `/v1/validate`、`/v1/explain` 再发布、启动 pipeline。

`deploy/reference-lookup.ndjson` 与 `deploy/reference-lookup.expected.json` 给出独立预期：a→10、unknown→NULL、b→20。参考表更新后，旧 pipeline revision 仍使用原版本；需要明确发布新绑定的 pipeline revision 并启动它。`status.reference_tables` 区分 `stored_latest` 与 `running_actual`，不能用 latest 的绑定冒充正在运行的绑定。

### 存储与兼容

- Catalog 升为 **schema v3**；升级前保留停机一致的原 catalog 备份。旧 v2 binary 拒绝 v3 catalog，回退必须配合旧备份，不能只换 executable 后手动降 version 数字。
- 初始上限为 64 KiB/表 revision、1024 rows、128 retained revisions/名称、1024 名称、8 MiB/名称、32 MiB/参考表目录逻辑数据；这些是应用数据与 metadata 配额，不是 SQLite 文件/RSS 硬上限。GC 也不承诺自动缩小 SQLite 文件。
- JSON 发布支持的标量由 schema 显式决定，不对数字/字符串做猜测转换。表主键非 NULL；流的 nullable key 不匹配任何表行时仍按 miss 输出 NULL。
- 每个 Job 另受自己的 reservation/retention 上限约束，wire payload 小于 64 KiB 不代表在任意 schema/并发组合下必然能装入 Job。超限在开放来源前失败、退还预留，不给表另建无限制配额。
- Embedding `ReferenceTable::snapshot` 的 `max_bytes` 现在是保守 resident/构造峰值约束，原来依赖极小逻辑字节上限的调用可能需要显式调整预算；重复主键不再静默覆盖。生产 Job 使用持有长期 lease 的 `snapshot_owned`。Temporal embedding 的版本及生效时间必须严格递增、schema/key contract 不变，但这不意味着 Server temporal 或 aligned 已开放。

## 时间与停机语义

维持当前边界：正 processing-time TTL、PT Window、受限制的 ET 组合不因可保存一段 timer 数据就开放 aligned。

- **可重放时间**：时钟输入或 timer 决策必须能够重放，并与 Source cut、状态、timer 队列和输出身份共同提交。单纯记录 `remaining_ttl` 不能保证崩溃后未提交后缀产生相同输出。
- **停机是否计时**：必须是显式合同，不能用重启机器当前墙钟隐式决定。若选择真实墙钟 resume，则声明回拨/跳跃及非确定性边界，不冒充可靠历史重放。
- **恢复顺序**：先恢复状态、时间域和 timer，再开放来源；到期边界与同时间输入顺序必须固定；取消后不再产生新 timer 输出。
- **DAG event-time**：需保留各来源 watermark、idle/EOF 状态及合流策略，仅保存 Window 的 effective watermark 不足以恢复原执行。

本批先关闭静态参考表操作与资源边界，随后才扩展具有确定性输入和独立 codec 的恢复/时间组合。旧 checkpoint 和 catalog 的升级、回退限制随对应实现和证据另列，不进行隐式状态迁移。

## 验收出口

1. API 鉴权/限额/CAS/原子失败、旧目录保护；错误数据没有副作用。
2. 真实 File → Lookup → HTTP：命中、miss 为 NULL、发布新表不影响旧绑定、显式更新后才使用新版本、进程重启仍读固定版本。
3. 历史 pipeline revision pin、并发发布与 GC、GC 后不可恢复的依赖拒绝；失败和取消退款。
4. Runtime malformed rows 不 panic、schema/key 一致性、索引/复制峰值/长期 lease 的有界测试与原 Lookup 回归。
5. 全库及已有可靠链路回归，独立 review；未执行的长稳、目标设备和真实网络门禁继续保留。

### 2026-09-17 B1 验证证据

服务器 `box@100.64.0.16`，证据根目录 `/workspace/bench-compare/core-b-artifacts-20260917/`，候选 `package-v2-default` / `package-v2-jetstream`：

- reliable/default-members **675 passed、17 ignored**；独立 no-demo **44 passed**。其中 ignored broker fixture 由其他专项执行，不算作本次普通测试已运行。
- 独立人工清单 **38 × 20 = 760** 通过，覆盖 Runtime 内存/row/schema/NULL 边界、精确绑定、catalog 事务与 GC、API。GC 与 pipeline 发布竞态测试使用两个 SQLite connection，每轮 16 个竞争切点，不只依赖同一 Rust mutex。门禁还通过 13 类 fake inventory/错误 summary 反例，拒绝零测试、漏测、重名、忽略、新增未登记测试及伪真假值。
- `validation-v2/process` 为真实生产 Server/File/HTTP：命中与 miss 独立 golden；发布新表不改变运行绑定；无引用 r2 可回收但历史绑定 r1 保留；SIGKILL 后固定 r1 重启；显式改绑 r3；再次启动历史 pipeline revision 仍使用 r1。实际 status 区分 stored latest 与 running actual，dependency preview 校验精确 pipeline/revision/SHA。旧 K4 binary 拒绝复制的 v3 catalog，catalog 内容 hash 不变。
- Review 修复过 row 越界/类型、重复主键覆盖、schema 未纳入 CRC、selection/复制预算、nullable stream key 的兼容回归、temporal publish 的选表竞态、blocking loader 的 admission guard 生命周期、v3 缺失 pin 表及 PK/FK 校验、GC 预览截断和 actual/stored 混淆。没有通过删除断言或放宽配额掩盖问题。
- v1 首轮因 `serde_json::Map::entry("name".into())` 类型推导失败停止，日志保留；v2 改为 `entry("name")`。Clippy/两套 Go vet 退出 0，Clippy **95** 条 warning，未宣称零 warning。工作区生产输入逐文件与 v2 source manifest 校验一致；后续 MD 补证据不代表重编译。

```text
base commit       1dd17c8186b5f52f2cea85a5fae1a94d79722b22
source manifest   17eb0e4e1d6f5c43d82ef86994b1457347fd5bff8a33453dc7bb7f6147ec638e
default server    bde69a0e83cb324f9b83ec60fef6535c37c3ea8af0a691ce22b7352f69a606a2
JetStream server  e6bb8579c938a270fd6e5cfa532cf2c264409f9e485fb071014820d9c1d17c41
Go B1 oracle      94aa200ae019657973c7a8e9a2f13dda2a456d51363083474d6db671ef00be4a
```

复用验证入口（不编译）：

```sh
bash scripts/production-core-b-validate.sh NEW_EVIDENCE PACKAGE FROZEN_TESTS CORE_B_DRIVER 20 OLD_SERVER
```

全局回归 `regression-v2d.exit=0`：Core-A **8×20** 及 5 种真实进程、K4 **50×20** 及 change/deadband 进程、K3 **24×20** 及进程、K2 **32 项**及真实 broker/进程、default smoke、K1 零/双 Count 与旧 codec 双向/历史 K1 回退均通过。各阶段使用已验证的独立结果接续，不把中途脚本失败标成成功。

三组预先安排的完整 ABBA 保留全部样本：fresh 零/双 Count 对照仍为 **K4 v5**，合并比值 **0.997751 / 1.001507**（原门槛 ≥0.97，RSS +412/+504 KiB）；周期零/双 Count 的候选 on/off 为 **1.004426 / 0.998130**（100 ms checkpoint、20 ms 应用响应等待，原门槛 ≥0.90，RSS +104/+504 KiB）。全部输出一致、完整，周期成功次数为正且失败 0。**第 1 组 fresh 双 Count 为 0.960720，单组未过门槛**；第 2/3 组通过，最终判断使用预定三组全样本合并，没有删样本。当前合并候选通过不意味着已找到此前 Core-A 性能波动的单一代码根因，也不改变 Core-A 独立候选的历史失败记录。

保留的测试入口失败及修正：

- `regression-v2`：旧 K4 oracle 用新 binary 创建了 schema v3 catalog，旧 K3 在 catalog 门禁处正确拒绝，尚未到达预期 v6 checkpoint guard。Go `k4-process-v3` 改为旧 binary 创建独立旧 catalog，仍要求原 profile 错误、CURRENT/历史/输出不变；两种算子重跑通过。
- `regression-v2b`：default smoke 初次 bind 遇到 `Address already in use`，未开始功能测试。原默认端口位于主机 ephemeral 范围；默认改到低端口段、保留监听预检与手动端口覆盖。端口原占用者未确认，不把推断当成产品故障根因。
- `regression-v2c`：default smoke 通过；旧 codec fixture 的独立 reader catalog 新建后漏发 start，停在 stopped。补 start 后在 `k1-smoke-v2d` 重跑完整场景通过。历史 K1 回滚使用升级前旧 catalog 副本，保留新 catalog 和新 CURRENT，不改 schema 数字绕过保护。

上述最后修正仅涉及 Go/shell 验证入口，未重编译 Rust；代码与 `package-v2-*` 相同，修正后的输入在 `fixtures-v3/v4/v5` 和相应 tar 中独立保存。`default-smoke-v2d`、`k1-smoke-v2d`、`k2-process-v2`、`file-performance-v2-block-{1,2,3}` 为最终接续产物。B1 证据不覆盖 aligned Lookup、checkpoint pin、PT/time 恢复、TLS/WAN、24/72 h 或掉电认证。
