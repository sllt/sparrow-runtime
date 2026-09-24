# K3：真实 DAG 执行与恢复

<a id="time-graph-recovery"></a>
## 时间型 File DAG：v18 / v19（2026-09-23，限定 Preview 已验收）

本节是独立新 profile，不修改下方历史 v5/v6/v11/v12 的合流和恢复合同。**本批实现、自查和匹配功能/故障/性能门禁已完成，不是生产长稳认证或正式发行。** [验收证据](PRODUCTION.md#time-graph-validation)。

- **v18：** 暂停的 processing-time，支持 PT tumbling、Count、Change/Deadband 正 TTL、TTL0 IoT、HoldFor/Debounce 的 required 分支/合流。
- **v19：** event-time tumbling/hopping、Count 和 TTL0 IoT；所有来源显式绑定时间列。PT/正 TTL/计时 IoT 与 ET 不混用。
- 仅 File（append-only、sealed、immutable）→全部 required、校验证书的 HTTP。最多 16 个源/汇/状态参与者，64 节点/128 边，仍受原 Job 总预算约束。不接受 JetStream 图、参考表、Dedup、side/lossy 分支或历史 checkpoint replay。
- 必须 `recovery:"aligned"`、独立 `checkpoint_dir`、`fail_on_decode:true`、`resume_latest:true`，决策空闲间隔 `checkpoint.interval_ms` 为 100～1000 ms。不是增大默认预算后放开任意拓扑。
- 时间决策对 Filter/Project 之前的**完整输入行**取指纹，当前只支持一等标量源字段；nested/Dynamic 源列在准入时拒绝，即使下游会丢弃该列或初始值是 NULL。旧无时间 journal 的 profile 和 live 图不因此收窄。

### 持久决策与确定性合流

一个协调 actor 按持久轮转位置选择**一条输入、一条永久 EOF 或一次空闲 tick**。先写入有大小限制、SHA-256 校验且 fsync 的 `TIME_PENDING`（GTD1），再向每个 Source 的同一 FIFO 发布：逻辑时间、前置 progress、可选数据、目标 progress、round-end。所有来源无论 active/idle/EOF 都参与之后的 barrier。

Union 不在新 profile 中沿用旧 ready-order。它有界收集该轮的所有端口，避免 fan-out→rejoin 因只等待一个端口而死锁；到齐后按**物理边顺序**重放，保留边内顺序。这是“同一已记录决策的重放稳定”，不是原始设备总序、事件时间全排序或跨源并发原序。缓冲受 Job `max_rows`、working reservation 的一半及元数据 credit 限制；超限明确失败，不无限增长。

Compact 当前 `max_rows=256`，这是每个 Union **整轮累计**上限，不仅是单个 batch 的上限；多分支 timer 同时到期也计入。大基数/大展开量必须先验证轮缓冲和字节预算，不能由每算子 1024 keys 的格式上限推导“1024 个 timer 可在同轮合流”。

Union progress、所有状态 freeze、各 HTTP Sink 的真实 flush/next-output ordinal 与全部 Source cursor/idle/EOF/逻辑时间，共同进入 `CPL1/CP01DAG2` 快照，再提交 `CURRENT`；完成前不读取下一决策。所有等待可取消，已启动的文件/fsync worker 必须 join。超时/失败保留 pending 并结束 attempt，不在丢失 round 上继续执行。

Rust embedding 必须遵守同一持久化/FIFO/round 协议；`GraphRuntime::complete` 只收集 ACK/control cut，并不代替落盘 `CURRENT`。调用方完成实际提交前不得启动下一轮，失败后必须结束 attempt。

### 时间、EOF 与重放身份

- PT/TTL 停机暂停；恢复及 pending 重放不消耗停机时间。计时控制先于 timer 派生行，下游先处理同一时刻已有的到期状态。
- ET 另行记录墙钟观察值，仅用于 future-skew 检查；pending 重放使用原观察值，不能因重启后的当前时间变化把旧拒绝行变成有效行。
- 可选 `graph_io.idle_after_ms` 为 100～86400000；按已记录逻辑时间、最近输入判空闲。未配置时，空 append-only 文件保持 active/未初始化，不能猜测为 idle/EOF。重新 active 不回退已经输出的 watermark。
- sealed/immutable 的永久 EOF 是持久 progress，不是异常 channel close。全部上游永久 EOF 会关闭 ET 的 positive-lateness 尾窗；PT 的来源 EOF **不停止分支 timer**。这些 Source 仍接收后续 round/barrier，Job 由显式 stop 结束。
- 每个 Sink 使用由 generation 和 Sink ID 派生的独立 epoch、持久 ordinal。A 已接受而 B 未接受时，恢复可能再次发送给 A，但同一输出的 ID/数据保持；下游需要按 ID 去重。**不承诺 exactly-once、跨 Sink 事务或业务落库 ACK。**
- 仅支持 `CURRENT` 与其唯一直接后继 `TIME_PENDING`；缺日志、校验失败、文件合同/idle 策略/图语义改变、缺参与者或源身份不符均拒绝。旧目录不自动升级，新目录也不能交给旧 binary 改写。

### 模板与验收入口

注册 `deploy/stream-time-graph.json` 为 `graph_signals`；按 `deploy/pipeline-time-graph-pt.json` 或 `deploy/pipeline-time-graph-et.json` 配置输入文件、目标 allowlist 和独立目录。ET 模板使用 sealed，必须先准备完整输入；持续追加时同时修改对应 `graph_io.sources` 与顶层最小 Source 镜像为 append-only，必要时明确 idle 策略。

测试：`time_graph_` 精确清单见 `tests/time-graph/expected-tests.txt`；冻结重复/进程入口为 `scripts/production-time-graph-validate.sh`，独立 Go 驱动 `--time-graph-only` 解码 GTC1/GTD1、执行真实 SIGKILL 和部分 Sink 接受切点。长稳、TLS/WAN、掉电和容量认证不能由这些短程测试代替。

该 profile 每条输入都支付决策 fsync、所有 required HTTP flush 和 snapshot/CURRENT 的成本，**不是高吞吐默认路径**。旧线性 hot path 与旧 DAG profile 不自动采用这套协议；批决策/日志摊销留在 OPT-012。

**默认 Server 图预算的实际边界：** 目前每条内部边配置 256 KiB，单 Job queue 上限为 2 MiB，另计观察/通道元数据；因此 64 节点/16 状态是格式上限，不是默认 Server 可启动规模。此次 12 边测试需要 3,220,480 B，明确被默认 2,097,152 B 配额拒绝，未发布输入/输出或 CURRENT。大图的低容量 mailbox embedding 测试与默认 Server 的可接纳进程形状分开记录；不通过扩大预算或删掉拒绝证据宣称大图已获默认支持。

**2026-09-17 核心完善增量：** required File→HTTP 图已接入固定 revision/SHA/CRC 的静态参考表，独立 profile11 支持最多16个 Count/IoT(TTL0)状态。真实进程覆盖 Lookup 分支、双来源 Lookup→Union，以及 Lookup→Count→Hysteresis→双 required HTTP 的 pending Count/latch 同切点恢复，见 [本轮证据](PRODUCTION.md#k1-k4-reference-validation)。无引用迟滞图使用独立 profile12；该声明仍是有限 Preview，不开放时间型状态、side/lossy、JetStream DAG 或确定性全局合流。

**K4 扩展（开发候选）：** 图内新增变化检测/Deadband，含 IoT 状态的 File→required HTTP checkpoint 使用独立 **v6**，不是下文历史 K3 验收的 v5；限制和新证据见 [IOT.md](IOT.md)。内嵌 aligned 图必须使用有序 events 输入，不能以有限 rows/raw row 通道代替可接收 barrier 的 Source；逻辑 EOF 不等于 checkpoint ACK，已关闭输入不得满足 Union 对齐。

状态：2026-09-16，K3 的 DAG-01～07 核心功能已完成实现、自查及下方限定矩阵复验；**仍是 Preview，不是全场景生产认证或发版声明**。Graph Designer 属于 K5，不在本批。既有 SQL/线性 File 与默认关闭的 JetStream 路径保留；本批不扩展 SQL 为通用 UNION/DAG 语法。

## 支持范围

| 能力 | 合同 |
|---|---|
| Branch | 显式广播每条记录；共享 RowBatch backing，不给每个分支深拷贝 payload；有限输出端口 continuation，相关 required 支路背压上游 |
| Route / Switch | 必须声明 `route_mode: first_match / all_match`、有序 `routes` 与 `default_out`；NULL 条件按不匹配；默认端口仅在没有匹配时收到记录；first-match 不执行后续条件 |
| UnionAll | 结构兼容的有序字段/type/nullability；逐物理输入轮转，保留每条输入边内的顺序与来源 metadata；**不保证跨输入全局顺序**，同源经过不同分支再合流也不恢复原始总序 |
| 多 Source | Server 的 File、MQTT、HTTP Push 可绑定到不同 Source operator；MQTT/HTTP 仍是 live best effort，不产生重放承诺 |
| 多 Sink | HTTP、MQTT、Log；required / 显式 best-effort 整条支路区别对待；A 已成功、B 失败不会回滚 A |
| Side output | File decode error、ET late、Filter rule reject；独立 schema、有限队列与明确的满队列策略 |
| aligned | 独立 v5 profile：1～16 个 File 输入、1～16 个 required HTTP 输出、零状态或最多 16 个 Count 状态参与者；还受实际 Job 总预算限制 |

上述 legacy v5 aligned 不接受 ET/PT、Lookup、Dedup、有损分支、side output、Source time 或 JetStream 图组合。它们不是静默降级为 `restart_fresh`；validate/start 明确拒绝。逐类型扩展 time/side-output snapshot 是后续能力扩展，不用没有测试的 codec 宣称“任意 DAG 可恢复”。

## 编写图和绑定 I/O

GraphSpec 保持 `version: 1`，原线性 JSON 不变；未使用的新增可选字段不写入 JSON，避免仅保存一次旧线性配置就破坏旧版 strict reader。图节点的 `out` 明确列出目标节点 ID；隐式 Source fan-out、普通节点 fan-in、循环、重复/不存在的边、错误端口数量均拒绝。使用 `branch` / `route` / `union_all` 表达拓扑，不依赖节点数组的书写顺序。

Server DAG Pipeline 增加 `graph_io.sources` / `graph_io.sinks`，键为 operator ID 的十进制字符串，必须完整、恰好覆盖所有 Source/Sink。为兼容原 PipelineSpec 的必填字段，顶层 `source` / `sink` 必须分别等于 **最小数字 ID** 对应的配置，不能留下影子配置。`stream` 仍须是已注册 stream；每个 source 的 `table` 决定其输入 schema。

运行时 pipeline/revision 由 Catalog 的具名 pipeline/已提交 revision 确定；用户原 GraphSpec 保存在 Catalog，但不能覆盖执行 revision。Rust embedding 的 `PhysicalPlan` 新增显式 edges/time/side metadata，`JobRequest.graph_inputs / graph_outputs` 按 OperatorId 绑定；这是 Rust API 扩展，需要调用方更新 struct literal。

示例：[`deploy/pipeline-k3-graph.json`](../deploy/pipeline-k3-graph.json)。替换文件路径、端口并配置数据根/目标 allowlist 后，仍走现有 validate、explain、put/start、status、checkpoint、stop/restore API 与 CLI；不新增另一套执行引擎。

Route 示例（`out` 中的每个端口必须恰好作为一个 case 或 default）：

```json
{
  "id": 10, "kind": "route", "route_mode": "first_match",
  "routes": [{"predicate": {"k":"bin","op":">","left":{"k":"col","name":"v"},"right":{"k":"lit","value":{"t":"int64","v":30}}}, "to": 20}],
  "default_out": 21, "out": [20, 21]
}
```

普通 Filter 保持丢弃不匹配行的旧语义；只有配置 `side_output.kind=rule_reject` 后，未匹配行才流到侧端口。

## required、best-effort 与资源

- `branch` / `route` 的 `best_effort: [target_id, ...]` 显式把指定边及后续支路标为有损，末端必须是 `best_effort_sink`。有损分支禁止重新 UnionAll 或接 required `capture_sink`，不能汇流后重新声称无损。
- required 等待有界 item/byte credit，所有等待可被取消。没有全局锁跨网络/队列等待；取消会 join tasks、清空 mailbox 并归还持有额度。
- best-effort 在数据队列满时丢弃并计数。**控制消息不会当作普通数据静默丢弃**：无法接纳控制的有损支路，会取消/脱离本 attempt，记录 `graph_detached_branches`，不能继续消费但丢失时间/切点。它不拖住 required 支路；下次 attempt 才重新连接。
- required Sink 的关闭/真实输出失败终止图 attempt；有限输入完成还要检查真实输出 flush。best-effort 子树的普通错误只终止该子树并计数；嵌套可选支路继承实际父支路的取消 token，外层脱离会取消内层，不能遗留阻塞中的叶子。panic 仍属于 Job 失败。HTTP 2xx 是传输接受，不是业务事务 ACK。
- 最多 64 个节点、128 条边；单路由最多 16 个输出端口（最多 15 个条件加一个默认端口）；UnionAll 最多 16 个物理输入。GraphSpec JSON 最大 64 KiB。Server 每图最多 16 个 Source、16 个 Sink。
- 图的 metadata/编译表达式/运行上下文有保守 working reservation；每条物理边分别计 item/byte 与 held/pending credits，payload backing 按原 owner 生命周期共享。Route 的选择 masks、表达式 scratch 和输出 builder 有界；状态继续共享同一个 Job owner。
- **上限不是默认配置保证能接纳这么大的图。** 默认 Compact Job 的队列/working/state 额度不自动放大；边数、宽 schema、fan-out、编码/冻结同时存在时可能更早拒绝。第三方分配与整个进程 RSS 仍按原 Runtime 合同，不把 credit 计数等同于 RSS 硬限制。

## Side output schema 与满时行为

Side port 也必须列在 `out`，主端口是另外一个目标。每个支持节点最多一个 side port。

```json
"side_output": {"kind": "decode_error", "to": 30, "full": "backpressure"}
```

`full` 必须显式为 `backpressure` 或 `drop`；后者要求有损子树及 best-effort Sink，控制满时同样脱离支路。

| kind | 节点/前置 | schema |
|---|---|---|
| `decode_error` | File Source；`fail_on_decode=false`；MQTT/HTTP 尚不提供这个 side port | `source_operator: uint64`、`error_code: utf8`，均 non-null；当前 code=`CodecViolation`，不复制原始 payload/密钥 |
| `late` | Event-time window；不接受 Count/PT 伪装 late | 原窗口输入 schema；future-skew 拒绝仍单独计 `future_dropped` |
| `rule_reject` | Filter；predicate false/NULL | 原 Filter 输入 schema；表达式执行错误仍失败，不冒充 rule reject |

`graph_side_rows` 统计生成的侧路行，不是外部送达证明；`graph_dropped_rows`、`graph_detached_branches`、`graph_branch_failures` 独立记录失败/丢弃情况。

## 时间与来源

ET DAG 每个 Source 显式声明 `event_time_field`，可选 `out_of_orderness_micros`、`max_future_skew_micros`。时间列必须是非空 Int64 / TimestampMicrosUTC；未知、负时间或不合法配置拒绝。过远未来时间不推进 Source watermark，窗口继续按 future-skew 策略计数。当前 Source future-skew 上限按全图所有 ET 窗口保守比对，不能大于其中最严格的窗口限制；尚未放宽为仅检查可达窗口。

Source 先发送数据，再发送由该批最大有效 event time 推导的 watermark；生成发生在 Filter/Route 之前。Project/Map 必须原样保留或重命名时间列，不能对时间列做算术后仍复用原 watermark；UnionAll 要求所有输入时间属性兼容。窗口消费上游合并后的 watermark，**不再从快输入数据自行抢跑时间**。窗口输出不自动成为下一 ET 窗口的有效来源时间属性。

UnionAll 为每条物理边预注册 time state：active 未初始化会阻止进度；idle 被排除；全部 idle 不推进 MAX；重新 active 不回退已发出的 watermark。永久结束使用显式 `StreamControl::EndOfInput`：只有所有输入明确完成，才可以推进最终时间。**未携带 EOF 的 channel close 是失败/取消，不是正常结束，不能提前输出 final window。** EOF 后不允许数据，但 aligned 来源仍必须处理 barrier；PT 窗口要等到定时输出完成后再转发 EOF。

Server File 的 append-only EOF 仅等待追加，不等同于永久 EOF；sealed/immutable 在有序数据之后发布 EndOfInput。HTTP/MQTT 空闲不被猜测为 EOF，也没有擅自引入 idle 超时。Embedding 的有限 `rows` 自动发布 EOF；有序 ingress 调用方须显式发布 EndOfInput，不能用 drop Sender 代替；Idle/Active 也是有序控制。

非聚合 RowBatch 携带 `source_operator`，UnionAll/Filter/Project/Map/Lookup/Dedup 保留；聚合输出不伪称只来自某一个源。此 metadata 不擅自混入用户业务 JSON。

## v5 多输入 checkpoint

1. 单飞 coordinator 给每个 File actor 发 cut 请求；actor 完成当前读取/按序入队，刷新文件身份，再在自己的同一 ingress 发布 barrier，返回 cursor。
2. Branch/Route 的 barrier 不能越过尚未完成的数据广播；即使某个路由分支没有数据，也必须收到控制。
3. UnionAll 在收到一个输入 barrier 后暂停该输入的后续数据，继续轮转其他输入。所有 required 输入（包括 idle）到齐后才向下游发 barrier。旧/重复 barrier 不能拼接新切点；超时/abandonment 会解除阻塞，不能把输入永远挂住。
4. 所有 Count 实例独立冻结，所有 required Sink 按自己的 outbox 等待真实 flush。仅看到 channel empty 不算成功。
5. 所有源 cursor、完整图语义/参与者清单与状态一起进入 v5 snapshot，经过既有 chunk/CRC/MANIFEST/CURRENT/fsync 发布协议。真实提交失败不移动 CURRENT。
6. 恢复在任何输入任务启动前校验全部参与者、完整图语义、所有源身份/消息边界并 seek。缺一个源、源身份不符、状态/边/条件变化均拒绝；不部分恢复、不忽略坏支路。沿用 File 的 inode/长度/有界内容采样检查，不是对任意原地篡改的全文件密码学完整性证明，来源仍须遵守 append-only/sealed 合同。

v5 聚合 source cursor 的 `record_index` / snapshot `ingested_rows` 是各 File 物理记录边界计数之和（包含按既有 decode 策略跳过的记录）；不把它当作成功解码、Runtime 收到或外部送达行数。运行时有效行数看独立的 `runtime_progress` / `input_progress`。

v5 和线性 File/v3、JetStream/v4 **使用不同 checkpoint 目录**，即使显式 fresh 也不混写。带 profile guard 的 K1/K2 binary（包括本次 R12 对照）会拒绝 v5，不让未知图 codec 触发回退到旧线性状态；不能倒推更早、没有该 guard 的 binary 也安全。旧快照不自动迁移；保留原目录/原 binary，需要切换时显式选新目录，禁止用旧版改写新图目录。

本批 File 图没有 JetStream 输出稳定 ID、独立 outbox、跨 Sink 事务或 exactly-once。未提交输出可重复；跨 Source 的 Count 合流不提供全局事件排序保证。不要把“多个 Sink 都参加 checkpoint”解释成跨远端原子提交。

## 诊断与验证

Explain 显示真实物理边、端口、required/best-effort、Source time 和 side-output 信息，不再以 `stage i → i+1` 代替 DAG。status/metrics 的 `graph_ports` 按有限 operator ID 展示每个 Connector 的队列、健康与输出结果；全图不拿某一个 Source/Sink 的最后样本冒充全部健康。

每条图边的 `input_progress` 提供接收行数、watermark、idle、EOF、收到/阻塞的 barrier。口径为 **physical-edge received，非已提交 source position**。Job/revision/attempt、节点/边身份与持有额度仍使用既有受鉴权诊断 API。单独的 source/sink 汇总健康在图上标记 unavailable，要求查看每个 port。

验证入口：

- `sparrow-plan` / `sparrow-runtime` / `sparrow-control` 的 `k3_` 测试：独立输入/输出预期、结构反例、路由、来源、时间、有限 side output、慢/失败/取消支路、barrier/abandonment、真实双 File/HTTP 恢复。
- `tests/k3-process/main.go`：标准库驱动，启动私有 loopback Server/双 HTTP fixture，覆盖零/双独立 Count 状态、CURRENT 提交失败、SIGKILL、各源 cursor/部分状态恢复和部分远端输出失败。
- `scripts/production-k3-validate.sh ART PACKAGE FROZEN_TESTS K3_DRIVER [ROUNDS]`：仅运行冻结测试与二进制，不重新编译；默认 `core`，使用冻结 reliable 测试时设置 `SPARROW_K3_TEST_PROFILE=reliable`。保留 hash/失败样本，不修改已有系统服务。
- 原 File 零/双 Count fresh 与周期 checkpoint ABBA 门槛继续沿用，不能因图实现扩大预算或降低验收阈值。

尚未完成的 TLS/WAN、目标设备、多 Job 容量、掉电/24 h/72 h 验收不借用 K1/K2 的历史短跑结论。

<a id="k3-validation"></a>
### 2026-09-16 匹配复验

基线 commit：`d52b15c`；K3 代码尚未提交。服务器 `box@100.64.0.16`，产物根 `/workspace/bench-compare/k3-artifacts-20260916/`，源码副本 `k3-source-20260916`。仅使用自建子进程与 loopback fixture，既有 Mosquitto 保持运行。

- **580** 项 release reliable/core 测试通过；其中单独 opt-in 的 16 项 broker 测试不计为本轮已执行，另由后面的 K2 专项覆盖。独立 no-demo production profile **35** 项通过，不能与 core 简单相加当作独立测试数。
- **24 项 K3 专项 × 20 轮 = 480 次通过**，复用 `frozen-v14`，不循环编译。计划/type/time/错误图反例、first/all/default、广播、慢/取消/有损支路、来源 metadata、ET 慢源、三类 side output、barrier/旧 barrier/abandonment、File 双输入双输出/文件替换反例覆盖在这些测试中。新增显式 EOF/异常关闭、全部 idle、EOF 后 checkpoint、PT 最终定时输出、嵌套支路取消、通知注册竞态与旧线性 JSON 兼容反例；StallGate 专项每轮含 1000 次 stall/release 交错，不把循环次数另算测试数。
- `validation-v14/process`：**真实 Server 进程**，零状态与两条独立 Count 支路均通过。双 File/双 HTTP、故意阻断 CURRENT.tmp、SIGKILL、每源 cursor 与部分 Count 状态恢复、A 成功而 B 返回失败、CURRENT 不误推进均有独立输出 oracle。不是掉电或远端事务测试。
- `default-smoke-v14` 与 `k1-smoke-v14` 通过：既有默认进程、File 零/双状态恢复、旧 codec 拒绝及已有升级/回退矩阵未被破坏。
- `k2-regression-v14.log`：同版本冻结 **32 项 K2**（含真实独立 broker）全部通过；复用 R12 的冻结 Go 驱动，`k2-process-v14` 的零/双 Count 进程、提交失败/ACK 丢失/保留过期故障通过。未把 K3 拓扑开放给 JetStream。
- 全 workspace/all-targets + JetStream 的 Clippy 成功，保留既有/样式 warnings，**不宣称零 warning**；Go 驱动 build/vet 成功。

原 File ABBA 门槛不变（fresh ≥0.97，周期 on/off ≥0.90，RSS 增量 ≤2048 KiB）；所有完整输出 hash/oracle 一致：

| 场景 | 吞吐比值 | RSS 增量 | checkpoint |
|---|---:|---:|---|
| fresh 零状态，R12 → K3 | 1.008902 | +824 KiB | 不适用 |
| fresh 双 Count，R12 → K3 | 1.002165 | +508 KiB | 不适用 |
| 零状态，K3 周期 on/off | 1.008261 | −152 KiB | 12 个 on 试次各 18 次成功，失败 0 |
| 双 Count，K3 周期 on/off | 1.002497 | +280 KiB | 12 个 on 试次各 11～12 次成功，失败 0 |

按 `file-performance-v14-plan.json` 预先固定三组完整 ABBA，无并行编译、无丢弃样本、无跑到通过即停；每组单独通过，合并所有组也通过。fresh 每场景 **36** 个测量样本，周期每场景 **24** 个测量样本；warmup 也验证输出，但不纳入性能中位数。周期参数保持 100 ms、6400 输入与原 20 ms 慢 HTTP fixture。结论是旧线性吞吐基本持平且通过门槛，不把不到 1% 的差异宣传为确定加速；也不是 DAG 最大吞吐、WAN p99 或 eKuiper 对比结论。

完整命令与结果保存在产物根的 `sparrow-k3-final-regression-v14.sh`、`final-regression-v14.log`（exit 0）；各组原始试次在 `file-performance-v14-block-{1,2,3}`，四份合并报告为 `file-performance-v14-*-summary.json`。默认源对照继续使用 R12 `package-v5-default`，周期 on/off 都使用同一个 K3 binary。K2 专项显式设置 fixture 的 `SPARROW_DATA_ROOTS` 和冻结 NATS 路径，未借用生产 broker。

紧凑证据块（完整日志、冻结测试、失败样本均在上述产物根）：

```text
default package:  package-v14-default
JetStream:        package-v14-jetstream
source manifest:  f1092d998bcd5f2da97b819255a466a473567b8d772009248247a0280d2e8137
default server:   7755db52ac1a5a7902e6bef74ec0965273d7034b2a8bbc8bef320cab5a4cf40d
JetStream server: e83b15a33624fec91796d1be16a9d99eedffcded59309b462d193494b0cd89e4
HTTP-only CLI:    4e404d6472371487bd39933e8a735be725fc5010cafa836e9bdb55e6636586b0
K3 Go driver:     eba8a9e9dddb9fe22eaa9541772fac170ba78815e66ab5c638eabf3f8f17ebc9
```

自查修复要点：

- **拓扑与时间**：旧线性排序不能丢掉第二个输入/分支；图窗口不能按快源数据自行推进 watermark；源时间列变换/不兼容 future-skew 必须拒绝。显式 EOF 与异常关闭区分，全部 idle 不产生最终 MAX；EOF 后仍可 checkpoint，PT 等定时输出完成再结束。
- **取消与通知**：有损支路不能把控制丢失隐藏成继续健康；嵌套可选支路必须继承父 token，不能遗留 stalled leaf。VirtualClock 与 StallGate 在检查条件前注册通知，修复 `notify_waiters` 注册窗口丢失唤醒；StallGate 保留非 stalled 的旧快路径。
- **恢复与输出**：每个 Sink 独立真实 flush；超时解除 Union 输入阻塞；checkpoint 独立 v5 profile，所有源身份/图语义校验在输入激活前完成。诊断不能拿最后一个 Connector 的状态代表所有端口。
- **兼容与旧路径**：新增可选 GraphSpec 字段为空时不序列化，避免破坏旧 strict reader；Source/Window 在 dispatch 选择 `const GRAPH` 特化，线性路径不承担逐行 EOF 图检查。性能是否通过仍以匹配构建实测为准，不能把代码优化直接当作收益。

保留的失败试次与处置：

- v2 传输缺 deploy fixture；v5/v6 新诊断字段触发 JSON macro recursion limit，改为分离小 JSON 构建而不是提高全局 recursion limit。
- 首次 K2 回归遗漏新 fixture 的 `SPARROW_DATA_ROOTS`，被 allowlist 正确拒绝；修正环境后同一冻结 binary 通过。属于 fixture 设置错误，不当作数据面性能/可靠性退化。
- v11 fresh 双 Count 吞吐比 **0.965535**，当时还并行进行 v12 编译；该试次保留但不是干净性能证据。随后无编译干扰的 v12 同场景仍为 **0.968754**，低于原 0.97 门槛，不能只归因于环境噪声；据此实施线性热路径特化，门槛不变。
- 将新增嵌套取消反例运行在旧 v12 内核，**2 s 超时失败**（`nested-negative-v12.log`）；v13 修复 token 父子关系后通过，不靠放宽 timeout。
- v13 重复验证在第 8 轮触发 PT EOF 测试超时（`validation-v13/repeat.log`）；定位并修复通知注册竞态，最终 v14 完整 20 轮通过。

v8/v9 是历史预验收，不与新增修复后的结果拼接；最终代码按上述 v14 指纹解释。默认包 source manifest 的 **264 个文件**已逐一匹配本地工作区；文档收尾不重新编译，包内文档另更新校验清单。尚未 commit 的工作区不是正式发行 tag。
