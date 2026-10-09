# JetStream 可靠输入/HTTP 输出（K2 Preview）

状态：实现与验证中，未发布、未部署；不能沿用 R11 的长稳或性能结论。设计决定见 [ADR-004](adr/004-jetstream-reliability.md)。默认构建仍不包含 NATS SDK。非持久的 NATS Core Source / Sink（`kind: "nats"`，at-most-once、无 ACK、无 replay）见 [NATS.md](NATS.md)，含与 JetStream 的对照表。

**2026-09-24 连接退出修复与来源观测前置：** 实际锁定的 `async-nats 0.50.0` 将 `max_reconnects(0)` 转为无限尝试，原配置并未禁用重连。真实 broker 停止测试复现了关闭等待超时、SDK 额度仍持有的问题。现改为最多与已配置端点数量相同的 SDK 尝试（1～4）；协议连续性丢失仍使本 reader 永久 unhealthy，即使 SDK 重连成功也不授权继续应用输入。关闭仍等待真实 SDK 退出，不以提前退款绕过 guard/目录独占。单节点 broker 停止后的退出与退款已重复验证；不把这项修复解释为透明恢复或 WAN 认证。

新增 `Reader::observe_feed` 是显式调用的**瞬时前缀观测**：fresh ownership/policy、consumer 创建身份/完整配置、stream head、broker pending/delivered/waiting 和本地 received/published/pull 联合校验；不靠 cached Ready，不改变 ACK、Source cut 或自动触发静默。`get_info()` 只读取新事实，不覆盖用于身份比较的创建期 cache。对应 File 观测检查活跃句柄、路径/截短、预读和未完成行。服务器 `feed-observation-artifacts-20260924/f3`：Connector 99 passed / 10 ignored、IO 17 passed，新增 17 项×20 轮（每轮含 3 项显式真实 broker 测试）、该 Connector binary 内全部 16 项 K2 和 no-default 编译检查通过，源码 hash 匹配。旧 f1 编译失败、f2 断连失败保留。这里仅验证 Connector 前置；持久健康决策、静默节点及其端到端恢复仍在后续实现，不宣称 IOT-07 完成。

**2026-09-17 核心完善增量：** 独立 v10 profile 已接入精确静态参考表＋0～2 Count/IoT(TTL0)，无引用迟滞使用独立 v13。真实 broker/HTTP 进程已验证 Lookup→Count、Lookup→IoT、迟滞的恢复和稳定输出 ID，CURRENT 失败不推进 ACK；旧 v4/v7 不混写、不升级。完整证据见 [组合验收](PRODUCTION.md#k1-k4-reference-validation)。仍不开放时间状态、JetStream DAG、历史 replay/fork 或生产认证；下方 Core-A/v7 数字保留为历史证据。

## 核心增强 A：IoT 可靠组合

2026-09-17 从 `1dd17c8` 继续开发，已完成实现、交叉 Review、功能与故障恢复验证。**Core-A 独立候选的默认 File 零状态性能门槛未通过**，原记录如下；后续包含 A+B1 的候选已对同一 K4 基线通过三组全样本合并门槛，且重跑本节全部专项/进程，见 [当前 B1 证据](REFERENCE_TABLES.md#2026-09-17-b1-验证证据)。不据此声称已定位单一性能根因。未 commit/push/tag，未生产认证。
目标是 `JetStream → Count / change_detect / deadband → required HTTP`，
仅线性、最多两个状态参与者，IoT `ttl_micros=0`。同一 checkpoint 保存
Source cut、所有状态及下一个输出 ordinal，复用既有 durable publication 后
Explicit ACK 协议，不另外建立一套 ACK 或 ID 机制。

- 新组合使用独立 **v7 ReliableIoT** profile；旧 JetStream 零/Count 仍为 v4，
  File IoT 仍为 v6。v7 同时包含 IoT 独立 codec 和可靠输出 cursor，不让旧版
  将新状态误读成 v4/v6。各 profile 不混写目录，不自动迁移。
- 抑制行仍然属于 Source cut：不能因“不输出 HTTP”而提前 ACK，也不能等待
  一定有输出才允许 checkpoint。恢复时保留比较基准，稳定 ID 只为实际输出
  分配 ordinal；按同一未提交输入重放时输出 ID 不得变化。
- Count 与 IoT 的两种顺序分别绑定各自 schema；全部状态与 required HTTP
  receipt 一起进入同一切点，不以 Source 收到数或提交后的 ACK 调度数冒充
  broker 已确认事实。
- 不开放正 TTL、ET/PT、Lookup/Dedup、DAG/多来源、固定历史 replay 或语义
  fork。新 profile 不绕过既有 ownership、保留范围、资源和 fail/held 合同。
- 更改旧 pipeline 的算子不能自动继承 v4 历史。需要新独立目录及合法的
  consumer 绑定；新建 pipeline 是新的输出 lineage，不代表原地升级或已实现
  历史迁移。旧目录/binary 保留用于原支持范围回退。

使用 [JetStream/IoT 模板](../deploy/pipeline-jetstream-iot.json)；其中 Count(3) → change_detect 使用独立 v7 目录。以下 K2/R12 记录继续按原构建解释，不把历史结果当作 v7 的测试证据。

### Core-A 功能证据与未通过门槛

候选 `package-v4-*` 在服务器 `box@100.64.0.16` 验证，证据根目录为 `/workspace/bench-compare/core-a-artifacts-20260917/`：

- reliable/default-members **637 passed、17 ignored**，独立 no-demo 入口 **36 passed**；专项独立清单 **8 × 20 = 160** 通过，包含必跑的真实 broker ignored fixture，不把普通测试中其余 ignored 算为运行。清单门禁的 8 类缺失/重名/多余/ignored 分组反例通过。
- 5 个真实进程场景：change、deadband、IoT→Count、Count→IoT、首次全部抑制。HTTP 已收到 body 但未返回 2xx 时强杀，恢复的值及 ID 一致；`CURRENT.tmp` 为目录造成真实提交失败时，CURRENT 与 broker ACK floor 不前进；清障后重放通过。检查全部输入（包括抑制行）的最终 ACK floor/pending。旧 K4 JetStream binary 以旧 Count 配置打开复制的 v7 历史，明确 profile 拒绝且历史/输出不变。
- K4 **50 × 20**、K3 **24 × 20**、K2 **32 项**及各自真实进程回归、default/K1 smoke 均通过。Clippy/Go vet 退出 0；Clippy 保留 **90** 条 warning，不声称 `-D warnings` 通过。
- 三组完整 File ABBA（冻结 K4 v5 为 fresh 对照；周期仍为同候选 on/off，100 ms checkpoint、20 ms 应用等待）：fresh 零状态合并比值 **0.960633**，低于原 **0.97** 门槛；三个单组为 **0.943589 / 0.982734 / 0.950758**，没有丢弃失败组。fresh 双 Count **0.999804**，周期零/双 Count **1.001285 / 1.000038**，后面三项通过原门槛。RSS 最大差均小于 +2048 KiB，全部输出完整、一致，周期无失败提交。**`regression-v4b.exit=1`，不是完整 PASS。** 性能原因仍待定位，不以本轮证据断言是噪声或设计问题。

冻结测试在 `frozen-v4`，160 次在 `validation-v4/repeat.log`；修正后的进程 oracle 在 `process-v6`。首次 Go 驱动误把 HTTP 请求次数当成返回响应所含行数（`batch_rows=1` 不会拆分已有 batch），导致 `validation-v4` 流程失败；保留该失败记录，修复后独立 Go v6 全部场景通过，Rust binary 未重编译。前面的错误方法名、fixture observer 初始化顺序和 Arc move 编译/测试失败也保留在 v1～v3。后续 B1 工作区更改不属于本次 Core-A 冻结构建。

```text
base commit       1dd17c8186b5f52f2cea85a5fae1a94d79722b22
source manifest   f9b2c68ecd0b007f80d178b20975063ee1f2265174f2f363bacee491ffd750e4
default server    e967b4dc35e4c8694f319e8063c242fc4f85fa9985bd40e776105cde833d194f
JetStream server  33b0b034619c8a4509f5c815c9ef801a6959b394c7c521884d8a141db09b212f
Go oracle v6      99aca449bb2fec95d06f97193e36923886628266bf07b8c180e2c84c42f8dd19
```

源码清单中的 Go 为 v5；v6 仅更正进程测试 oracle，独立源码保留为 `core-a-process-v6.go`。B1 候选继续对同一 K4 基线守原性能门槛，不以较慢的 Core-A 候选重设基线。TLS/WAN、容量、24/72 h、目标介质/掉电门禁仍未执行。

## 支持合同

- 显式选择 `source.kind=jetstream`、`delivery=checkpointed_at_least_once`、`recovery=aligned`，指定独立 checkpoint 目录及周期 `resume_latest=true`。首次接收用户数据前先提交空 checkpoint，固定来源、state generation 和输出 epoch。
- 首批仅 **单来源 → 零状态/单 Count/双 Count → 单 required HTTP Sink**。按 stream 的完整顺序消费，不接受 subject filter、ET/PT 窗口、多源/DAG 或未纳入参与者协议的算子。
- HTTP **2xx 接纳**及本地 checkpoint durable publication 两者均成立，才逐消息确认对应 source cut；不是已读、入队或最大 sequence 即 ACK。2xx 不等于业务事务已落盘，响应体不完整仍按已有 HTTP 合同处理。
- 未提交输入从 broker 重放，外部输出可能重复。输出是 `[{"id":"<48 位小写 hex>","data":{原始结果字段}}]`；ID 为 128-bit epoch + 64-bit 输出 ordinal，不受 HTTP 合批、并发和重试影响。消费者须自行按 ID 实现幂等，Sparrow 不声明 exactly-once。
- 满 pending 触发 checkpoint/背压，不静默丢弃。checkpoint 仲裁 busy 不结束 attempt；排空/对齐超时只放弃本次 checkpoint、不 ACK，满 pending 时保持背压并有限间隔重试。bootstrap 超时仍禁止用户输入。真实输出失败、快照编码/落盘失败、解码和保留范围错误才结束 attempt；重启有有限退避及 held。可靠任务仅在有新的 durable 输入进展后才清除连续失败计数，单纯运行 30 秒或空 checkpoint 不算恢复。
- 本批选择 **fail/held** poison 策略，未实现 DLQ 或独立 HTTP outbox。没有本地离线持久接纳能力；broker 保留和下游恢复是继续运行的前提。不要删除、跳过或 ACK 毒消息来假造成功。

## Broker 与资源前置

当前 Preview 严格匹配受测的 **NATS Server 2.14.6** 和 `async-nats 0.50.0`，`max_payload <= 65536`。升级服务端先重新跑 conformance；不是声称所有 NATS 版本均可恢复。

输入 stream 由运维预先创建，Sparrow 不自动建 stream 或放宽配置：

- `storage=file`、`retention=limits`、`num_replicas=1`，正值且有限的 `max_bytes`、`max_msg_size <= 65536`、`max_consumers <= 128`。
- `deny_delete=true`、`deny_purge=true`；禁用 rollup、per-subject eviction、TTL/schedule/counter/batch/atomic publish、mirror/source/republish/subject transform。全局按前缀淘汰/过期可以存在，但必须覆盖所需恢复范围；范围过期会拒绝恢复，不跳到最新消息。
- ownership KV bucket 使用 File、单副本、history=1、正值 `max_bytes`、discard-new，不允许 TTL/全局条数淘汰。容量满时拒绝新绑定，不淘汰其他消费者所有权。不得由外部程序删除/改写绑定。
- [本机配置示例](../deploy/nats-jetstream-local.conf.example) 只监听 loopback，使用 `sync_interval=always`；测试中的进程强杀不是设备断电认证。真实存储、备份、容量和保留时间须单独核定。

连接前先预留真实 Job slot 与 **同一 Job owner**，不是额外独立配额；slot 随 Kernel 和 SDK 最后一个生命周期 guard 释放，等待启动的连接不抢已运行任务的预留额度。pull、订阅、命令、publish-ACK 和 pending 条数/字节均有限。

SDK 预留包含 `512 KiB + 32 × pull_bytes + subscription_capacity × 4608`，考虑 headers 展开而非只计原始字节；默认 pull 为 **72 KiB**。保留的原始记录另外覆盖共享 read-buffer pin 与解码 headers；稀疏 JSON 的工作区还计入全部 schema 字段。它们是保守额度，不是敌对 broker 的 RSS 硬上限。Broker 保持 `max_control_line <= 4096`，这是受信配置前置而非从 INFO 推断的已测数值。

当前 Server 每 Job reservation 为 4 MiB；默认 SDK 预留 **2,929,664 B**，并发 ACK 工作区另外 **65,536 B**。原始记录在解码期间仍持有 `32 × message.length + 256 KiB`，解码 scratch 为 `64 × payload + fields × 2 × sizeof(Scalar) + 4096`，另有批次、状态冻结和 HTTP。仅按这些同时存活的基础额度估算，payload 的乐观上界已低于 **10 KiB**（忽略 metadata/schema/其他使用时约 9.49 KiB），不是 18–19 KiB，更不是协议允许的 64 KiB。实际可接纳值由 schema、并发工作区决定，超限明确失败、不 ACK；不能把估算当作已认证的消息大小。Explain 输出固定预留，status/diagnose 输出可用 retention；大记录需独立预算 profile 验证。

SDK `read_buffer_capacity` 只是初始容量；不能把它或订阅条数当成网络字节上限。生产使用一次一个 bounded `fetch(no_wait)`，不使用 SDK 默认自动拉取；已缓存消息跨控制等待/慢 mailbox 保留。stream/KV 策略在启动、每次 checkpoint 和 5 秒 health tick 检查；每条交付都检查 consumer/source sequence。完整检查含 stream info、KV policy 和 binding 查询，目前仍有控制 RPC 成本，不为吞吐绕过验证。broker 管理员是配置可信边界，不支持外部人在两次检查之间修改并复原策略。

## R12 执行形态

已观察到的积压、资源及空闲时效取舍统一维护在 [项目优化清单 OPT-001～004](OPTIMIZATION_BACKLOG.md#opt-001)；生产验证缺口与按需扩展仍在 [DEVELOPMENT_TODO §8.4](DEVELOPMENT_TODO.md#k2-followups)。K2/R12 Preview 已提交为 `d52b15c`；这些待办不因本轮功能代码完成而自动关闭。

- Source 合并同一 pull 中已经就绪的记录，受 Kernel rows/bytes 限制；不为凑批等待新消息。整批发布后才推进连续 published cut，尚未入队的预取/延后记录不进入 checkpoint。
- 非空批次结束立即继续；仅真正空拉取使用 5→10→…→250 ms 退避，有数据即复位。空闲稳定状态约不超过 4 pull/s，代价是最长约 250 ms 的空闲唤醒等待；控制/ACK/取消不被该等待阻塞。旧 v13 每 8 条还会额外等 5 ms，不能沿用其 NATS 容量假设。
  - 容量批新增可选 `source.jetstream.idle_backoff_max_ms`（5～250ms），缺省仍250。只用于普通可靠Actor；持久时间/来源观测profile拒绝。调低后空闲请求率会增加，status的effective上限随配置变化；不是端到端SLA，也没有改SDK拉取期限、ACK或恢复规则。配置和复验口径见 [CAPACITY](CAPACITY.md)。
- 保留 **Explicit** ACK：独立 worker 最多 16 个确认请求，共享有界队列/工作区，每条最多 3 次尝试；负响应不算确认。提交后仅调度 ACK，不在 source select 分支串行等待 N 个 RTT，停止时取消并 join worker；ACK 失败耗尽重试才结束 attempt。broker 断线仍 fail-closed，不承诺透明重连。
- `AckAll` 未采用。不能忽略重投递后的 consumer sequence 与旧 reply token，直接替换确认策略；以后改变需独立 conformance/恢复测试。
- `timeout_ms` 应覆盖目标负载下的正常 HTTP 排空时间。过小不会通过 ACK 丢弃输入，但会增加 checkpoint 失败与积压；持续慢端点仍需限流/容量规划。HTTP waiter 超时不取消已开始的 durable commit。

## 独占、恢复与回退

`JETSTREAM_OWNER` 私有文件与 canonical checkpoint 目录绑定。KV 记录逻辑 consumer 的 owner 及 attempt reader nonce；旧 reader 删除完成后才改写绑定，新 reader 从 **已提交 cut + 1** 创建，而非采用旧 consumer ACK floor。不同目录误用同一 consumer 会拒绝。

本地 `WRITER_LOCK` 持有到 actor、blocking commit 和实际 SDK I/O 关闭。`drain()` 返回仅代表命令入队；SDK event-loop 生命周期哨兵负责关闭确认、资源与锁保留。即使关闭 waiter 超时，旧 SDK 仍持有锁，不能重叠接管同一目录。这是**协作式单节点独占，不是分布式 fencing/HA**；禁止复制相同 owner/catalog 后在第二节点并行运行。

- snapshot v4 保存 Source cut、完整参与者状态、输出下一个 ordinal；File 仍写 v3。两种生产 writer 拒绝混写同一目录，旧格式不自动迁移。
- **v30（第11批子批1）**：线性 JetStream → 1～2 个 Count 窗口（含 FIRST/LAST/VAR_*/STDDEV_*，participant codec 3）→ required HTTP JSON；next_output 与状态同切点、epoch 必须等于 generation。ET 窗口与新聚合的组合不在本子批范围，启动前拒绝。v30 目录不与 v4 等混写，不迁移。恢复解码先按 Job owner（`admission.owner()`）预留额度，额度不足不回退旧代。见 [恢复支持矩阵](PRODUCTION.md#recovery-support-matrix)。
- v4 恢复要求完整计算语义及来源/reader 绑定匹配。当前拒绝 downstream 语义变更、固定历史点 replay/reset 和移动目录；不能为允许更新而在内存里临时换 epoch，制造崩溃后的 ID 冲突。
- `sparrowctl checkpoints NAME`、`status NAME`、`diagnose NAME` 可检查恢复点/有效保证；列表不会改变 CURRENT。`checkpoint.reliable_source` 明确区分 published/committed cut、pending 条数/字节与采样年龄，pending 包含尚未确认的 ACK。
- `sparrowctl checkpoint NAME` 与自动/source-full checkpoint 共用单请求仲裁；busy 是可重试冲突，不是已提交。HTTP waiter 超时不取消已经开始的 durable commit。
- 故障排除后使用已有、带鉴权/审计的 `sparrowctl start NAME` 从最新提交点恢复。毒消息、过期或语义不兼容需先制定保留原始 broker/checkpoint 的修复或迁移方案；此 Preview 不提供危险的跳过/任意历史回放入口。

远端使用 `tls://` 和 `token_secret`（SecretRef），不得在 URL 放用户名/密码/token，不允许 skip-verify；信任根使用 SDK 的系统证书配置。仅连接 allowlist 中明确列出的地址并忽略自动发现的新节点。TLS/真实 WAN/故障时效尚须匹配构建验证，不以本机明文测试代替。

### 运维修复与清理

- 删除管线不会自动删除 stream/KV。停掉引用该逻辑 consumer 的全部 Job，确认 SDK 已退出后，保留 checkpoint/owner/KV 备份；由管理员核对绑定中的 attempt nonce，删除对应 reader。reader 另设 24 h inactivity 回收，**KV 绑定不自动过期**。仅确认永久弃用这个逻辑身份时才按 broker 的 KV 管理流程清理绑定；需要复用身份应优先恢复原目录，不强行覆盖。
- `JETSTREAM_OWNER` 损坏默认拒绝。只有确认是首次创建中断、没有 CURRENT/历史快照且 broker 尚无该绑定时，才可备份并移除这个未完成 marker 后重试。已有绑定或来源不明时必须恢复匹配备份/制定迁移方案，不能一概删除重建。

## JetStream Sink

`sink.kind: "jetstream"`（feature `jetstream`，Preview）把每个输出行发布到**已存在**的
stream，并等待服务器 PubAck；决策与取舍见 [ADR-005](adr/005-jetstream-sink.md)。

```json
"sink": {
  "kind": "jetstream",
  "outbox_capacity": 16,
  "jetstream": {
    "servers": ["nats://127.0.0.1:4222"],
    "stream": "OUT",
    "subject": "out.events",
    "msg_id_column": "id",
    "ack_timeout_ms": 2000,
    "max_inflight_acks": 8,
    "max_retries": 5,
    "flush_timeout_ms": 5000
  }
}
```

| 字段 | 默认 | 范围 / 说明 |
|---|---|---|
| `servers` / `token_secret` / `reconnect_attempts` / `connect_timeout_ms` / `max_payload_bytes` | 同 NATS Core | token 仅 `tls://`；端点经 TargetPolicy |
| `client_capacity` | 4 | SDK 命令缓冲（消息数）；独立于默认 8 个在途 PubAck，不暗中钳制显式配置 |
| `stream` | 必填 | 1..=255 可见字节，不含 `. * > / \`；必须已存在 |
| `subject` | 必填 | 字面 subject，必须被 stream 的 subjects 覆盖 |
| `ack_timeout_ms` | 2000 | 100..=30000；每次尝试上限 2× |
| `max_inflight_acks` | 8 | 1..=256；`1` = 本 Sink 的相对行顺序，非跨生产者全局顺序 |
| `max_retries` | 5 | 0..=20，100 ms→2 s 退避 |
| `flush_timeout_ms` | 5000 | 10..=60000，stop 的在途与队列共用 deadline；EOF 排空队列 |
| `msg_id_column` | 无 | utf8/int64/uint64 输出列 → `Nats-Msg-Id`；stream 需 `duplicate_window > 0` |

语义：

- **at-least-once 进入 stream**：批次只有全部行拿到 PubAck 才算交付；重试可能
  重复（`jetstream_sink_duplicates` 为服务器报告的去重命中）。设置 `msg_id_column` 时
  服务器在 `duplicate_window` 内去重；超过窗口的重放仍会重复。不提供内容哈希 id
  （会合并合法的相同行）。id 须对合法输出唯一且重放稳定；空值行不带 msg-id，仍可能重复。
- **Fail closed**：stream 缺失/不可写/未绑定 subject、超限行、非法 msg id、重试耗尽、
  stop 时未能在 `flush_timeout_ms` 内确认 → job `JobFailed`（`JetStream Sink failed closed`）。
- **从不创建 stream**：由运维按保留/配额策略预先创建。
- **发布目标**：启动拒绝 `no_ack`，每次发布带 Expected-Stream 并核对 ack.stream；wrong-stream
  不盲重试。fresh-only 可接纳 Memory stream，但不承诺它在 broker 重启后保留。
- **Aligned JSON v27 / CSV v28**：仅独立线性 File 源 profile（`recovery: aligned` + `checkpoint_dir`），
  prepared 目标必须是可写的 **File/Limits** stream。先在同一 job owner/slot 上验证目标，
  再打开/seek File、激活状态；运行复用该 session，失败显式 close 并等待真实 SDK 退出。
  JSON snapshot 用 `JSI1` 保存 canonical endpoints、token SecretRef、stream 精确 created nanos、
  subject、msg-id 策略；CSV 用 `JSI2` 另绑定编译后的 delimiter、quote、header、null_value。
  显式默认 CSV 选项与省略等价；每行消息各带可选表头。恢复严格比较**完整**计划，不能沿用 v3 的 downstream-prefix 宽松规则。
  v1..v26、JSON v27 与 CSV v28 不混写、不自动升级；HTTP↔JetStream、JSON↔CSV、任一 CSV 编码选项、目标/策略/下游表达式变化
  必须用独立新历史。拒绝不推进 CURRENT/STATE_GENERATION，也不发布新行。checkpoint 只在全部前置 PubAck 和批后目标复验成功后提交。
  每批前/后及 retry 前做实时 INFO 检查；同名重建/配置变化拒绝 receipt/CURRENT。
  恢复后从最后 checkpoint 重放 File 输入，msg-id 仅在 duplicate_window 内去重。
  NATS/MQTT/HTTP 等 live 源与 graph/IoT/引用表/JetStream source 的 checkpoint profile
  返回 `UnsupportedRestore`（这些 profile 依赖 HTTP 稳定输出 ID）。
- **预算**：SDK command queue 与 writer batch 双缓冲、在途 payload、bounded JSON/CSV codec
  scratch 和 prepared identity/config 都记入同一 owner，超出额度失败，不增大预算或暗中钳制配置。
- **顺序**：`max_inflight_acks > 1` 不保证输入行入 stream 的顺序。
- **可信边界**：管理员不能在检查之间修改又恢复目标/策略来绕过检查；保留/淘汰归运维。
  File/PubAck 不是消费方业务提交、设备掉电/fsync 或 HA/exactly-once 认证；本功能仍为 Preview，
  专项回归由当前候选单独验证，不沿用旧 Source profile 的测试数字；10k/20k 性能压测未执行。
- **指标**：`jetstream_sink_{acked,duplicates,retries,ack_timeouts,failed,batches,discarded_on_close,dropped_bad,dropped_oversize,msg_id_missing,disconnects,reconnects,client_errors,sessions,fatal,inflight}`。

真实 broker 测试：

```bash
SPARROW_NATS_SERVER=/path/to/nats-server \
  cargo test -p sparrow-connectors --features jetstream --lib jetstream_s -- --include-ignored
SPARROW_NATS_SERVER=/path/to/nats-server \
  cargo test -p sparrow-control --features jetstream --lib jetstream_sink -- --include-ignored
```

覆盖：PubAck 后才回执、stream 缺失/subject 未绑定且不创建、ack 超时重试与去重后
fail closed、broker 重启（无 msg id：不丢、重复计数；有 msg id：无重复）、stop 时刷新、
超限行失败、NATS → SQL 过滤 → JetStream 的精确 stream 计数、aligned checkpoint 等待
PubAck 且恢复去重、aligned job 在 stream 缺失时失败；新增 CSV v28 checkpoint/恢复与显式默认等价、JSON↔CSV 和四项编码选项变化拒绝（原历史/发布数量不变）。这些 v28 回归须由当前候选单独执行，不沿用旧测试结果。

## 构建和验证

```sh
SPARROW_JETSTREAM=1 bash scripts/production-build.sh /absolute/new/package-directory
```

仅给 Server 增加 `jetstream` feature；HTTP 运维 CLI 不链接 SDK，默认 `SPARROW_JETSTREAM=0` 不改变。使用 [pipeline 模板](../deploy/pipeline-jetstream.json) 前先建立 schema、allowlist、broker stream/KV 与数据根目录，替换环境对应的目标和路径。

短程验证入口（在隔离测试机，编译输出重定向；不自动安装服务）：

```sh
SPARROW_NATS_SERVER=/absolute/nats-server \
  bash scripts/production-k2-validate.sh /absolute/new/evidence /absolute/package 20 \
  > /absolute/k2-validate.log 2>&1
```

设置 `SPARROW_TEST_ARTIFACTS` 时，把该目录同时加入测试用 `SPARROW_DATA_ROOTS`。测试仅创建并停止自己的 broker/HTTP/Server 子进程；保留原始 JSON、日志、失败样本和构建指纹。长稳、TLS、真实 RTT、吞吐/内存预算和完整发行门槛分别记录，未跑的不勾选为通过。

脚本冻结一次 reliable 测试可执行文件，按真实清单运行所有 `k2_`（含 ignored 与 formats/model/server），再跑 Go 进程故障与零状态/单 Count 的 backlog、2k/s、10k/s 六组基线。缺 broker 明确 SKIPPED/exit 4，不宣称完整 PASS。可用 `SPARROW_K2_FROZEN_DIR` 复用同源码的冻结二进制；操作者必须核对源码对应关系，不能用旧清单验证新代码。

<a id="k2-v13"></a>
## 历史 v13 证据（不是 R12，也不是 NATS 性能基线）

2026-09-15，服务器 `box@100.64.0.16`，候选 **v13**：

- reliable 核心 **553**、独立 no-demo **35**；K2 专项 **23**，冻结二进制重复 **23 × 20 = 460** 通过。clippy 退出 0，原有风格警告及少量新测试借用/单次返回 enum 警告保留，不宣称零 warning。
- 真实 broker 重投递未重复更新 Count；部分 Count 状态恢复，HTTP 400、poison、消费者绑定冲突和满 pending 均有独立输出/拒绝 oracle。
- 零状态与双 Count 真实进程：HTTP 成功后使 `CURRENT.tmp` 写入失败；强杀后以相同 ID/内容重放未提交输出；实际丢弃 `+ACK` 后在 committed/unacked 区间再次 SIGKILL，恢复不重放已提交前缀；保留范围过期明确拒绝且 CURRENT 不变。零状态唯一输出 12、预期未提交重复 6；双 Count 唯一输出 2（21/57）、预期重复 1。
- R11 冻结基线对照、相同 fresh ABBA 参数与原 ≥0.97 门槛：零状态 **0.9980**、双 Count **1.0149**，RSS 差 +16/+432 KiB，全部输出校验一致。v12 的零状态 **0.9582** 失败样本保留；问题是新变体把普通输入消息撑大，v13 为带独立 metadata lease 的间接句柄，不放宽门槛。
- 默认进程 smoke 通过：20 次生命周期 FD 12→12，恢复/备份结果 60/150/240、7 类拒绝及 SIGTERM join 通过。File 零/双 Count、R10 双向 codec 拒绝、历史 K1 升级/回退矩阵也通过。
- 100 ms checkpoint / 20 ms **应用响应等待**的 File on/off 对照：零状态 **1.0076**、双 Count **1.0133**，均过原 ≥0.90 门槛；每轮分别 18/12 次提交、失败 0。它不是实际网络 RTT，也不是 NATS 容量数据。

证据目录：`/workspace/bench-compare/k2-artifacts-20260915/`，`validation-v13`、`process-v13`、`performance-v13-fresh`、`package-v13-default`、`package-v13-jetstream`。首次 K1 smoke 调用把基线目录误传成可执行文件，零/双 Count 已通过后在该调用失败；保留原日志，以正确 binary 路径和全新目录重跑，不归因为产品通过。

最终收尾 `completion-v13.exit=0`；正确基线重跑位于 `k1-smoke-v13-replay`，周期数据在 `performance-v13-periodic`。布局反例记录旧内联形状 104 B、当前事件 32 B；本地工作区逐文件通过候选 source manifest 校验。自查问题已按上述受限范围修复并复验，仍不宣称无条件生产稳定或全部 REL 完成。

```text
base commit    6fb20c21692b38e0e0dbf50fa1d4c60631f4f568 (K2 尚未提交)
source manifest 588f7d7910ce2ed87406157122cf4a6a63f7a1ba44bdf2f4972ac68d98cf378c
default server  be0c76c525224e87a0d484014188623e9b53bdc0a01a6ef09accc392b541e4f3
JetStream server a34caa53fe4b56ab9673a1c0819233d7c039ecdca138912cddb648f2a7aed07a
HTTP-only CLI    945cc25652cf54579105130421175e4232793cb9538eec6044907c5a4f4e7502
process driver  67062aca3d9fdb7ab68d9e8a4fdd9a3beb4e8427bd1abfaa6cb1bbecda4a4e4a
```

源码清单覆盖代码、测试、脚本及部署输入；MD 的后续证据补充不代表重编译代码，包内文档更新后另核 SHA256SUMS。没有 push/tag/安装服务，已有 Mosquitto 未改动。**K2 未整体完成，不替换 R11 受测生产基线**；没有 NATS TLS/WAN、目标设备掉电、24/72 h 或生产吞吐容量认证。

<a id="r12-validation"></a>
## 当前 R12 验证（2026-09-16）

代码自查及本批匹配复验完成，随 K2/R12 提交归档，未发布/部署。受测候选为 `package-v5-default` / `package-v5-jetstream`；它们与 v4 的对应 Server 二进制逐字节相同，v5 只修正 Go 基线驱动的发布计数等待并重新包装来源清单，Rust 源码未变。

- **556** reliable 核心、**35** 独立 no-demo 通过；普通核心中的 **16** 个 opt-in broker 测试不冒充已运行，已另在专项中执行。**32 × 20 = 640** 次冻结专项通过，修正后的完整仓库入口又跑 **32** 项、进程故障和六组 NATS 基线，退出 0。clippy/Go vet 退出 0，保留 clippy 警告，不宣称零 warning。
- 仲裁竞争、真实 1 s 周期、慢成功 HTTP 的排空超时后同 attempt 继续提交、已有积压启动、失败计数仅在 durable 进展后清零、恢复语义/目录迁移拒绝均有真实 Supervisor/broker 用例。
- 并发 ACK 测试要求 16 个请求全部到齐才回复；负回复必须触发有限重试，停止时取消未回复请求并 join。KV history/TTL/discard 反例、运行期 KV 策略变化、SDK 精确预留与回收也覆盖；Server 版本/payload 拒绝是共享校验函数的单元测试，不伪装成跨版本部署认证。
- 真实进程零状态/双 Count 均通过：CURRENT 发布失败后 broker 的原 consumer pending 与 ACK floor 不变；重启删除旧 reader、未提交输出保持相同 ID/内容；实际丢弃 ACK 后 SIGKILL，不重放已提交前缀；最后 broker ACK floor=12、pending=0。保留过期仍拒绝且 CURRENT 不变。
- 默认进程 smoke、File 零/双 Count 恢复、R10 双向 codec 拒绝、旧 K1 升级/回退通过。默认 File fresh 相对冻结 R11 的比值为 **0.9921 / 1.0095**（零/双 Count，均过原 ≥0.97 门槛）；周期 on/off 为 **1.0073 / 0.9931**（过原 ≥0.90，失败提交 0）。没有放宽吞吐或 RSS 门槛。

### NATS 第一组容量与延迟基线

同机 loopback、单管线、默认 4 MiB reservation、pending=128/pull=8、required HTTP batch=64/linger=5 ms/inflight=1、checkpoint=1000/5000 ms，NATS File/Limits/single replica、`sync_interval=always`。不是 eKuiper 对比，也不是 TLS/WAN/多规则或长期容量认证。

每种形状用同一个冻结 Go 驱动做两组 ABBA，共 **8** 个试次；每次预先持久化 **8190** 个输入，计时含 Sparrow Job 启动及积压排空，但**不含预装输入的 broker 写盘时间**。每轮独立校验全量内容/顺序/唯一 ID，并等最终 committed cut 与 ACK 清空。

| 形状 | v13 到 HTTP 接收 | R12 到 HTTP 接收 | 比值 | R12 到最终提交并确认 ACK |
|---|---:|---:|---:|---:|
| 零状态 | 1,104 输入/s | 19,854 输入/s | 17.99× | 18,625 输入/s |
| 单 Count(3) | 1,108 输入/s | 12,165 输入/s | 10.98× | 11,831 输入/s |

另做持续写入方式的短程基线，每组仍为 8190 条。下表延迟是**生产者写出 → HTTP 捕获**，Count 按窗口最后一个输入计；不等于设备年龄、checkpoint 延迟或下游业务完成。

| 输入形状/请求速率 | 到 HTTP 的实际输入速率 | p50 / p99 |
|---|---:|---:|
| 零状态 / 2k/s | 1,996/s | 9.42 / 16.53 ms |
| 单 Count / 2k/s | 1,994/s | 9.46 / 16.85 ms |
| 零状态 / 10k/s | 7,952/s | 294.67 / 435.65 ms |
| 单 Count / 10k/s | 6,861/s | 404.08 / 460.07 ms |

六组均全量正确、无丢失/重复/意外 attempt 重启。**10k 档出现积压，不能据预装排空的 19.9k/s 宣称能持续低延迟接纳 20k/s。** paced 结果包含生产者、broker 持久写入和消费/输出的竞争，尚未用分项实验确定瓶颈归属。真实部署仍需按消息/schema、介质、网络、目标端和并发规则重测；空闲退避的首消息等待上界约 250 ms 也应纳入时效合同。

### 可复验产物与失败样本

证据根目录：`/workspace/bench-compare/r12-artifacts-20260916/`。`frozen-v4`、`frozen-production-v4` 对应核心；`validation-v4/repeat.log` 是 640 次专项，`validation-v5` 是完整入口成功记录与六组基线，`nats-abba-v5` 是相对 v13 的对照。`default-smoke-v5`、`k1-smoke-v5`、`file-performance-v5` 为默认回归，最终 `validation-v5.exit=0`、`completion-v5.exit=0`。

保留的失败记录：v1 ACK 测试 responder 队列过小；v2 新测试漏包 `Some` 的编译错误；v3 API 测试目录未加入白名单，以及 ACK 测试将本地 flush 当成 broker 已注册订阅的竞态（未放宽断言，改同连接真实往返屏障）；v4 benchmark 把异步 Core PUB 后的 INFO 当成持久计数屏障。最后一个在 Sparrow 开始消费前失败，改为等待真实 stream count 后重跑。**v4 验证入口整体退出 2，不冒充 0；其中已完成的 640 次专项及进程 oracle 仍是有效独立结果。**

```text
base commit       6fb20c21692b38e0e0dbf50fa1d4c60631f4f568
source manifest   7d0568b219621f39beae4d670d6e173d1766238ace66fe31acec984272ebb2b7
default server    296b9636294d3c38e46ba7894483452f10c56d171bd1379139b877e35e67a80f
JetStream server  486b8246dee9bd3b7cbb98e557aa317171174df7509123896a1a9d8737a55fa3
HTTP-only CLI     945cc25652cf54579105130421175e4232793cb9538eec6044907c5a4f4e7502
Go oracle/bench   a0dfc556e4ecaaaf91068b6e21ccfaf089a2d3479028c8f11abb5620b872980e
```

最后只补正式文档并重算包校验和；代码清单和二进制不变。根目录过程 MD 不随本次 K2/R12 提交收录；未 push/tag/部署，也未变动现有 Mosquitto。TLS/WAN、设备掉电、24/72 h、KV 容量满等尚无本批完整故障认证；REL 父任务继续保持未完成。
