# Sparrow Post-V1 完整开发 TODO

**先看开发顺序：[简版开发顺序 TODO](DEVELOPMENT_ORDER.md)。** 该文件维护当前执行先后与每批效果；本文件保留详细任务、设计对应和历史证据。

更新日期：2026-09-24。

**当前进展（2026-09-23）：开发顺序第 1、2 批的限定时间恢复已完成验收。** v14～v17 线性能力之外，required File→HTTP 时间图 PT v18 / ET v19 已通过 774/44、专项重复、真实进程故障、旧矩阵和原三组 ABBA，[证据与边界](PRODUCTION.md#time-graph-validation)。未声明来源/依赖/时间域组合和完整 IoT 生命周期仍未完成；逐决策串行提交的容量成本仍见 OPT-012。当前按 [开发顺序第 3 批](DEVELOPMENT_ORDER.md) 推进 K4 业务闭环，不将限定图恢复写成 K1～K4 全部完成。

以下带日期的早期“当前/最新”记录保留为历史证据；实际当前状态以 `DEVELOPMENT_ORDER.md` 为准。工作分支为 `feat/core-completion`，告警图真实进程验收代码已提交为 `a5997dd`。后续完整位置投影共享候选已通过 805/44 和完整功能回归，验收编排漏选风险另以同一冻结二进制补验 K2 32 项；原性能三组合并通过，但第一组零状态仍未过，整批不放行。来源观测在独立文件准备接口与测试，尚未接入持久决策或静默算子，也不表示 IOT-07 完成。

设计依据：根目录 `Sparrow_Post_V1_Roadmap_Arrow_JIT_Final.md`，包含 2026-09-12 实施补充；同时追踪原始蓝图、计划评审与历史 ADR，文档范围、取舍及章节对应见 [§18 覆盖矩阵](#design-coverage)。运行合同与已验证证据以 [RUNTIME.md](RUNTIME.md) 为准，并核对其对应源码/构建。本文件负责把设计转成执行任务，不替代设计文档，也不把设计接口当成已存在的 API。

**当前批次：本次交付收录通过独立自查和匹配源码复验的 K1/R11，基线 R10 为 `9a92527`。核心 540、独立无 demo 34、关键 140 × 20 轮通过；原零状态/20 ms 慢 Sink/100 ms 周期门槛已通过，32k/131072 fresh 三组 ABBA 合并也通过原阈值。修前失败样本、HTTP flush 拆批根因，以及自查发现并修复的旧 K1 回滚风险均保留，详见 [R11 记录](PRODUCTION.md#r11-validation)。版本仍为 0.1.0，未 push/tag/部署；目标设备、真实网络、24/72 h 和掉电门槛仍保留。用户已确认下一大阶段按 NATS JetStream 路线推进 K2，K1 提交不混入 K2 半成品。**

**最新优先级决定（2026-09-14）：核心优先。K1 为当前整批；之后是 K2 可靠输入与输出、K3 真正 DAG、K4 IoT 状态算子。运维平台、Graph Designer 与体验扩展后置，Arrow/JIT 仍有独立证据门槛。K1 只开放受测线性组合，不等于核心全部完成或任意业务都可生产使用。完整范围和依赖见 [§3.4](#core-first-batches)，版本顺序见 [§17](#release-sequence)。**

**K2 实施进展（2026-09-15）：K1/R11 已提交 `6fb20c2`，未 push；后续工作在 `feat/k2-jetstream-reliability`，没有混入该提交。JetStream 首批 Preview 已贯通真实 Source→Kernel→HTTP→checkpoint→ACK/恢复，完成自查修复及限定矩阵测试，见 [JETSTREAM.md](JETSTREAM.md)。默认关闭；选择 fail/held、broker 保留/重放，不实现独立 outbox 或 DLQ。固定历史 replay、语义 fork、TLS/真实 RTT/容量与长稳等剩余门禁不假报完成，REL 父任务保持未勾选；当前仍收尾 K2，不自动转入 K3 或平台。**

R12 收尾（2026-09-16）已完成本批实现、自查及匹配复验，提交为 `d52b15c`：checkpoint 仲裁/超时分流、真实空闲退避、输入合批、有界并发 Explicit ACK、broker 侧提交失败 oracle 和验证入口已落实。556 核心、35 no-demo、32×20 专项及进程故障通过；NATS 预装排空相对 v13 为零状态 17.99×/单 Count 10.98×，但持续写入 10k 档仍有积压，不承诺低延迟 20k/s。默认 File 原性能门槛通过；详见 [R12 证据](JETSTREAM.md#r12-validation)。未 push/tag/部署；仍是 Preview，TLS/WAN/长稳与完整 REL 未完成。已知限制、生产验证缺口和按需扩展分开登记在 [§8.4 后续优化台账](#k2-followups)，不把“本轮代码完成”理解为完整 K2 只剩长测。

**最新进展（2026-09-16，替代上方历史记录中的“当前批次”）：K3 的 DAG-01～07 已在声明矩阵内完成实现、自查和匹配复验，尚未 commit/push/tag。最终 `package-v14-*`：580 核心、35 no-demo、24×20 专项、零/双状态多输入真实进程故障通过；三组完整 File ABBA 单组/全样本合并均通过原门槛；32 项 K2 与其真实进程回归通过。自查补正 EOF、嵌套可选支路取消、通知注册竞态和旧 JSON 兼容；此前失败样本保留。详见 [K3 合同与证据](DAG.md#k3-validation)。这是 Preview，不是任意 DAG aligned 或生产认证。下一建议核心模块为 K4，尚未启动；K2 长稳/真实网络/容量门禁仍保留，K5 平台继续后置。**

## 导航

**当前执行更新（2026-09-17，替代上方历史排期）：用户授权补齐 K1～K4 剩余核心，前端 K5 后置，不另开 R13 报告。** K3/K4 首批已随 `1dd17c8` 提交，未 push/tag。上一未提交候选又完成 JetStream＋IoT TTL0、静态参考表管理和 File 无状态 Lookup checkpoint，700/44、专项和三组 ABBA 合并通过；其历史产物仍保留 A/B1/B2-A 名称，失败样本不删除。当前新增代码不能继承这些测试结果；剩余时间状态、其他组合、高级 IoT 及生产验证均按下表单独验收。

### K1～K4 核心完善阶段（2026-09-17 用户确认）

统一采用 K0～K5 大阶段与原 STATE/REL/DAG/IOT/TAB 任务号。此前 A/B/C 是临时工作批次，**不是新的正式阶段**；只在历史构建、源码清单、测试名称及证据链接中保留，不能把首批或某个恢复组合完成写成整个 K1～K4 完成。

| 大阶段 | 已有受测能力 | 本阶段剩余核心工作 |
|---|---|---|
| K1 / STATE / TAB | File 零/单/双 Count、受限 ET、IoT TTL0；静态 Lookup v8/v9；不可变表发布/固定绑定/pin/GC；单个 HoldFor/Debounce v14；PT tumbling、Change/Deadband 正 TTL 和最多两个线性状态的暂停时间 v16 | 时间型 DAG、时间与 Lookup 等未声明组合仍需独立准入；目标预算、迁移/发行矩阵随新组合扩展 |
| K2 / REL | JetStream 受限零/Count/IoT TTL0、静态表 v10/迟滞 v13、稳定输出身份、HTTP确认→checkpoint→ACK；单个 HoldFor/Debounce v15；线性 PT/正 TTL/双状态时间 v17 | 未声明拓扑/依赖组合；重连/容量/异常处置与发行缺口。独立 outbox/DLQ 仍是需求触发项，不假装已实现 |
| K3 / DAG | Branch/Route/UnionAll、多来源/required HTTP、受限 File Count/IoT；静态表 v11 的分支/合流/混合状态恢复 | 时间状态和 Source 时间进度恢复；确定性 timer/合流合同及慢/失败支路、预算与控制进展回归 |
| K4 / IOT | ChangeDetect、Deadband、Hysteresis、TTL0 恢复；线性 processing-time HoldFor/Debounce、Change/Deadband 正 TTL、限定双状态组合与模板 | event-time/图/未声明依赖组合；冷却/通知限频、静默/离线检测、告警生命周期及采样/重采样；对应 schema、身份、恢复与业务模板 |
| K0 / QA / PUB | 短程/故障/性能证据、冻结二进制与独立清单 | 匹配源码的全局Review、真实网络/目标容量/24～72h/存储失败模型、CI和发行证据；未跑保持NOT RUN |
| K5 | 后置 | 运维工作台、Graph Designer及前端不进入本次核心完善 |

**当前实施顺序**：静态表＋状态/可靠输入/required DAG 的组合恢复与 K4 迟滞已完成本轮匹配功能验收；接下来补齐可恢复时间与高级 IoT 状态机。每项必须贯通 API→实际执行→失败/取消→恢复和独立预期，不能只补 helper/trait 或放宽准入。本轮冻结 v7 为726/44、26×20专项及9种进程，完整旧矩阵回归通过；性能和来源单列见 [本轮证据](PRODUCTION.md#k1-k4-reference-validation)，不继承上一候选700/44和ABBA成绩。

2026-09-22 补充：历史 v14/v15 为 747/44、22×20、6+3 进程。本批 v16/v17 已完成限定线性 PT/正 TTL/最多两个状态：759/44、25×20 与 38×20（清单存在交集）、24 种新增真实进程场景、旧矩阵及三组 ABBA 通过，[匹配证据](PRODUCTION.md#linear-time-validation)。**下一批是时间型 DAG（K3）**，不跳到 K5；第 1 批完成不表示任意时间/Lookup/DAG 组合或生产长稳已完成。

**计时决策（用户已确认）**：高级 IoT 采用停机暂停，恢复后继续剩余时长。该选择必须进入显式配置与兼容语义；有未提交输入重放时，还需证明逻辑时间、timer 与 Source cut 的顺序，不得只保存 `remaining_ttl` 就宣称确定性可靠重放。设备事件时间/watermark模式是另一个明确时间域，不与暂停的 processing-time 偷换。

**新增后端排期（2026-09-17）**：时间恢复/高级 IoT 之后安排 [Action/Sink 与常用函数首批](#actions-functions)，先于容量验收和 K5；更丰富的连接器/复杂函数/插件按需推进。当前下一大批仍是可恢复时间＋HoldFor/Debounce，完整顺序见 [简版 TODO](DEVELOPMENT_ORDER.md)。

阶段验收清单：

- [x] K1：限定静态 Lookup＋Count/IoT(TTL0) 状态/依赖恢复与旧版本拒绝，v8不扩写。
- [x] K2：真实 JetStream 新组合的稳定 ID、HTTP 未知结果、CURRENT失败与重投递；旧 ACK 丢失场景回归通过。
- [x] K3：required File DAG 的 Lookup 分支/合流、全来源切点，以及 Count pending＋迟滞 latch 恢复。
- [x] K4：迟滞双阈值、整数精度、坏值、首值、预算、恢复和真实模板，仍为 TTL0 Preview。
- [ ] K1/K3/K4：暂停时间、timer/state/source/output 同切点及停机/重放反例。
- [ ] K4：持续条件、防抖、冷却、静默/离线、告警触发/恢复/episode 和采样模板闭环。
- [x] 本轮静态表/迟滞候选交叉Review与原支持矩阵功能回归；不替代后续时间协议的全局Review。
- [ ] 适用生产门禁：真实网络/设备容量、24～72h、介质故障及发行包。

本清单未全部完成时，只报告具体完成项，不再笼统宣布“K1～K4 已全部完成”。Arrow/JIT、插件、Kafka、HA 等独立路线不混入。必要 API/CLI、鉴权、审计、诊断仍随核心操作交付；不自动 commit/push/tag 或启动未确认时段的长稳。

- [维护规则与完成标准](#rules)
- [当前基线与未完成边界](#baseline)
- [执行看板与下一批任务](#execution)
- [核心优先的整批开发与验收](#core-first-batches)
- [发布资产、所有权与语义基线](#foundation)
- [真实健康、队列与延迟](#observability)
- [过载与业务时效](#freshness)
- [V1.1 工作台、CLI 与首批 IoT](#v11)
- [共享状态参与者与可靠输入](#recovery)
- [K2 已知限制与后续优化](#k2-followups)
- [项目非阻断问题与优化清单](OPTIMIZATION_BACKLOG.md)
- [V1.3 DAG](#dag)
- [V1.4 高级 IoT 与参考表](#iot)
- [Connector 与格式](#connectors)
- [性能 P0、Arrow 与 JIT](#performance)
- [配置、Explain 与后端能力](#configuration)
- [V1.5～V2.x 条件任务](#future)
- [统一测试与发行验收](#verification)
- [ADR、依赖与任务登记](#decisions)
- [实际执行顺序、阶段效果与版本发布](#release-sequence)
- [设计覆盖矩阵、历史取舍与门禁证据](#design-coverage)

<a id="rules"></a>
## 1. 维护规则与完成标准

### 1.1 如何勾选

- `[x]`：对应**限定范围**已有实现和验证；不表示已经 commit、发布或通过全部设备认证。
- `[ ]`：尚未完成。标为“部分完成”的父任务也保持未勾选，已完成的子项单独标记。
- “立即”：建议下一批处理；“近期”：当前产品阶段；“条件”：触发条件满足才开工；“远期”：保留设计，不默认安排实施。
- 每节标出的优先级、依赖、负责人要求适用于节内任务；个别任务另写更严格前置。
- 原设计的 BASE/OBS/AGE/UI/OPS/EMB/SQL/REL/DAG/IOT/TAB/ANA/EXT/HTTP/P0/P1/P2/J0/J1/J2 编号保留。PUB、MEM、SEM、STATE、CLI、TPL、WIN、CONN、FMT、CFG、SCALE、FLEET、V2、QA 编号，以及既有组末尾标明的新增项，为本执行清单补充。
- `P0/P1/P2` 是性能实验阶段，**不是任务优先级**。不要把所有 `P0-*` 理解为紧急任务。
- 负责人统一为**待认领**，除非任务登记中另有明确分配；排期和投入上限在开工前填写，不虚构日历承诺。
- 当前代码、配置和状态 codec 的能力优先于规划示例；发现漂移时先核查实际实现，再更新本清单。

### 1.2 每项任务的共同 Definition of Done

只有同时满足以下要求才勾选完成：

1. 交付物可执行/可使用，不只是空接口、示意文件或 TODO 注释。
2. 明确输入输出、NULL/错误、顺序、时间和资源上界；不支持项明确拒绝。
3. 贯通实际入口到执行、取消、失败和适用的恢复路径。
4. 有独立预期/反例；差分测试不能代替全部规格验证。
5. 资源在共享、取消、异常、扩容、更新和最后 owner 释放时正确结算。
6. 记录 API/配置/state/函数语义兼容变化及迁移、回退限制。
7. 正式说明进入 `docs/`；原始证据包含源码与构建指纹、命令、结果、失败样本和已知限制。
8. capability、Explain、status、模板和真实运行一致；未知不能填成确定的零或成功。

功能成熟度另行记录：`Experiment → Preview → Supported → Profile-certified`。一次测试通过不能自动跳到 Profile-certified。

### 1.3 执行约束

- 保持一个主产品切片；有投入上限的性能实验可另排，但不同时启动完整 UI、DAG、Arrow 和 JIT。
- 文件查看/修改使用 Codex 内置工具；不使用 `cat`、Python 等用户禁止的工具。
- 开发先静态核查和补测试，再集中远端验证；尽量不本地编译，不在每个小改动后重编译全部工程。
- 文档变更不触发编译。修改代码后必须做必要验证，不能为了少编译省略关键回归。
- 本文件是计划，不授权自动 commit/push、部署、启动 72 小时任务或删除旧 checkpoint；执行时依照用户当次授权。
- 不再为每次执行生成一份根目录过程 MD。更新本清单、正式运行文档及对应任务证据即可。

<a id="baseline"></a>
## 2. 当前基线与未完成边界

### 2.1 已知工作区状态

历史验证基线：`3619676b4636dfce4bbd477ea79b028e6a6a4efb`；`4f70407` 为 BASE/OBS/R9 阶段，K1/R11、K2/R12 后续提交分别保留独立证据。当前 HEAD 为 **`1dd17c8`**，包含 K3/K4 及正式文档；匹配构建与源码指纹见 [K3 验证](DAG.md#k3-validation) 和 [K4 验证](IOT.md#k4-validation)。工作区包含已验证的核心增强 A/B1/B2-A，不把新补丁混称为已提交构建；各冻结包只覆盖其匹配源码，不自动证明后续代码已测。未 push/tag/部署，发行文档和证据的版本化仍属 PUB 收尾。

| 项目 | 当前状态 | 下一步如何处理 |
|---|---|---|
| SQL/Graph、prepared Row、现有窗口/参考表 | 旧线性基础保留；K3 新增显式 DAG Preview，SQL 未新增通用 UNION 语法 | 复用 bind-once/fusion；以 [DAG 合同](DAG.md) 区分图执行与受测恢复矩阵 |
| BASE-01/02 | 本轮限定修复已验证 | 进入持续回归，不冒充已支持多参与者 |
| BASE-03 | owner handle、detach 顺序等局部已修复 | 受控路径审计与完整 arena 边界仍待补齐 |
| BASE-04 | 保持现有 eager/首错语义 | 独立 golden 与候选后端语义合同仍待补齐 |
| BASE-05 | 无 demo 生产组合已验证；输出计数已修复 | 工具链锁定、CI、完整 capability、真实指标和设备验收未整体完成 |
| MQTT | keepalive、有界等待、字节计费、可选 QUICKACK 已实现 | 继续守住已声明的 best-effort 范围，补健康与现场证据 |
| HTTP sink | 响应体消费/复用、合批、linger、并发、预算和 flush 已实现 | 不再重复建设；补真实网络、过载时效、完整业务 receipt/身份能力 |
| Arrow/DF、WASM spike | 有历史实验或示意代码 | 不作为当前生产后端，也不冒充公平 prepared Row 对照 |

### 2.2 可复用的已有证据

以下为已记录的 2026-09-12 验证，不是本 TODO 编写时又重跑了一遍：

- 核心/default-members release：**419 通过、0 失败**。
- `sparrow-server --no-default-features`：**20 通过、0 失败**，是另一 feature 组合，不能与前者相加声称 439 个独立测试。
- 恢复、准入、所有权、输出计数：**48 项 × 10 轮**，全部通过。
- 两套新目录的真实生产进程：10+20 后 checkpoint，恢复并输入 30，仅输出一次合计 60；上游 Map 或更早 Filter 改变后拒绝恢复，保留 CURRENT。
- 首次 smoke 暴露的 `emitted_rows` 重复计数失败记录保留；最终测试对应修正后的 V2 补丁。

证据位于服务器 `box@100.64.0.16` 的 `/workspace/bench-compare/`：

| 产物 | 路径/指纹 |
|---|---|
| 验证源码补丁 | `base-gates-source-20260912/source-v2.patch` |
| 补丁 SHA-256 | `fa5253cd2bbe74c88c8e517133e596462cd396c1af62a8f96ee4bc7bdeb27c62` |
| 核心测试 | `base-gates-tests-20260912-v2.log` |
| 无 demo 测试 | `base-gates-production-tests-20260912-v2.log` |
| 原始证据包 | `base-gates-artifacts-20260912/v2/`，含 `summary.json`、`source-files.sha256`、`SHA256SUMS`、二进制与复现脚本 |
| 生产二进制 SHA-256 | `5871c431e05b729cced0d65e52e7e228a82e7bb13848192b8bdfc451c9643cc6` |

补丁先于结果说明归档，之后补充的文档不改变受测 Rust 源码。历史 408/0 和旧性能矩阵是另一基线，不自动继承为未来改动的性能证明。

### 2.3 不可忽略的兼容变化

- Snapshot family 仍为 `SPV1`，snapshot codec 已为 **V2**；manifest `MAN2` 版本仍为 1。
- V1 checkpoint 缺少完整语义证明，只能读取检查，不能直接恢复或自动升级为 V2。
- 旧部署升级前保留二进制、catalog、数据和 checkpoint；确认输入历史及重复副作用可接受后，使用新 checkpoint 目录显式 reset/replay。条件不足时暂缓该 Job 升级。
- `RowBatch::into_rows()` 返回 `OwnedRows`，不再返回可拆开的数据/lease 元组。下游库使用者需要迁移。
- `BatchValue` 会保留整个 batch 的 credit；适合受控拥有型引用，不是长期 state 持有少量字段的最终方案。
- 默认仍为 `live_best_effort + restart_fresh`；没有新增 MQTT replay、exactly-once、HA、完整 Scalar arena 或 RSS 硬上限承诺。

### 2.4 基础能运行，不等于核心已全部完成

| 核心边界 | 当前可以做什么 | 未完成部分及实际影响 | 优先批次 |
|---|---|---|---|
| 执行与算子 | 现有线性 SQL/Graph、prepared Row、已支持窗口/聚合和有界队列 | 分支、多输出、多源合流还不是完整运行能力 | K3 |
| 状态恢复 | K1 已实现 File 零状态、单 Count/ET tumble/ET hop、双 Count 串联 | 其他多状态/混合时间/DAG 仍拒绝；旧 snapshot 不自动迁移；恢复能力不等于可靠输入输出 | K1 验收 / K2 / K3 |
| 实时来源 | MQTT 接入、keepalive、背压与有限重试 | best-effort 不承诺断连/过载/崩溃无损；缺 checkpoint-aware ACK、来源身份、重投递及保留范围协议 | K2a |
| 输出交付 | HTTP 响应体消费、连接复用、合批、并发和真实 flush | 没有持久 HTTP outbox；未知响应可能导致重复，不能保证崩溃后继续交付所有未确认输出 | K2b |
| IoT 业务状态 | 可复用现有表达式、窗口和参考表 | 变化检测、Deadband、迟滞及其状态/时间/reset 合同是待做的核心业务能力，不是 UI 周边 | K4 |
| 生产可信度 | 当前受测二进制有短程、故障恢复和性能证据 | 目标设备、真实网络、长稳和适用存储失败模型仍有缺口；测试数量不能替代支持矩阵 | K0 及每批发行门禁 |

工作台缺失主要影响操作便利性；上述恢复、交付、图执行和算子缺口会直接影响业务是否适用。Arrow/JIT 未做不意味着当前 Row 核心不可用，反之完成优化也不能补成可靠交付。现有部署只能按真实支持组合、负载和可接受的丢失/重复边界评估，不作“核心已全部完成”的总体结论。

<a id="execution"></a>
## 3. 执行看板与下一批任务

### 3.1 建议推进顺序

本表保留批次编号，方便关联已有任务和证据；**编号不是所有任务必须串行完成的顺序**。发布主线、可跳过项及版本推进统一按 [§17](#release-sequence) 执行。

| 批次 | 优先级 | 工作 | 前置 | 本批结束应得到什么 |
|---|---|---|---|---|
| B0 | 本批实现/短程验证完成，见 §3.3.5 | PUB/OPS-04/QA 等生产化闭环，前移最小 CLI/诊断子集 | `4f70407` + 本轮受测 patch；未 commit/push | 集中 review 后冻结 RC，阶段 3 网络/设备/长稳门禁另行放行 |
| B1 | 当前 Server 范围已实现 | OBS-02 + AGE-01：Source/Runtime/Sink 队列与本机时间基础 | 复用现有 mailbox，补充实际边界队列；完整业务 lineage 仍有明确限制 | 按 attempt 的真实占用、等待、年龄、确认层级与 API |
| B2 | 整批验证收尾 | OBS-01/03：健康与延迟闭环 | 连接/处理/确认分开，已有功能/重复测试；netem 未运行保持缺口 | 静默/断连/堵塞/失败可区分；有限直方图和组合测试证据 |
| B3 | 核心之后按需求启动 | AGE-02～04 | AGE-01、OBS；先选一个业务策略 | 显式过载/过期行为及失败、恢复反例，不默认开启有损策略 |
| B4 | 用户要求 UI/平台后置；保留已有 CLI/诊断 | V1.1a：CLI/薄工作台、诊断包 | B1/B2 能力；复用已有 API，§3.3 只前移有限运维子集 | 不打开源码即可部署、Explain、停止及定位故障 |
| B5 | 核心业务批 K4，不再排在恢复协议之前 | IOT-01/02 + TPL：变化检测、Deadband 首批场景 | 节点使用的 state-owned 预算/时钟合同；声明恢复时接入 STATE | 至少两个有精确预期、重置行为和资源测试的可运行模板 |
| E0 | 有限实验 | P0-01～06 | 登记目标负载、硬件、投入与停止标准 | prepared Row 热点画像、公平候选结果及继续/停止/证据不足决策 |
| C1 | 最高功能优先：K1 → K2 | STATE/REL + HTTP-07：恢复底座与可靠数据链路 | File 恢复底座不等 broker 决策；外部来源/独立持久日志先明确部署与交付合同 | 空状态/多状态恢复、选定来源与输出的故障闭环；不等待 UI |
| C2 | 核心批 K3 | DAG a → b → c；Designer 后置 | 相应图预算/控制/参与者前置；不强制依赖可靠 broker | 每个子阶段独立开放真实图执行 capability |
| C3 | 实验证据通过后 | Arrow P1/P2 或 JIT J0/J1/J2 | 各自门槛，二者不互相强制依赖 | 特定范围的可选后端，而非默认替换内核 |
| L | 远期/需求驱动 | 高级 IoT、分析、WASM、分片、多设备、V2.x | 对应 ADR 与目标用户 | 按后续章节逐项立项，不一次全部开工 |

B/C/E 编号只用于追踪旧计划；当前优先级以 K0～K5 为准：**当前 review/发行收尾 K0；新增功能 K1 恢复协议 → K2 可靠数据链路 → K3 DAG → K4 IoT 状态规则；K5 工作台后置。** K2 的外部部署决策未就绪时，可推进前置已满足的 DAG 或 IoT 后端，不能靠做平台页面回避核心缺口，也不能擅自安装 broker。

2026-09-14 用户最终调整：核心功能优先，运维工作台与可视化平台后置；本决定替代同日较早的“下一批先做 B5”建议。保留已有 CLI/API/必要安全诊断，不启动前端重建。薄工作台不是完整编排平台，DAG-08 只展示内核真正支持的图。当前候选 review、测试证据与后续功能分开，不因重排计划改变支持承诺。

### 3.2 OBS 切片与提交粒度

下列是内部实现/PR 边界，不是要求每做一项就停下来询问用户。第 1～6 项作为一个可观测性批次推进，当前支持路径的实现与 R9 跟进代码已随 `4f70407` 提交，剩余环境/验收限制见 §5；文档未随提交加入。后续同样按完整可验收批次交付，内部仍保持小步修改与集中验证。

| 顺序 | 建议 PR | 覆盖任务 | 验收重点 |
|---|---|---|---|
| 1 | 队列与时间计量合同 | OBS-02、AGE-01 的基础定义 | batch/row/control、排队/处理中/信用预留分别定义；单调时钟与未知时间明确 |
| 2 | Mailbox 指标接线 | OBS-02 的 Runtime 路径 | 发送成功、失败、取消、接收、drop、Receiver 关闭都正确计量与归还 |
| 3 | Job/stage 快照及 API | OBS-02 的展示路径 | 按 attempt 隔离、有界标签和历史保留；局部未接线继续 unavailable |
| 4 | Connector 健康与处理进展 | OBS-01 | 重连期间不报健康，正常静默不报故障；读写失败与背压原因可追溯 |
| 5 | 延迟与链路诊断 | OBS-03 | 各阶段计时边界、批次统计口径、HTTP 确认层级、有限直方图 |
| 6 | 集中组合回归与开销检查 | OBS 组退出验收、QA-02/03 | 正常/慢 Sink/过载/断连/取消/checkpoint；无新丢弃、输出语义不变 |

**B1/B2 不做：** 自动过期丢弃、按 key 合并、可靠 ACK 协议、完整 Dashboard、新 Arrow/JIT 后端或全量 Scalar arena 重写。

<a id="next-production-batch"></a>
### 3.3 当前交付批：首版生产化与运维闭环

**目标不是只新增一个定时器或一份 CI 配置，而是让当前 Sparrow 能以固定构建安装，完成配置/启停/诊断，自动保存有效恢复点，并有明确的故障、升级和回退操作。** 本批落实阶段 2，并把原阶段 4a 的 CLI-01、OPS-02 最小子集前移；不把完整 UI、RBAC 平台或远期执行后端一起拉入。

九个工作包沿用原任务 ID；以下保留开工范围与验收合同，实际交付见 §3.3.5。已经存在的能力补合同和回归，没有另造第二套 Coordinator、配置入口或执行器。父任务超出本批的部分继续保持未完成。

#### 3.3.1 本批工作包与验收出口

| 工作包 | 对应任务 | 必须覆盖的内容 | 交付/验收出口 |
|---|---|---|---|
| 1. 生产构建与交付资产 | PUB-01～04/06、QA-10 | 固定 Rust/target/lock/features；生产 Server 与运维 CLI 不隐式启用 demo；可重复构建/打包、版本与源码指纹；安装/运行目录、启动停止和升级回退说明 | 在干净目录按脚本生成可追溯产物，生产 demo 拒绝；发行包、配置模板、迁移说明与受测源码对应。正式文档是否提交单独确认，不把工作区文档误当已经随代码发行 |
| 2. 周期 checkpoint 调度 | OPS-04、QA-04/07 | 周期/超时配置及关闭行为；单 Job 最多一个活跃 checkpoint；手动/自动统一仲裁；错过 tick 合并或跳过；失败有限重试；stop/update 后旧 attempt 调度退出 | 虚拟时钟覆盖空闲、持续输入、满队列、超时、取消和并发请求；自动生成的恢复点可以被真实恢复，不积累后台任务 |
| 3. 恢复点与 File 运维 | OPS-04、CONN-03、PUB-04、QA-04 | 最近成功恢复点及年龄、可用恢复点/兼容原因；保留数量/字节与 GC；File 追加/轮转/截断/替换身份；选择恢复点、缺失/损坏/旧 codec、失败后的 CURRENT 保留 | 可诊断地接受或拒绝恢复；不删除最后有效或仍被依赖的恢复点；失败不默默改为 fresh；不把配置回退说成撤销已发送 HTTP |
| 4. 生命周期与控制面 | QA-02/06、OPS-04、CFG-04 | `/start` 幂等、并发 start/stop/update/checkpoint/restore；revision/attempt 归属；容量等待、重试退避、失败阈值/held 与人工解锁；进程重启后的既有 desired/actual 收敛 | 旧 attempt 不再调度/改新状态；无重复启动、幽灵 Job 或遗留连接/任务；停止与失败原因明确。恢复/重放策略显式配置，不能私自改变默认行为 |
| 5. 资源与输入输出边界 | MEM-01～04 适用子集、CONN-01～03、FMT-01、HTTP-06 的拒绝路径 | 追踪当前 decode→队列→算子/state→encode/snapshot 的 owner/credit；变长值/复制峰值、HTTP 超大结果、慢输出、多 Job 配额与隔离；磁盘满及 checkpoint 清理失败 | 已启用路径中的提前退还、无界增长或分配后补账问题不能只写限制了事；不能受控的外部/raw API 单独声明。大输出可明确有界失败，不为本批强行引入拆分协议；不承诺全进程 RSS 硬上限 |
| 6. 能力、配置与 Explain 一致性 | PUB-05/06、CFG-01～04 适用子集、WIN-01、HTTP-07 取舍 | 机器可读 Source×Sink×time×graph/state×recovery 矩阵；生产/演示能力区分；参数上下限/默认值/未知项拒绝；validate/explain/start/status 一致；盘点 SQL/窗口真实支持与依赖版本 | 未支持组合在激活 ingress 前拒绝；stored-latest 与 running-attempt 不混淆；构建可用、配置允许、可恢复与运行健康分别报告。新窗口及持久 outbox 这里只作需求取舍，不假记已实现 |
| 7. 最小 CLI 与诊断包 | CLI-01、OPS-02 的最小子集、QA-06 | 补齐面向真实 Server 的 validate/explain、配置发布、启停、status/diagnose、checkpoint/restore；稳定 JSON/退出码、有限超时；构建/计划/错误/指标及有限日志的脱敏诊断包 | 一套命令完成「配置→启动→诊断→checkpoint→停止/恢复」；不是运行固定 demo 二进制。复用已有 API；包有容量/范围限制，不能导出秘密明文、任意宿主文件或无限历史 |
| 8. 安全与可操作性 | QA-06、PUB-04、OPS-02 | 持久 secrets key/safe mode、鉴权、目标与数据 allowlist、TLS、目录独占、日志脱敏；新 CLI/API 的危险操作拒绝与审计；容量/磁盘/恢复失败的处置手册 | 未授权访问、越界路径、错误密钥/证书、重复目录占用等有负向回归；操作者能明确知道下一步如何处理。先复用现有权限模型，不把完整多角色平台/RBAC 拉入本批 |
| 9. 独立正确性模型、CI 与组合验证 | BASE-04、SEM-01/02、QA-01～10 适用子集、HTTP-05、OBS-03 | 当前线性/单窗口的独立规格模型与虚拟时钟；NULL/Missing/数值边界/首错 golden；取消/控制/恢复随机序列和最小反例；快速/集成/长周期 CI 分层；真实 IO、网络与资源回归 | 不能把同一生产 evaluator 调两次当 oracle；CI 有可重复 seed/fixture/原始产物。正常、过载、失败、未运行分开记账；长稳与介质/断电等平台验收按下表独立放行 |

#### 3.3.2 支持范围先冻结，不靠扩大项目凑批次

- 默认先以已验证的 Linux x86_64 为实现/验证平台；其他硬件在有环境和负载定义后单独认证，不自动扩大支持声明。
- I/O 复用现有 MQTT、HTTP Push、File 与 HTTP/MQTT/Log 路径，只承诺能力矩阵允许的组合；不默认将全部 Source×Sink 笛卡尔积标为已支持。
- 自动 checkpoint/恢复先限定 **File + 当前已支持的单窗口 aligned 组合**；非恢复路径可继续使用已有算子。MQTT 保持 best-effort，不借此引入可靠 ACK、零/多状态恢复或任意历史回放。
- 内存/语义工作按本次启用路径收口；完整 Scalar arena、通用 VM、大状态后端和新的 SQL 方言能力不作为这批前置。
- 本批不做完整工作台/Graph Designer、业务 Preview、全部 IoT 节点、自动过期/合并、JetStream/Kafka、真正 DAG、WASM、Arrow/JIT、分片或 HA。
- CLI/诊断前移是明确的范围调整；UI、完整发布历史、导入导出和多角色体系仍留后续。对应父任务只在其全部合同达标时勾选。

#### 3.3.3 集中测试安排与放行边界

| 层级 | 覆盖 | 何时执行 / 通过意味着什么 |
|---|---|---|
| 快速确定性回归 | 配置与能力拒绝、计量守恒、语义 golden、虚拟时钟、checkpoint 仲裁/取消、缓存/身份与资源释放 | 工作包内部集中运行受影响测试；小修不重跑整套性能和长稳 |
| 实际进程组合 | 手动/自动 checkpoint 竞争；持续输入/空闲/EOF/慢 Sink；并发更新/停止；进程中断、损坏/不兼容恢复、磁盘满；CLI、诊断脱敏和升级回退 | 实现整合后按支持矩阵执行；模拟故障与真实进程证据分开，`kill -9` 不能代替 OS 崩溃或断电 |
| 性能与资源 | 对照 `4f70407` 的 File/MQTT/HTTP；checkpoint 关闭和开启分别测；多 Job、低速/稳定/突发/过载；吞吐、完整性、p99、RSS/账本、task/fd/连接、写率、暂停/恢复耗时 | 开跑前固定配置、输入、SLO 和开销预算。R9 File 结果不是所有场景的通用阈值；不能改变队列单位或删除丢弃样本来制造通过 |
| 网络与 TLS | 真实链路 0/20/100 ms 条件，HTTP/HTTPS、断连/超时/慢响应及连接复用 | 必须有可用隔离网络环境；当前 netem 缺口不因代码完成而关闭，应用层响应等待不替代 RTT |
| 冻结候选长验收 | 24 小时无人干预后从自动 checkpoint 恢复；72 小时组合长稳；10,000 次生命周期循环；本版声明的存储失败模型 | 属阶段 3，先确认设备、负载、时段再启动。短程测试通过不关闭 OPS-04/QA-08 全部父任务；改了候选代码需重审或重跑受影响证据，不能拼接不同构建时长 |

#### 3.3.4 内部实施顺序与整批交付

1. **冻结范围与基础入口**：工作包 1/6 先明确构建、能力和配置合同；工作包 9 先落关键规格/反例，不等实现结束才补测试。
2. **打通自动恢复主线**：工作包 2/3/4 一起覆盖调度、恢复点和生命周期；工作包 5 的资源问题随实际路径修复。
3. **补齐操作与交付**：工作包 7/8 接通 CLI、有限诊断和安全操作；收齐工作包 1 的生产包与迁移/回退资产。
4. **集中回归、review 与候选冻结**：完成适用短程/故障/性能测试、失败样本归档和九个工作包的范围核对；一次交付完整开发批次，不每完成一个 helper 就停下来询问。
5. **另排正式发行验收**：按阶段 3 执行网络/目标设备/长稳等剩余门槛。开发交付可用于集中 review、准备 RC；正式发版必须另过适用门禁，版本分配、提交范围、push/tag 仍按授权执行。

**整批应交付：** 固定构建/打包脚本与分层 CI、周期 checkpoint 与恢复点管理、生命周期和资源修复、机器可读能力矩阵、最小运维 CLI/脱敏诊断包、升级回退与故障操作说明、独立模型/反例及对应源码的组合测试证据。不是交付几项代码后就宣称“生产稳定”。

<a id="production-batch-result"></a>
#### 3.3.5 本轮交付与验证（2026-09-14）

| 工作包 | 本轮交付 |
|---|---|
| 1 构建/交付 | Rust 1.98.0 固定工具链；分别无 demo 构建 Server/HTTP-only CLI；source/build/binary 指纹、生产包、systemd/env/pipeline 模板与迁移回退手册。未实际安装服务，未触发 GitHub 云端 CI |
| 2 周期 checkpoint | opt-in interval/timeout、单 attempt 单请求、手动/周期共用 gate、skip ticks、等待者超时不提前释放正在提交的任务；虚拟时钟反例 |
| 3 恢复/File | 有界 CURRENT/MANIFEST/chunks/目录扫描、PUBLISHED 发布证明、保留/字节配额、数值恢复点 pin；修复空文件启动后追加数据未纳入 cut 身份及活动 fd 被替换的问题 |
| 4 生命周期 | 替换先 stop/join 再启动；启动中途失败清理；SIGINT/SIGTERM 收敛；并发 update/checkpoint/stop 与真实进程重启恢复 |
| 5 资源 | eager transform、Window/Dedup、restore 暂存预准入；state 容器真实容量估计；MIN/MAX 先准入候选、失败保留旧值；修复恢复快照按 stage 深拷贝和持有至 Job 结束；不移除保护来换性能 |
| 6 能力/配置 | 机器可读 Source×Sink/time、窗口/SQL/后端/恢复边界；Explain checkpoint policy；state_keys 改为实际状态项并明确最近 Window 采样口径；PT hopping/sliding 和持久 outbox 留到需求驱动阶段 |
| 7 CLI/诊断 | `sparrowctl` 配置→validate/explain→启停→status/diagnose→checkpoint/选点恢复；结构化退出码、超时/大小约束；128 KiB allowlist 诊断、0600 独占文件。完整配置/计划/历史错误归档仍属于 OPS-02 后续扩展 |
| 8 安全/运维 | 鉴权先于 body 处理、CLI 远端明文/重定向拒绝、catalog/checkpoint 协作锁、受控目录/持久密钥合同与负向回归；恢复备份实际回读，未引入 RBAC |
| 9 模型/验证 | 独立手算表达式 golden、线性 SQL/Graph/fusion 与 per-key count 参考模型、ET 边界、虚拟时钟、分层 CI 文件与固定二进制重复脚本；完整原始失败/成功样本保留 |

**自审前 v7 受测源码**：基线 `4f70407` + patch SHA256 `f0eec40539e717436998d0b3d4701579689de4b4bad3e57d876e02e9f7a27d2e`。231 个构建/源码/fixture/脚本文件与服务器一致，补丁在干净基线目录重放并逐文件校验。以下 v7 数字不是后续修复的新证据，也不是把旧 R9 测试移植为新证据。

**服务器原始产物**：`/workspace/bench-compare/production-artifacts-20260914/v7/`；使用 `box@100.64.0.16`、Linux 6.12.94+ x86_64。完整数据与 SHA256 见 [PRODUCTION.md](PRODUCTION.md)。

- 核心 **490/0**；独立无 demo Server **25/0**、CLI **2/0**；关键 **79 × 20 = 1,580/0**（分 profile 执行，不能当成全是不重叠用例）。
- 真实进程：自动 checkpoint、指定恢复点 sum=60、进程崩溃重启 sum=150、实际备份恢复 sum=240；20 次启停 fd **12→12**；SIGTERM 后可重新取得锁。
- 对 R9 的 File ABBA：80k 吞吐比 **1.0164**，400k **0.9882**；RSS 增量 **440 / 392 KiB**。全部 warmup/正式试次输出校验通过；不是所有负载都提速。
- 周期 checkpoint 100 ms 开/关匹配试次：吞吐比 **0.9961**，4 次正式开启试次分别成功 **13/13/13/14** 次、失败均 0。
- MQTT 正常 **7,500/7,500**，120 s 连续 **90,000/90,000**，无重复/缺失；刻意过载仍 **124 缺失**，明确是无损判据下的 invalid 试次。
- **267** 个观察视图、**534** 个边界队列、**547** 个 Runtime mailbox 样本守恒检查通过。
- 保留 v1～v6 的失败/中间证据。v5/v6 的 400k 约 3.1%/3.3% 开销未过预设 3% 门槛；最终去掉固定宽度聚合的多余查表/重计费及 key 的重复 detach，保留所有资源保护后通过，未修改阈值或丢弃失败轮次。

**自审后修复与复验**：修复生命周期等待取消后提前释放锁、restore 分步写入、恢复 scratch 过度预留、sink flush 写死 5 秒及打包 target/复制路径不一致。核心 **495/0**、独立无 demo Server **26/0**、CLI **2/0**；关键 **87 × 20 = 1,740/0**；真实进程 sum=60/150/240、20 次启停 fd **12→12** 再次通过。当前二进制 File 80k/400k 对 **v7** 的吞吐比分别 **0.9974/1.0212**；周期开关比 **0.9825**，正确性与原门槛均通过，不混作对 R9 的结果。自审 patch 为 `8f6d35f3f831cce88466efa59d33648e2c538470a39c669f45903c40c36e8972`，在干净基线重放并核对 231 文件；新证据目录为 `/workspace/bench-compare/production-self-review-20260914/`，详见 [自审记录](PRODUCTION.md#self-review-20260914)。旧取消失败与 v7 证据不覆盖。生命周期嵌入 API 的 Arc receiver 变更见同页。

**仍未放行**：netem（内核 qdisc 不可用，NOT_RUN）、真实 WAN/目标设备、24 h 无人值守恢复、72 h soak、10,000 次生命周期循环及所声明介质故障模型。CLI/诊断前移不等于完成整个 V1.1 工作台；Arrow/JIT/DAG/HA/可靠 MQTT/新 IoT 节点均未混入本批。正式发版先 review、确定提交/正式文档范围，再执行阶段 3。

**R10 收尾（2026-09-14）**：修正全状态 scratch 预留/O(keys) 计量、MIN/MAX 虚增保留与每行替换、融合表达式乘算、freeze resident/wire 混用；补固定恢复点失败/重启 held、提交非物化校验、弱 token 拒绝与 draining。源码与 233 文件清单已在干净基线重放并核对服务器。默认核心 **513/0**、独立无 demo **30/0**、固定 **107 × 20 = 2,140/0**；多 key 吞吐比 **1.0201**，小 key 80k/400k **1.0181/0.9722**，周期开关 **0.9594**，均满足原门槛。400k 有约 2.78% 开销，不解释为全面提速。MQTT 120 s 的 90,000 条预期输出无缺失/重复，仍不等于长稳。完整支持边界/失败样本/指纹见 [PRODUCTION.md](PRODUCTION.md#r10-validation)，最新证据为 `r10-artifacts-20260914/`；MEM-01～04 的完整 arena/第三方边界和发行门禁仍未勾完。

<a id="core-first-batches"></a>
### 3.4 核心优先：整批开发、依赖与验收

K0～K5 是执行批次别名，不增加或替换原任务 ID，不预留真实版本号。**每次交付一个完整、可运行且有失败反例的核心增量，不以新增 trait、codec 或几个 helper 作为整批完成。** K1/R11 已提交并通过原高频压力点门槛；K2/R12 的选定 Preview 闭环已提交，但完整 K2 与生产验收未关闭，剩余项见 §8.4；K3/K4 首批范围已实现并复验，随 `1dd17c8` 提交；K5 前端仍后置。当前按顶部清单继续补齐 K1～K4 核心；历史工作包不替代最新支持矩阵。

#### 3.4.1 大模块顺序与交付效果

| 优先批次 | 模块 / 原任务 | 必须交付的闭环 | 做完后新增的实际能力 |
|---|---|---|---|
| K0：当前收尾，持续最高优先 | 当前 review、PUB/OPS-04/QA 适用范围 | 修复 review 阻断问题；冻结可追溯候选；按平台/负载做网络、设备、长稳与恢复验收 | 为现有受限支持范围取得发行证据；不宣称新核心已实现 |
| **K1：已实现、review 修复及提交** | **通用 checkpoint / 状态参与者**；STATE-01～05、REL-14 的 File 子集、所用 MEM/SEM/CFG/QA | 协议、零/单/双 Count 保存恢复、generation、API/CLI/codec 拒绝矩阵与匹配服务器证据已闭合；R11 已通过原 100 ms 压力点门槛，完整生产门禁仍保留 | 恢复不再硬编码为“必须恰好一个 Window”；不等于任意图可恢复 |
| K2a：可靠入口 | REL-00～06/08～14、AGE-01 来源身份子集 | 只选一条来源路线；拉取/预取/未确认预算、checkpoint-aware ACK、运行中/重启后去重、保留范围、独占/旧 attempt 隔离、毒消息与有限 retry/DLQ | 对选定来源解释哪些数据可以重放、何时确认、历史何时失效；不是只增加一个 broker 连接器 |
| K2b：可靠输出，与 K2a 对齐 | REL-07/08、STATE-04、HTTP-06/07 | 稳定输出 ID、required 确认层级、未知 HTTP 结果、重试/重放与重复语义；按下游断网自治需求决定是否交付独立持久 outbox | 明确已接纳/已持久化/远端已确认的差异；选用 outbox 后才可承诺其持久接纳范围内的重启续送 |
| K3：真实 DAG 内核 | DAG-01～07、STATE/AGE/MEM 适用部分 | 单源 Branch/Route/多 Sink → 多源 UnionAll/时间进度 → 多输入 aligned；分别通过预算、慢/失败支路、取消与恢复矩阵 | 真正执行分支和合流，而不只是画图；按已通过的阶段开放 capability，DAG-08 页面不在本批 |
| K4：IoT 状态算子 | IOT-01/02、TPL-01/02、所用 STATE/MEM/SEM；IOT-03 后续增量 | 变化检测和 Deadband 的类型/首次值/异常值、key/TTL/预算/reset、精确模板；所声明的恢复组合接入 K1 协议并验证 | 可用于减少重复上传和无效通知，状态连续性与重置行为可解释；不把值过滤冒充可靠告警平台 |
| K5：后置产品层 | UI-01～06、DAG-08、OPS-01～03 剩余体验范围 | 复用已稳定 API/CLI 和真实图能力，再做运维工作台、编辑/发布/诊断与可视化编排 | 降低部署和操作成本，不另造运行时，不成为 K1～K4 的前置 |

K2a/K2b 是同一可靠数据链路的两个工作包，确认层级、身份和故障切点必须一起设计；必要时独立发布已闭合的子集，但不能提前宣传端到端可靠保证。K3a/b 不依赖选用 JetStream，K4 的基本值过滤也不依赖完整 DAG；这是默认优先级，不是强制等待所有前项清零的瀑布。

#### 3.4.2 当前整批 K1：不是只放开准入判断

| 内部工作包 | 实现与验证范围 | 验收出口 |
|---|---|---|
| 1. 全计划参与者清单 | prepare 从实际计划收集稳定 operator/slot/shard 及 required Source/Sink；集合、条目/字节和单 Job 归属有上限；ACK 绑定 attempt/checkpoint/participant | 空集合合法但不等于没有 Source/Sink 责任；未知/重复/旧 ACK 不能拼出成功提交；重复实例 ID 在输入激活前拒绝 |
| 2. File 零状态路径 | File → Filter/Project → 一个受支持 required Sink；source cut 与输出完成共同进入提交，无虚拟 Window 或伪快照 | 空文件、无输出、全部被过滤仍正确推进 cut；保存后实际杀进程/恢复，无静默 fresh 或越过未完成输出 |
| 3. 单/多状态路径 | 既有 Count/ET tumble/ET hop 单状态迁入共同协议；再贯通至少两个不同 OperatorId 的真实窗口状态组成的有效线性计划 | 每个实例的状态分别冻结和恢复，schema/上下游语义匹配；精确状态与输出等价，不能只用两个 mock ACK 代替运行链路 |
| 4. 编码、依赖与实例身份 | manifest 列全参与者、codec、语义和所需 timer/依赖；区分 revision、attempt、state generation；fresh/reset 与兼容恢复规则明确 | 缺失、额外、重复、未知 codec、参数/节点变化均按明确规则拒绝；旧单窗口格式若未显式验证兼容，则拒绝并给出迁移/回退说明，不改写旧证据 |
| 5. 全局成功与资源释放 | 单次 checkpoint 的总 snapshot/恢复工作量有上限，不能每个参与者各自用满整 Job 预算；保持单请求 gate、deadline、独占 writer、取消安全生命周期 | 部分 freeze/Sink/磁盘失败不发布 CURRENT；在途失败、恢复失败、取消和替换不泄漏 owner/task/fd，旧 attempt 不能写新状态 |
| 6. 控制面与能力声明 | validate/explain/start/status/checkpoint/restore 同步新矩阵；周期调度、选点、保留/GC 与多参与者兼容 | 明确分开运行资格、可恢复资格和认证状态；仍只开放单 File/单 required Sink 及验收过的线性组合，多源、分支及未覆盖状态节点继续拒绝 |
| 7. 故障、性能与交付 | 独立预期、确定性模型、真实进程和固定二进制比较；恢复大小、暂停/恢复耗时、吞吐/p99/RSS、账本/task/fd；正式 docs、示例和操作命令 | 保存完整成功/失败证据；以相同负载和配置比较受测基线，开跑前冻结阈值。代码、测试和最小 API/CLI 操作能共同复现才交付 |

**K1 最小必测矩阵：**

1. 零/一/至少两处真实状态；空输入、全过滤、窗口未闭合/已闭合；手动/周期 checkpoint。
2. 并发请求、超时、调用者取消、重复/延迟/未知 ACK、部分 freeze 失败、Sink 失败；旧 ID 不能完成新 checkpoint。
3. 提交前后进程中断、写入/同步/发布失败、损坏或缺失参与者；恢复成功或明确拒绝，失败不得提升未发布状态为 CURRENT。进程级注入不冒充断电认证。
4. 慢 Sink、满队列、总 snapshot/状态/工作区预算边界；多参与者占用总和、失败释放与停止后无继续提交。
5. 既有单窗口恢复和 best-effort 默认行为无回归；新支持形状的静态校验、真实执行与 API capability 一致。

**K1 明确不做：** 新 broker、MQTT 无损承诺、独立持久 outbox、跨 Job checkpoint、unaligned、自动通用状态迁移、任意多源/DAG 恢复、全部 Dedup/Lookup/PT/新 IoT 节点恢复、完整 arena、UI/Designer、Arrow/JIT。未纳入本批 codec/语义/依赖验证的状态节点继续拒绝 aligned。

**开工方式：** 先处理当前 review 的阻断问题并记录基线；若与候选验收并行，新功能使用独立分支/工作目录，不能把 review 中的代码与 K1 半成品混成一个受测候选。K1 可用现有 File 和测试 Sink 开始，不要求先决定 NATS/Kafka。内部按上述工作包小步实现，整批完成后集中交付，不每完成一个 helper 就停下。

#### 3.4.3 可靠输入/输出需要确认的边界，不阻塞 K1

- REL-00 先记录目标保证、可接受重复、保留/回退范围、允许的部署组件、断网自治时间及容量上限，再选 **一条** 路线。允许 NATS 时按设计先验证 JetStream pull；已有 Kafka 标准或不允许外部 broker 时另选，不自动安装新服务，不同时做三套持久化。
- 来源确认只能覆盖已达到声明输出确认层级且 checkpoint durable 的范围；运行中 redelivery、提交缺口和重启后 replay 必须共同验证，最大 sequence 不代替连续完成证明。
- 输出可靠合同是必做核心项；独立磁盘 outbox 的实现取决于是否要求“下游断开时仍本地持久接纳”。如果选择用可靠源保留/重放而不做独立 outbox，必须明确这一取舍及吞吐/阻塞限制，HTTP-07 不能因只做需求决策而勾完，也不能声称具有本地离线续送能力。
- 选用 outbox 时，输出持久化、checkpoint 发布、来源 ACK、远端确认、删除/GC 的崩溃切点须联合验收；输入日志、输出 outbox 与 DLQ 各负其责，不互相替代。HTTP 成功响应或稳定 ID 不单方面保证下游幂等，更不泛称 exactly-once。
- 连接所需资源尚未确定时，保留 K2 未完成状态；可转入前置已满足的 K3 或 K4 后端子集，不降级为做 UI，也不放松可靠保证。

#### 3.4.4 每批完成、验收和版本动作

- **开发完成**：真实入口到输出/状态/失败/恢复闭合，必要的 API/CLI、能力声明、正式文档、独立预期与回归齐全；不是只实现抽象协议。
- **可发布**：在明确平台、负载和失败模型下，通过与该源码匹配的 §15/§17 验收。K0 的现有范围可以独立发版，不必等所有核心新功能；新功能也不能借用旧候选长稳证据。
- **版本安排**：K1、K2 的完整交付切片、K3 各阶段、K4 可以分别成为下一功能 minor；当前仍为 0.1.0，若先发行 0.1.0 且无占用，K1 才可考虑 0.2.0，后续按实际发布递增，不给工作台预留 0.2.0。codec/API 不兼容必须单列迁移与回退，不因仍是 0.x 省略；不是“多状态恢复完成”就直接打 1.0。
- **保留范围**：核心优先不是删掉平台、扩展或性能任务；仅调整投入顺序。必要鉴权、审计、可观测性、资源与运维入口随核心操作同交付，不能拿“周边后置”当作省略安全和排障的理由。

<a id="foundation"></a>
## 4. 发布资产、所有权与表达式基线

优先级：发布收尾立即；所有权与独立语义基线近期按使用路径推进。负责人：待认领。

### 4.1 BASE 补正状态

- [x] **BASE-01 — 当前支持矩阵的 aligned 准入。** K1 Server 与 participant-mode Kernel 共用 `CheckpointPlan`：File 零状态、单 Count/ET、双 Count 通过；PT、Dedup、Lookup、多源、其他多状态拒绝。旧 legacy embedding gate 仍限单窗口，新实例不能绕过 prepare。
- [x] **BASE-02 — 完整规范语义与恢复兼容检查。** 旧单窗口保留 `SS02`；K1 `CP01` 覆盖完整线性计算（含下游）、字段/字面量/窗口语义，`CPL1` 绑定实例身份与 codec。64 KiB/深度 64 上限、内容比较和旧格式明确拒绝保留；函数语义变化须显式版本化。
- [ ] **BASE-03 — 受控数据所有权与分配准入整体闭合（部分完成）。** 依赖 MEM-01～04。完成标准是声明范围内不存在提前退款或分配后补账，不是仅引入一个 wrapper。
  - [x] detach 先获取目标信用，覆盖保守复制峰值。
  - [x] `OwnedRows` 避免 shared container 的隐式复制与数据/lease 分离。
  - [x] `BatchValue` 的父 batch/最后 owner 生命周期回归。
  - [ ] 审计并封闭受控路径的 raw Row/Scalar 逃逸与未计费分配；完成长期 state 的独立拥有型存储。
- [ ] **BASE-04 — 独立表达式 golden 与可观察语义合同。** 依赖 SEM-01/02；现有 eager 行为只是保留了，并不等于完整 golden 已完成。
- [ ] **BASE-05 — 发行构建、能力和证据整体闭合（部分完成）。** 依赖 PUB-01～05、OBS 及声明 profile 的 QA。
  - [x] 独立无 demo feature 的 Server 测试与真实进程验证。
  - [x] 修复 Window/Sink 重复累计 `emitted_rows`，验证最终 Sink 口径。
  - [x] 关联本轮原始日志、源码补丁、二进制和失败证据。
  - [ ] 锁定工具链、接入 CI、正式 capability inventory、声明设备与长期运行验收。

### 4.2 发布收尾

- [ ] **PUB-01 — 固定可回退基线（代码已提交，发行范围仍待闭合）。** `4f70407` 已固定代码、构建配置和验证脚本；README/正式 docs 和根目录文档按用户要求未加入。发行前核对 API 变化、最终提交与受测源码关系及正式文档的版本化范围。根目录评审/过程 MD 不默认纳入，不能把已提交代码当成发行资产已齐全。发布/tag/push 另按授权执行。
- [x] **PUB-02 — 固定生产构建（Linux x86_64）。** Rust 1.98.0、Cargo.lock、独立 no-demo Server/CLI 构建与包脚本已验证；CLI 不依赖 runtime/testkit/connectors，demo 启动拒绝。其他 target 不继承认证；见 §3.3.5。
- [ ] **PUB-03 — 回归接入 CI。** 基础静态检查/受影响测试、集成测试、周期故障测试分层；复用缓存，日志归档。恢复 codec/内存 owner/关键 I/O 改动必须命中相应反例；不把长稳放进每个小提交。
  - [x] `.github/workflows/ci.yml`、固定二进制重复与真实进程脚本已交付；对应 Linux 命令已执行通过。
  - [ ] commit 后在实际 GitHub runner 触发并保存 CI 产物；本轮没有 push，不能把远端 shell 测试说成云端 CI 已成功。
- [ ] **PUB-04 — 发布证据与迁移包。** 导出 build/source/config 指纹、capability、命令、原始结果、负向样本及已知限制；复用 §2 的现有证据。加入 V1→V2 checkpoint 和 `into_rows` 的迁移/回退操作说明，验证备份可读取。
- [ ] **PUB-05 — 全计划能力矩阵。** 建立 Source × Sink × time × recovery × graph/state 的机器可读 inventory 和契约测试。区分构建可用、配置允许、计划 eligible、checkpoint compatible 和运行健康；未测试组合不标 Supported。
- [ ] **PUB-06 — 设计追踪、历史决策与文档一致性（新增执行项）。** 阶段 0 登记范围，阶段 2 结束前关闭本版相关冲突；维护 §18 的“来源章节→任务→阶段→证据/取舍”。蓝图 001～028 与 Final P-001～P-015 分开编号，逐项指向现有正式 ADR、RUNTIME 合同或待决定任务，不为凑数生成 26 个空 ADR。
  - 对本版每条适用的“必须”、反例和门禁细化到可执行验收；仅章节有行、ID 齐全不算完成。已有实现关联回归，延期写触发条件，被替代写新合同和兼容影响。
  - 核对 README、`docs/v1-report.md`、`docs/ARCHITECTURE_SUMMARY.md`、`docs/bench.md` 的历史范围/当前能力。旧 codec、V1 里程碑、过时的缺项或测试结论不能无日期地当现状；保留历史证据而非抹掉失败记录。
  - 发布前确认设计来源有可追溯版本/指纹；根目录过程文档不自动入库。若其不随发行提供，把实际采用的合同与出处摘要归档到正式 docs，不让发布验收只依赖本机未跟踪文件。

### 4.3 所有权剩余任务

- [ ] **MEM-01 — 受控分配/逃逸清单。** 逐条追踪 decode、builder、transform、mailbox、sink、state、snapshot 的 allocation/owner/credit；记录复制、共享、外部库临时空间和不受控 API。产出有限范围的修复清单与基准，而非先承诺全进程硬预算。
  - [x] 本批当前 Kernel/HTTP/File/checkpoint/restore 路径的修复及外部/raw API 边界见 PRODUCTION.md；包括恢复快照一次性接手、暂存预准入、state key 计量、失败更新与释放顺序。
- [ ] **MEM-02 — state-owned 值与更新接口。** 小 key/变长值进入长期 state 时独立拥有，不长期 pin 整个输入 batch；新值扩容、替换、输出与旧值共存峰值先准入，失败不破坏旧状态。首批 IoT 所使用的类型先闭合。
  - R9 提醒：当前 `RowBatch::detach` 的保守 `max(2 × resident, tracked)` 信用保持到副本释放；不得直接当作长期 state 的精确常驻成本。实现 state-owned 时另验峰值准入与复制后的精确目标计费/安全收缩，不提前退还仍存活分配的信用。
- [ ] **MEM-03 — 受控 API 隔离与完整 arena 决策。** 在 MEM-01 的证据上选择迁移方案；受控拥有型 handle 不能拆开数据与 owner，raw constructors/clone 明确隔离或收窄。完整 arena 是否实施、覆盖哪些第三方分配，以 ADR 和使用范围决定。
- [ ] **MEM-04 — 生命周期与峰值验证。** 覆盖父/子 owner、并发共享、取消、共享后 detach、变长扩容失败、state 替换、snapshot/输出共存及最后释放；同时观察账本与 RSS，但不把二者等同。

入口：`crates/sparrow-model/src/batch.rs`、`scalar.rs`、`memory.rs`；`crates/sparrow-runtime/src/state.rs`、`transform.rs`、`window.rs`、`checkpoint.rs`。

### 4.4 语义剩余任务

- [ ] **SEM-01 — 独立规格 golden。** 手工确定 NULL/三值逻辑、Missing→NULL、整数边界/溢出、除零、signed/unsigned、NaN/±0、cast/try_cast、时间单位与函数求值次序的结果或错误。
  - 验证 Binary 两侧与 Call 参数的现有 eager 行为；若引入 lazy/short-circuit，独立修改共享语义版本，不作为后端私有优化。
  - 验证 Filter 跳过行不触发其 Project 错误；跨行/跨步骤首错次序及失败批次不发布输出。
  - `eval`/`eval_bound` 的一致性仅算共享实现差分，不算独立 oracle。
- [ ] **SEM-02 — 函数语义描述。** 复用现有 binder，逐步登记类型、nullable、确定性、上下文、错误/overflow、输出/工作量上界与语义版本；仅为真实使用者增加实现选择字段，不先造通用 VM。

入口：`crates/sparrow-expr/src/bind.rs`、`lib.rs`；`crates/sparrow-runtime/src/transform.rs`；`crates/sparrow-plan/src/canonical.rs`。

<a id="observability"></a>
## 5. OBS：真实健康、队列与延迟

优先级：**当前完整批次，验证收尾**。依赖：现有 Job/attempt 生命周期；新增统计自身满足 MEM 的局部预算规则。新方向负责人：待认领。

### 5.1 队列计量先行

- [x] **OBS-02 — Job/stage 真实占用与等待（当前 Server 支持路径及 File 开销回归）。** Source inbox、Runtime mailbox、Sink outbox、HTTP 编码信用/在途/终结计量已接通；R9 的有界批量操作保持一行一个队列槽，独立进度计数不削弱交付/队列守恒快照。80k / 400k ABBA 吞吐比分别为 1.169 / 1.174，超过预先登记的 0.97 目标；其他场景/硬件不继承该开销结论。future Connector、新图、netem 和设备认证不随之完成。首批及 v12 历史证据保留，新证据见 `docs/RUNTIME.md` 的 R9 章节。
  - [x] 区分 source 入队、Runtime envelope/data/control、consumer-held、Sink 收集/在途/终结与编码信用；逻辑字节不等于 RSS，非 MQTT 的 pending working-row 字段为 null。
  - [x] Runtime 不把 Semaphore 预留信用当作成功入队；等待、排队、consumer-held 与 drop 分开记录。
  - [x] Runtime 成功入队、失败、取消、接收、Envelope drop、Receiver 关闭/drain 已接线，并修复 Receiver 关闭后字节等待者的唤醒。
  - [x] Runtime 当前值/峰值、容量等待次数/时长、FIFO 最老排队年龄已接线；空队列年龄 null，未覆盖路径 unavailable。
  - [x] Runtime 时间戳 ring 预分配且与实际 publish/pop 同步，连同 channel Envelope/observer 元数据先计费；无逐事件无限表。
  - [x] Runtime 快照按 pipeline/运行 revision/Kernel attempt/物理 stage-edge 索引隔离，不保留全局历史。外部 observer handle 只保留有界元数据信用；标签不含 device_id/payload。
  - [x] 现有 status/metrics API 已新增 `mailboxes` 命名空间；全路径旧 gauge 不冒充已接线，busy/initializing/no-active-attempt 显式区分。
  - [x] 新增 `observation` / `observations.jobs` 的真实 Source/Sink 队列；发布与 pop 同步、关闭/晚到 permit/drain 分别计数。队列占用与最老年龄精确，边界驻留直方图显式 1/16 采样，等待统计不采样。
  - [x] HTTP worker 持有完整 Delivery 到请求结束，修复 disjoint capture 导致的在途信用提前释放；取消后 group/rows/encoded credit/request 归零的回归已加入。
  - **验收**：生产者/消费者并发与满队列取消下计数正确，停止后归零；共享 payload 的排队逻辑字节不冒充独占物理内存；开销满足事先登记的预算。

### 5.2 实际健康而非 Running 标签

- [x] **OBS-01 — Source/Sink 与 Job 健康诊断（当前 Server 支持路径）。** Source、Sink 与 catalog 生命周期分开；HTTP Ready 表示请求准备/最近结果，不虚构持续连接状态。
  - [x] 分开 desired/actual、连接、处理进展、背压和有限失败；状态变更原因/年龄及最后进展可查。
  - [x] 区分静默、File EOF/等待追加、重连、输入拥塞、下游重试/失败、取消与停止；被回收 attempt 由 catalog 状态解释，不把无活跃 handle 当业务完成。
  - [x] 未规定活跃度时静默不标故障；MQTT Sink 增加控制包读取、PINGREQ/PINGRESP 与有限握手/写入等待，断连可被观测。
  - [x] Source/Runtime/Sink 进展按 attempt 隔离；旧混合角色 I/O 计数明确保留为 legacy，不充当 Source-only 守恒账。
  - [x] status 返回运行 revision、实际入口/出口 kind、队列容量及状态依据；与 stored-latest 配置/eligibility 分开解释，跨组件快照先比 attempt ID。
  - **验收**：空闲健康、MQTT 断连恢复、慢/失败 HTTP、输入阻塞、主动停止均有不同可解释状态；健康查询本身不阻塞数据面。

### 5.3 延迟统计与时间范围

- [ ] **OBS-03 — 分阶段延迟及完整样本口径（实现已接通，链路延迟验收受环境限制）。** 响应等待、验证证书的 HTTPS、失败/取消与阶段统计已有测试；服务器内核不支持 netem，真实链路延迟注入记 NOT RUN，不能因其余测试通过而勾完整项。
  - [x] 明确本机接收/decode、队列、同步批次 compute、encode、HTTP header/body 与 delivery 观测点；wire 精确发送、设备/broker/业务确认未知。
  - [x] HTTP 2xx 与完整响应体、业务 ACK 分开；响应体不完整有计数、不计完整 body 样本，也不重试已经接受的 POST。
  - [x] Source 操作、batch、request/coalesced delivery 分开；传播保守输入批次时间边界，不伪装精确逐行延迟或完整 Window lineage。
  - [x] 固定 32 桶、attempt 累计、不存事件列表；边界 residence 的采样率公开，低样本/溢出分位数为 null。
  - [x] 入队/Runtime ingest/过滤/输出、终结/失败/取消/在途分别报告；仅验证单组件守恒，不能把非原子快照拼成全链路 receipt。
  - [x] 提供 p50/p95/p99 桶上界估计，至少 100 个实际样本；不是 population 的精确分位数。
  - **验收**：受控响应等待、真实链路延迟分别测试并正确标注；断连/过载时延迟分布不掩盖缺失；broker 内等待未知不宣传为完整端到端测量。

### 5.4 OBS 组退出条件

1. 真实 Server 路径能够定位“没有输入”“卡在队列”“下游慢”“连接失败”，不只验证 helper。
2. 正常、静默、持续过载、慢 Sink、断连恢复、取消、Job 更新和 checkpoint 全部通过。
3. 统计条目和内存有界；不能引入每条消息全局锁或无限 label/cardinality。
4. 观测本身不新增过滤/过期/合并策略；随本批修复的 HTTP lease、MQTT Sink liveness/receipt、Log encode-failure receipt 与 HTTP Push task cleanup 单独列出生产行为修复和回归。
5. 完成同负载、固定二进制的观测前后开销检查，再把字段交给工作台。当前 Server 观测始终开启，不虚构运行时开关；R9 以有界批量操作/独立进度计数优化，80k 与 400k ABBA 的吞吐目标预先登记为 ≥ 0.97，不能测试后改阈值。必要的分项实验另留证据，不从总耗时反推每个锁的占比。

**首批历史证据（仅 Runtime mailbox）**：430/0、21/0、12×20，正常 MQTT 7,500/7,500；过载 1,409/1,536，File ABBA 峰值 RSS 差 +172 KiB。该组结果只属于先前版本，不替代本批 Source/Sink 闭环验收。最新实现、失败样本、前后开销和范围见 RUNTIME 的 Connector/runtime observation closure；测试脚本为 `scripts/obs-closure-verify.sh`、`scripts/obs-check-snapshots.jq`。netem 未运行继续保留为 OBS-03/QA 的缺口。

**前批最终 v12 证据（历史）**：核心 **445/0**、无 demo **21/0**、相关 **28×20** 通过；干净目录恢复 sum=60、拒绝不兼容 Map/Filter 并保留 CURRENT。正常 MQTT **7,500/7,500**，120 秒持续流量 **90,000/90,000**，无丢重/无效行；刻意过载 **1,413/1,536** 仍作为有损反例。**254 个观测快照 / 508 个队列快照**的单组件守恒通过。File ABBA 中位吞吐 **499,938→448,929 events/s（约 -10.2%）**、峰值采样 RSS **+468 KiB**；该版本未过 0.90 调查线，不追认通过。netem 为内核能力缺失的 NOT RUN。证据在服务器 `obs-closure-artifacts-20260912/v12/`；详细口径和指纹见 RUNTIME，未进行发布或 72 小时长稳。

**R9 跟进证据（2026-09-14）**：核心 **463/0**、无 demo **22/0**、相关 **48×20 = 960/0**；新增初始化、批量取消/顺序、缓存失效、恢复切点等反例。80k ABBA **484,361→566,333 events/s（+16.9%）**，400k **504,933→592,683（+17.4%）**；20 个计量轮全部有效，RSS 差分别 +1,000 / +464 KiB。正常与 120 秒 MQTT 持续流量分别 **7,500/7,500、90,000/90,000**；刻意过载 **1,412/1,536** 有 124 条缺失，保留为有损反例。254 个观测 / 508 个边界队列 / 508 个 mailbox 快照通过守恒检查；干净恢复 sum=60，兼容性拒绝及 CURRENT 保留通过。源码补丁从 HEAD 在干净目录重放后 36 个 Rust/Cargo/lock 文件与受测源码相同。证据：`r9-artifacts-20260914/v4/`；netem 重新尝试仍 NOT RUN，未进行发布或 72 小时长稳。

入口：`crates/sparrow-runtime/src/mailbox.rs`、`metrics.rs`、`kernel.rs`；`crates/sparrow-connectors/src/diag.rs`、`mqtt/source.rs`、`http.rs`；`crates/sparrow-control/src/supervisor.rs`、`status.rs`；`crates/sparrow-server/src/lib.rs`。

<a id="freshness"></a>
## 6. AGE：过载与业务时效

优先级：AGE-01 与 B1 同做；其余在 OBS 和具体策略需求确认后。负责人：待认领。

- [ ] **AGE-01 — 时间、来源与消息年龄合同（本机观测范围闭环，完整业务 provenance 保留后续项）。** 本批完成局部接收/解码批次就绪→队列→计算→Sink 的保守时间边界；设备 event-age、聚合完整输入 lineage、跨重启业务年龄与可靠来源身份不随之实现，继续与 P0-05/REL/STATE 共用合同。
  - [x] 明确本机接收、File decoded-batch-ready、真实入队与 event time 的区别；无法证明的时间 unknown。
  - [x] Source/Runtime/Sink 观测使用单调 `Instant`，不持久化，不与跨重启墙钟混用；恢复的新输入重新观测，旧聚合时间未知。
  - [x] 未知输入污染合并边界；未来 monotonic origin 不伪造成零年龄；wall/event time 不参与本机 residence 算法，原有时钟语义不改。
  - [x] Filter/Project/share/detach 及 HTTP 合批保留保守输入批次边界，Window 输出明确清除；不把批次边界说成逐输出行精确来源。
  - [ ] 按后续业务需求补可靠来源身份、跨 state 的完整 provenance、可重建/持久化年龄和对应恢复反例；未做前禁止据此启用 AGE-02～04 自动策略。
  - **验收**：虚拟时钟覆盖回拨/未来/未知；恢复前后时间判定按合同一致；不改变现有数据处置。
- [ ] **AGE-02 — 过载策略与能力矩阵。** 先保留已有背压/有限失败，再由业务选择一个显式过期策略；写清入口/计算/出口阶段、最大等待、默认值、失败模式和支持的恢复组合。未实现字段直接拒绝。
- [ ] **AGE-03 — 过期、合并和状态更新边界。** 区分丢过期通知与丢输入；状态仍需观察恢复条件。latest-per-key 仅在明确允许合并的快照流中 opt-in，不能用于悄悄修改 COUNT/SUM/事件流。输出拆分、receipt 与业务身份不因策略改变而失配。
- [ ] **AGE-04 — 过期策略的失败/恢复验收。** 慢 Sink、持续过载、断连恢复、时钟异常、停止与重放逐项验证；可重放确定性来自可重建输入事实或持久化决策。允许“按当前时效重新判断通知”时单独声明语义，不能仍声称与旧输出相同。

**共同底线**：有限入队等待、linger 和请求 timeout 都不是硬实时完成期限；在途外部请求有取消/确认不确定性。可靠路线不能静默丢弃后推进成功 cut。

<a id="v11"></a>
## 7. V1.1：工作台、CLI 与首批完整场景

优先级：按最新核心顺序，§7.2 的 IoT 为 K4；工作台/可视化与体验扩展为后置 K5。已有 CLI/诊断继续复用，新核心暴露操作的必要安全与排障不后置。共同前置：复用现有 Store/Supervisor/鉴权/Explain，先核实现有 CLI/API 能力；诊断展示依赖 OBS。负责人：待认领。

### 7.1 V1.1a：不用打开源码也能部署和排障

- [x] **CLI-01 — 最小运维命令闭环。** `sparrowctl` 已贯通 validate/explain、配置发布、启停、status/diagnose、checkpoint/restore；结构化输出/退出码、非交互与大小/超时/危险 URL 拒绝均有测试，真实进程闭环通过。
- [ ] **UI-01 — Dashboard。** 先薄的离线可用界面；展示 OBS 的真实输入输出、排队/背压、恢复风险、attempt 与原因。unknown/unavailable 不显示绿色成功。
- [ ] **UI-02 — Connections/Streams 最小管理。** Schema、时间字段、SecretRef、有效能力和有限连接测试；测试连接复用生产目标 allowlist/超时/权限，不能成为旁路网络代理。
- [ ] **UI-03 — SQL/Graph 基础工作区。** 清单、错误 span/node、Explain、结果 schema 和线性图展示；复杂 IDE 编辑体验后置。不支持的 SQL/图形状明确拒绝。
- [ ] **UI-05 — 发布历史闭环（分 a/b）。** a 复用已有 revision/desired/actual 的发布与停止，显示实际生效版本；b 补 draft/diff/history/operation。并发更新产生冲突，保存草稿不激活 ingress。
- [ ] **OPS-02 — 最小诊断包。** 构建指纹、脱敏配置、有限日志/指标、错误与计划，离线可读；总字节/TTL/下载范围有限。不能导出 token、SecretRef 解密值或任意宿主文件。
  - [x] 本批前移子集：128 KiB allowlist 诊断、有限审计摘要、当前 attempt 观测与恢复状态，CLI 独占 0600 文件；无任意文件/秘密/自由文本导出。完整配置/计划/历史错误归档仍未完成。
- [ ] **OPS-03 — 最小角色与审计。** 可先静态 token 映射 viewer/operator/admin；读取、发布、reset/restore、密钥操作分别授权。每次暴露操作同时交付拒绝测试与审计，不能等 UI 完成才补。

### 7.2 首批 IoT：推荐先变化检测 + Deadband

新节点共同前置：MEM-02 对所用类型闭合；显式 key/state/timer 上限、TTL、首次值/异常值语义、输出 schema 与重置行为。首批不依赖完整 UI、DAG 或 JetStream；未参与 snapshot 的新节点拒绝 aligned。

- [x] **IOT-01 — 变化检测（K4 Preview）。** 每 key 前值和比较规则；首值、NULL/非有限值、TTL、重复与 reset 已有明确合同。预算失败不半更新基准；File→HTTP、TTL=0 的 v6 恢复已验证，其他组合不自动降级。
- [x] **IOT-02 — Deadband（K4 Preview）。** 绝对/相对阈值、last_input/last_output、零基准、初值和异常值已实现；阈值边界、逐步漂移、整数极值、资源与恢复反例通过。
- [ ] **IOT-03 — Hysteresis。** enter/exit 双阈值、等号边界、Normal/Unknown/Active 与恢复行为；坏数据不把 Active 清零。可作为第三个场景，若业务更需要告警可替换首批中的一项。
- [x] **TPL-01 — 至少两个完整可运行模板。** change/deadband 均交付 schema、NDJSON、精确 golden、配置/预算、reset/过载说明与命令；测试实际加载模板经 File→Kernel→HTTP 验证，不只检查字符串或输出行数。
- [x] **TPL-02 — 现有能力模板化（现有入口范围）。** File/v3 阈值与双 Count Server 模板、静态/版本化 Lookup embedding 示例的效果、命令与故障/恢复/time 边界统一写入 [IOT.md](IOT.md)。不将 embedding 表能力冒充 Server 表发布/绑定；该运营流程仍属 TAB 后续项。

首批如果输出被当作稳定告警事件，必须落实 STATE-04/REL-07 的最小身份子集；单纯值过滤也必须暴露 fresh reset/连续性丢失，不能复用旧 episode 身份。

### 7.3 V1.1b：按现场需求扩展

- [ ] **UI-04 — 有界 Preview。** 限制输入、输出、CPU、内存、并发及调试 TTL；虚拟时钟和输入 watermark 可控，默认 CaptureSink，无真实外部副作用。Preview 生命周期不占住长期 Job 的额度。
- [ ] **UI-06 — 回退与安全模式工作流。** 展示配置/state 兼容决策与显式 reset/replay 风险；回退不宣称撤销已发出的 HTTP。错误状态、超时、取消与恢复点缺失有明确反馈。
- [ ] **OPS-01 — 导入/导出。** 包含配置和依赖版本，不含秘密明文或未经授权机器码；导入先验证 capability/依赖/冲突，不直接覆盖当前运行状态。
- [ ] **EMB-01 — 宿主 Runtime 接入。** 仅有嵌入需求时推进；显式传入 Handle/执行上下文，区分 borrowed/owned 生命周期，不关闭宿主 runtime，不安装全局信号/日志。文件/SQLite blocking 隔离不重复建设。
- [ ] **SQL-01 — 场景驱动补充 SQL。** 先 inventory HAVING、CASE、IN/BETWEEN 等实际支持，再按模板补缺项；每项进入 SEM 的类型/NULL/错误/资源测试。函数维护成本成为瓶颈时评审 DF 深接入，不零碎复制通用数据库。
  - 2026-09-17 用户新增“丰富 Action/函数”的规划要求：常用函数首批和后续函数分类统一见 [§11.4](#actions-functions)，与 SQL/Graph 共用 binder/语义，不另建一套 evaluator。

**V1.1 退出条件**：用户可以通过已有入口部署、解释、停止和诊断；至少两个完整场景可复现；所有未支持恢复组合拒绝；不以完整 IDE/Designer 为门槛。

入口：`crates/sparrow-cli/src/`、`crates/sparrow-server/src/lib.rs`、`crates/sparrow-control/src/`、`crates/sparrow-plan/src/`、`crates/sparrow-runtime/src/state.rs`、`timer.rs`、`clock.rs`。UI/新节点模块按实际切片新增，不先铺空 crate。

### 7.4 从旧评审补入的运维与窗口切片

本节编号属于对应能力组，但**阶段不由章节位置决定**：OPS-04 的现有 aligned 子集放在阶段 2～3，不等 V1.1 工作台；WIN-01 先盘点，新增能力需求驱动。

- [ ] **OPS-04 — 周期 checkpoint 与无人值守恢复（新增执行项）。** 先核实现有调度入口，复用 Coordinator 和实际 aligned 准入；对声明支持的 File 单窗口组合补齐可配置周期、启停/更新与 attempt 归属，不借此放开零/多状态或 MQTT 恢复。
  - [x] 周期调度、手动仲裁、超时/取消、GC/pin、实际进程恢复与短程开销已通过；见 §3.3.5。下面的 24 h 要求仍未通过，父任务保持未勾选。
  - 同一 Job 最多一个活跃 checkpoint；手动/自动请求统一仲裁，错过的 tick 合并或跳过且可观测，不积累无界任务。空闲、满队列、失败重试、timeout、stop/update 后旧调度器均有反例。
  - status 提供最近成功恢复点及年龄、触发/完成/失败原因；周期不是实际 RPO 保证。按 state 大小、写率、介质和恢复目标选值；保留数量/字节与 GC 不能删除最后有效恢复点。
  - **验收**：虚拟时钟验证调度；真实进程无人干预运行 24 小时后，从最近成功的自动 checkpoint 恢复并核对输入位置、状态与输出。可纳入 QA-08 的同一 72 小时试次，但单独记录此结果。源码若已有实现，只补缺失合同/回归，不另造一套调度。
  - 本版若完全不发行 aligned，可经 PUB-05 明确排除并延期；否则所承诺的无人值守恢复范围必须通过。REL 引入后再验证周期与 ACK deadline/pending ACK 的协调，不能把本项当 REL 协议完成。
- [ ] **WIN-01 — 窗口能力盘点与 PT hopping/sliding 取舍（新增执行项）。** 阶段 0～2 核对 SQL/Graph→WindowKind→运行→恢复的实际矩阵；当前类型区分 PT tumbling 与 ET hopping，不能用“已有 hopping”概括为 PT hopping 也支持。
  - 已有窗口保留回归；PT hopping/sliding 在阶段 4b/5 有场景时再实现。先定义 sliding 是 hop 的命名还是逐事件滑动，明确时间轴、边界、overlap/工作量、timer/state 预算、首次/空窗口/停止行为。
  - **验收**：独立边界与重叠 golden、SQL/Graph 一致、虚拟时钟、预算/取消测试；未进入快照协议的组合明确拒绝 aligned。L=0 Session 仍归 ANA-01，迟到合并归 V2-02，不重复立项。

<a id="recovery"></a>
## 8. 共享状态参与者与一条可靠实时入口

优先级：**§8.1 恢复底座是下一整批 K1，不以 broker 决策为前置；§8.2/8.3 的可靠路线为 K2a。** 新来源仍须确认所需历史范围、重复容忍、断网自治和允许的部署组件，与 K2b 的输出合同一起选定。完整批次见 §3.4，负责人待认领。

### 8.1 先演进共享状态协议，不给纯转发塞假窗口

- [x] **STATE-01 — 有限参与者集合（K1 支持矩阵）。** prepare 收集稳定 operator/slot/shard 及 Source/required Sink；最多 64 个实例、2 个 state、slot=1/shard=0，冻结负载属于同一 Job owner。ACK 绑定 attempt/checkpoint/participant，重复幂等、冲突/未知拒绝，旧 ACK 不补齐切点。
- [x] **STATE-02 — 零状态 checkpoint（File 子集）。** 无窗口清洗/转发实际贯通 source cut、required Sink、manifest 和进程恢复；覆盖空 append-only 文件与全过滤，不塞虚拟窗口。REL-14 的其他实时入口仍未完成。
- [x] **STATE-03 — 多状态 snapshot/restore（K1 支持矩阵）。** 单 Count/ET 与双 Count 共用 v3/CPL1，RCP2 将完整诊断语义与状态依赖前缀分离；plain CP01 保留原完整计算严格规则。新外层保持旧 K1 可读后明确兼容拒绝，避免未知格式触发回退；R10 codec 不迁移，Server 拒绝在旧格式目录混写。两处各 1024 keys 共享预算；其他组合、节点及 DAG 需另扩 codec/依赖/时间矩阵。
- [x] **STATE-04 — generation/实例身份（K1 File/Row 子集）。** fresh/reset 用安全随机源生成并在输入前持久化 128-bit SG01；恢复保留 generation，revision/attempt 单列。写入失败不激活 Kernel；真实重启验证身份延续。业务输出 ID/episode 归 REL-07/IoT，其他后端切换仍须随功能验证，不能借此宣称已实现。
- [x] **STATE-05 — 协议故障与控制进展（K1 支持矩阵）。** R11 补齐恢复拒绝分支、冲突/幂等 State ACK、超时与配额失败区分、即时退款、升级/回滚及进程输出反重放 oracle；Source/barrier 后并行推进且退出等待真实提交结束。修复 HTTP checkpoint 强制 flush 拆批后，原 100 ms/20 ms Sink 门槛在 6400 与 25600 输入均通过，保留全部旧失败样本；不是用 500 ms 替代。140 × 20 重复通过；真实 kill -9 仍不是断电认证。

**STATE 不是完整 DAG 恢复**：先支持线性零/多状态及已声明输出，多输入对齐在 DAG-07 验收后开放。

入口：`crates/sparrow-runtime/src/barrier.rs`、`coordinator.rs`、`checkpoint.rs`、`aligned.rs`、`kernel.rs`；`crates/sparrow-plan/src/compat.rs`；`crates/sparrow-control/src/supervisor.rs`。

### 8.2 来源选择和 REL 完整合同

本批已实现的 **Preview 子集**（不替代下列完整父任务）：

- [x] 用户批准 NATS 路线；固定 SDK 0.50.0 / Server 2.14.6，feature-off 入口明确拒绝。
- [x] 单 stream 全顺序来源/reader 身份、consumer sequence 连续性、有界 pending 与运行中重投递去重。
- [x] 同一 Job owner/提前 Job slot/实际 SDK 关闭与锁保留；headers、保留原始 Bytes、稀疏 schema 和 ingress Box 元数据计费。
- [x] 空 bootstrap checkpoint、v4 Source/state/输出 cut、HTTP 2xx 后 durable publication 再 ACK；稳定逐行 ID，不由 batch 划分生成。
- [x] 零/单 Count/双 Count 的限定路径；进程强杀、输出后写入失败、提交后 ACK 实际丢失、保留过期、误用 consumer、poison/业务拒绝反例。
- [x] 静态验证/Explain、现有带鉴权审计的启停/checkpoint/恢复列表、状态及 Preview 模板；普通 File 路径性能回归已定位并按原门槛复测。
- [ ] 完整 REL 发布关闭：剩余恢复/迁移操作与选用的扩展处置、实际部署 TLS/RTT/容量/长稳和完整平台失败模型；不把以上短程证据等同 Supported/Profile-certified。

- [ ] **REL-00 — 需求、来源与部署决策（新增执行项）。** 允许 NATS 且确需可靠输入时，按 Final 的建议先验证 JetStream pull；已有 Kafka 标准或不能新增 broker 时另选路线。记录选定 SDK/版本、保留策略、确认层级和投入上限；不同时建设三种持久化体系。
- [ ] **REL-01 — 稳定来源身份。** stream/partition/源代次/sequence 或 position 规范编码；重连、过滤和恢复 reader 不改变身份，复用 AGE/P0 来源合同。
- [ ] **REL-02 — 拉取和未确认预算。** 限制消息数/字节/等待、整个 consumer 的 pending ACK、SDK 私有预取、重投递集合和进度延长；超限不偷偷扩大缓存，控制通道有进展。
- [ ] **REL-03 — checkpoint-aware ACK。** 只有 required 输出达到约定确认且 checkpoint durable commit 后才确认相应范围；ACK 失败/重试不造成状态重复提交。初版逐消息确认，批量确认另证没有越过缺口。
- [ ] **REL-04 — 运行中 redelivery。** 同一 attempt 的重复输入按来源身份去重，不能重复更新 accumulator；去重条目/保留期限受预算。
- [ ] **REL-05 — 崩溃后 redelivery。** 已提交范围可按合同跳过/确认，未提交输入重放；恢复检查先于 source seek/接纳，不用“看到过”代替“已提交”。
- [ ] **REL-06 — retention 与可读取恢复范围。** 来源保留覆盖最早允许 checkpoint，历史过期明确失败；验证旧 durable reader 能否读取所需范围，必要时重建并隔离旧 reader。ACK 导致删除时不能继续承诺任意旧点回退。
- [ ] **REL-07 — 稳定输出身份与确认层级。** 根据来源、算子、窗口实例或展开 ordinal 产生业务 ID，与 STATE-04 统一；合批/拆分/并发/重试不改 ID。明确接纳、持久化、业务完成，不用当前 batch ACK 冒充业务幂等。
- [ ] **REL-08 — 有限 retry/DLQ。** 限制次数、总时长、bytes、退避、DLQ 容量/TTL和满时策略；重放须授权并留审计。DLQ 未达到约定持久点不能提前确认来源。
- [ ] **REL-09 — 恢复操作工作流。** 列出恢复点、兼容决策、所需输入/依赖范围、重复风险、运行状态和取消语义；读取恢复列表不修改 CURRENT。
- [ ] **REL-10 — 消费者独占与旧 attempt 隔离。** 绑定目录、Job、reader 的所有者；替换/误启动第二实例时旧 attempt 不能继续改状态、发输出或推进消费。无有效 fencing 时拒绝重叠接管，不宣传 HA。
- [ ] **REL-11 — 有界去重与提交缺口。** 定义 source cut、已完成集合、holes 和清理；区分 subject 过滤的序号跳跃与真正未完成输入。最大 sequence 不是连续完成证明。
- [ ] **REL-12 — 崩溃切点矩阵。** 覆盖收到未处理、处理未提交、输出成功但 checkpoint 失败、checkpoint 提交但 ACK 丢失、重投递、旧数据过期；核对状态、业务 ID、重复及拒绝，必须实际注入故障。
- [ ] **REL-13 — poison message 与失败处置。** 校验、decode、业务拒绝分别选择 fail/明确跳过/持久 DLQ；每种终结事实有证据，避免无限重投递或静默 ACK。
- [ ] **REL-14 — 参与者与空状态恢复发布。** 依赖 STATE-01～05；按实际计划列出零状态、多状态、required Sink 的已验收组合。不把来源 replayable 当成整个 Job 可恢复。

当前实施顺序：先独立交付 K1 的 File 零/单/选定多状态恢复；REL-00 的需求确认可同时进行，但不是 K1 前置。随后按选定路线补来源/时间身份 → conformance → ACK/重投递/独占/DLQ 与 required 输出 → 联合故障矩阵 → API/CLI 恢复操作与 capability 发布。REL-09 的最小操作入口随核心交付，完整 UI 后置。

### 8.3 无外部持久 broker 时的备选

- [ ] **REL-15 — 本地 durable ingress（条件分支，不与默认路线并开）。** 先确认需要本地持久接纳；实现有界原始 frame/envelope、不可变 segment、校验、同步点、索引、恢复、磁盘满和 GC。日志不进入 SQLite catalog 热路径。
  - GC 同时受活跃消费者、保留 checkpoint、合法重放任务约束；永久停止的慢消费者不能无限 pin 磁盘。
  - 仅对 durable receipt 后的范围声明可靠；MQTT 接收前/同步前的丢失不能被本地日志补成历史重放。

<a id="k2-followups"></a>
### 8.4 K2 已知限制、验证缺口与后续优化台账

登记日期：2026-09-16；基线：`d52b15c`（K2/R12 Preview）。依据为 [R12 匹配证据](JETSTREAM.md#r12-validation)，原始产物根目录为 `/workspace/bench-compare/r12-artifacts-20260916/`。本节编号是原 REL/HTTP/MEM/QA 等任务的跟踪子项，不另造平行路线；负责人均待认领，排期在开工时确认。**全部保留未完成状态，本次只登记，不自动开工或放行。**

#### A. 优化项索引（明细单独维护）

性能、资源和时效取舍已迁入项目级 [非阻断问题与优化清单](OPTIMIZATION_BACKLOG.md)，这里只保留关联，不重复维护状态或验收条款：

| 原跟踪号 | 统一条目 |
|---|---|
| K2-OPT-01 | [OPT-001：10k 档积压与尾延迟](OPTIMIZATION_BACKLOG.md#opt-001) |
| K2-OPT-02 | [OPT-002：默认预算下的消息容量](OPTIMIZATION_BACKLOG.md#opt-002) |
| K2-OPT-03 | [OPT-003：空闲首消息等待](OPTIMIZATION_BACKLOG.md#opt-003) |
| K2-OPT-04 | [OPT-004：远端控制查询与 ACK 成本](OPTIMIZATION_BACKLOG.md#opt-004) |

该清单也收录其他模块的优化候选。生产验证与未实现功能继续保留在下面 B/C，不能统称为“不影响使用的小问题”。

#### B. 尚未完成的生产验证（不是已确认的代码 BUG）

- [ ] **K2-VAL-01 — TLS 与真实网络（生产门禁；关联 HTTP-05、REL-12、QA-04/10）。** 在明确授权的隔离环境测试正常 TLS、无效证书/鉴权失败、延迟/抖动、断连和慢响应；核对 fail-closed、有限恢复/held、ACK/重放、资源和取消。区分实际网络与应用层 sleep；未跑的环境标 NOT RUN，不从 loopback 推导认证。
- [ ] **K2-VAL-02 — 目标设备与多管线容量（生产门禁；关联 QA-07/10、MEM、REL-02）。** 测单/多 Job、零/单/双 Count、消息/schema/状态大小和慢下游组合；验证准入、共享进程预算、公平性和过载恢复。发布所测设备/介质/负载的支持上限，不借一次单管线排空结果承诺通用容量。
- [ ] **K2-VAL-03 — 24 h 无人值守恢复及 72 h 长稳（生产门禁；关联 OPS-04、QA-08）。** 安排独立测试窗口，交替空闲/突发/持续/慢 Sink/断连；记录 RSS、fd、任务、队列、consumer/KV/checkpoint 存储增长和失败计数。24 h 后恢复核对切点/状态/输出，72 h 核对无无界增长、失控重启或静默丢重；短程 640 次重复不替代本项。
- [ ] **K2-VAL-04 — 存储与故障矩阵补齐（生产门禁；关联 REL-10/12、QA-09/10）。** 优先补 ownership KV 容量满不淘汰他人、broker/checkpoint 盘满、写入/同步失败和损坏/修复流程；对声明的介质另验 OS 崩溃/断电模型。使用 broker 与 CURRENT 双侧 oracle，确认不提前 ACK、不静默 fresh/跳过、不损坏其他 owner；SIGKILL 证据不得等同掉电认证。

#### C. 未实现的按需扩展（不是性能 BUG，也不因补测而自动具备）

- [ ] **K2-EXT-01 — 历史 replay、语义 fork 与迁移工作流（条件；关联 REL-06/07/09、STATE-04）。** 有真实回放/变更需求再立项；先定义合法历史范围、持久 lineage/输出 epoch、依赖/版本、授权审计、重复风险与回退。验收覆盖操作中断/重启与 ID 冲突，未交付前继续明确拒绝，不通过删除 owner/checkpoint 绕过。
- [ ] **K2-EXT-02 — 独立持久 outbox（条件；关联 HTTP-07、REL-07/08）。** 只有需要下游断网时仍本地持久接纳才启动；容量/同步点、重启续送、checkpoint/输入 ACK/远端确认/GC 的联合切点须一起验收。本轮 broker 保留/重放方案不冒充已实现 outbox。
- [ ] **K2-EXT-03 — 持久 DLQ 与受控重放（条件；关联 REL-08/13）。** 只有需要隔离毒消息并继续处理后续输入才启动；定义持久点、容量/TTL/满时策略、授权审计和输出身份。DLQ 未持久不能确认来源，重放不能无限循环；现有 fail/held 是已选策略，不是漏做了静默跳过。

**后续处理顺序：** 按独立优化清单先定位 OPT-001 的持续负载瓶颈，其他优化按消息/时效需求推进；在相应环境补 K2-VAL-01/02，生产放行前补齐所声明范围的 K2-VAL-03/04。K2-EXT 项由需求触发，不为勾完 K2 强行同时建设多套持久化。以上不要求所有项目清零后才能开发 K3，但未完成的生产门禁不能因切换开发阶段而消失；新发现的正确性/数据安全问题优先修复，不能按普通性能优化无限延期。

<a id="dag"></a>
## 9. V1.3：分阶段真实 DAG

**2026-09-23 时间图增量已验收：** required File→HTTP 的独立 v18/PT、v19/ET，持久决策、来源 progress、Union 确定性轮合流和每 Sink 输出身份已完成。774/44、14×20、默认/JS 包各 6 个真实故障场景与预算拒绝、完整旧矩阵及三组原性能门禁通过，见 [合同](DAG.md#time-graph-recovery)、[匹配证据](PRODUCTION.md#time-graph-validation)。主线第 2 项按限定组合完成；大图实际准入、每轮 256 行上限及未开放的依赖/时间域仍按合同拒绝，不等于整个 K1～K4 或生产认证。

**K3 本批完成（2026-09-16，尚未提交）：** DAG-01～07 的运行/控制面/Connector/故障闭环已实现并自查，限定能力矩阵通过 `package-v14-*` 匹配复验，详见 [DAG.md](DAG.md#k3-validation)。下方勾选表示这些声明组合的开发验收完成，不表示任意图、所有 codec 或生产认证。aligned 仅开放 required File→HTTP 的零状态/Count 图；time/side-output/有损/JetStream 图 aligned 明确拒绝。K2 的生产验证/优化剩余项继续保留，DAG-08/K5 不在本批。

优先级：核心批 **K3**，按单源分支、多源合流、多输入恢复依次交付；不等工作台，也不强制依赖已选可靠 broker。共同前置：图级预算/owner、稳定 edge/input 身份；恢复还依赖 STATE 和所选可重放来源/输出合同。DAG-08 Designer 单独后置为 K5。负责人：待认领。

### 9.1 V1.3a：单源分支与多输出

- [x] **DAG-01 — Branch。** 共享 payload 广播；有限 continuation；required 背压相关上游；广播中取消和额度释放已测，不持全局锁等待。
- [x] **DAG-02 — Route/Switch。** 显式 first-match/all-match/default；有界 masks/scratch/builder；路由条件错误归因原节点；不随机挑边。
- [x] **DAG-05 — 多 Sink 与分支保证。** 显式有损子树不准重新合流/接 required；数据满时丢弃并计数，控制满时取消该可选子树及嵌套子支路，不静默丢控制继续健康或遗留 stalled leaf；A 已成功/B 失败不回滚 A。
- [x] **DAG-06 — 有限 Side Output（已声明来源/节点）。** File decode-error、ET late、Filter rule-reject 具有独立 schema、预算和显式 backpressure/drop 策略；MQTT/HTTP decode side port 尚不开放，side-output aligned 明确拒绝。

### 9.2 V1.3b：多源与时间进度

- [x] **DAG-03 — UnionAll。** 结构 schema/time 校验、来源 metadata、每物理输入顺序与轮转调度；不保证跨输入全局排序，同源经不同支路合流也不恢复原总序。
- [x] **DAG-04 — 多 Source。** 独立 Connector actor/位置切点、每物理边 watermark/idle/EOF/barrier 进度与每端口诊断；Source 时间在 Filter 前生成，窗口不自行抢跑快源；全部 idle 不推进 MAX，append-only EOF/异常 channel close 不冒充永久结束；显式 EOF 后仍参与 checkpoint，PT 等定时输出完成再结束。

### 9.3 V1.3c 与按能力演进的 Designer

- [x] **DAG-07 — 多输入 aligned checkpoint（v5 File/Count 矩阵）。** 对齐 required 输入/分支/Count 状态/HTTP Sink，暂停快输入切点后数据，超时解除阻塞；全部 source cursor 和完整图严格校验。真实零/双状态进程恢复、提交失败与部分外部输出失败通过。v5 不混写 v3/v4；其他状态/time/side/lossy/JetStream 图继续拒绝。
- [ ] **DAG-08 — Graph Designer（K5 后置）。** typed ports、节点/边限制、预算、Explain、diff 与错误位置；保存未优化 GraphSpec，展示实际计划。不是 K3 内核交付前置；页面只暴露已通过 a/b/c 验收的形状，不要求任意图逆转为一条 SQL。

**每个子阶段单独放行**：禁止数据环；测试慢/失败分支、广播中取消、队列满、重复/旧 Barrier、idle/EOF、部分输出成功及恢复。未认证图显式拒绝 aligned，不静默降级。

<a id="iot"></a>
## 10. V1.4：高级 IoT 与参考表运营

优先级：按真实场景启动。共同前置：首批 IOT 状态合同、AGE、MEM-02；恢复型节点需 STATE，稳定告警身份需 STATE-04/REL-07。负责人：待认领。

- [ ] **IOT-04 — HoldFor。** 条件开始、缺样本、反转、processing/event-time 选择及停机时间规则；processing-time 按 timer 到点触发，不等下一条消息；event-time 由 watermark 推进。
  - [x] 单个线性 paused processing-time、File/JetStream 恢复与真实故障验证（v14/v15）；缺样本维持有效条件、false 取消、等 deadline 时 timer-before-input 明确。
  - [x] v16/v17 的最多两个线性状态组合，统一有序时间、下游等时到期优先、完整状态/输出 cut 恢复；已完成本批故障与重复回归。
  - [x] required File 时间图中的 paused-time 组合（v18）；确定性合流与分支 timer 共切点。
  - [ ] HoldFor 自身的 event-time/watermark 模式与未声明依赖组合；父项不因限定组合通过而勾选。
- [ ] **IOT-05 — Debounce。** leading/trailing、最大等待、重复输入是否延长及每 key timer 上限；timer 替换/取消/恢复和连续抖动有确定性测试。
  - [x] 单个线性 paused processing-time 的上述参数、File/JetStream 恢复、预算和故障测试（v14/v15）。
  - [x] v16/v17 的最多两个线性状态组合及 PT/TTL 边界、重放验证。
  - [x] required File 时间图中的 paused-time 组合（v18）及恢复验证。
  - [ ] Debounce 自身的其他时间域与未声明依赖组合；逐决策提交的容量优化单列 OPT-012。
- [ ] **IOT-06 — 冷却/通知限频。** 限的是通知而非上游状态观察；冷却期间恢复条件仍生效，等待通知有 bytes/数量/年龄上限。
- [ ] **IOT-07 — 静默/离线检测。** 设备集合、最近接收、停机宽限与链路健康关联；从未出现且未登记的设备不凭空判离线，来源断连时不把全部设备判故障。
- [ ] **IOT-08 — 告警生命周期。** Normal/Pending/Active/Recovering、activate/resolve、稳定 episode ID 和重复通知语义；Active/Pending 不默认 TTL 静默淘汰，资源不足时明确 expired/unknown 或失败。
- [ ] **IOT-09 — Sampling/Resample。** last/mean/interpolate、缺值、区间边界、输出时间和未来点需求明确；输入丢弃与输出缺值分开计数，展开与 timer 有界。
- [ ] **TAB-01 — 参考表发布。** 不可变 revision、原子切换、依赖预览/兼容校验；运行 Job 与 checkpoint pin 所需版本，GC 不删除仍可恢复的依赖。不重做已有 as-of lookup。
- [ ] **TAB-02 — 增量表输入。** 主键 upsert/delete、版本来源、有限历史、初始加载/断连状态和乱序处置；操作顺序、回退和恢复依赖可验证。
- [ ] **TAB-03 — 外部 Lookup。** 明确有限并发、timeout、cache bytes/TTL、顺序与 checkpoint drain；今天的请求结果不能当昨天的历史快照。不能版本读取/记录结果时降低重放保证。

验收模板包含“温度持续超限→进入告警→冷却→降低到 exit 阈值→恢复”，验证时间推进、坏数据、断连、reset、恢复和重复通知，不用单一 SQL 样例替代生命周期。

<a id="connectors"></a>
## 11. Connector、HTTP 与格式任务

优先级：现有主链路认证近期；新增协议/格式需求驱动。每项必须复用目标策略、SecretRef、预算、取消及能力契约。负责人：待认领。

### 11.1 已实现 HTTP 能力：保留并持续回归

- [x] **HTTP-01 — 有界合批/linger 基础。** 默认一批一 POST、单在途；行数为合批目标、字节为硬上限。该完成项不包含端到端年龄上限或自动拆分大上游批次。
- [x] **HTTP-02 — 编码容量信用与失败结算基础。** 扩容先增量准入，失败/合批不足按现有合同结算，重试 payload 保持。稳定业务输出 ID 仍属 REL-07。
- [x] **HTTP-03 — Barrier/停止 flush 基础。** Barrier 强制 flush 并等待此前 required 请求结算，失败/取消不假 ACK；保留已有回归。
- [x] **HTTP-04 — 显式受控并发基础。** 默认 1，配置可提高；完成重排和压力/重试放大不等同于 exactly-once。
- [ ] **HTTP-05 — 真实网络/多规则/长稳认证（部分已有证据）。** 复用既有应用响应等待结果；补真实 0/20/100 ms 链路条件、TLS、低速/稳定/突发/过载、批目标 1/8/64、4xx/5xx/断连。测 rows/s、POST/s、完整性、p99、RSS、连接复用；实际设置、限制与未覆盖项一起记录。
- [ ] **HTTP-06 — 输出上界与必要的拆分/continuation（新增执行项）。** prepare 检查可推导编码上界；不可预估的超大结果明确失败，或经独立验收有界拆分。拆分不改变来源/业务身份、局部顺序和 Barrier 覆盖的 receipt；严格顺序场景默认串行，按 key 排序另立范围。
- [ ] **HTTP-07 — 独立 Sink 持久 outbox 的需求与可选实现（新增执行项）。** 当前列入核心批 K2b，不再默认为平台工作流之后才考虑。可靠输出的身份/确认/重试合同必做；独立 outbox 是否实现取决于“下游断开期间本地持久接纳”的需求。选用时随可靠链路完整验收；不选时明确源保留/重放方案的限制，本项保持未完成而非把需求决策当实现。它不同于现有内存重试、REL-15 原始输入日志和 REL-08 的 DLQ，不能互相冒充，避免重复落盘。
  - 选用时定义输出记录/稳定 ID、接纳与 fsync/发送/确认/删除切点、未知 HTTP 结果、重启重发、顺序、配额/TTL/GC、校验和密钥/权限。持久缓存不承诺接纳前的 MQTT 消息不丢，也不能靠幂等键单方面保证下游去重。
  - required 输出只有达到明确声明的确认层级才允许推进 cut；若要以本地 durable receipt 替代远端完成，必须独立修改保证合同并认证，不静默改变现有 Barrier 语义。满时背压/有限失败，只有显式 best-effort 策略允许计数后丢弃。
  - **验收**：目标断开 10 分钟后恢复，核对已持久接纳范围的送达/重复/未完成；另测配额满、进程重启、部分写/损坏、未知成功与重放/GC。未通过不列 Supported，不因完成需求取舍就勾完实现。

响应等待是应用延迟，不等于真实网络 RTT。对比 eKuiper 时先核对版本、batch/linger/并发、输入输出语义及完整成本，不把不同配置或只成功送达的样本直接排名。

### 11.2 接入能力

- [ ] **CONN-01 — MQTT 主路径认证。** 对现有 Source/Sink 配置、连接状态、断连/重连、TLS、inbox 满、keepalive 和 EOF/停止保持组合回归；QoS/session 新能力逐项认证，不把 persistent session 冒充任意历史 seek。keepalive 试次必须长到覆盖原持续来包超时风险。
- [ ] **CONN-02 — HTTP Push。** 梳理接纳/背压/过载返回码、请求体上限、超时、取消与 Source 入队确认；区分 HTTP 接纳和 pipeline 输出成功。
- [ ] **CONN-03 — File replay 运维。** 在受限恢复上补轮转/截断/替换身份、AppendOnly/Immutable/Sealed 行为及恢复诊断；源历史不足或身份改变失败，不跳到最新。
- [ ] **CONN-04 — NATS Core（按需）。** 独立 kind，明确非持久/重放边界；不与 JetStream 共用模糊名称。JetStream 实施以 REL 为准。
- [ ] **CONN-05 — Kafka（按需）。** 现场已有标准可在 REL-00 选为首条来源，否则共享协议稳定后再做；partition、rebalance、offset commit、旧 consumer 隔离和 SDK 缓存必须 conformance。
- [ ] **CONN-06 — HTTP Poll/WebSocket/TCP（分别立项）。** 每一种先定义 framing、心跳、重连、背压、响应/消息上限和重放语义，不创建一个没有边界的万能网络 Source。
- [ ] **CONN-07 — Local DataBus。** 嵌入用户提供 frame/batch 时仍经过相同 schema/预算/控制准入；与 EMB-01 生命周期、内存所有权和关闭责任一致，不强迫宿主建立 MQTT 链接。
- [ ] **CONN-08 — 工业采集接入边界。** PLC/工业总线由独立采集组件通过受支持入口接入；在需要 native SDK 时评审 EXT-06，而不是把驱动全塞入核心。

### 11.3 格式

- [ ] **FMT-01 — JSON bounded codec 与类型提取。** 先 inventory 现有能力，再补深度/字段/单值/总字节限制、Schema 演进、Missing/NULL/错误；热字段直接类型化，避免重复 stringify/parse。
- [ ] **FMT-02 — Protobuf/CSV（需求驱动）。** 固定 descriptor/schema 版本、字段/行/字节限制、编码错误和兼容策略；实际 fixture 与 round-trip/拒绝测试通过后列 capability。
- [ ] **FMT-03 — Arrow IPC（可选）。** 用于受限分析/文件/IPC 交换；受预算验证 schema/长度/来源。不用它代替 checkpoint manifest，不把 C Data Interface 指针直接用于跨进程。
- [ ] **FMT-04 — File Sink 基础能力与可选 Parquet。** 不可变文件、临时写入/发布/失败清理协议、磁盘限额、重复输出与恢复；不默认引入完整数据湖体系。
  - 2026-09-17 排期调整：有界 File Sink 基础能力进入下方 Action 首批；Parquet 编码与其他文件格式仍按需，不要求与首批一起交付。

<a id="actions-functions"></a>
### 11.4 Action/Sink 与函数库扩展（2026-09-17 新增计划）

用户希望参考 eKuiper 丰富规则的输出动作和函数。eKuiper 将规则的 Action 定义为 Sink 实例，一条规则可配置多个动作；计算函数另有独立分类，不能将二者混成一种执行模型。参考其版本化官方文档：[Action/Sink 概念](https://ekuiper.org/docs/en/v2.3/concepts/sinks.html)、[Sink 类型与公共参数](https://ekuiper.org/docs/en/v2.3/guide/sinks/overview.html)、[函数分类](https://ekuiper.org/docs/en/v2.3/sqls/functions/overview.html)。这些仅用于能力对照，不表示 Sparrow 已实现、兼容 eKuiper 配置，或照搬其默认交付保证。

**排期：** 放在时间恢复/高级 IoT 闭环之后、容量验收与 K5 前端之前，作为一个完整的后端首批。先交付现有输出增强＋有界 File Sink＋高频纯函数；数据库/消息系统等更多 Sink、复杂函数与动态插件继续按需求分批，不要求清空整个扩展清单才能发行。不新增 K 阶段或另一套 Action 引擎，复用 CONN/HTTP/FMT、SQL/SEM/ANA、EXT 的既有任务。

- [ ] **能力盘点与对照清单。** 分列 Action/Sink、纯标量函数、聚合/分析/窗口函数和扩展函数，记录已实现/缺失/语义不同/暂缓、优先级与测试入口。当前 Server Sink 为 HTTP/MQTT/Log；当前已有 `abs`、`lower/upper`、`length/char_length`、`coalesce/nullif`、`greatest/least` 及 COUNT/SUM/AVG/MIN/MAX 等，不重复开发。核对真实 binder/evaluator，而非仅按名称判断兼容；例如当前大小写映射为 ASCII、参数采用 eager 求值，不能为对齐名称偷偷改旧规则语义。

- [ ] **Action 公共能力首批（CONN/HTTP/FMT）。** 在已有合批、linger、并发、flush 和 DAG 多输出之上，补输出字段映射/有界数据模板、空结果/单条与批量编码的明确行为、受控动态 MQTT topic/HTTP 参数、配置复用与逐动作诊断。动态目标展开后仍校验 allowlist、SecretRef 和编码大小；重试使用同一已确定 payload/身份，模板版本纳入适用的兼容校验。多动作复用 required/best-effort 合同，一处失败不意味着已成功的外部副作用能回滚。

- [ ] **File Sink 首批（FMT-04）。** 先实现明确格式的有界文件输出，例如 NDJSON；定义路径权限、轮转/容量、写入与发布/flush 确认、部分写入、停止、磁盘满和重放重复行为。普通文件写入成功不冒充 fsync 或可靠 receipt；没有完成对应 checkpoint 协议前拒绝 required aligned 组合。

- [ ] **更多 Action/Sink 候选（CONN/FMT，按需）。** Memory/Local DataBus（复用 CONN-07）、Redis、SQL 数据库（按所需 SQLite/PostgreSQL/MySQL 驱动选择）、InfluxDB、NATS/Kafka 等分别立项；这是 Sparrow 候选清单，不是声称这些都是 eKuiper 内置项。通知平台先用 HTTP/Webhook 或 MQTT 模板，不为每个平台复制一套网络栈。每项有生命周期、共享预算、连接池/并发、错误/重试、停止和 receipt conformance；Source 支持某协议不代表对应 Sink 已实现。数据库 insert/upsert/delete 的幂等、事务与恢复语义独立定义，不因此声称已具备通用 Changelog/Exactly-once。Memory/跨规则输出需有界慢消费者与生命周期合同；调试 Nop/Capture 不可冒充业务送达。

- [ ] **常用计算函数首批（SQL-01 / SEM-01/02）。** 按实际缺口补字符串拼接/截取/替换/匹配，数值舍入与类型转换，JSON 提取/构造，以及日期时间解析/格式化等高频能力。明确类型、Missing/NULL、字符计量、溢出/首错、时区和输出/工作量上限，使用独立 golden；SQL/Graph、融合/未融合共用语义。函数能力同步 capability/Explain/模板；改变已有行为时单列语义版本和恢复兼容性。

- [ ] **复杂/扩展函数后续（SQL / ANA / EXT）。** 数组/对象、编码/哈希按类型和资源边界补充；新增聚合、分析、窗口、多行/多列函数复用 ANA 的 state/timer/展开协议，不塞进无状态标量 evaluator。当前时间/随机等非确定性值不能在恢复时随意重算；外部函数的 I/O、并发、超时、结果身份与可重放性独立设计，未闭合则拒绝对应可靠组合。自定义纯 UDF、函数 Registry/版本固定、有界 Transform 和自定义 Sink 接入既有 EXT-01～06，不要求首批引入完整动态插件系统。

**整批验收：** 真实配置→bind/validate→执行→输出→取消/适用恢复；至少包含字段整形后 MQTT/HTTP 推送、告警 Webhook、文件落地等模板与精确预期，不能只补函数名/trait。覆盖超大展开/模板、无效动态目标、慢/失败 Sink、部分动作成功、资源退款和适用的稳定 ID/ACK/重放。新 Sink 默认不继承 HTTP 的 aligned 支持；测试、能力矩阵和文档必须同时交付。必要 API/CLI 随后端实现，K5 以后再展示这些能力。

<a id="performance"></a>
## 12. 性能线：先 P0，再决定 Arrow/JIT

优先级：P0 为有限实验；生产后端条件启动。负责人：待认领。不要新建六个空编译 crate，也不要在现场为每条规则生成 Rust 项目并运行 cargo。

### 12.1 开工门槛

E0 开始前在 §16 的任务卡填写：目标 workload/device、待解决热点、实验投入上限、最低净收益和 p99/RSS/启动/体积/构建开销的允许代价。未填写不启动长期 benchmark 或新增大型依赖。

首轮可用 x86_64 排除无收益候选；Profile-certified 必须实测目标 AArch64/实际 Compact 设备。JIT 支持平台根据锁定后端与构建验证，未经验证的平台不得默认列入。

### 12.2 P0：当前 prepared Row 与公平候选实验

- [ ] **P0-01 — prepared Row 语义基线。** 依赖 BASE-04/SEM；测量前 bind/prepare，直接复用 `CompiledTransform`，保留手工 golden。旧逐行 `eval()`/bind 实验不充当生产基线。
- [ ] **P0-02 — Row/Arrow/DF 实际实验 runner。** 固定输入与结果 materialization、NULL/错误、预算与测量窗口；真实构建并执行候选，消费并校验输出。分别测纯 compute、decode→compute→encode、真实 I/O；编译/冷启动与稳态分开。
- [ ] **P0-03 — 最小 KernelSpec/eligibility。** 在共享 TypedExpr/函数语义上定义支持的类型、上下文、输出/工作上界；不支持项明确拒绝或整体回 baseline。没有第二个稳定使用者前不先抽通用 kernel-api。
- [ ] **P0-04 — 热点与净收益画像。** 区分 decode/compute/state/调度/encode/I/O，记录真实 batch 分布与转换/host gather 成本；先排除 I/O 瓶颈。到投入上限给继续/停止/证据不足 ADR，不无限追加实验。
- [ ] **P0-05 — 共享来源/时间/控制 metadata。** 与 AGE-01、REL、DAG 共用身份和切点原则；P0 用有限线性 test metadata 即可。Filter selection、空批、Project 和展开 ordinal 不丢 source cut/watermark/Barrier。
- [ ] **P0-06 — 候选版本/features/开销清单。** 记录当前实验锁定基线，再单独选择兼容 Rust/Arrow/DF 组合；核对实际源码/API 与分配边界，保留锁文件、构建成本和体积。不能把滚动 latest 文档当本地已锁定依赖能力。

入口：`experiments/arrow-evaluation/`、`experiments/layout-rowbatch/`、`crates/sparrow-runtime/src/transform.rs`、`crates/sparrow-expr/`、`crates/sparrow-cli/src/bin/sparrow_bench.rs`。

P0 最低实验矩阵：

| 维度 | 覆盖 |
|---|---|
| Batch | 实际分布 + 1/8/32/128/512/1024；同时记录 byte cap 与聚批等待 |
| 输入 | 低速间隔、稳态、突发、过载、恢复重放 |
| 数据 | 窄数值、宽表、短/长字符串、NULL、多路径 Dynamic、明确支持的嵌套 |
| 表达式 | 阈值、算术链、多 Project、已支持的 CASE/函数、边界错误与跨行首错；不支持项是拒绝用例 |
| 图与范围 | 已支持线性链、独立多规则、慢 Sink；不把模拟循环当真实 fan-out 认证 |
| 度量 | rows/s、完整性、p50/p95/p99、阶段 CPU、受控分配/峰值 RSS、启动/体积/构建、转换占比 |

不存在“Arrow 必须快两倍”的通用指标。预先约定的完整路径净收益、语义与资源门槛不满足，就保留 Row；停止实验也是完成一次选型任务。

### 12.3 P1/P2：可选 Arrow 连续计算段

启动门槛：P0 的 Arrow 决策通过；BASE-03 对受控路径闭合、BASE-04/P0-05、对应 CFG 与故障验证到位。纯实验可记录未受控第三方分配，但生产硬预算声明不能忽略它。

- [ ] **P1-01 — Arrow 转换与 backing owner。** 确有生产实现时再引入可裁剪 `sparrow-arrow`；Row→列→Row 转换的输入、目标、selection、scratch 和输出共存峰值先准入；测试 NULL/slice/offset、共享、取消、逃逸、异常与释放。不支持嵌套明确拒绝。
- [ ] **P1-02 — Filter/Project/数值 Map 连续段。** 每个支持区域边界只转换一次，不每节点往返；同输出、同首错、同批次发布粒度，不变更现有 state/window/coordinator。第三方无法限制分配的 kernel 不进入硬受控 profile。
- [ ] **P1-03 — metadata/provenance。** 选择向量同步作用于时间和来源映射；全被过滤也推进消费 cut/control；Barrier/Stop/相关标点先 flush 切点前数据，不能把两侧数据并进一批。
- [ ] **P1-04 — DataFusion adapter 复验。** 仅在 P0 值得接入时建设；限制函数/语义子集，记录依赖、分配和维护成本，不泄漏为唯一 GraphSpec/state 表示。不支持 fallback 与运行语义错误严格区分。
- [ ] **P1-05 — 生产接入或否决 ADR。** 完整路径、低速、资源和目标设备通过才 opt-in 发布；明确 auto/force、已认证范围及回退。没有净收益或所有权不闭合则不发布，不能因已有代码而降低门槛。
- [ ] **P2-01 — 原生列批 codec/边界（条件）。** 仅当转换妨碍已证明收益时启动；按需引入内部闭集 payload 和直接列构建，仍共用 state/accumulator/control。改默认布局或淘汰重复热路径另经 ADR，不自动发生。

所有 Arrow 预算分清 reservation、真实 backing owner、逻辑 retention/queue；库的 size/pool 指标不是可失败准入或 RSS 上限。小 slice 长期 pin 大 backing 时按 state-owned 规则复制。

### 12.4 J0/J1/J2：有限纯计算 JIT

**JIT 不依赖先采用 Arrow。** J0 依赖 BASE-04、规范身份、P0-03/04 的热点和摊销证据；J1 依赖 J0；J2 依赖 J1 和对应平台验证。Arrow+JIT 必须另有匹配布局的实现，Row JIT 不自动满足它。

- [ ] **J0-01 — 有限 typed IR 与 Cranelift 实验。** 首批仅纯、确定、有界的 Bool/数值/Timestamp 比较、受检算术、NULL、常量及已在共享语义支持的 CASE；不允许用户循环、I/O、随机、任意分配或 state mutation。
  - [ ] 输入只能是验证过的 IR；节点/深度/指令数上限；禁止直接接收用户机器码/LLVM IR/Cranelift IR。
  - [ ] 机器类型、signedness、位宽、overflow/FP、求值次序显式 lowering；不猜 Rust Scalar enum 的内存布局，不启用改变语义的 fast-math。
  - [ ] 版本化 KernelFrame，验证 buffer 长度/type/layout/offset/validity、输出容量与错误槽；owner 覆盖整个调用。
  - [ ] 每 batch/有限 quantum 调用，块间响应取消；scratch 成功后才发布，不能半更新 state 或提前发 HTTP。
  - [ ] 字符串透传/selection gather 由宿主承担并计入成本；实测编译/安装、稳态、额外固定/逐行成本与规则更新频率的摊销。
- [ ] **J1-01 — 编译服务与 code owner。** 宿主级有限队列、并发、IR/workspace、代码页、缓存条目/bytes/世代和 Job 份额；覆盖旧/新代码与在途调用共存峰值。失败候选有有限负缓存/退避，timeout 不假装终止了阻塞编译。
  - [ ] Cache key 含规范表达式、schema/nullable/字段位置、函数/语义/ABI、layout、backend/compiler、target/CPU features 和优化策略；不只 hash SQL。
  - [ ] 旧 revision 编译结果不得安装到新 Job；淘汰等待调用结束，禁止未经审计的裸指针共享/unsafe Sync。
  - [ ] 按平台验证代码页写入/执行权限及 helper 白名单。首版内存缓存；磁盘代码缓存/AOT/编译进程隔离另评审来源与加载协议。
- [ ] **J1-02 — batch 边界安装与 shadow。** baseline 在编译期间继续运行；验证后边界切换，shadow 无真实输出/状态副作用。编译失败可保留 baseline，真实计算错误不靠静默 fallback 掩盖。
- [ ] **J2-01 — 按平台 opt-in 发布。** 依赖 CFG、目标设备/压力/故障测试；允许关闭，Compact 可不编译依赖。恢复重建 baseline/可信代码，不保存函数指针进 checkpoint；未经验证的 CPU/ABI 拒绝加载。

若需要硬终止编译或更强隔离，再设计有限编译 worker/产物加载；不能从独立进程返回函数指针。原生 JIT 不是用户程序沙箱，机器码错误不能靠 `catch_unwind` 保证恢复。

<a id="configuration"></a>
## 13. 配置、Explain 与后端有效能力

优先级：随 P1/J1 的首个生产接点交付，规划接口可先定义。负责人：待认领。以下均为未来能力，不是允许直接提交给当前 PipelineSpec 的配置。

- [ ] **CFG-01 — 宿主级能力与总额度。** 可信启动配置决定可用 backend/JIT、编译并发、队列、workspace、代码页/cache 及每 Job 份额；Pipeline 偏好不能扩大宿主上限。多 Job 组合验证总额度。
- [ ] **CFG-02 — Pipeline 执行偏好与版本化 DTO。** 区分布局 row/auto/arrow、JIT off/auto/require 和 batch/等待预算；保留旧字段读取、显式拒绝未知/冲突项。部署策略不能无声改变已承诺 delivery/recovery。
- [ ] **CFG-03 — 强制、自动与失败语义。** auto 只选有证据的支持段，force/require 不满足时在激活 ingress 前拒绝。按 prepare/段选布局，不逐批抖动；I/O/state 不被强制误解为必须 Arrow/JIT。
- [ ] **CFG-04 — Explain 与运行状态一致。** 每段列请求/选定 backend、拒绝原因、语义/ABI版本、输入输出布局、估算/实测成本与 workspace、queued/compiling/installed/cache 状态；未安装不能显示 JIT 已生效。业务状态身份与机器代码身份分开。

验收矩阵：row+off、auto 无候选/无预算、force 不支持函数/类型、require 平台禁止、编译失败、旧 revision 安装、宿主限额被 Pipeline 越权、未知配置、后端切换前后相同逻辑 state。

<a id="future"></a>
## 14. 后续产品切片：按需求启动，不一次全做

负责人：各任务开工前认领。以下所有任务均未因本 TODO 建立而获得排期或生产支持承诺。

### 14.1 V1.5：高级但有界的分析

共同前置：实际用例、SEM、MEM/state-owned 和相应时间/恢复合同；双输入能力需 DAG-03/04，声明 aligned 时另需 DAG-07/STATE。

- [ ] **ANA-01 — Session window。** 首版 final-only、L=0；gap、最大 session 时长、状态/合并/timer 上限明确，延长/合并/重启 golden 通过。迟到修改已发出结果不混入首版。
- [ ] **ANA-02 — Interval Join。** key 等值和显式前后时间范围，先 inner；双侧 watermark/idle/清理、输出膨胀与恢复有界。
- [ ] **ANA-03 — Window Join。** 同窗口关联，定义迟到、空窗口、输出 timestamp 和 duplicate；状态清理与恢复测试通过。
- [ ] **ANA-04 — Left interval join。** 在右侧时间进度证明不会再匹配后才发 unmatched；不能收到左行就提前输出 NULL，等待和状态有上限。
- [ ] **ANA-05 — UNNEST/flat-map。** 单事件展开数、输出 bytes 与 continuation/work quantum 有界；来源 identity+ordinal、局部顺序和 control 切点保持。
- [ ] **ANA-06 — 补充聚合。** first/last 的稳定顺序、方差/近似聚合精度、版本化 accumulator/merge 合同；浮点合并或整数 overflow 次序变化须显式语义决策。
- [ ] **ANA-07 — 有限分析查询。** 有界 Preview/历史文件查询使用独立 admission/生命周期，限制 CPU、内存、输出、并发和时间；不挤占长期 Job 到无限等待。

### 14.2 V1.6：WASM、SDK 与必要隔离

共同前置：明确扩展用户、SEM registry、受控资源与信任边界。先纯 scalar UDF，再受限 Transform，不先开放任意 stateful native 插件。

- [ ] **EXT-01 — UDF 包协议。** manifest、来源/签名策略、module hash、ABI/函数语义版本；拒绝超限/损坏/未知版本包。
- [ ] **EXT-02 — Function Registry 集成。** SQL/Graph 使用相同函数版本和类型/NULL/错误/确定性/资源声明；运行 Job 与 checkpoint pin 依赖，升级兼容可解释。
- [ ] **EXT-03 — 有界实例池。** 验证所选 WASM 引擎的 memory/fuel/时间限制、实例/并发/cache；默认无文件/网络/时钟/随机权限。失败、取消、超限、回收和目标设备开销有证据。
- [ ] **EXT-04 — Pure Transform。** 有版本的有限 buffer ABI、长度/错误码、已知或受协议约束的输出 schema、展开/bytes 上限；未成功前不发布输出或变更状态。
- [ ] **EXT-05 — Connector SDK。** prepare/open/read/write/close、capability、预算、取消、ACK/恢复 conformance；用可运行正负样例测试插件，而非只提供 trait 文档。
- [ ] **EXT-06 — IPC connector。** 仅 native SDK/崩溃隔离确需时实施；消息版本、frame/共享内存 owner、额度、取消/重启与故障协议完整。不得跨进程传 Rust trait object 或裸指针。

WASM 与 JIT 分别评审。已有 `experiments/wasm-spike/` 只作历史参考，不能直接标记 EXT 任务完成。

### 14.3 V1.7：单 Job 多核、大状态与条件性能发行

触发条件：观测证明单 Job CPU/状态是瓶颈；已有独立 Job 并行不重做，完整 Server 多规则认证不推迟到这里。

- [ ] **SCALE-01 — 单 Job 分片。** HashPartition→shard-owned state；固定 key 编码/hash/seed/shard 数/Operator 映射，同 key 保序、跨 key 不加全局排序承诺；分片资源与倾斜可观察。
- [ ] **SCALE-02 — 离线重分片。** 先停机/checkpoint 后离线转换，所有 shard/required Sink 与 source cut 对齐；分片数变化不得原地读取旧状态。在线 rescale 另立项。
- [ ] **SCALE-03 — 大状态 backend 选型与接入。** MemoryState 达到目标负载瓶颈后比较候选读写/缓存/后台任务/写放大/恢复；同步 `get_mut` 不能直接塞异步 I/O，先设计 continuation/事务边界，禁止锁跨慢 I/O。
- [ ] **SCALE-04 — 低峰值 checkpoint。** 根据画像选择增量/COW 等方案；同时限制复制峰值、保留世代、GC、磁盘、恢复链长度，验证损坏/断电/恢复，不预设比全量 freeze 更省内存。
- [ ] **SCALE-05 — 共享输入治理。** 有真实多规则重复 decode 成本后立项；cursor、生命周期、租约、慢消费者、来源身份与恢复隔离有界。不把共享输入误当已经实现 DAG，也不隐藏无限 backlog。
- [ ] **SCALE-06 — 按 profile 裁剪发行。** 只有对应 P1/J2 与 QA 通过后，才在特定 Performance 发行开放可选后端；Compact 可关闭依赖，共享同一语义/state/control 回归。

### 14.4 V1.8：多设备运维，不改单机数据面

触发条件：确有设备规模、升级和离线管理需求；与 Runtime 计算/状态协议分层。

- [ ] **FLEET-01 — 设备身份与凭据。** 注册/撤销/轮换、设备权限与最小诊断范围；失联不泄漏或重复下发秘密。
- [ ] **FLEET-02 — 配置包与 desired revision。** 签名/来源、依赖/兼容校验、冲突、幂等收敛和离线缓存；云端不可用时当前本地流水线继续运行。
- [ ] **FLEET-03 — 签名升级/灰度/回退。** 同时检查 binary/config/state 兼容，保留可操作的旧集合；失败不静默删状态，禁止导入不可信 native cache。
- [ ] **FLEET-04 — 诊断汇总。** 上传有总容量/速率/保留限制、脱敏、断网重试与现场授权；远端管理不可成为数据面运行的硬依赖。

### 14.5 V2.x：每个新边界独立立项

- [ ] **V2-01 — Changelog/EMIT CHANGES。** 业务确需早出/更正时，定义 insert/update/delete、稳定主键、旧值和 retract-capable 算子/Sink；完整链路验证后开放。
- [ ] **V2-02 — Session late merge。** L=0 final-only 不足时再做；依赖 V2-01 或等价更正协议，定义已输出 session 合并后的撤回/替换与恢复。
- [ ] **V2-03 — Transactional Sink。** 仅针对真实下游可验证事务协议实现 prepare/commit/abort/recover，与 checkpoint 故障矩阵协同；普通 HTTP 幂等键不替代事务证明。
- [ ] **V2-04 — 单机多进程 worker。** 有隔离/独立资源域需求再做 IPC、监督、owner 和节点级失败；进程重启不自动补齐重放与副作用协议。
- [ ] **V2-05 — HA/remote workers。** 有跨机执行/容灾需求才设计 fencing、leader/ownership、分区、跨节点 state/scheduling；两个 Server 共读目录或互相拉起不算 HA。

<a id="verification"></a>
## 15. 统一测试、性能证据与发行门槛

每项 QA 作为可重复机制建设，不代表测试只需运行一次；具体负载数字与允许损失/延迟在开工任务卡和 profile 中冻结。

- [ ] **QA-01 — 单元、独立 golden、property 与 codec 防御。** 覆盖类型/NULL/overflow/首错、预算边界、截断/损坏/过大输入、未知版本、取消与资源归还；新功能增加针对性反例，保留已修复回归。
- [ ] **QA-02 — 实际 Server 组合矩阵。** 从 API/CLI 到 Source→计算/state→Sink→停止/恢复，不只测 helper。包含支持/拒绝组合、1/多 Job、最大声明并发、慢/失败 Job 对其他 Job 的影响。
- [ ] **QA-03 — 公平性能与观测开销。** 相同输入/语义/输出成本/预算/预热/统计窗口；记录前后版本与 raw artifacts。未完成和丢弃样本保留；不只以成功样本 p99 或发布条数宣称吞吐。
- [ ] **QA-04 — 故障与恢复矩阵。** 网络半包/断连/超时、慢输出、磁盘满、损坏 snapshot、取消、配置冲突、丢失/旧 ACK、旧状态不兼容；验证有限失败、CURRENT 保留和重放行为。
- [ ] **QA-05 — 目标硬件与长稳。** 明确 CPU/RAM、payload/key/state、规则数、RTT/TLS、速率/突发与允许损失；发布前安排至少一次设计建议的 72 小时组合 soak，并周期性更长现场验证。未经用户确认不自动启动。
- [ ] **QA-06 — 安全与运维发行检查。** 持久 secrets key、强鉴权、目标/数据 allowlist、必要 TLS、权限/脱敏/审计、磁盘告警、目录独占、升级回退；检查 demo 不可用，未知指标不冒充可观测。
  - 阶段 2 显式回归旧评审指出的 `/start` 幂等、并发 start/stop/update、`consecutive_failures` / `restart_blocked` 与 held 解锁、日志过滤前缀边界；先查当前行为，不把旧报告中的缺陷直接认定为仍存在。
  - 发布 runbook 覆盖容量准入/`max_jobs`、held/失败诊断、持久密钥、窗口输出列名、周期 checkpoint、文件轮转和升级回退；API/CLI 操作对应实际权限与退出结果。
- [ ] **QA-07 — 确定性 reference executor 与时间/控制性质测试（新增执行项）。** 阶段 2 建立有限输入、虚拟时钟和独立规格模型，复用已有 testkit 但不把生产 evaluator 调两次当独立 oracle。先覆盖当前支持的线性/单窗口，再随 DAG/新节点扩展。
  - 随机生成 data/watermark/idle/EOF/barrier/stop 及失败序列，保留 seed 和缩小后的反例；验证 watermark 不回退且不越过数据、无回滚执行中窗口不重复 final、状态不重复应用、未提交 checkpoint 不可恢复、取消后 owner/credit 归还。涉及恢复时按已提交切点比较模型；合同允许的重放/HTTP 重复不能与同一 attempt 的重复状态更新混为一谈。
  - SQL/Graph、融合/未融合与模型对照结果、首错、时间信号及发布粒度；复杂并发使用受控调度/模型测试，wall-clock sleep 不作为正确性依据。与 SEM-01、MEM-04 分别验证语义和资源，不用模型吞吐代替生产性能。
- [ ] **QA-08 — 72 小时组合长稳与 10,000 次生命周期循环（新增执行项）。** 阶段 3，先冻结目标 profile、事件/key/状态/速率及阈值，再确认设备和时段。真实 MQTT/HTTP 与适用 File 恢复 Job 同时覆盖断连、慢输出、定时更新、OPS-04 周期 checkpoint 和已启用日志/DLQ 滚动。
  - 记录至少 10,000 次启停循环；可在同一长稳中执行或使用关联的独立生命周期试次，不把 10,000 条输入当成 10,000 次启停。采集 task/fd/连接、RSS、账本、磁盘/写率及停止后的基线趋势，计数不能仅靠最终退出码。
  - DLQ/新协议尚未实现的子项明确不适用，不能伪造试次；漏掉已声明路径、失败轮次或长稳不足不能勾完。重用历史证据需满足 §15.3 和 §17.6 的源码/配置适用性要求。
- [ ] **QA-09 — 四类失败模型与存储恢复认证（新增执行项）。** 阶段 3 为声明的恢复 profile 分开记录进程崩溃、OS 崩溃、突然断电、损坏介质；写入 chunk、同步、manifest/CURRENT 发布、输出成功和 ACK 前后各切点验证一致恢复或明确拒绝。
  - `kill -9` 不能替代断电，损坏测试文件不能冒充目标介质故障认证；模拟注入与真实设备证据分别标识，记录 filesystem/mount、介质缓存/同步假设、恢复点与输入保留条件。
  - 磁盘满、校验损坏、部分写入不得偷偷空状态继续或覆盖最后有效恢复点。真实断电/介质测试只在另行确认的专用设备/数据上进行，不对现有服务器直接执行。
  - 某失败模型没有条件验证时标 NOT RUN 或 BLOCKED，并在本版支持矩阵中缩限承诺或暂缓该 profile，**不得写成通过完整蓝图 V1 存储门禁**；已承诺范围不能仅靠豁免文字放行。
- [ ] **QA-10 — G0～G6 门禁执行台账（新增执行项）。** 阶段 0 建表，阶段 2 整理已有证据，阶段 3 完成本次适用门禁；逐项维护 §18.5 的 commit/build、fixture/命令、平台/负载、原始产物、判定和缺口。
  - G1a 历史选型不是当前 prepared Row/Arrow/DF 全路径胜负证明；G1b 不能用 loopback 或微基准代替目标环境；G6 保留 1/10/100/1000 负载阶梯及准入拒绝，不把成功运行 1000 Job 当每台设备的必需容量。
  - 超出声明上限的阶梯可按预先约定验证有界拒绝，不通过调大预算把硬件压垮；不支持的共享入口单列，不冒充已测共享 fan-out。G5 不进入发行时为带理由的 NOT APPLICABLE，不能填 PASS。

### 15.1 按变化选择验证层级

| 变化 | 最少需要的验证 | 不必机械重做 |
|---|---|---|
| 仅文档/任务状态 | 链接、ID、依赖、勾选状态、证据指纹和 diff 检查 | 编译、性能 sweep、长稳 |
| 单模块代码 | 受影响测试 + 对应反例 + 静态检查 | 每改一行重编译全 workspace |
| owner/checkpoint/control | 模块测试、负向 codec/资源/取消、实际恢复链路 | 与改动无关且证据完整的全部旧跑分 |
| Connector/队列/调度 | 真实输入输出、慢/失败/过载/停止、多 Job 与相应性能对照 | 将全部参数笛卡尔积不加选择地重跑 |
| 新后端/函数语义 | 独立 golden、完整路径差分、预算/生命周期与目标 profile | 用纯计算微基准替代生产验收 |
| 发布或声明新 profile | 独立生产构建、完整能力/故障矩阵、设备/长稳/回退验证 | 仅凭旧总测试数直接放行 |

### 15.2 少编译的具体做法

1. 每批先一次性读清路径、补完反例并检查 diff；静态确定问题优先静态处理。
2. 在服务器隔离源码目录使用既有 Cargo cache；不修改其他任务/服务的 checkout、broker 或业务数据。
3. 默认与无 demo 等必要 feature 组合分开验证，防止 feature unification 掩盖生产条件。
4. 测试二进制构建后，重复/随机种子/故障轮次直接运行该二进制；源码变化后不可把旧二进制当新版本验证。
5. 日志定向到证据目录，只汇总决定性结果。失败与成功版本分开存，记录每次源码/构建对应关系。
6. 对 soak、真实网络故障注入、服务器部署先确认时间窗口、作用范围与清理规则。

### 15.3 证据最小清单

每次需要发布结论的验证记录：源码 commit/patch + 文件 hash、工具链、target/features、二进制 hash、有效配置/负载、脚本版本与 seed、原始 stdout/stderr、观测窗口、接纳/终结/在途口径、资源峰值、失败样本、结论与限制、清理/回退结果。测试退出成功不自动证明 workload 达标，负向试次按预期失败可有效通过。

<a id="decisions"></a>
## 16. ADR、关键依赖与任务登记

### 16.1 需形成或落实的 ADR

编号沿用 Final 的 P-001～P-015；以下是决策主题，不表示对应 ADR 文件已经创建，也不与 `docs/adr/002-narrow-binder.md`、`003-layout-decision.md` 的历史编号混淆。

| 决策 ID | 主题 | 触发/最晚完成点 |
|---|---|---|
| P-001 | 产品与性能双线、投入上限 | B1/E0 开工登记 |
| P-002 | Arrow 连续段、继续/停止/统一布局 | P0 结束，P1/P2 前分别确认 |
| P-003 | DataFusion 接入深度 | P0-02/06 证据后，P1-04 或 SQL 维护成本变化时 |
| P-004 | 纯表达式 JIT、平台和摊销 | J0 前定门槛，J0 后决定是否 J1 |
| P-005 | 可靠来源与部署路线 | REL-00，新增 broker/持久日志前 |
| P-006 | required/best-effort 分支保证 | DAG a 前 |
| P-007 | 首批 IoT/state/TTL/异常值 | IOT-01～03 实现前 |
| P-008 | WASM/native/IPC 信任边界 | EXT 立项前 |
| P-009 | 分片与大状态 | SCALE 画像通过后 |
| P-010 | 多设备控制/数据面分离 | FLEET 立项前 |
| P-011 | 新鲜度与过载的阶段/终结事实 | AGE-02/03 前 |
| P-012 | 全计划恢复、参与者与空状态 | STATE-01/02 前；记录当前单窗口修复 |
| P-013 | 规范语义、来源/业务身份/代码身份 | 基于 SS02 补记录；STATE-04/REL-07/新函数或 ABI 前 |
| P-014 | eager、首错、发布粒度与候选等价 | SEM/P0-01，任何语义修改前 |
| P-015 | backing owner、编译资源与世代峰值 | MEM 设计、P1-01/J1-01/EXT-03 前 |

### 16.2 关键依赖，避免排成大瀑布

| 待做能力 | 必须先满足 | 不需要等待 |
|---|---|---|
| OBS 基础 | 指标/时间/生命周期口径、统计自身有界 | 全量 Scalar arena、完整 UI、Arrow/JIT |
| K1 线性恢复底座 | 当前候选基线、有限参与者/总预算/codec/语义/身份、File cut 与 required 输出合同 | NATS/Kafka 决策、独立 outbox、多输入 DAG、完整工作台 |
| 自动过期/合并 | AGE-01、观测、业务许可、状态/恢复语义 | 全部 Connector 或完整告警平台 |
| 首批值过滤 IoT | 所用类型的 MEM-02、key/TTL/异常/reset 合同 | JetStream、完整 DAG/工作台、多状态恢复 |
| 可恢复告警状态 | STATE 参与者/codec、稳定身份、所用 clock 恢复 | 全部高级分析节点 |
| 可靠输入 | 来源部署决策、参与者/ACK/保留/独占/去重/输出合同 | 完整工作台；多输入 DAG 不是单源首版前置 |
| K2b 可靠输出/独立 outbox | 稳定输出身份、required 确认层级；选择落盘时补同步/配额/GC/重启重发与 ACK 联合切点 | UI；不指定必须新建 broker，不把稳定 ID 等同下游去重 |
| Arrow 实验 | prepared Row、独立语义、有限投入与候选成本 | 生产 Arrow crate、完整 DAG/REL |
| 生产 Arrow | 对应 P0 通过、路径所有权/预算与语义闭合、CFG/QA | JIT |
| JIT 实验/生产 | 共享语义与 typed IR、热点/摊销；生产另需生命周期/资源/平台 | 先采用 Arrow |
| 多输入 aligned | DAG b + STATE + 相应可重放 Source/Sink 合同 | 指定必须使用 JetStream |
| 多核/大状态 | 真实瓶颈、分片/存储边界与设备证据 | 把所有 V1.x 产品功能做完 |

### 16.3 开工任务卡

每个实际开工项在 issue/PR 或本节登记，未填写负责人、范围、门槛和投入上限的远期项不进入执行队列：

```text
任务 ID / 标题：
状态：待开始 / 进行中 / 部分完成 / 待验证 / 完成 / 暂缓
负责人 / 验收人：
用户与目标 workload / 硬件 / feature：
优先级 / 依赖 / 是否需要外部条件：
本批交付物 / 明确不做：
涉及的现有入口与拟新增模块：
输入输出、时间、错误、资源及恢复合同：
验收测试 / 性能或资源阈值：
投入上限 / 超限后的停止或重新决策条件：
API / 配置 / state / 语义兼容变化：
commit / build / raw evidence：
部署 / 迁移 / 回退 / 清理方案：
下一动作 / 阻塞条件：
```

**当前执行队列（2026-09-17）：** K1/R11 与 K2/R12 的选定 Preview 已提交；K3 DAG-01～07 和 K4 首批 IoT 已完成限定矩阵，K4 后的全局 Review 修复与跨阶段回归已通过，见 [IOT.md](IOT.md#k4-validation)。等待用户 review/提交决定，不另开 R13，不自动转入 K5 平台。独立 outbox/DLQ 仍按需求选择，K2 生产门禁与 OBS-03、完整 AGE-01、OPS-04、QA-08 未因此关闭；不自动 commit/push/tag/发行。

### 16.4 后续维护步骤

1. 按 §3.4 认领一个完整核心批次，填写任务卡、内部切片与投入上限；原任务编号不重排，不以最小 helper 代替整批交付。
2. 小提交贯通一条可验证路径；发现额外问题单独记录范围，不无限扩张当前批。
3. 验证通过后更新勾选、状态、受测 commit/patch 与证据；父任务仅在全部声明范围关闭后完成。
4. 每个里程碑重新审核能力矩阵和已知限制，不把计划版本号当成熟度证明。
5. 条件不满足的任务标暂缓并保留原因；允许否决无收益后端，不把“全部 TODO 最终都实现”当项目成功标准。

<a id="release-sequence"></a>
## 17. 实际执行顺序、阶段效果与版本发布

### 17.1 怎么读这份 TODO

**§4～15 是按能力分类的任务库，不是从上往下逐条清零的施工顺序。本节才是默认执行顺序。** 原任务 ID 和勾选含义不变；某阶段完成一个任务的限定子集，不等于可以把整个父任务勾完。

当前采用“现有范围发行”与“核心增量开发”两条受控队列；同一时间只推进一个核心开发批次。**保留原阶段 0～8 作为设计引用编号，不再按 4→5→6 的数字顺序施工。** 核心优先是用户的新排期决定，不改变原任务的语义和验收责任。

```text
现有候选：原阶段 0/1/2 的已交付范围
  → K0 用户 review / 修复 / 固定基线
  → 原阶段 3：RC、目标网络/设备/长稳、升级回退 → 按实测范围发行

核心增量：在记录过的独立基线上推进，不污染正在验收的候选
  → K1 通用 checkpoint / 零状态与选定线性多状态恢复（原阶段 6 的 STATE）
  → K2a 可靠输入 + K2b 可靠输出合同及选定 outbox（原阶段 6 / HTTP-07）
  → K3 真正 DAG：单源分支 → 多源合流 → 多输入恢复（原阶段 7a/b/c）
  → K4 IoT 状态规则与精确业务模板（原阶段 4b）
  → K5 运维工作台 / 可视化编排 / 体验扩展（原阶段 4a、DAG-08、5）

每个可发布核心增量 → 重新绑定源码/支持矩阵 → 适用发行门禁 → 下一功能版本
其余 AGE/高级分析/扩展/规模能力按需求；P0/Arrow/JIT 按证据，不抢占核心主线
```

- **阶段 0～3 仍是现有受限范围的发行主线；新增核心不被说成已完成，也不要求全部做完才能发行现有范围。** RC 必须基于干净、可定位的提交；新核心实现可与旧候选验收隔离推进，但性能/长稳证据不得跨源码拼接。
- **默认核心顺序为 K1 → K2 → K3 → K4，平台 K5 后置。** K1/K2 的选定 Preview 已提交，K3/K4 已完成限定矩阵且待用户 review/提交；后续批次另行确认。可靠路线的部署决定尚未具备时，只推前置已满足的其他核心切片，不擅自新增 broker。DAG a/b 不必等待可靠 broker，IoT 值过滤不必等待完整 DAG；调整顺序须登记原因，不虚构技术依赖。
- MEM、SEM、CFG、QA、Connector/格式任务随所用路径进入各阶段，不单独排成“先全量重写，再交付功能”。已知影响本次支持范围的正确性缺陷必须关闭，不能用缩减文案掩盖仍启用路径中的缺陷。
- P0/Arrow/JIT 是有投入上限的证据线，不是主线必经站。可另行安排实验；收益不成立就停止，不能为了技术选型推迟已满足门槛的产品发行。

### 17.2 阶段 0～3：先把当前能力做成可以交付的版本

| 顺序 | 做什么、对应任务 | 做完后的实际效果 | 阶段完成门槛 | 版本动作 |
|---|---|---|---|---|
| 0：固定基线与范围 | PUB-01，盘点 PUB-05/06、QA-10；核对 BASE/OBS 已有证据、兼容变化和待提交范围 | 后续测试能回答“测的是哪个源码、承诺哪些组合”；不再混用旧 HEAD、补丁和新工作区 | 明确目标平台、构建 features、Source/Sink、图/state/time/recovery、负载与 SLO；设计取舍与门禁有登记，受测源码可追溯 | 暂不打正式 tag；实际提交按授权执行 |
| 1：观测闭环 | 先 OBS-01；再补 OBS-02 的 Source/Sink 范围与 AGE-01，随后 OBS-03 | 不看源码也能从 API 区分静默、断连、积压、慢输出和失败；延迟知道从哪里量到哪里 | 声明范围的真实 Server 链路贯通；unknown 不冒充零，统计有界且不改变消息处置；正常/慢/断连/取消与开销回归通过 | 通常不单独发稳定版；必要时只交付明确标为开发预览的构建 |
| 2：生产化与最小运维闭环 | §3.3 九个工作包：PUB-02～06；MEM/SEM/CFG 适用子集；QA-01～04/06/07/10、OPS-04；前移 CLI-01/OPS-02 最小子集；盘点 WIN-01/HTTP-07；HTTP-05 目标网络子集 | 别人拿到包可以安装、配置、启停、诊断和自动保存恢复点；支持/拒绝/迁移边界有正式说明 | 固定生产构建/CI；独立模型与资源/故障/恢复反例；适用周期 checkpoint；CLI/有限诊断、安全/runbook；本版设计冲突与剩余验收缺口有明确处置 | 准备集中 review 与 RC 源码/发行包；不是测试数足够大就直接打正式 tag |
| 3：候选版与发行验收 | QA-02～06/08～10、PUB-04、OPS-04；冻结候选上执行多 Job、真实 RTT/TLS、故障/恢复、升级回退、72 小时 soak 与 10,000 次启停 | **得到一个在明确平台、配置和负载范围内可验收交付的版本**；不是宣称任意设备、任意速率都稳定无损 | 损失/重复/延迟/资源趋势达标；24 小时自动恢复验收及适用的四类失败模型分开有证据；无阻断问题，产物对应最终源码 | 建议先 `v0.1.0-rc.1`，修复后递增 RC；放行后 `v0.1.0`，具体分配先核查既有发行，见 §17.5 |

阶段 1 的最小闭环按冻结的支持范围验收，不要求把所有 Connector 的每个内部阶段都观测出来；无法观测的部分必须明确标识。AGE-01 来源/时钟合同先于对应延迟实现，不能把 Runtime enqueue 年龄、HTTP 响应耗时和业务完成年龄互相替代。

阶段 2 的 WIN-01/HTTP-07 是**盘点与取舍**，不把可选新窗口或持久 outbox 偷加成首版前置。阶段 3 的受限首发 profile 与“通过蓝图完整 V1 门禁”是两种结论；缺少目标设备、OS 崩溃、断电或介质证据时按 QA-09/10 明确范围，不能用一个 72 小时计时器代替其余门禁。

2026-09-14 的批次补充将最小运维 CLI/诊断包前移到阶段 2，详见 §3.3；这不是把阶段 4a 全部前移，完整 UI、多角色体系、发布历史等仍按后续切片推进。已经前移并验收的子项在阶段 4a 复用，不重复开发。

**首版必须守住、但不要求扩展的边界：** 现有 MQTT best-effort 不升级成无损承诺；只发布通过验证的恢复组合；未闭合 raw API/第三方分配不承诺全进程 RSS 硬上限。若先只验证 Linux x86_64，就只发布该平台的支持结论，其他设备保持未认证。72 小时通过是本项目建议的发布门槛之一，不是普遍稳定性的数学证明。

**首版不等：** 完整 Dashboard/Graph Designer、Prometheus/OTel 全套、自动过期或合并、全部 IoT、JetStream、完整 DAG、Arrow/JIT、WASM、分片/集群。最小安全配置、API 可诊断性和本次支持路径中的正确性问题不能后置。

### 17.3 后续功能：核心优先，保留原设计阶段编号

下表的“下一 minor”表示在届时实际版本上推进一个功能版本，不提前预留所有 tag；可独立验收的小切片可以分别发布，不能为了凑一个大版本捆绑。

按 2026-09-14 用户最新决定，恢复和可靠链路优先于 IoT，所有这些核心优先于平台。下表按当前执行优先级排列；原 V1.x/阶段号是设计标签，不强制成为真实 tag 或排队顺序。详细整批边界见 §3.4。

| 当前顺序 / 原阶段 | 内部路线图阶段 / 对应任务 | 做完后的实际效果 | 必要前置与退出条件 | 建议怎么发 |
|---|---|---|---|---|
| **K1 / 6 的 STATE：线性恢复底座** | STATE-01～05、REL-14 的 File 子集；MEM/SEM/CFG/QA | 零/单/选定多状态的 File 计划真正保存并恢复，不再要求虚拟或唯一 Window | 当前基线；有限参与者、总预算、codec/语义/身份；§3.4.2 的真实进程与故障矩阵，不等 broker 决策 | 可独立下一 minor；若先发行 0.1.0 且版本未占用，可考虑 `v0.2.0`；先 RC 与兼容说明 |
| K2a / 6 的 REL：可靠实时入口 | REL-00～06/08～14；K1 的参与者基础 | 对选定来源和计划解释重放、ACK、保留与去重；不是仅连接 broker | 只选一条部署路线；与 K2b 对齐 required 输出；独占、毒消息、预算和崩溃切点齐全 | 完整路线通过后下一 minor；未闭合仅 Preview，不泛称 exactly-once |
| K2b / HTTP-07：可靠输出 | REL-07/08、STATE-04、HTTP-06/07 | 稳定 ID/确认/重放边界；选用持久 outbox 后具备其 durable receipt 范围内的重启续送 | 先确认下游断网自治需求；写入/同步/checkpoint/来源 ACK/发送/删除的联合故障矩阵；不保证接纳前不丢或下游必然去重 | 与 K2a 同一 minor 或完整子集独立下一 minor；不选 outbox 则明确限制，不能假报其实现完成 |
| K3a / 7a：单源分支 | V1.3a；DAG-01/02/05/06 | 一条输入可 Route/Branch 到多个输出，慢支路及 required/best-effort 行为可解释 | 图预算、取消、失败、输出增长有界；未验收的 aligned 图明确拒绝 | 单独下一 minor；不等 7b/7c 或 Designer |
| K3b / 7b：多源合流 | V1.3b；DAG-03/04 | 多个来源可以 UnionAll；watermark、idle、EOF 按输入身份正确推进 | 7a 的图基础及多输入时间/顺序合同；不冒充多输入可恢复 | 单独下一 minor；不等待可视化页面 |
| K3c / 7c：多输入恢复 | V1.3c；DAG-07、STATE 与适用 REL 合同 | 多 required 输入、状态和输出在一致切点恢复 | 7b + 全参与者协议 + 所选可重放来源/输出合同；崩溃切点矩阵通过；不强制依赖 JetStream | 单独下一 minor，恢复能力逐组合开放 |
| K4 / 4b：IoT 状态规则 | IOT-01/02、TPL-01/02、所用 STATE/MEM/SEM | 变化检测与 Deadband 各有完整模板，实际减少无效上传或通知；IOT-03 另作下一增量 | key/TTL/异常/reset 和预算明确；声明恢复则接入参与者 codec 并验证，未支持组合拒绝 | 独立下一 minor；不等完整 UI，不虚构告警连续性 |
| K5 / 4a、DAG-08：后置工作台 | 复用已验收 CLI/诊断；补 UI、OPS 剩余体验与 Designer | 运维控制台和实际受支持图的可视化编排，不另造执行引擎 | 复用稳定 API、真实观测与 DAG 能力；必要权限/审计随操作交付 | 轮到该切片时再分配下一 minor，不预留 0.2.0，不阻塞核心发行 |
| 后置 / 5：现场体验与按需策略 | V1.1b、AGE-02～04；Preview、导入导出、发布历史等；EMB/SQL/WIN 按核心实际需求前移 | 按现场需求提升操作或增加一个明确的业务时效策略；HTTP-07 已独立前移 K2b | 一次一个闭环；Preview 无外部副作用，时效/新窗口不绕过资源与恢复合同 | 完整增量可下一 minor；没有需求则暂缓 |
| 8a：高级 IoT 与参考表 | V1.4；IOT-03～09、TAB | 持续条件、静默/离线、告警生命周期和参考表运营逐项可用 | 先有基础 state/clock/预算；恢复 active 告警需相应 codec/身份，未要求恢复的切片可早于完整 REL/DAG | 每个场景闭环可独立下一 minor，不一次承诺全部节点 |
| 8b：按瓶颈或客户需求立项 | V1.5～V1.8；ANA、EXT、SCALE、FLEET；所需 CONN/FMT | 分别解决复杂有界分析、安全扩展、单 Job 多核/大状态、多设备运维或具体接入需求 | 各自 ADR、资源/安全/兼容合同和目标设备证据；这些方向之间没有通用的串行依赖 | 一次一个主切片，验收后下一 minor；不能只因为路线图写了版本就开工 |
| 8c：新的业务边界 | V2.x 候选；V2-01～05 | 按需获得更正/changelog、事务输出或专门部署能力 | 明确用户需求，单独设计迁移/失败模型；不默认演变成集群系统 | 实际 public contract 不兼容才按 major 规则处理，内部“V2.x”标签不自动等于发布 `v2.0.0` |

**性能线插入点：** 另行认领性能实验后，可在阶段 0～2 期间建立公平 prepared Row 基线、独立语义与热点画像，再做 P0 有限对照；没有额外投入就后置，**不阻塞阶段 3 发版**。Arrow 的 P1/P2、JIT 的 J0/J1/J2 分别满足自己的门槛后，可插入任一合适的功能发行，以显式可选的 Preview 起步。生产路径仍须完成生命周期、预算、故障与目标设备验收；JIT 不依赖先采用 Arrow，也不能跳过自身实验门槛。

### 17.4 各种“版本号”不要混用

| 标识 | 表达什么 | 本项目应怎样使用 |
|---|---|---|
| 路线图 V1.0.x、V1.1a/b、V1.3a/b/c 等 | 产品能力分组与推进里程碑 | 保留原文方便追踪，**不作为已经发布的证据，也不直接拿来当 Git tag** |
| Cargo package / Git release tag | 实际源码与发行版本 | 当前 workspace 为 `0.1.0`；建议本批相关 workspace crates 统一版本，tag 用 `v` 前缀，例如 package `0.1.0-rc.1` 对应 tag `v0.1.0-rc.1` |
| REST `/v1`、配置/Graph DTO 版本 | 各自接口与数据协议 | 独立维护兼容策略；发新的二进制版本不自动改 API 路径或 DTO 版本 |
| Snapshot / manifest / 语义描述 | 持久状态能否读取、恢复及语义是否匹配 | 当前 `SPV1` family 的 snapshot codec 为 **2**，`MAN2` manifest 为 **1**，规范语义描述使用 `SS02`；它们都不是软件 release 版本 |
| Experiment / Preview / Supported / Profile-certified | 单项功能成熟度与设备负载认证范围 | 随发行附 capability 矩阵；正式版本可以包含显式可选 Preview，但不得把它列为默认生产支持 |

### 17.5 实际版本策略：先按 0.x 推进，1.0 另设兼容承诺门槛

**建议采用以下规则，而不是立即把所有路线图标题变成发布版本。** 当前 `Cargo.toml` 是 `0.1.0`，本地没有列出 tag；这不证明远端从未发行。实际分配第一个 tag 前先核查远端 tags/releases 和已有使用者，若已有版本占用或兼容承诺，沿用并递增，不能重用版本覆盖旧包。

1. **当前阶段的建议编号：** `v0.1.0-rc.1 → v0.1.0-rc.2 … → v0.1.0`。这是待核查后执行的建议，不是本次已经改 Cargo、打 tag 或发布。无预发布后缀表示本次发行已放行，不表示 0.x 的全部 API 已作长期稳定承诺，也不表示所有场景生产认证。
2. **0.x 期间的项目约定：** `0.y.z → 0.y.(z+1)` 仅用于保持已声明 public contract 的修复/文档/发行维护；新功能或有意的不兼容变更进入 `0.(y+1).0`，不兼容处必须单列迁移、拒绝行为和回退条件。不要以“仍是 0.x”为理由随意破坏 patch 兼容性。
3. **正式进入 `v1.0.0`：** 阶段 0～3 门槛已通过，并明确承诺维护的 API/CLI、配置、函数语义、资源/交付保证及 state 兼容政策，有实际升级/回退证据和维护责任。这个决策可以在首版验收后或某个 0.x 功能版后作出，**不必等阶段 4～8 全部做完**；不能仅因通过一次 benchmark 就升级 1.0。
4. **1.0 之后的项目约定：** 保持兼容的修复发 patch（如 `v1.0.1`）；保持兼容的新能力发 minor（如 `v1.1.0`）；破坏已承诺 public contract 发 major（如 `v2.0.0`）。minor/major 发布前均可用 `-rc.N` 验收；新功能不应混入仅承诺修复的 patch。
5. **状态/API 迁移独立审核：** 当前 `into_rows()` 返回类型变化、V1 checkpoint 不再可恢复，若相对一个已公开发行版构成破坏，就不能包装成无迁移的 patch。软件版本升高也不能让旧 codec 自动获得恢复资格；保留旧二进制/数据，按 §2.3 决定迁移、显式 reset/replay 或暂缓升级。
6. **开发提交不必逐个发版：** 任务完成先进入可追溯提交和回归；一个小而完整、通过验收的交付切片再出 release。已发布分支若需要紧急修复，单独做受影响回归和 patch，不等待主线下一大功能。

### 17.6 一次真正发版的操作顺序

这是后续发布流程，不是本次执行授权；真正的 commit/tag/push、部署及长稳窗口仍按用户确认执行。

1. **选范围和编号：** 核查远端发行历史，冻结本版支持矩阵、已知限制、迁移说明及验收 profile；不支持项必须拒绝或标为不可用。决定是修复版、功能版还是兼容承诺变化。
2. **固定源码：** 审核提交范围，只纳入代码、README、正式 docs 和必要发行资产，不夹带根目录过程/评审 MD；记录干净 commit 与已有受测源码的差异。
3. **统一版本与构建输入：** 同步 workspace package、相关 workspace path dependency 版本及受影响 lockfile；检查 CLI/API 实际报告的版本。实验 crate 按其版本策略处理，不机械发布实验。固定工具链、target 和生产 features，独立无 demo、locked 构建。
4. **生成 RC 并绑定证据：** 先通过基础测试、安装/启动及关键 smoke，再对确切 commit 创建不可变候选 tag（如 `v0.1.0-rc.1`）；发行包、校验和、源码/二进制指纹、配置、测试日志对应同一候选。
5. **跑本版验收：** 先约定用户允许的服务器/设备/时段，执行目标矩阵、故障/恢复及长稳，保留失败与未完成样本。代码、依赖或有效配置改变后递增 RC，重跑受影响验收；改变长稳路径的修复需重新取得该路径的长稳证据，不把旧候选成绩转贴给新候选。
6. **生成最终产物再放行：** 去掉预发布后缀也会改变 package/build 信息，不能假定最终二进制与 RC 哈希相同。记录最终版本提交与 RC 的差异；若仅版本元数据变化，审核后至少对最终包重新构建并验证版本、安装/启动和关键链路；若还有功能或依赖变化，退回 RC 验收。长稳证据引用哪个候选、为什么适用于最终包必须写清。
7. **发布和留退路：** 放行后创建指向最终版本提交的 annotated tag（项目要求签名时使用 signed tag），按授权推送该 tag 和对应提交；附 changelog、支持矩阵、安装配置、校验和、原始证据索引、迁移与回退步骤。发布二进制不等于自动发布所有 crates；crate registry 发布另核查范围和依赖顺序。
8. **发布后观察：** 按确认的范围试部署，核对健康/积压/丢弃/资源与实际版本；保留上一包和备份。回退二进制不等于可以直接读取新状态，也不能撤销已发出的 HTTP 副作用。发现问题发新 patch/RC，不覆盖已发布 tag 或悄悄替换同版本资产。

**R9 已完成可用环境下的可观测性整批验证、性能收尾与证据归档，可以进入阶段 2；netem 等未关闭项继续登记并在适用发行门槛前补齐，不跳到 Arrow/JIT 或马上打 1.0。** 本节把任务库变成有出口的执行路径：先让当前能力可观测、可安装、可验收，再逐版增加业务价值。

<a id="design-coverage"></a>
## 18. 设计覆盖矩阵、历史取舍与门禁证据

### 18.1 覆盖范围与判定规则

本节建立**可追踪的章节级入口**，不是宣称所有设计细则已经实现或审计通过。2026-09-12 的编号核对：Final 的 **91 个原始任务 ID 全部保留**；本清单原有 150 个主任务，本次补入 PUB-06、OPS-04、WIN-01、HTTP-07、QA-07～10，共 **158 个主任务**。新任务均未勾选；矩阵建好不能自动关闭 PUB-06 或 QA-10。

| 文档简称 / 实际文件 | 本节覆盖范围 | 使用方式 |
|---|---|---|
| 蓝图：`Sparrow_Architecture_Blueprint_Reviewed_R1.md` | 01～52 章、附录 A～E；含 28 项历史 ADR 和 G0～G6 门禁 | 保留架构不变量与验收要求；早期时序、暂定布局、容量示例不直接当当前事实 |
| 计划评审：`Sparrow_Blueprint_Plan_Review.md` | §0～5 的缺口、建议切片、旧方案取舍 | 建议逐项采纳、替代或延期；其中“现状”是报告当时的观察，开工前重新核查 |
| 初稿：`Sparrow_Post_V1_Roadmap_Arrow_JIT.md` | 早期产品/性能路线 | 同主题由 Final 替代；差异不能因此被当成已实现 |
| Final：`Sparrow_Post_V1_Roadmap_Arrow_JIT_Final.md` | §01～22、附录 A 与证据说明，含 91 项编号任务 | 后续产品/性能设计的主依据；阶段号不等于实际 release 版本 |
| 正式运行与概览：[RUNTIME](RUNTIME.md)、[ARCHITECTURE_SUMMARY](ARCHITECTURE_SUMMARY.md)、[v1-report](v1-report.md)、[README](../README.md) | 已记录合同、兼容变化、历史能力与操作入口 | PUB-06 整理历史/当前范围；描述仍须绑定实际源码和运行证据 |
| 历史决策：[ADR-002](adr/002-narrow-binder.md)、[ADR-003](adr/003-layout-decision.md) | 窄 Binder、默认 RowBatch 选型 | 保留原决策日期和证据；扩展或替代时记录新决定，不改写历史测量 |
| 测试与实验：[bench](bench.md)、`COMPARE.md`、`experiments/wasm-spike/README.md` | 已有测试方法、对照数据、spike 边界 | 作为证据线索，不当新增生产能力或当前设备认证 |

覆盖限于当前工作区列出的文档；聊天中曾提及、但当前没有提供文件的 R3～R8 等评审不能在此虚记“已逐条核对”。后续补入文档时由 PUB-06 增加来源和差异记录。

**发生冲突时：** 实际行为/受测构建决定“现在是什么”；Final、已确认正式合同及用户后续决定共同约束“准备做什么”，但不能静默取消蓝图不变量。2026-09-14 的核心优先决定仅替代原先的排期：K1/K2 对应原阶段 6 与 HTTP-07，K3 对应原阶段 7 的内核，K4 对应原阶段 4b，平台/Designer 归 K5 后置；下面保留原设计编号以免断开交叉引用。旧方案与新方案冲突时按 §18.4 登记替代关系；缩减已承诺范围或改变语义仍需兼容审核。实际执行顺序见 §3.4/§17，不因本节按文档章节排表而改变。

矩阵中的“保留并回归 / 待补 / 条件启动 / 已替代 / 暂缓”是**计划处置**，不是 PASS。证据列使用 §18.6 的代号；“待”表示尚需产出，不能将其误读为已有产物。

### 18.2 蓝图 52 章与附录：任务、阶段和证据入口

| 蓝图章节 | 设计主题 / 当前处置 | TODO 归属 | 执行阶段 | 验收证据或待补内容 |
|---|---|---|---|---|
| 01～04 | 定位、最小交付、Non-goals、不变量：保留；早期版本时序由 §17 替代 | PUB-05/06、CFG-01～04、QA-06 | 0～3，后续持续 | E-PUB 待；入口一致、能力拒绝、依赖/安全与资源范围 |
| 05 | 参考系统与外部经验：保留为设计来源，不继承其实现保证 | PUB-06、P0-02/06、CONN-01～08 | 0～2 / E0 / 相应接入 | 锁定所用上游版本/入口；第三方实测与本项目证据分开 |
| 06～07 | 分层、Rule/Pipeline/Job、逻辑/物理对象：保留 | PUB-05/06、CFG-02/04、DAG-01～08 | 2，7 | E-PUB 待；SQL/Graph→同一计划、revision/attempt 清晰 |
| 08～09 | 数据布局、Schema、所有权/准入、类型演进：部分已修，剩余闭合 | BASE-03/04、MEM-01～04、SEM-01/02、FMT-01/02、P1-01～05 | 2；新格式/后端随功能 | E-BASE 有限范围；E-MODEL/E-FEATURE 待，不能只测 wrapper |
| 10～12 | SQL/Binder、优化器、物理计划：复用 prepared 基线，按场景扩展 | SQL-01、SEM-01/02、PUB-05、CFG-03/04、P0-01～06 | 2、5 / E0 | E-HIST；E-MODEL/E-GATE 待，方言正反例及解释/准入一致 |
| 13～16 | Operator/chain/fusion、背压与 fan-out：已有线性路径保留 | MEM-04、OBS-02、AGE-02～04、QA-07、DAG-01～06、SCALE-05 | 1～2、5、7、8b | E-OBS 有限范围；控制顺序、公平性/取消及慢支路证据待 |
| 17～19 | time/watermark/idle、窗口、增量聚合：保留并补独立性质验证 | AGE-01、WIN-01、SEM-01、QA-07、ANA-01/06、V2-01/02 | 2；4b/5、8b/8c 按需求 | E-MODEL 待；时间轴、边界、holdback、final/更正不可混用 |
| 20～21 | state/TTL、checkpoint、source cut 与掉电假设：先受限恢复，再协议演进 | BASE-01/02、MEM-02、OPS-04、STATE-01～05、REL-01～15、QA-09 | 2～3、6、7c | E-BASE 有限单窗口；E-FAIL/E-FEATURE 待，含 next-to-emit/解码边界 |
| 22～23 | Join、静态/版本表与外部 Lookup：已有能力回归，新增分层开放 | TAB-01～03、ANA-02～04、PUB-05、STATE-03 | 2、8a/8b；恢复依赖阶段 6/7c | E-FEATURE 待；依赖 revision、异步结果顺序/预算与恢复 |
| 24～26 | Source/Sink 能力、ACK、重试、SDK：逐路径验证 | CONN-01～08、HTTP-01～07、REL-00～15、EXT-05 | 1～3；5/6、8b | E-OBS 仅部分网络场景；outbox/可靠入口/SDK conformance 待 |
| 27～29 | 函数、WASM、插件 ABI：纯函数先行，扩展条件启动 | SEM-02、EXT-01～06、ANA-06、PUB-06 | 2、8b | E-HIST spike 不等于 Supported；自定义 UDAF 取舍见 §18.4 |
| 30 | GraphSpec/BoundPlan 分离与 Designer：保留；内核先于页面 | CFG-02、UI-03、DAG-01～08 | 2；K3 的原阶段 7 内核；K5 的 Designer/4a 后置 | E-MODEL/E-FEATURE 待；同语义错误/结果，不承诺 SQL↔图无损互转 |
| 31～32 | SQLite 元数据、事务/启动边界、API/desired/actual：先核实回归 | PUB-05、OPS-04、QA-02/06、UI-05 | 2～3、4a/5 | E-PUB 待；并发/幂等、held、有效保证，不把保存配置当启动 |
| 33～35 | UI/debug/健康：首版 API 可诊断，完整界面后置 | OBS-01～03、AGE-01、CLI-01、UI-01～06、OPS-01～03 | 1～3、4a/5 | E-OBS 部分；E-FEATURE 待，Preview 无外部副作用、指标有界 |
| 36～38 | 并发隔离、内存/磁盘容量、两档 profile：复用，不承诺固定规则数 | MEM-01～04、CFG-01、QA-02/03/05/08/10、SCALE-06 | 2～3；8b | E-GATE/E-SOAK 待；工作量公平、task/fd、账本/RSS、flash 写率 |
| 39～40 | Embedded/Standalone、workspace 依赖方向：保留 | PUB-02/03、EMB-01、CONN-07、QA-06 | 2；5 按嵌入需求 | E-PUB/E-FEATURE 待；无 demo、可裁剪、宿主 runtime 归属 |
| 41～44 | lifecycle/cutover、兼容/安全：本版路径先闭合 | PUB-04/06、BASE-02、QA-04/06/08/09、UI-06、REL-10 | 2～3；5/6 | E-BASE 部分；E-PUB/E-FAIL 待，旧 attempt 隔离及迁移拒绝 |
| 45 | 工具链、依赖成本与候选选型：锁定版本和 feature | PUB-02、P0-06、EXT-03、SCALE-06 | 2 / E0 / 8b | E-PUB/E-FEATURE 待；构建目标、依赖图、体积与成本 |
| 46～47 | G0～G6、模型/性质测试、故障/长稳：明确保留门禁 | QA-01～10、OPS-04、PUB-04 | 0 建账，2～3 验收；新功能持续 | §18.5、E-GATE/E-MODEL/E-SOAK/E-FAIL；未测不能 PASS |
| 48 | 早期 roadmap：不重做已有 M0～V1，改用当前主线 | PUB-06、全部功能组 | §17 的 0～8 / E0 | §17 阶段出口与 capability；里程碑不等于 tag |
| 49 | 28 项 ADR：保留历史身份，逐项回写 | PUB-06；§16.1 的 P-001～P-015 | 0～2；对应新决策开工前 | §18.4；仅两个现有 ADR 文件不等于其余决定均已正式归档 |
| 50 | 风险/开放问题：需求、容量、持久入口、布局、MSRV 等 | PUB-02/06、REL-00、OPS-04、P0-01～06、SCALE-03 | 0～2；相应条件阶段 | 任务卡须填写触发/投入/停止规则；没有数据不选默认胜者 |
| 51～52 | 最小演示、查询验收、最终建议：复用现有入口补缺 | CLI-01、TPL-01/02、WIN-01、QA-02/07、PUB-05/06 | 2～4 | 当前版本实际演示与 §51.2 时间查询预期；不是重建全部早期 PR |
| 附录 A～E | 需求索引、阅读路线、不变量、参考来源、R1 修改映射 | PUB-06、QA-07/10；各主题同上 | 0～3，持续维护 | 不变量逐项落测试；来源锁定与 R1 未执行项由当前证据回填 |

### 18.3 Final：所有能力组的执行与证据映射

范围写法覆盖区间中的每个原任务编号；阶段中的 E0 是独立性能实验线，不是首版必过的实现阶段。

| Final 章节 / 内容 | 对应 TODO | 执行阶段 / 处置 | 验收证据入口 |
|---|---|---|---|
| 01～04：定位、基线、双线、阶段总览 | PUB-01～06；§2、§17 | 0～3；已有部分复用、余项待补 | E-BASE/E-OBS 仅其限定范围；E-PUB 待 |
| 05.1：发行与设备 | BASE-05、PUB-01～06、QA-01～10 | 0～3 | E-PUB/E-GATE/E-SOAK/E-FAIL 待 |
| 05.2：真实观测与时效 | OBS-01～03、AGE-01～04 | 1；策略阶段 5 | E-OBS 部分；全路径与策略 E-FEATURE 待 |
| 05.3：基线补正 | BASE-01～05、MEM-01～04、SEM-01/02 | 0～2 | E-BASE；完整 owner/golden/发行仍待 |
| 05.4：共享参与者 | STATE-01～05、REL-14、OPS-04 | 2 的现有调度；K1 优先线性恢复，K3c 再扩多输入 | E-BASE 不覆盖零/多状态；E-FEATURE 待 |
| 06：工作台、运维、嵌入、模板 | UI-01～06、OPS-01～03、EMB-01、SQL-01、CLI-01、IOT-01～03、TPL-01/02 | K4 的原 4b 业务；K5 的原 4a/5 平台；必要 API/CLI 随核心交付 | E-FEATURE 待；至少两个精确场景、权限/诊断同交付 |
| 07：可靠来源与本地备选 | REL-00～15、STATE-01～05 | K1 先恢复；K2 的原阶段 6 可靠路线，部署选择仍需确认 | E-FEATURE/E-FAIL 待；ACK/切点/去重/身份/毒消息一起闭合 |
| 08：真实 DAG | DAG-01～08 | K3：7a→7b→7c 内核；DAG-08 Designer 后置 K5 | DAG-01～07 的限定模型/功能/进程故障证据见 [K3](DAG.md#k3-validation)；Designer、任意 state codec 与生产长稳不在此完成范围 |
| 09：IoT 与表 | IOT-01～09、TAB-01～03、MEM-02、STATE-04、REL-07 | 首批 K4 / 原 4b；高级 8a 按需求 | E-FEATURE 待；状态/时间、reset/连续性、输出身份 |
| 10：有界分析 | ANA-01～07 | 8b，条件启动 | E-MODEL/E-FEATURE 待；膨胀、清理、时间与恢复 |
| 11：WASM/SDK/IPC | EXT-01～06 | 8b，条件启动 | E-HIST 仅 spike；G5 和生产隔离/版本/资源待 |
| 12：单 Job 多核、大状态、性能发行 | SCALE-01～06 | 8b，需热点/容量证据 | E-FEATURE/E-FAIL 待；离线重分片、世代/GC/峰值 |
| 13：多设备、新业务边界 | FLEET-01～04、V2-01～05 | 8b/8c，分别立项 | E-FEATURE/E-FAIL 待；离线收敛、安全升级、专门失败合同 |
| 14：接入/格式、HTTP | CONN-01～08、FMT-01～04、HTTP-01～07 | 已有路径 1～3；可靠输出/选定 outbox 前移 K2b；其余按需 | E-OBS/历史报告有限范围；真实 RTT/TLS/新协议待 |
| 15：Arrow 的布局/时间/语义/预算/DF | P0-01～06、P1-01～05、P2-01、MEM-01～04、SEM-01/02、CFG-01～04 | E0→各自证据门槛；生产版本不绑定大阶段 | E-HIST 不作当前胜负；公平全路径及 owner/控制证据待 |
| 16：JIT 形状/ABI/编译/回退/资源/安全/平台 | J0-01、J1-01/02、J2-01、SEM-01/02、CFG-01～04 | 热点实验→生产准入，不依赖先采用 Arrow | E-FEATURE 待；净收益/摊销、代码 owner、取消、平台与回退 |
| 17：性能实现切片 | P0-01～06、P1-01～05、P2-01、J0-01、J1-01/02、J2-01 | §12，有限投入、允许停止 | 固定候选/负载/输出成本；逐切片独立放行 |
| 18：配置与 Explain | CFG-01～04、PUB-05 | 2 的现有范围；每个新后端同步 | E-PUB/E-FEATURE 待；force 不偷退、auto 记录原因 |
| 19：新功能验收合同 | §1.2、QA-01～10、PUB-04/05；每项任务卡 | 所有阶段 | §15.3 与 E-FEATURE；未覆盖项不可假报 Supported |
| 20、22：执行顺序与最终建议 | §3、§16.2、§17、PUB-06 | 当前 K0～K5 核心优先；保留原 0～8 / E0 引用 | 用户新排期替代旧顺序，不改协议约束；不以清空 TODO 为放行条件 |
| 21：后续 ADR | P-001～P-015（决策 ID）；PUB-06 | §16.1 的触发点 | 决策文件/正式合同及证据指向待补，不伪造 15 个已存在文件 |
| 附录 A、参考说明 | PUB-01/04/06、QA-10 | 0～3 | 文档/源码归档指纹、已执行与仅静态评审分别记账 |

### 18.4 历史评审与决策的取舍

#### 18.4.1 计划评审的建议，不静默遗漏也不照单复刻

| 来源 / 旧建议 | 当前取舍 | 任务 / 阶段 / 证据 |
|---|---|---|
| 评审 §0～2：蓝图时序陈旧、V1 功能与认证混淆、决策未回写 | 采纳问题，冻结历史而补当前执行/证据；不据旧报告断言今天仍无 benchmark | PUB-06、QA-10；0～3；E-PUB/E-GATE 待 |
| 评审 §3.1、§4：目标板、72 h、资源与性能口径 | 采纳；平台/负载先冻结，蓝图候选 p99 50 ms / 聚批 5 ms 不是现有通用 SLO | QA-03/05/08/09/10；3；E-SOAK/E-FAIL 待 |
| 评审 §3.2：周期 checkpoint、24 h 自动恢复 | 补为独立任务；不等待 REL 或完整 UI，不放大现有 aligned 支持形状 | OPS-04；2～3；自动恢复结果待 |
| 评审 §3.2：start 幂等、失败/held 状态、日志前缀 | 采纳为现状核查和回归，不重复修复已关闭问题 | QA-06、OBS-01；1～2；实际 API/runbook 待 |
| 评审 §3.3：MQTT persistent session/QoS1 或本地日志 | **由 Final 的 REL-00 来源决策替代默认二选一**；允许 NATS 且需要可靠输入时先验证 JetStream；MQTT QoS 不自动构成全计划恢复 | REL-00～15、CONN-01；6；所选协议 conformance 待 |
| 评审 §3.4：独立磁盘重发缓存，HTTP 断 10 分钟后补送 | 可靠输出合同前移核心 K2b；独立 outbox 按断网自治需求选定，不等同已做 HTTP 合批/重试，不把满时静默丢弃用于 required 路线 | HTTP-07、REL-07/08；K2b；断连/重启/满盘与身份结果待 |
| 评审 §3.5：PT hopping/sliding、L=0 Session | 保留需求但先区分 PT/ET 与真实缺项；Session 不重开编号 | WIN-01、ANA-01；0～2 盘点，4b/5 或 8b 实现；边界/时钟预期待 |
| 评审 §3.6：WASM 有需求才做 | 采纳；旧 spike 不算生产能力 | EXT-01～06；8b；G5 与全路径资源/隔离待 |
| 评审 §3.7：布局已冻结，Arrow/DF 只留 Future，不重开 | **被 Final 的有界 P0 与证据门槛替代**；保留当前 Row，允许新证据推动可选区域或后续布局决策，不自动重写内核 | P0/P1/P2、J0/J1/J2；E0 及条件发行 |
| 评审 §4：确定性模型与时间/恢复性质测试 | 明确采纳，不只放在“有 property tests”大标题下 | QA-07；2；E-MODEL 待 |
| 评审 §4：UI 不阻塞最小交付 | 采纳；首版 API 可诊断，后续 CLI/薄工作台不是永久取消 UI | OBS、CLI-01、UI-01～06；1～5 |
| 蓝图 §27/48：自定义 UDAF、retract 等 Future | 暂缓自定义 UDAF 包/ABI；内建聚合补充归 ANA-06，changelog/retract 归 V2-01；确有用户扩展聚合需求时先立 add/merge/retract、state codec、预算与版本 ADR，不冒充 EXT 纯函数已覆盖 | PUB-06、ANA-06、V2-01；需求触发的 8b/8c |
| 蓝图 / 历史报告：unaligned、跨 Job checkpoint、通用迁移 | 保留为非目标/未立项，不因 SCALE-04 或 STATE 多参与者已编号就宣称已安排实现 | PUB-05/06；新增需求先 ADR；现阶段拒绝未支持组合 |

上表“延期/条件”不是免除已支持路径的正确性责任；如果本次发行明确要承诺某能力，就必须把对应任务及验收前置。“被替代”只替换方案，不删除其要解决的问题。

#### 18.4.2 蓝图 001～028 的逐项登记入口

以下 ID 是**蓝图的历史决策索引**，不是本次新增的 ADR 文件。只有 002、003 指向目前已有正式文件；其余行给出保留/演进规则与归档任务，正式记录尚待 PUB-06 核对。P-xxx 是另一命名空间，见 §16.1。

| 蓝图 ADR ID | 主题 / 处置 | 当前任务或决策入口 | 阶段 / 待证据 |
|---|---|---|---|
| 001 | Dataflow-first：保留 | PUB-05/06、SEM-01、QA-07 | 2；SQL/Graph 一致 |
| 002 | 窄 Binder：保留历史 ADR，新增语法另验收 | `docs/adr/002-narrow-binder.md`、SQL-01、P-003 | 2/5；当前方言 inventory，不用早期拒绝表冒充现状 |
| 003 | RowBatch：已有 Accepted 决策，不再称未冻结；新证据可重开 | `docs/adr/003-layout-decision.md`、P-002、P0-01～06 | E-HIST；E0 当前公平实验待 |
| 004 | 不运行时编译 Rust：保留；受控表达式 JIT 是独立决策 | J0-01、J1-01/02、J2-01、P-004 | 条件性能线；不生成整 Job Rust |
| 005 | chain task：保留 | QA-07、OBS-02、QA-10/G2 | 2；融合/控制/公平性 |
| 006 | Source/Sink 隔离：保留，优化须有证据 | HTTP-05、CONN-01、P0-04 | 2/E0；I/O/计算边界 |
| 007 | 请求容量预算：保留，声明范围尚需闭合 | BASE-03、MEM-01～04、P-015 | 2；E-BASE 部分，完整路径待 |
| 008 | 显式 fan-out：保留 | DAG-01～06、SCALE-05、P-006 | 7a/8b；慢支路保证 |
| 009 | task-owned MemoryState：保留，大状态按瓶颈演进 | MEM-02、SCALE-03、P-009 | 2/8b；更新峰值与 backend 成本 |
| 010 | CheckpointStore 与 state 分离：保留 | STATE-01～05、SCALE-04、P-012 | 6/8b；参与者/持久协议 |
| 011 | 单 Job aligned：保留；现有单窗口限制与后续图能力分开 | BASE-01、STATE-01～05、DAG-07、P-012 | 2/6/7c；切点与恢复 |
| 012 | 有效保证按实际组合：保留 | PUB-05、REL-00～15、CFG-04 | 2/6；requested/effective/拒绝 |
| 013 | final-only windows：保留；更正另立项 | WIN-01、ANA-01、V2-01/02 | 2/5/8；final 与更正合同 |
| 014 | per-input watermark 与 holdback：保留 | QA-07、DAG-04/07、AGE-01 | 2/7；单调/idle/恢复 |
| 015 | 增量 accumulator：保留 | SEM-01、MEM-02、ANA-06 | 2/8b；原始输入独立预期 |
| 016 | Reference/Lookup 优先：保留 | TAB-01～03、ANA-02～04 | 8a/8b；版本依赖与时间 |
| 017 | 内建 Connector SDK：保留，动态边界有条件 | CONN-01～08、EXT-05/06 | 2/8b；conformance |
| 018 | Wasmi 暂定：历史 spike 不是当前生产引擎选择 | EXT-03、P-008、QA-10/G5 | 8b；目标设备成本/限额 |
| 019 | GraphSpec 与 BoundPlan 分离：保留 | CFG-02、DAG-08、UI-03 | 2/4a/7；版本化输入与 Explain |
| 020 | SQLite metadata only：保留 | PUB-06、QA-02/06、OPS-04 | 2；事务/启动分离，不逐事件写 Catalog |
| 021 | Embedded core 无 server 依赖：保留；宿主 Handle 是新增范围 | EMB-01、PUB-02、CONN-07 | 2/5；可裁剪与生命周期 |
| 022 | 一个引擎、多预算/feature：保留 | CFG-01、SCALE-06、QA-05/10 | 2～3/8b；设备 profile 同语义 |
| 023 | prepare 后 fenced cut：保留，不能凭名字宣称外部 fencing | QA-06/08、UI-06、REL-10 | 2～3/5/6；并发更新/旧 attempt |
| 024 | 不通用 state migration：保留 | BASE-02、PUB-04、UI-06、P-013 | 2～3/5；显式拒绝/reset/replay |
| 025 | JSON authoring 与版本化状态：保留 | CFG-02、FMT-01～04、PUB-04、STATE-03 | 2/6/新格式；codec 独立版本 |
| 026 | 观测/调试有预算：保留 | OBS-01～03、UI-04、OPS-02、P-011 | 1/4a/5；E-OBS 部分，剩余待 |
| 027 | 少 crate、多 module：保留演进原则，不倒推删除现有 crate | PUB-02/06、P0-06 | 2/E0；依赖方向/可裁剪与真实复用 |
| 028 | 单节点优先：保留；新部署边界分别立项 | FLEET-01～04、V2-04/05、P-010 | 8b/8c；不提前建集群骨架 |

### 18.5 G0～G6 门禁台账

此表是 QA-10 的入口，不把旧评审的勾选抄成当前 PASS。最终记录必须带完整 commit/build、脚本/fixture、平台/负载、证据路径和验收人。状态使用 PASS / FAIL / PARTIAL / NOT RUN / BLOCKED / NOT APPLICABLE；NOT APPLICABLE 必须给出不在发行范围的理由。

| 门禁 | 任务 / 阶段 | 应验证什么 | 当前证据与未关闭项 |
|---|---|---|---|
| G0 | SQL-01、SEM-01、QA-01/07/10；2 | 固定 parser/Dialect，SQL 正反 fixtures、AST/错误范围、SQL/Graph 语义 | E-HIST 有窄 Binder 决策；当前完整方言/fixture 与发行构建关联待整理，不单凭 ADR 判 PASS |
| G1a | P0-01～06、PUB-06、QA-10；历史归档 0～2 / E0 | 历史布局选择可回溯；新候选同 prepared 基线、语义/输出/资源/冷暖/全路径 | E-HIST 有 Row/Arrow 历史数值，DF 未实跑；不是当前公平三方实验通过。当前 Row 发行不等待重新选型 |
| G2 | MEM-04、OBS-02、QA-02/03/07；2 | chain 与对照等价、控制顺序、预算、公平性、满队列/取消/慢输出 | E-OBS 提供 mailbox 局部并发/预算证据；独立模型和完整发行组合仍待 |
| G1b | HTTP-05、CONN-01/03、QA-02/03/05/06；3 | 实际 MQTT/HTTP/TLS、codec、Catalog 并存、目标设备 RSS/延迟和准入 | 已有服务器短试次不等于目标板/真实 WAN 认证；按首发设备补 E-GATE，其他平台不得泛化 |
| G3 | MEM-02/04、WIN-01、QA-01/07；2 / 新 state 同步 | accumulator 对原始输入、detach、key/timer 上限与清理工作量 | E-BASE 部分 owner 证据；独立状态模型与新节点完整上界待 |
| G4 | BASE-01/02、OPS-04、STATE、REL、QA-04/09；2～3 / 6/7c | 暂停/冻结/峰值、写盘/ACK/发布切点、四类失败模型与恢复 | E-BASE/E-OBS 仅限定单窗口与负向恢复；自动调度、完整存储模型/目标介质待，不可标整个 G4 已过 |
| G5 | EXT-01～06、QA-03/06/10；8b 条件 | 所选 WASM 引擎的 native 对照、限额、隔离、ABI 和设备成本 | E-HIST 仅 spike；未纳入发行时登记 NOT APPLICABLE，纳入后重新验收 |
| G6 | CFG-01、DAG/SCALE 的适用子集、QA-02/03/05/08/10；3 / 7/8b | 1/10/100/1000 负载阶梯、声明最大并发、慢 Job 隔离、独立/共享入口及拒绝行为 | 单 Job 短跑不能放行；现有并发测试按源码核对后归档，未实现共享入口不算已测；E-GATE/E-SOAK 待 |

蓝图完整 V1 的门禁还包括 QA-07 模型、QA-08 的 72 小时与 10,000 次启停、QA-09 的分模型证据。平台或失败模型未覆盖时可以按 §17 发布**明确缩限的产品范围**，但不能写成“已经满足全部蓝图门禁”；是否接受该发行范围必须在放行前决定，而不是测试失败后改口径。

### 18.6 证据索引与后续维护

以下 E-* 只是本节的索引代号，不是新建文件或捏造的测试报告。

| 证据代号 | 已有 / 待产出 | 位置或必要内容 |
|---|---|---|
| E-BASE | 已有，限定基线修复 | §2.2、[RUNTIME](RUNTIME.md) 的 BASE verification；服务器 `base-gates-artifacts-20260912/v2/`。只能证明关联补丁及所列组合 |
| E-OBS | 已有，Runtime 首批 | §5.1/5.4、[RUNTIME](RUNTIME.md) 的 OBS verification；服务器 `obs-artifacts-20260912/v3/`。含过载丢弃试次，不能概括为全路径无损 |
| E-OBS-CLOSURE | 前批 v12 历史证据；该版本性能/netem 门槛未通过 | §5.4、RUNTIME closure verification；服务器 `obs-closure-artifacts-20260912/v12/`。445/0、21/0、28×20；正常/持续流量、过载、TLS、恢复及前后开销分别记录 |
| E-R9-OBS | 当前 Server 范围及 File 开销门槛通过；netem 仍 NOT RUN | §5.4、RUNTIME R9；服务器 `r9-artifacts-20260914/v4/`。463/0、22/0、48×20；80k/400k ABBA、真实 IO/恢复及新旧失败样本分开保存 |
| E-HIST | 已有文档/历史实验，需核对适用性 | ADR-002/003、bench、COMPARE、WASM spike。日期、源码、负载不同，不直接继承成当前测试结论 |
| E-PUB | 待 | PUB-01～06：正式源码/版本、构建/包哈希、能力/迁移/安全/runbook、文档冲突处置和审批 |
| E-MODEL | 待 | QA-07/SEM：独立模型/fixture、seed、最小失败样本、结果/错误/时间/资源断言及受测构建 |
| E-SOAK | 待 | QA-08/OPS-04：72 小时原始时间序列、10,000 次循环计数、24 小时自动恢复、清理/基线趋势与未覆盖项 |
| E-FAIL | 待 | QA-09：四种失败模型分开的平台/介质/切点/注入方法、状态/输出/恢复结果及拒绝；模拟与真实实验分别标记 |
| E-GATE | 待完整归档 | QA-10：逐门禁状态、证据路径、目标设备/SLO、未运行/不适用原因、放行范围和验收人 |
| E-FEATURE | 随新功能产出，当前不代表已有 | 对应 task ID 的实现、版本/配置、capability、正负/故障/性能测试和迁移；不能只有示例或编译结果 |

以上服务器目录相对 `/workspace/bench-compare/`，受测源码与产物指纹以 §2、§5 及 RUNTIME 的记录为准；本次只整理文档，没有重跑或重新认证这些测试。

维护时按以下顺序关闭覆盖项：

1. 新文档或设计修改先进入 §18.1；保持原文版本/指纹，登记替代关系，不覆盖历史事实。
2. 章节级入口进一步拆出本次适用的具体合同/反例，关联主任务或子任务、§17 阶段、负责人和投入上限。
3. 先写验收条件，再实现/测试；证据必须指向实际源码、构建和有效配置。代码存在、编号命中、表格有行都不等于完成。
4. 对每个本版适用项给出“已验收”或明确阻断项；延期/不适用写原因、影响范围与重新触发条件。不能以弃用文档为名绕过仍对外承诺的行为。
5. 每个 RC/正式版复核 PUB-05/06 与 QA-10：关闭支持范围内所有阻断项，保证 README、capability、运行合同、版本/迁移说明和证据一致，再按 §17.6 放行。
