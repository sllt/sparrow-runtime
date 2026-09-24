# 开发顺序 TODO

更新：2026-09-24。只记录接下来做什么、先后顺序和交付效果，不重复历史过程。

**原则：先补齐 K1～K4 核心，再做 K5 前端。下面的数字只是执行顺序，不是新的阶段编号。**

本文件是后续完整开发计划的统一入口，分为必须推进的主线和按需扩展；不再另建重复的过程计划文档。

**从当前状态往后：性能与告警验收收尾 → 补齐 K4 → Action/常用函数增强 → 可靠链路与容量 → 核心版本发行 → K5 工作台 → 按需扩展。**

## 当前起点

- [x] 已完成限定范围的静态参考表、Count/IoT 组合恢复、JetStream 可靠组合、required File DAG，以及 Change/Deadband/Hysteresis。
- 上述为已验证的 Preview，不代表整个 K1～K4 已完成，也不代表已经通过生产长稳认证。

## 主线：按这个顺序推进

- [x] **1. 可恢复时间 + HoldFor / Debounce（限定线性范围完成；K1/K2/K4）**
  - [x] 单个线性 HoldFor/Debounce：File v14 / JetStream v15、持久时间决策和稳定输出身份；747/44、22×20、6+3 进程场景、旧矩阵及三组 ABBA 已通过。[本批证据](PRODUCTION.md#paused-time-validation)。
  - [x] PT Window/正 TTL、多状态时间组合：独立 v16/v17，759/44、25×20 和 38×20 专项、24 种新进程场景、旧矩阵及三组 ABBA 已通过；限定线性最多两个状态、Change/Deadband 正 TTL，Hysteresis 仍不静默过期。不含 ET、参考表或时间型 DAG，不代表整个 K1～K4 完成。[本批证据](PRODUCTION.md#linear-time-validation)。逐行持久化成本仍见 [OPT-012](OPTIMIZATION_BACKLOG.md#opt-012)。
  - 统一逻辑时钟、timer 和输入顺序；停机暂停，恢复后继续计时，未提交时间决策能够重放。
  - 时间、状态、Source cut 和适用的输出身份一起提交；逐组合开放 PT/正 TTL 恢复。
  - 完成 HoldFor、Debounce 的 File/JetStream 端到端运行、恢复、模板及故障测试，不只交付 Clock/Timer 接口。
  - **效果：持续超限才输出、停止抖动后再输出；无新消息也能按时触发，重启后结果一致。**

- [x] **2. 时间型 DAG 恢复（K3，限定 File 图范围）**
  - 2026-09-23：File→required HTTP 的 PT v18 / ET v19，持久 round、确定性 Union、来源 idle/EOF 和每 Sink 输出游标已完成；774/44、14×20 专项、12 种包/进程组合、旧矩阵及原三组 ABBA 全部通过。[合同](DAG.md#time-graph-recovery)、[匹配证据](PRODUCTION.md#time-graph-validation)。不包含 JetStream 图、混合 PT/ET、参考表、有损/侧路或不受预算限制的大图。
  - 保存多来源的时间进度、watermark、idle/EOF；补齐分支 timer 的一致切点。
  - 明确 Union 合流与 timer 的可重放顺序，逐个验证允许的拓扑，不宣称任意 DAG 都能恢复。
  - **效果：时间型规则可用于多来源、分支与合流，而不只限于线性管线。**

- [ ] **3. 完整 IoT 业务闭环（K4；IOT-06～09）**
  - 当前主线已切到本批，先落实告警状态/episode 与通知边界，再接静默检测和采样；内部步骤见 [执行合同](IOT.md#business-loop-plan)。尚未开放的新节点不在 capabilities 中冒充支持。
  - 告警生命周期：Normal/Pending/Active/Recovering、activate/resolve、稳定 episode ID。
  - 冷却/通知限频；静默/离线检测，区分设备故障、来源断连和管线停止。
  - Sampling/Resample，明确 last/mean/interpolate、缺样、时间边界及恢复行为。
  - 开发中：Alarm/通知冷却已接通独立事件 schema、generation/episode 与 v20～22。2026-09-24 的 `alarm-closure` v1 已通过 795/44、14×20、线性 6 个进程场景、新告警图 2 类拓扑×2 个包及完整旧功能矩阵；双 Count 原性能门禁通过，但零状态路径合并比值 0.885299，未过 ≥0.97，继续定位，整批未放行。之后再做来源健康与静默检测、Sampling/Resample；整项不勾选完成。
  - 2026-09-24：Window 固定大小聚合的等价额度预留优化已独立提交（`53e06d2`）；提交树/集成工作区分别通过 293/314 项 Runtime 测试，新 7 项各重复 20 轮。仅移除临时列数组分配和重复额度增长，不降低预算；性能收益尚未验证，不替代上一条失败的 ABBA 门禁，也不代表告警子批已放行。
  - 本批内部执行顺序（不是新增阶段编号）：
    1. 新告警图独立 SIGKILL、恢复及旧版本拒绝已补齐；继续解决零状态性能回归，最终源码与完整验收证据匹配后才放行告警子批。
    2. 建立可持久化、可重放的来源健康观测，不能用无消息或旧 Ready 指标代替健康事实。
    3. 实现静默/离线检测与恢复通信；来源断连、管线停止不误报为全体设备故障，未见且未登记的设备不凭空离线。
    4. 实现 Sampling/Resample 的 last、mean、interpolate，明确缺样、等待未来点、输出时间及状态/展开上限。
    5. 补齐业务模板、所声明组合的恢复与诊断，再做整批 review、功能/故障/性能回归。
  - **效果：完整实现“持续超温→告警→通知限频→降温恢复”等业务场景。**

- [ ] **4. Action/Sink 与常用函数首批增强（后端；CONN / HTTP / FMT / SQL / SEM）**
  - 对照 eKuiper 盘点：Action 是输出动作/Sink，SQL 函数是计算能力，两条清单分别维护；不承诺配置或语义全兼容。
  - 先增强已有 HTTP/Webhook、MQTT、Log：输出字段映射/模板、受控动态参数、多动作配置与诊断；复用现有 DAG，不另造 Action 执行引擎。
  - 首批新增有界 File Sink；按缺口补常用字符串、数值、类型转换、JSON、日期时间函数，已有函数不重做。
  - **效果：计算结果可按业务格式推送、记录或落文件，常见规则不必靠自写插件；新 Sink 不自动获得 aligned/可靠交付资格。**
  - 更多 Sink、复杂函数和插件按下方扩展清单逐批做；范围与验收见 [Action/函数计划](DEVELOPMENT_TODO.md#actions-functions)。

- [ ] **5. 可靠链路与容量收尾（K2 / CONN / HTTP）**
  - 定位高负载积压和 p99，验证消息大小、pending、空闲唤醒和 ACK 成本。
  - 补齐多管线、慢下游、断连重连、过载恢复及已有 Source/Sink 的支持矩阵与诊断。
  - 按目标负载评估时间型 profile 的逐决策 fsync、完整 checkpoint 和 HTTP 等待成本；有证据再优化批处理/提交，不削弱持久性、恢复或 ACK 保证，也不单靠扩大队列制造提升。
  - **效果：给出目标设备、配置和负载下可承诺的容量，不用短程跑通代替容量结论。**

- [ ] **6. 全局 Review、生产验收与发行（K0 / QA / PUB）**
  - 冻结候选，跑完整功能/故障/性能回归；核对安全、预算、取消和资源释放。
  - 补真实 TLS/网络、目标设备、24/72 小时长稳、生命周期循环、存储故障和升级回退。
  - 完成 CI、发行包、操作文档和能力矩阵；长稳/破坏性介质测试另行确认环境与时段。
  - 在声明的核心支持范围通过发行门禁后，可以先发行无前端的核心版本；不必等待 K5 或所有远期扩展。
  - **效果：形成有明确适用范围和证据的可发行版本；未测项保持 NOT RUN。**

- [ ] **7. 运维工作台 + Graph Designer（K5）**
  - Dashboard、连接/Schema 管理、SQL/Graph 编辑、Explain、无外部副作用的 Preview。
  - 发布历史、导入导出、权限审计、诊断和可视化编排，复用已有 API/CLI。
  - **效果：不打开源码也能配置、部署、排障；不另造一套执行引擎。**

## 后续扩展：按需求或证据选择，不要求依次全部做完

以下是主线之外的完整方向清单；必要能力可经明确范围调整前移，不作为当前核心发行的统一前置。

- [ ] **K1/K2/K3/K4：恢复组合与时间语义扩展**——按需求逐个开放未声明的来源、拓扑、参考表和时间域组合，例如 HoldFor 自身的 event-time 模式。每个组合单独定义语义、预算、恢复与验收；未开放项继续明确拒绝，不能由现有 Preview 的通过结论推导为任意组合已完成。
- [ ] **TAB-02/03：动态参考表与外部 Lookup**——增量 upsert/delete、有限版本历史、有界异步查询及恢复依赖。
- [ ] **K2-EXT：可靠链路扩展**——指定历史 replay、语义 fork/迁移；按断网自治、毒消息隔离需求选择持久 outbox/DLQ。
- [ ] **AGE：新鲜度与过载策略**——来源/年龄追踪、通知过期、显式合并策略；不偷偷改变聚合或可靠交付语义。
- [ ] **CONN/FMT：接入与格式扩展**——按需增加 NATS Core、Kafka、HTTP Poll/WebSocket/TCP、Local DataBus、Protobuf/CSV；Arrow IPC、Parquet 格式可选。工业采集优先独立接入。
- [ ] **CONN/HTTP/FMT：更多 Action/Sink**——在首批 File/现有输出增强后，按需求增加 Memory/DataBus、Redis、SQL 数据库、InfluxDB、NATS/Kafka 等；通知平台优先复用 HTTP/MQTT 模板。File Sink 基础能力归主线第4批，Parquet 等格式单独按需做。
- [ ] **SQL/SEM/ANA/EXT：更丰富的函数**——数组/对象、编码/哈希、聚合/分析/窗口/多行多列函数，以及 UDF/外部函数；有状态、非确定性和有副作用的能力分别定义预算与恢复合同，不混进纯表达式。
- [ ] **EMB/SQL/WIN：嵌入与语言能力**——宿主 Runtime 接入，按实际缺口补 SQL、函数和窗口，不重复实现已有能力。
- [ ] **P0 → Arrow / JIT：性能路线**——先公平对照和热点画像，有净收益再引入。JIT 不依赖 Arrow，无收益可以停止并保留 Row。
- [ ] **ANA：高级有界分析**——Session、Interval/Window Join、受限 Left Join、UNNEST、补充聚合与有界查询。
- [ ] **EXT：插件与扩展 SDK**——先 WASM 纯 UDF，再有界 Transform、Registry/实例管理、Connector SDK；必要时做 native/IPC 隔离。
- [ ] **SCALE：单 Job 多核与大状态**——分片、离线重分片、按需状态 backend、增量/COW checkpoint、共享输入治理。
- [ ] **FLEET：多设备管理**——设备身份、凭据轮换、签名配置/升级、灰度回退、离线收敛和诊断汇总。
- [ ] **V2.x：独立立项的新边界**——Changelog/迟到更正、Session late merge、事务 Sink、多进程 worker、HA/remote workers；不是必须全部实现。

## 执行与发版约定

- 每次交付完整一批：实现 → 自查 Review → 服务器集中测试 → 更新状态；不每完成一个 helper 就停下。
- 内存所有权、函数语义、安全、诊断、兼容和回退随功能一起交付，不留到最后统一补。
- 主线顺序与下一批以本文件为准；协议、任务细目和证据见 [详细 TODO](DEVELOPMENT_TODO.md)、[运行合同](RUNTIME.md)、[验收记录](PRODUCTION.md#k1-k4-reference-validation)。两处有变化时同步更新。
- 按完整验收批次安排 RC/后续 0.x 功能版本，具体 tag 不预先写死；不等全部远期功能才发版，不自动 commit/push/tag。
- 非阻断问题集中维护在 [优化清单](OPTIMIZATION_BACKLOG.md)，不与未实现的新功能混在一起。

### 交付与版本里程碑

| 节点 | 可以交付什么 | 不能据此宣称什么 |
|---|---|---|
| 当前告警子批收尾 | 匹配功能/故障/性能证据的告警与通知冷却 Preview | K4 全部完成、性能问题已经解决或生产认证 |
| K4 业务闭环完成 | 告警、静默检测、采样及声明组合的可运行模板和验收结果 | 任意来源/时间域/拓扑都支持恢复 |
| Action/函数及可靠容量收尾 | 更完整的后端业务能力与目标负载支持矩阵，形成核心 RC 候选 | 未跑的 TLS/目标设备/长稳/介质测试已通过 |
| 全局发行门禁完成 | 有明确适用范围的核心正式版本，可不含前端 | 全部长期扩展都已实现 |
| K5 或后续独立扩展完成 | 经对应验收的后续功能版本 | 自动提升未受测链路的可靠性保证 |

当前仍是开发候选。具体版本号按实际发行状态确定，不预先占用；涉及 codec/API 不兼容时单列迁移、备份和回退要求。每个里程碑都以匹配源码的证据为准，commit/push/tag 仍需用户授权。
