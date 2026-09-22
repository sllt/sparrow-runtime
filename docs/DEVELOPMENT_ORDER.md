# 开发顺序 TODO

更新：2026-09-19。只记录接下来做什么、先后顺序和交付效果，不重复历史过程。

**原则：先补齐 K1～K4 核心，再做 K5 前端。下面的数字只是执行顺序，不是新的阶段编号。**

## 当前起点

- [x] 已完成限定范围的静态参考表、Count/IoT 组合恢复、JetStream 可靠组合、required File DAG，以及 Change/Deadband/Hysteresis。
- 上述为已验证的 Preview，不代表整个 K1～K4 已完成，也不代表已经通过生产长稳认证。

## 主线：按这个顺序推进

- [ ] **1. 可恢复时间 + HoldFor / Debounce（进行中；K1/K2/K4）**
  - [x] 单个线性 HoldFor/Debounce：File v14 / JetStream v15、持久时间决策和稳定输出身份；747/44、22×20、6+3 进程场景、旧矩阵及三组 ABBA 已通过。[本批证据](PRODUCTION.md#paused-time-validation)。
  - [ ] 剩余：PT Window/正 TTL、多状态的时间恢复组合；不要把这个限定 Preview 写成整个时间阶段完成。新 profile 的逐行持久化成本另记 [OPT-012](OPTIMIZATION_BACKLOG.md#opt-012)。
  - 统一逻辑时钟、timer 和输入顺序；停机暂停，恢复后继续计时，未提交时间决策能够重放。
  - 时间、状态、Source cut 和适用的输出身份一起提交；逐组合开放 PT/正 TTL 恢复。
  - 完成 HoldFor、Debounce 的 File/JetStream 端到端运行、恢复、模板及故障测试，不只交付 Clock/Timer 接口。
  - **效果：持续超限才输出、停止抖动后再输出；无新消息也能按时触发，重启后结果一致。**

- [ ] **2. 时间型 DAG 恢复（K3）**
  - 保存多来源的时间进度、watermark、idle/EOF；补齐分支 timer 的一致切点。
  - 明确 Union 合流与 timer 的可重放顺序，逐个验证允许的拓扑，不宣称任意 DAG 都能恢复。
  - **效果：时间型规则可用于多来源、分支与合流，而不只限于线性管线。**

- [ ] **3. 完整 IoT 业务闭环（K4；IOT-06～09）**
  - 告警生命周期：Normal/Pending/Active/Recovering、activate/resolve、稳定 episode ID。
  - 冷却/通知限频；静默/离线检测，区分设备故障、来源断连和管线停止。
  - Sampling/Resample，明确 last/mean/interpolate、缺样、时间边界及恢复行为。
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
  - **效果：给出目标设备、配置和负载下可承诺的容量，不用短程跑通代替容量结论。**

- [ ] **6. 全局 Review、生产验收与发行（K0 / QA / PUB）**
  - 冻结候选，跑完整功能/故障/性能回归；核对安全、预算、取消和资源释放。
  - 补真实 TLS/网络、目标设备、24/72 小时长稳、生命周期循环、存储故障和升级回退。
  - 完成 CI、发行包、操作文档和能力矩阵；长稳/破坏性介质测试另行确认环境与时段。
  - **效果：形成有明确适用范围和证据的可发行版本；未测项保持 NOT RUN。**

- [ ] **7. 运维工作台 + Graph Designer（K5）**
  - Dashboard、连接/Schema 管理、SQL/Graph 编辑、Explain、无外部副作用的 Preview。
  - 发布历史、导入导出、权限审计、诊断和可视化编排，复用已有 API/CLI。
  - **效果：不打开源码也能配置、部署、排障；不另造一套执行引擎。**

## 后续扩展：按需求或证据选择，不要求依次全部做完

以下是主线之外的完整方向清单；必要能力可经明确范围调整前移，不作为当前核心发行的统一前置。

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
