# 输入隔离与恢复操作（第 11 批，限定 Preview）

本批补齐输出 outbox 之外的输入处置和恢复操作。**不修改窗口 codec，不开放子批 2/3/4 的新窗口、Join、多来源或业务组合恢复，也不做通用快照字节转换。** 既有 HTTP outbox 合同见 [DURABLE_OUTPUT](DURABLE_OUTPUT.md)。

范围：`aligned`、JSON File/JetStream、零状态或一个旧 Count 窗口；恢复操作的输出限定普通 HTTP JSON。输入隔离和重放均 opt-in。资源不足、I/O/协议错误、运行时表达式/聚合错误仍然 fail/held，不把已部分执行的状态当成可以跳过的坏消息。HTTP 业务拒绝由输出侧 DLQ 处理。

## 输入 DLQ

在新 pipeline 的 `source` 中配置：

```json
"input_dlq": {
  "directory": "/var/lib/sparrow/input-dlq/telemetry",
  "max_disk_bytes": 8388608,
  "max_payload_bytes": 1048576,
  "max_entries": 1000
}
```

同时要求 `fail_on_decode=true`、显式新 `checkpoint_dir`；路径属于 `SPARROW_DATA_ROOTS`。首次启用不得收养旧 checkpoint。普通未启用规则的错误策略不变。

- 只隔离**完整且不超过 64 KiB 的原始记录**发生的 JSON/字段类型/记录限制错误。File 超长行已被 framer 丢弃的部分不能假装完整原文，因此仍失败，不提交该输入切点。
- 顺序：独立 SQLite `DELETE/FULL` 隔离原文、位置、SHA-256 和错误码 → source 发布该已处置切点 → required 输出确认 → CURRENT 发布 → JetStream ACK。隔离本身不是 ACK；File 同样只能从已提交 checkpoint 恢复。
- 相同 source incarnation/位置和正文重复发现是幂等的，不重复占容量；同位置不同正文拒绝。目录 UUID 固定到 catalog，缺失/替换拒绝启动，不静默创建空 DLQ。
- 原文最多 64 KiB，条数 1～10000；磁盘 8 MiB～4 GiB，payload 64 KiB～磁盘预算的 1/4。数据库页另留 rollback journal 空间。满额保留旧数据并有界等待，checkpoint/管理命令仍可执行；先提交已处置前缀，再停止、清理、续跑。未隔离的下一条不越过 cut，不驱逐、不自动 TTL 清除；真实存储错误仍失败。
- source/解析 schema/checkpoint 绑定固定；更换这些依赖使用新 lineage，而不是编辑现有 DLQ 绑定。普通 schema 更新如果改变该绑定，将拒绝后续启动。
- `input_quarantined` 是本次 attempt 隔离计数；持久的 `captured_total` 是去重后的累计条数。运行时计算失败不会进入该计数。

### 查询、原文和 purge

```bash
sparrowctl input-dlq telemetry
sparrowctl input-dlq-entries telemetry 0 50
sparrowctl input-dlq-entry telemetry POSITION
sparrowctl input-dlq-purge telemetry purge.json
```

对应 `/v1/pipelines/{name}/input-dlq`、`/entries?after=0&limit=50`、`/entries/{position}` 和 `POST /purge`，全部沿用管理鉴权。列表不返回原文，单条查询显式返回 base64；分页最多 100 条。

purge 要求 pipeline **desired 和 actual 均 stopped**，审批当前 checkpoint 及丢弃的记录：

```json
{
  "approve_uuid": "COPY_FROM_INPUT_DLQ_STATUS",
  "position": 120,
  "approve_replay_floor": 120,
  "approve_checkpoint": 7,
  "reason": "已在独立修正重放中处理，批准删除原文"
}
```

`position` 是记录之后的 File 字节偏移或 JetStream stream sequence，必须已经被批准的 CURRENT 覆盖。删除与 `replay_floor` 推进、审计在同一事务：**此后拒绝从该 floor 之前恢复/历史重放原 lineage**。不能既删除唯一的处置证据，又假称所有旧 checkpoint 都还能恢复。保留的其他 DLQ 记录仍可单独重放；purge 是明确的数据删除，不是业务成功。

## 四种恢复操作

| mode | 行为 |
| --- | --- |
| `resume` | 同名 pipeline 发布兼容修订，从审批的 CURRENT 继续；source、schema、Sink、目录和计算语义不变，保存原状态/身份。可调整兼容的运行配置。 |
| `fork` | 从审批 checkpoint 的 source cut **之后**启动新 pipeline/new consumer，**空状态**、新输出 lineage，继续未来输入。用于明确的语义切换，不冒充状态转换。 |
| `replay` | 新 lineage，空状态重算 `(from_checkpoint, checkpoint_id]`；省略 `from_checkpoint` 从来源开头重算。范围固定，末端停止读新输入，但保留 Job 等待显式 finish。 |
| `dlq_replay` | 选择 1～64 条已被 checkpoint 覆盖的 DLQ 记录，允许显式 JSON 对象修正，写入独立不可变 File artifact，在新 pipeline 中处理；不注入原来的有状态 Job。 |

对于 fork/replay，清空状态必须明确审批。若从一个半满窗口的中间切点重算，新窗口自然不继承此前的半个窗口；要重建完整业务状态，应选择包含所需历史的范围。这里的迁移方案是“兼容状态续跑，或审批后重放重建”，不是 arbitrary codec conversion、分布式接管或 exactly-once。

JetStream 新 lineage 必须使用不同的逻辑 `consumer`，其余来源绑定保持一致；来源 retention、真实 incarnation 和可读取范围在激活 reader 时再次核对，历史过期直接拒绝，不能跳到最新。File 预览会验证 checkpoint 的文件身份/切点；其完整性仍受已有文件合同约束，不把采样 fingerprint 称为全文件密码学证明。

### 预览 → 审批 → 发布 → 启动 → 完成

先停止原 pipeline，等待 actual stopped。必要时先手动 checkpoint，再停止。

```bash
sparrowctl checkpoint telemetry
sparrowctl stop telemetry
sparrowctl status telemetry
sparrowctl recovery-preview telemetry request.json
# 将返回的 approve_digest 填入同一个 request.json，不改其他参数
sparrowctl recovery-execute telemetry request.json
sparrowctl start telemetry-history
sparrowctl status telemetry-history
sparrowctl recovery-finish telemetry replay-001
```

`request.json` 的 `spec` 是**完整的目标 PipelineSpec**，不接受部分 patch：

```json
{
  "operation": "replay-001",
  "mode": "replay",
  "approve_parent_revision": 3,
  "checkpoint_id": 12,
  "from_checkpoint": 8,
  "target": "telemetry-history",
  "spec": { "...": "完整目标配置，使用新的 checkpoint/可选队列目录" },
  "accept_state_reset": true,
  "accept_duplicate_outputs": true,
  "reason": "修正规则后重新计算这一段历史",
  "approve_digest": "COPY_FROM_PREVIEW"
}
```

`source.replay_start` 是服务端管理字段，不能自行设置 cursor 绕过审批；API 发布/启动会校验对应 lineage 记录。受管目标的计算、Source、Sink 和 checkpoint 绑定固定，只允许兼容的 checkpoint 运行参数调整且保持 `resume_latest=true`；改变语义、换目录或回退到旧点须新建审批操作，不能借普通 PUT 让已完成操作重新运行。新目标需新名称和空存储目录；不能复制旧恢复点改个名字当新状态。

DLQ 模式额外提供 `dlq_positions`、`corrections`（以数值位置的字符串为 key）、`artifact_directory`；目标使用 File、`delivery=live_best_effort`、aligned 和独立 checkpoint，不能再配置自动输入 DLQ 形成递归隔离。重放文件总计最多 1 MiB，逐条最多 64 KiB，原始/修正 SHA-256 进入操作来源记录。原文含换行或为空白记录时须提供 JSON 对象修正，避免改变 NDJSON 帧边界或静默忽略空行。未提供 correction 的其他坏记录可能再次失败，这是有限 fail/held，不会暗中修正数据。

- 预览不修改 CURRENT、创建 reader 或启动 Job；执行重新校验审批摘要、父 revision/schema、已提交 checkpoint 和目标依赖。
- execute 仅发布**stopped** 目标，不自动开始外部输出。同一 operation 和相同请求可重试，已发布操作返回同一 revision，不二次创建；不同请求复用 ID 拒绝。
- 操作先持久 reservation，再生成可校验 artifact，最后 catalog 事务原子发布 pipeline/lineage。崩溃后用原请求重试；完整文件校验相同则复用，部分临时文件可续作，已发布文件不覆盖。修改父配置/依赖后旧审批失效，不能强行继续。
- `finish` 仅适用于有限 replay/DLQ replay。确认 source 到达 end，再提交 checkpoint、记录完成切点并停止目标。尚未达到 end 时明确返回未完成；已有 checkpoint 排队/执行时返回可重试 busy，应等待它完成再重试。若使用输出 outbox，完成仍是**本地持久接受**，不是远端业务成功。
- 停止父 pipeline 不会暂停其已接纳的输出 outbox；如不希望旧输出继续投递，应另行 outbox pause。新 lineage 输出可能重复，不撤销旧输出，接收方负责业务幂等。

### 查询与中断操作

```bash
sparrowctl recovery-operations telemetry
sparrowctl recovery-operation telemetry replay-001
sparrowctl recovery-abort telemetry replay-001 reason.json
```

`reason.json` 为 `{"reason":"取消尚未发布的操作"}`。abort 仅终止 preparing 操作、释放目标 reservation；不删原 source/checkpoint/DLQ，也不自动删除可能用于审计的 artifact。ready 操作使用正常 pipeline stop，不假称能撤销已投递数据。

接口为 `POST /v1/pipelines/{name}/recovery/preview|execute`、`GET /recovery/operations[/{id}]` 和 `POST /recovery/operations/{id}/finish|abort`。预览并发 2，操作沿用串行生命周期协调；catalog 最多 128 个操作记录，每个 pending 操作最多 16 次显式尝试，无自动无限重试。历史 lineage 元数据是运行依赖，不随普通日志轮转删除。操作原因、摘要、来源位置/原文摘要、目标和阶段持久保存；输入 DLQ 另保留最近 200 条本地审计，不声称无限合规审计。

## 备份、回退和验证

停 Server 后一起备份 catalog、完整 checkpoint、input DLQ、output outbox，以及 DLQ replay 的 artifact。不能将同一 UUID 的旧数据库副本与较新的 CURRENT 混用。旧版本不识别新增 source 字段；回退使用匹配的完整备份，不能让旧二进制继续操作 pending 恢复流程。

测试入口：`crates/sparrow-control/src/input_recovery_tests.rs`、`tests/input-recovery-process/main.go`。真实进程场景覆盖 File/JetStream 的坏记录落盘、ACK、SIGKILL、兼容迁移、原文/修正重放、有限范围、新 lineage 重启、fork 和 purge floor。未做新窗口/Join 组合、长稳、性能扫描、掉电或其他平台认证；子批 2 合入后再推进 3、4，不从本批推导这些能力。

### 2026-10-10 验证记录

基线 `d662308`，Linux x86_64 / Rust 1.98.0 / locked Release；在 `box@100.64.0.21` 完成，未在开发机编译。证据根 `/workspace/bench-compare/input-recovery-20261010`。

- `validation3`：四个受影响 crate 的一轮回归 **636 passed / 0 failed / 86 ignored**；不是全 workspace/all-features，也不把 ignored 算通过。
- 自查补正受管配置不可绕过审批、持久原文去重时的完整性检查等后，`reviewed/tests.log` 的 **4 项定向测试通过**，no-demo Server/CLI 构建成功；没有重复全量矩阵。最终 Rust 差异指纹见 `reviewed/rust-source.sha256`。
- `reviewed/process-combined-status.txt` 为 **exit=0**，File/JetStream 各一条完整场景：同时启用 input DLQ/output outbox，HTTP 503 期间完成本地持久确认及 checkpoint，DLQ 满时保持控制进展且不确认未隔离输入；清理后续跑、两次进程 SIGKILL、兼容修订、修正重放、历史区间重算、fork、purge floor 全部通过。两条链路最终输出均为 `[3,7,11,42,4,5,6,15]`，输入 DLQ 两条隔离/两条明确 purge、剩余 0 条。JetStream 另外查询实际 broker ACK floor。
- 一次进程试验曾在自动 checkpoint 在途时直接 finish，返回预期的 retryable busy。最终脚本先等待已批准区间的 terminal cut 完成，再调用 finish；保留原失败记录，没有改小数据范围、放宽运行时校验或将 busy 当成成功。脚本指纹见 `reviewed/driver-combined.sha256`。
