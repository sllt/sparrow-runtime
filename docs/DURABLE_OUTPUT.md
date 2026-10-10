# Durable HTTP output（限定 Preview）

可选的本地持久 outbox，将计算完成/来源确认与远端 HTTP 投递分开。默认 HTTP Sink 不变；不修改窗口 codec 或 checkpoint profile，不提供 exactly-once。

## 确认与恢复

启用 `sink.durable_outbox` 后：编码有界 JSON 请求 → 独立 SQLite `DELETE/FULL` 事务提交 → Runtime 收到本地确认 → checkpoint 发布 → 可靠来源 ACK。**本地确认不代表 HTTP 2xx，更不代表业务处理完成。** 独立 sender 投递成功后删除记录，永久失败/重试耗尽转入持久 DLQ。

- `restart_fresh` 支持单线性 HTTP 输出；来源仍保持原有语义，未落盘的非可靠输入不会获得恢复能力。
- `aligned` 首批只支持 **File / JetStream → 零状态或一个旧 Count 窗口 → HTTP JSON**。新窗口、新聚合、ET/PT 窗口、IoT、参考表、多来源/DAG、side output 恢复组合暂不开放。
- catalog 固定队列 UUID；队列固定 catalog namespace、pipeline、URL、SecretRef、编码和重试策略。队列缺失/被替换时拒绝启动，不能新建空队列冒充恢复。首次启用要求新的 checkpoint 目录。
- 必须在新建 pipeline 时选择 durable 模式；已有 pipeline 不可添加/撤除模式或改变目标、目录、策略。迁移使用新 pipeline 名称和独立存储。旧二进制不认识该配置，会拒绝加载，不允许静默降级为普通 HTTP。
- 请求携带 `Idempotency-Key` 和 `X-Sparrow-Outbox-Id`，值为 `UUID-sequence`。同一持久请求的自动重试、重启和人工 DLQ 重放保持 ID、正文不变，投递前校验 SHA-256。
- HTTP 已接受但本地尚未删除时崩溃会重复 POST，接收方需要实现幂等。**来源从旧 checkpoint 重放可能生成另一个请求 ID**；JetStream 原有逐行 output ID 保留在 JSON 中，File 不获得跨来源重放去重。人工重放也不保证维持原始输出顺序。

## 配置与边界

完整示例：[`deploy/pipeline-durable-http.json`](../deploy/pipeline-durable-http.json)。先注册 `sensors` schema（`device_id: utf8`、`v: int64`，均非空）、创建输入文件、配置实际 HTTP 目标 allowlist。目录必须属于 `SPARROW_DATA_ROOTS`；每队列独占目录，不混放 catalog/checkpoint/测试结果。

`durable_outbox` 所有字段均需显式配置：

| 字段 | 范围/含义 |
| --- | --- |
| `directory` | 本地持久目录的绝对路径 |
| `max_disk_bytes` | 8 MiB～4 GiB；数据库页上限另留 rollback journal 空间 |
| `max_pending_bytes`, `max_dlq_bytes` | 各不小于单条上限；合计不超过磁盘预算的 1/4 |
| `max_pending_entries`, `max_dlq_entries` | 各 1～10000，正文和条数同时约束 |
| `max_record_bytes` | 2 B～1 MiB；整个上游 batch 必须可编码为一个请求，不自动拆分 |
| `max_attempts` | 每次重放代次 1～100 次，包括已 claim 但未确定发出的尝试 |
| `max_retry_elapsed_ms` | 从本代入队计时，1 秒～7 天；停机和暂停也计时 |
| `retry_base_ms` | 25～60000 ms |
| `retry_max_ms` | 不小于 base，最多 1 小时；指数退避封顶 |

要求 `max_inflight=1`、`batch_rows=1`、`linger_ms=0`、`batch_bytes <= max_record_bytes`；仅显式 URL 的普通 JSON HTTP，无 action/demo。`batch_rows=1` 表示不合并上游 batch，不代表 batch 只有一行。

传输失败、408、429、5xx 重试，数字型 `Retry-After` 秒值遵守上限；其他 4xx、重定向直接进入 DLQ，不跟随跳转。HTTP timeout 仍由原 Sink 配置决定。2xx 视为远端接受，即使响应体读取不完整；响应体有界排空以便复用连接。

首版串行投递，最多 32 个独立 sender；空闲/退避轮询间隔 250 ms，加上落盘开销，不是低延迟或高吞吐承诺。SQLite 页/缓存和请求正文有界，编码/投递工作空间使用 Runtime memory owner，但不等于精确 RSS 上限。存储必须保证 SQLite 锁和 fsync 语义；不要手动编辑在线数据库。

## 满额与故障

- pending 满、磁盘/本地提交失败：拒绝新的本地确认，Job 失败/停止推进，保留已有记录；可靠来源不能确认未提交输入。解决容量后按正常 Job 生命周期恢复，不自动驱逐数据。
- DLQ 满：终止重试的记录成为 `blocked`，仍占 pending 容量，**不再 POST**；队头阻塞后续请求，直到操作员清出 DLQ 空间，不自动丢弃。
- 正文损坏、UUID/策略不匹配、时钟倒退到入队之前：停止 sender，不假装成功；检查状态及 `sender_failed` 审计，修复后显式 resume。
- DLQ 无自动 TTL 清理。人工 replay 开启新的有限预算，`created_ms` 更新为本代开始时间，`replay_generation` 加一（最多 1000）；它不是最初业务事件时间。

## 生命周期与观测

sender 独立于输入 Job：**pipeline stop/kill 不停止积压投递**，应使用 outbox pause。pause 不能撤回已到达远端的请求；返回时当前 sender 已取消回收，记录保留。暂停投递仍允许输入入队，按需同时停止 pipeline。

正常 desired-running 启动先准备 sender 再激活来源；来源启动失败时旧输出仍可投递。Server 整体关闭等待 sender 回收。重启后 stopped/held pipeline 的队列不承诺自动 drain，可显式 resume；这不解除来源 safe-mode，也不启动来源。resume 重新绑定当前 allowlist/凭据。

status 中 `accepted_total` 是本地落盘，`delivered_total` 是已收到 2xx 且完成本地删除；`sender.http_posted` 等是当前 sender 的运行计数。全局 `outbox_persisted_batches` 与 `http_acked_batches` 分开。跨字段采样不是原子全系统快照。

## API / CLI

全部接口使用既有管理鉴权。首次启动初始化后才可查询；正文可能包含业务数据，按管理凭据权限保护。

```bash
export SPARROW_URL=http://127.0.0.1:43180
export SPARROW_TOKEN=your-management-token
sparrowctl outbox telemetry
sparrowctl outbox-entries telemetry pending 0 50
sparrowctl outbox-entries telemetry blocked 0 50
sparrowctl outbox-entries telemetry dlq 0 50
sparrowctl outbox-entry telemetry UUID-SEQUENCE
sparrowctl outbox-command telemetry command.json
```

- `GET /v1/pipelines/{name}/outbox`：持久计数、字节/条数、`blocked_for_dlq_space`、暂停/sender 状态、最近 32 条审计。
- `GET /v1/pipelines/{name}/outbox/entries?state=pending|dlq|blocked&after=0&limit=50`：不含正文，`after` 是上页末尾数值 `sequence`，`limit` 为 1～100。pending 列表不含 blocked，pending 总量包含 blocked。
- `GET /v1/pipelines/{name}/outbox/entries/{UUID-sequence}`：显式读取 base64 正文。
- `POST /v1/pipelines/{name}/outbox/command`：以下命令。

暂停/恢复需当前队列 UUID 和操作原因：

```json
{"action":"pause","approve_uuid":"COPY_FROM_STATUS","reason":"maintenance"}
```

`action=resume` 恢复投递。重放/清除需精确选择一条 **DLQ** 记录并确认当前代次：

```json
{"action":"replay","approve_uuid":"COPY_FROM_STATUS","id":"UUID-SEQUENCE","replay_generation":0,"reason":"receiver fixed"}
```

`action=purge` 是显式永久丢弃，不是投递成功；pending/blocked 不能直接 purge。命令和审计原子提交，保留最近 200 条本地审计，并非无限保留的合规审计。paused 时 replay 只回到 pending，不自动 resume。

## 备份、升级与验证范围

必须把 **catalog + checkpoint + outbox 目录** 作为同一离线备份集：正常停止 Server，确认进程/sender 退出后复制完整目录，保存匹配的二进制、配置和凭据。不能只还原 checkpoint、只回滚 catalog 或在线复制 SQLite 主文件。UUID 校验无法区分同一 UUID 的旧副本，仍须保证备份集一致。回退旧版本应使用启用前的完整备份，不能消费当前恢复点。

本批交付 **HTTP outbox + 输出侧 DLQ + 对应运维操作**。后续的输入 DLQ、兼容续跑和显式新 lineage 重放工作流见 [RECOVERY_OPERATIONS](RECOVERY_OPERATIONS.md)；不是通用快照转换。多 Sink 原子提交、其他 connector 的持久 outbox，以及另一子批的新窗口恢复不由本合同覆盖。

针对性测试为 `crates/sparrow-control/src/outbox_tests.rs` 和 `tests/durable-output-process/main.go`。后者各跑一次真实 File/JetStream → Count → HTTP，覆盖远端失败时 checkpoint/来源 ACK、SIGKILL 后续投、重试状态、DLQ 重放/清除和实际 Server/CLI。没有 20 轮重复矩阵，也不把 SIGKILL 等同掉电/长稳认证。

```bash
go run ./tests/durable-output-process/main.go \
  --server-bin /absolute/sparrow-server --cli-bin /absolute/sparrowctl \
  --nats-server /absolute/nats-server --out /absolute/new-evidence-directory
```

### 本轮验证（2026-10-10）

在 `box@100.64.0.21` 的 Linux Release 构建验证，未在开发机编译。证据根 `/workspace/bench-compare/outbox-20261010`：

- `validation2`：四个受影响 crate 的一轮回归 **631 passed / 0 failed / 86 ignored**；ignored 不算通过。不是全 workspace 或所有 feature 组合。
- 后续 UUID 依赖和观测修正只做定向复验：`final/tests.log` **5 passed**，no-demo Server/CLI 构建成功。Rust 差异指纹保存在 `final/source.sha256`，对应本地提交前源码。
- `final/process-verified-status.txt` 为 **exit=0**；File、JetStream 各一个完整场景，最终成功输出均为 `[3,7,11]`，另外一条 DLQ 经明确批准 purge，pending/DLQ 均清空。JetStream 另核对了 HTTP 尚未接受时 broker ACK floor 已到已提交 cut。此前测试夹具配置不完整的失败日志保留，不算通过，也未因此放宽运行时限制。
- 未运行长稳、性能扫描、掉电/磁盘设备故障矩阵；不据此宣称整个项目生产认证。
