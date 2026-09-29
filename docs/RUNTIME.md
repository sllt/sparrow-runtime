# Runtime ownership (A3 / A4)

2026-09-29 外部扩展：版本化copy-only IPC接入Source/Sink及Graph Pure Transform，各session固定一个阻塞actor和独立进程，不逐行创建脱离生命周期的任务。先取得Job scratch credit，再Open/编码/解码；Source Poll行数收紧到Kernel ingress批次，数据、水位、EOF有序背压，Accepted只表示易失准入。Transform完整校验并取得输出credit后发布，保留origin/source_operator；输入关闭/取消后join worker再释放计费及pin。Sink Flush/Close错误不能被记成completed；歧义失败不重发，失败和进程重启要求显式start。8个external进程槽、每IPC 1s、子进程128MiB地址空间，kill进程组＋wait后返还槽；清空环境/继承FD、no_new_privs不构成多租户OS沙箱。配置JSON的保守内存估计包含小容器开销，不只按wire bytes计费。完整合同见 [EXTENSIONS](EXTENSIONS.md)，验收证据单列。

2026-09-29 包生命周期：可选/强制Ed25519策略、不可变format2依赖图在进入任何插件代码前完成预检；所有开启后端的依赖必须先获批准并启用。catalog v4随不可变revision原子保存SQL/Graph包引用，受管卸载按catalog→registry锁序和SQLite写事务检查历史引用；停止不等于可卸载，显式ETag退休仅清理无checkpoint的停止态fresh历史。原生format1身份不变，metadata可跨目标签名但native激活仍核验本机；ELF未知依赖/搜索路径拒绝，平台库不是包内hash闭包。1005/48、包4×5、三后端CLI及签名/撤销/迁移验证见[证据](PRODUCTION.md#package-lifecycle-validation)。旧v3二进制回退须配套catalog备份。

2026-09-29 WASM Preview：wasmi 2.0.0在独立worker中校验/缓存不可变Module，每次新Store/Instance；每版本同时一个实例，JS/WASM共享4个worker名额。无imports/WASI/JIT，memory16MiB/1M fuel/128MiB进程上限与父进程100ms期限同时生效。WASM复用task-local总预算/取消，有限查询只允许可抢占后端；pure native仍拒绝。`memory.grow`的-1代表拒绝，不冒充成功或一律要求trap。SDK wasm32只传有界偏移/长度，宿主验证完全部输出才发布；Module元数据bytes为源模块大小，不是IR/RSS。997/48、WASM5×5、JS17回归及三后端真实CLI通过，见[WASM验收](PRODUCTION.md#wasm-plugins-validation)。仍fresh-only，未借此开放任意插件恢复。

2026-09-29 JavaScript Preview：QuickJS-ng 0.16.2 只进入独立 worker，不链接 Server/CLI 地址空间。版本复用进程，每次调用新 Runtime/Context；默认关闭，脚本和原生使用独立 opt-in。单包串行、全进程 4 个 worker 名额、每 worker 128MiB 地址空间/64MiB VM 堆；Job 另按声明类型预留有界 IPC scratch，不能把子进程内存冒充计入 Job owner。含脚本的 stage/router future 使用 task-local cancellation，避免给跨 Job 共享 Function 绑定一个 Job 的 token；无脚本的热路径不安装该 scope。100ms 到期/取消后 kill＋wait，pin 持有至 Job tasks 退出；脚本停用后可卸载，原生驻留合同不变。仍 fresh-only。首批982/47、新12×5及真实CLI/独立worker边界通过；部署、故障恢复、选型与待优化项见 [PLUGINS](PLUGINS.md#javascript-标量函数)，精确验收见 [PRODUCTION](PRODUCTION.md#script-plugins-validation)。以下历史记录按各自日期/构建解释。

JS 收尾：worker 从已校验源文件编译最多256KiB不可变缓存，每调用重载到新VM，保留严格模式/源码位置，不接受外部bytecode。安全诊断仅输出阶段及最多8个数值行列坐标，不透传异常文本、不访问用户getter；原始UTF-8字节列转换为UTF-16列，包含中文/emoji及JS换行符测试。有限查询开放JS，使用同一 `query_work` 原子余额按每非NULL调用10,000＋参数字节预扣，并向stage/router传递同一绝对截止时间；不按行重置，不称作VM指令计量。失败/取消join后退出、无部分成功；native仍拒绝，aligned仍不开放。内部IPC升级ipc2，Server/worker必须配套。992/48、17×5集成＋4×5单测及真实JS/native CLI回归通过；缓存微测不是生产容量，见[匹配验收](PRODUCTION.md#script-completion-validation)。

2026-09-27 可信原生函数Preview：Catalog限定的绑定scope生成不可变函数句柄；SQL/Graph使用同一类型/NULL合同，行执行不查可变registry。Job级pin持续到全部task结束，停用/卸载不能替换运行依赖。host output buffer、表达式元数据和pin容器预留额度；原生内部资源/ELF不是Job硬配额，不能伪装沙箱。默认不加载，启用需显式配置与完整hash批准；sealed memfd加载后驻留至退出，不做热卸载。新表达式无恢复codec，有限查询因不可抢占明确拒绝native。970/47、新13×5及真实CLI升级/回退/重启通过，见[合同](PLUGINS.md)和[匹配证据](PRODUCTION.md#native-plugins-validation)。脚本/WASM及Source/Sink插件尚未开放。

2026-09-27 有界分析Preview：UNNEST保留单个输入batch、逐行计费续行；双输入ET Join分别接收水位，Idle不证明无匹配，Left未决行约束输出水位。行/key/输出fan-out都有额度，清理/取消退还所有权；新future和扩展聚合冷路径Box先计费，旧codec不变。有限查询使用独立Kernel、总工作计数及2个准入槽，响应字节持有额度/槽直到发完，取消后join查询任务才释放worker。新算子/聚合仅restart_fresh；957/45及新20×5通过，合同和未实现项见[ANALYSIS](ANALYSIS.md)，精确[验证范围](PRODUCTION.md#analysis-validation)。没有新的性能/长稳/发行放行结论。

2026-09-27 窗口补齐Preview已完成本批自查与验证：PT hopping、SlidingCount、PT/ET逐事件Sliding、PT/ET Session；匹配候选937项回归、44项no-demo及19×5专项通过。新窗口仅 `restart_fresh`，Control、CheckpointPlan、PlanLayout复用判定、Kernel及新窗口旧codec恢复入口均明确拒绝aligned/restore。PT hopping复用增量store；其他新族使用独立冷路径，保存有界、detached聚合输入，以支持MIN/MAX和乱序Session重分段；key/event/deadline索引、候选、输出分别计费，按key/rows/bytes/timers上限失败，不静默丢弃。旧窗口快照编码不变，匹配旧恢复回归通过。详见 [窗口合同、初次失败及最终证据](WINDOWS.md)；没有新族容量/长稳或整体生产认证。

**2026-09-23 时间型 DAG（限定 Preview 已验收）**：独立 v18/PT、v19/ET 使用 GTD1 决策日志和 GTC1 聚合 cut，持久化来源时间/idle/EOF、固定边序 Union progress 与各 Sink 输出 cursor。File actor 先记录再发布一轮，所有状态/barrier/真实 HTTP flush 到齐后提交 CURRENT；ET future-skew 使用已记录墙钟，PT 使用停机暂停时钟。新协议不改变旧 Union ready-order 或扩大默认预算。完整范围、内存边界、EOF 和重复输出合同见 [DAG](DAG.md#time-graph-recovery)；774/44、专项/真实故障/旧矩阵及原三组 ABBA 通过，见 [匹配证据](PRODUCTION.md#time-graph-validation)。仍非目标容量、长稳或生产认证。

本批性能 Review 将新增有序 Window future 放到计费的冷构造路径，Box 内存先释放再退 credit，避免旧窗口热路径携带新协议内联 future。严格 JSON 对象最多 8 个键时使用有界线性去重，第 9 个唯一键开始晋升随机 HashSet；重复键/转义/嵌套/深度等拒绝合同不变。两项随最终 v13 全量及原门槛验证，不改 checkpoint 正确性或资源配额。

**2026-09-22 线性时间扩展（限定 Preview 已验收）**：独立 v16/v17 承载 PT window、Change/Deadband 正 TTL 和最多两个状态的组合恢复。沿用 TIME_PENDING→时间→数据→barrier→HTTP→CURRENT→ACK；每一级先向下游传播时间，再输出到期行，保证下游的等时到期先于上游衍生行，防止串联窗口落回旧窗或 TTL 被旧时间刷新。所有参与者在输入激活前校验 cut/schema/预算并重建 timer；原 v3～v15 的选择与旧目录拒绝保留。759/44、重复/实际 SIGKILL/旧矩阵及三组 ABBA 已通过；精确范围见 [线性时间合同](IOT.md#linear-time-completion)，来源见 [本批证据](PRODUCTION.md#linear-time-validation)。下面的 PT/TTL 未开放描述是对应历史版本的边界，不代表新 v16/v17；该批不包含时间型 DAG、目标容量和生产长稳。

**2026-09-19 时间型 Preview（首批验证完成）**：仅面向单个线性 HoldFor/Debounce，不改变旧 profile 的时钟。Source actor 按 `TIME_PENDING fsync → ProcessingTime → 可选输入行 → barrier → HTTP flush → CURRENT → JetStream ACK` 串行推进；初始 generation/bootstrap 先于任何输出。算子不采样宿主时间，到期队列按 `(deadline, encoded key)` 有界逐项排出；完整行/索引和快照编码归实际 SourceAdmission owner。新 v14/v15 cut 保存逻辑时钟、序号和原连接器位置，并与 next-output、状态和 timer 同时提交。匹配功能 747/44、22×20、6+3 进程场景、完整旧矩阵与三组 ABBA 已通过，[本批证据](PRODUCTION.md#paused-time-validation)，不继承旧候选的通过结论。停机暂停、逻辑时间采样边界、CURRENT-only、每决策 checkpoint 成本和备份范围见 [时间型 IoT](IOT.md#paused-time-preview)。PT/正 TTL/多状态/DAG 时间恢复仍未开放；新模式的吞吐成本见 OPT-012，不等于高吞吐或长稳认证。

2026-09-17 的 K1～K4 核心完善候选新增独立 snapshot profiles9～13：静态参考表与 Count/IoT(TTL0)、JetStream 可靠输出和 required File DAG 的组合，以及 Hysteresis Bool latch。引用沿用 CPL3、无引用迟滞沿用 CPL1 的外层结构，由新 snapshot/profile 与 kind6/新语义 tag 隔离旧 reader；旧3～8不改语义。26×20专项、9种真实进程及旧矩阵回归已通过，精确范围与性能见 [当前证据](PRODUCTION.md#k1-k4-reference-validation)。停机暂停的可恢复时间协议和高级 IoT 尚未实现，不因新增 snapshot version 就开放 PT/正TTL。

K3 开发候选新增真实 DAG：显式物理边、Branch/Route/UnionAll、多 Connector、有限 side output 与 File 多输入/required HTTP checkpoint。图使用独立 v5 profile 与完整图严格兼容，不改变线性 File/v3 或 JetStream/v4 合同；ET 图必须显式声明 Source time，不能由快输入抢跑 watermark。详细范围、预算、best-effort 脱离策略、恢复限制与本轮验收见 [DAG.md](DAG.md)。这不是任意图/全场景生产认证。

K4 Preview 新增 `change_detect` / `deadband`：按 key 保存 detached 前值，显式首值、无效值、绝对/相对阈值与比较基准；正 TTL 为单调 processing-time 空闲过期，受 key、逻辑 timer 和 bytes 预算约束。支持的 File→HTTP aligned 组合要求 TTL=0，并使用独立 v6 profile，完整语义严格恢复；K4 批次自身不扩展 JetStream、ET/PT 或告警生命周期。已完成全局 Review 和限定矩阵复验：630 核心、36 no-demo、50×20 K4、24×20 K3、32 项 K2、真实进程故障与三组 File ABBA 均通过。配置、限制与构建证据见 [IOT.md](IOT.md#k4-validation)；已随 `1dd17c8` 提交、未 push/tag，不替代目标设备/真实网络/长稳门禁。

后续 Core-A 将 **JetStream→线性 Count/IoT(TTL=0)→required HTTP** 纳入独立 v7 profile，最多两个状态参与者，保持 v4/v6 不混写。抑制行同样进入持久 Source cut，输出 ordinal 只在实际输出时推进；完整状态、输出 cursor、HTTP receipt 与提交后 Explicit ACK 同切点。功能及真实重放已验证，Core-A 自身候选的 File 零状态性能比值 0.960633 未过原门槛，详见 [JETSTREAM.md](JETSTREAM.md)。不把功能通过当作整批性能/生产放行。

B1 静态参考表 Preview 复用 Lookup：catalog v3 保存不可变 revision/SHA，pipeline 固定绑定，所有持久历史 revision 保守 pin，GC 不读截断预览来决定删除。Server 加载表时先占同一 Job admission/owner；blocking 构造及返回结果持有 slot guard，取消等待不会提前释放容量。表 retention lease 覆盖 resident 与构造峰值；Lookup 的选表 Arc、scratch、复制和输出 builder 有预算，nullable stream key 仍按 miss 输出 NULL。B1 **仅 linear restart_fresh**，未将表依赖塞进旧 aligned codec，PT/TTL/temporal 恢复仍不开放。匹配测试、catalog 回退要求与资源限制见 [REFERENCE_TABLES.md](REFERENCE_TABLES.md)；本批回归/性能放行状态以对应证据为准。

B2-A 独立增加 File → stateless Transform/static Lookup → required HTTP 的 **v8/CPL3** checkpoint profile：精确 revision、canonical SHA-256、runtime CRC32 与完整计算语义共同约束恢复；表不伪装成 mutable state participant。必须先解析确切 catalog 依赖并按当前 Job owner 保留，再开放来源；foreign owner、未 verified 表、动态表或依赖变化拒绝。HTTP 未确认或 CURRENT 提交失败不得推进切点，允许旧点后输出重放，不提供 File exactly-once/稳定业务 ID。旧 v3～v7/CPL1 不混写；实现与验证状态详见 [参考表合同](REFERENCE_TABLES.md#b2-a静态-file-aligned-合同)。

R12/K2 执行更新：JetStream 在 Source 侧合并已就绪记录，只有整批成功入队后才推进 published cut；非空 pull 完成不 sleep，仅真正空闲时按 5～250 ms 退避。durable cut 通过有独立 64 KiB 额度的 worker 做最多 16 路 Explicit ACK、每条最多 3 次确认尝试，stop 取消并 join；默认 File 仍不携带 NATS SDK。checkpoint busy 不是 Job 错误，flush 超时单独标记（不伪造 dropped），JetStream 放弃该次提交但继续背压；真实输出/存储错误仍结束 attempt。具体合同与本批测试证据见 [JETSTREAM.md](JETSTREAM.md)。

> K2 新增可选 JetStream Preview，**默认关闭，不替代下方 R11 生产基线**。单来源、零/单 Count/双 Count、单 required HTTP；空 checkpoint 固定 epoch，真实 Sink cut 保存输出 ordinal，durable publication 后才逐消息 ACK。v4 与 File/v3 目录隔离，语义切换/历史 replay/HA/DLQ/outbox 未开放。SDK bootstrap 先占真实 Job slot，同一 owner 的资源与目录锁保持到实际 I/O 关闭；详见 [K2 合同与证据](JETSTREAM.md)。

K2 自查纠正了默认 File 输入的布局回归：新批次不能把完整 `RowBatch` 内联到所有 `IngressEvent` 中。可靠批次改用有独立 metadata lease 的间接句柄，普通事件保持薄布局；Box 先释放再归还 metadata credit。原 v12 零状态 fresh 比值 0.9582 未过 ≥0.97 门槛，失败样本保留；相同参数的 v13 ABBA 为 0.9980（双 Count 1.0149），输出 oracle 一致。这不是提升所有链路性能的声明。

K2 v13 历史收尾：553 reliable 核心、35 独立 no-demo、23×20 专项重复、真实 SIGKILL/ACK 丢失/写入失败/保留过期、默认进程与 File 升级回退通过。100 ms 周期 / 20 ms 应用响应等待 on/off 为 1.0076/1.0133，失败提交 0；源码与二进制指纹见 [历史 K2 证据](JETSTREAM.md#k2-v13)，不作为 R12 或 NATS 容量数据。

R12（2026-09-16）匹配复验：556 核心、35 no-demo、32×20 专项、进程故障与默认路径门槛通过。NATS 单管线预装排空 ABBA 相对 v13 为零状态 17.99×/单 Count 10.98×；默认 2k/s 短程 p99 约 16.5～16.8 ms，10k/s 有积压、p99 约 436～460 ms，不冒充持续 20k/s 或生产容量认证。详细口径、失败样本、源码与二进制指纹见 [R12 证据](JETSTREAM.md#r12-validation)。仍无 TLS/WAN、目标介质掉电与 24/72 h 认证。

> R11：Source 在切点/barrier 按序发布后继续读取，独立单飞 worker 收集 ACK 并提交，退出等待真实阻塞工作收尾。恢复使用 RCP2 的全部状态上游前缀，完整计算保留作诊断；plain CP01 仍按旧严格合同。CPL1 外层可被旧 K1 完整读取后明确拒绝，避免未知新格式导致偷偷回退。详见 [当前恢复合同](PRODUCTION.md#周期-checkpoint-与恢复)。下方 K1 v8 数字是历史基线，不是 R11 复测结果。

R11 HTTP flush 修复：强制 flush 只跳过已排队前缀最后不足一批的 linger，不能把每个 Runtime batch 都拆成 POST。强制状态按请求时的 sent 计数退出，避免新 epoch 到达而错过 `pending()==0` 时把强制模式永久带入后续流量。该计数仅控制合批策略，**不是** checkpoint 的前缀 ACK；Sink barrier 仍等待全部真实 receipts 完成。`Receiver::is_empty()` 仅作合批调度提示，不作为提交证据。

R11 最终匹配复验（2026-09-15 / package-v8）：540 核心、34 独立无 demo、140×20 重复与进程恢复/旧版本回滚反例通过。零状态 100 ms/20 ms Sink 在 6400/25600 输入的 on/off 比为 0.9994/1.0059；双 Count 为 0.9475，均过原 ≥0.90 门槛。fresh 三组 ABBA 过原 ≥0.97 门槛；详见 [完整 R11 证据](PRODUCTION.md#r11-validation)。不是 WAN、设备或 24/72 h 认证。

Window/Dedup 输出 lease 按 `resident_bytes() + 64` 而非逻辑编码字节计费，mailbox permit 使用真实 lease bytes。相同字节容量容纳的输出行可能更少、背压更早，不能沿用旧 `tracked_bytes` 的行数估计。输出先构建本次全部 chunks，再释放 scratch 后发送；瞬时峰值包括 scratch 与这些输出 leases，不是只有一个 chunk。辅助 retention/index 计数使用 checked subtraction/addition；不变量错误记录 `state_accounting_errors_total` 后 panic，不静默 wrap 或清零。正常 stage 路径由 Kernel 捕获为 JobFailed；prepare/恢复调用者路径不能据此假定具有同一个 stage panic 边界，合法恢复输入先经完整校验。owner leases 随 unwind 释放。

> K1 已将 Server aligned 路径扩展为 File 零状态、单 Count/ET 窗口和双 Count 串联；当前协议、codec 迁移与限制见 [PRODUCTION.md](PRODUCTION.md#周期-checkpoint-与恢复)。下文 R9/R10 历史记录中的 single-window 限制仅描述当时受测版本，不覆盖 K1。旧嵌入 `PlanLayout`/`CheckpointSnapshot` helper 仍限单窗口。

> K1 匹配复验：核心 525、独立无 demo 32、关键 120×20 通过；真实进程/旧 codec 双向拒绝、双实例各 1024 keys 通过。零状态 + 20 ms 慢 Sink + 100 ms 周期未过性能门禁，500 ms 频率对照通过；不是全配置/长稳认证。完整数据见 [K1 记录](PRODUCTION.md#k1-validation)。

`compact_kernel()` builds a 2-worker Tokio runtime for tests and in-process
demos. It is **not** the production I/O pool.

`sparrow-server` uses `host_kernel()` (`available_parallelism`, clamped 4–16)
for mailbox / stage futures.

Blocking work must stay off those async workers:

| Work | Where it runs |
| --- | --- |
| SQLite catalog (`Store::*`) | `Store::run_blocking` → Tokio blocking pool |
| Checkpoint encode / fsync / CURRENT | `tokio::task::spawn_blocking` on the aligned commit path; metrics use the real duration and payload length (N15) |
| FileReplay `next_frame` / `decode_frame` | `spawn_blocking` batches of up to 32 frames or ~64KiB (N14), then back to async |

A host that already owns a Tokio runtime should construct `Kernel` with that
process's `Handle` in a later change; until then the Kernel still owns a
dedicated runtime, but SQLite/checkpoint/file no longer sit on the 2 compact
async workers.

`host_kernel()` defaults to Performance-sized process memory caps and per-job
Compact quotas (`max_state_keys = 1024` per operator). `SPARROW_MAX_JOBS=N`
or server `--max-jobs N` (CLI overrides env; 1–256, default 16) sets process
reservation/retention/queue caps to N × the corresponding Compact job cap
(4/4/2 MiB). `host_kernel_with_max_jobs(N)` is the explicit embedding API.
No memory is preallocated by this setting. Child leases bill both ledgers;
admission checks both per-job and process queue capacity and space for every
admitted job's quotas. A plan exceeding its own queue quota is a configuration
failure (not retryable capacity contention).
`Kernel::new_with_job_budget` lets embedders set both budgets explicitly.

K1 的状态参与者共享上述 Job owner。key 数上限沿用每个 operator 的定义；最多两个 state 限制聚合恢复工作量，snapshot 总字节/暂存/恢复 credit 不按参与者倍增。所有实例在输入激活前一次性校验、逐个重建并释放 decoded handoff，不把第一份状态复制给每个 stage。

The root budget is **process-wide**. Each job's child `MemoryOwner` also
bills the root. Admission tracks live mailbox reservations and per-job
quotas so jobs cannot each take a full process budget. `budget()` returns
the process budget; `job_budget()` returns the job quota. `Kernel::new`
defaults to one quarter of process reservation/retention per job. The host
defaults (64 MiB process / 4 MiB per-job reservation and retention) admit
at most 16 jobs; queue capacity may impose a lower limit.

The production Kernel ACK carries leased encoded freeze bytes, not a
cloned `WindowFreeze`. Supervisor wraps those bytes in-place and calls
`commit_encoded` once; oversized state fails before CURRENT. Freeze bound,
reservation acquisition and encoding failures reject only the checkpoint,
not the running job. Structural checks enforce separate retention/reservation
caps; atomic lease acquisition checks current reservation headroom. Each
attempt has a fresh ID and a request-scoped ACK inbox; timeout (including
barrier injection), failure or cancellation drops queued snapshots, and late
ACKs cannot retain leases in an abandoned inbox. Filter/project
and window aggs bind column names at construction (P2-30). Dedup expire pops a
deadline heap prefix (P2-32). Reference tables store a crc32 and fail
closed on mismatch (P2-36).

Decode errors stay counted by default. `fail_on_decode` on the pipeline
spec, or `SPARROW_FAIL_ON_DECODE=1`, fails the job (P1-17).
File records above 64 KiB are discarded with bounded memory until newline
or finite EOF, then counted once as a decode error. AppendOnly EOF retains
discard progress; the durable cursor stays at the preceding record boundary
until the oversized record ends. A scan-budget yield is `FilePoll::Pending`,
never a terminal EOF, so huge records cannot monopolize a blocking worker
or prematurely finish a sealed stream. CRLF blank lines are ignored.

Legacy raw-row MQTT library ingress still rejects `inbox_capacity × max_record`
above 4 MiB or its queue budget (P1-27). The server now uses byte-accounted MQTT
ingress, described below; the static guard is not removed from the legacy API.

R3 also moves synchronous management-API catalog operations to the blocking
pool, makes sink loss sticky across checkpoint retries, and keeps failure
counts until 30 seconds of stable running. Retry backoff is capped at 32
seconds. Retryable capacity admission failures do not consume the crash
cap; actual status is `waiting`, with a separate 500 ms → 1 s → 2 s → 4 s →
5 s capped capacity backoff. Successful start, explicit start, revision change
or stop clears the capacity retry state. Held jobs preserve their original
failure reason.

Capacity waiting requires the explicit `admission=capacity` error context in
addition to retryable `ResourceExhausted`; other resource errors remain failures.
Repeated capacity checks for the same waiting revision update status details
without appending `starting`/`waiting` history rows or increasing `attempt_id`.
A successful admission records a new running attempt. The history remains
globally bounded to 100 rows, but it no longer decides whether a job may restart.

Repeated `/start` for the same **actually running** revision preserves its actual
status, revision and attempt ID; it does not launch a duplicate job. Other states
(including failed/waiting/completed) and revision changes still reset to stopped
for reconciliation. Explicit start continues to clear the crash count and latch.

Safe-mode reads the durable `actual_state.restart_blocked` latch. Recording a
failure sets the latch in the same transaction as the failed status; history
eviction, process restart, and crash-counter reset cannot clear it. An explicit
`POST /start` (also used by `/restore`) atomically commits desired running state
and clears both the latch and crash count. A successful run clears the latch too.

Pipeline status responses expose `actual.consecutive_failures`,
`actual.restart_blocked` and top-level `safe_mode`. The latch is persisted even
outside safe-mode, so `restart_blocked=true` alone does not mean auto-restart is
currently held. Likewise a running job may retain a nonzero crash count until
the 30-second stable-run threshold. `held:` messages distinguish safe-mode from
the consecutive-failure cap; do not treat a positive counter as a failed status.

Converge reads and validates actual state once per start candidate. A per-pipeline
state/hold-check error skips that pipeline (never authorizes its start), logs
`pipeline_converge_skipped` with its name and error, and continues other pipelines'
start/stop handling. Global failures such as reading the desired-state list log
`supervisor_converge_failed` and retry on the next iteration.

Catalog schema **v2** adds this latch. Opening a v1 catalog transactionally
migrates existing failure evidence from actual state and any retained history;
missing history falls back conservatively to the failure count/hold message.
V2 reopen never reconstructs a cleared latch from old logs. Back up the catalog
before upgrading; older binaries reject v2, so rollback requires the matching
pre-upgrade catalog backup. Checkpoint encoding and `FORMAT_VERSION=1` are unchanged.

`sparrow-server` installs a stderr `tracing` subscriber (`RUST_LOG`, default
`info`). Aligned checkpoint commits log `checkpoint_commit` at info with id /
bytes / duration.

## Compatibility and implementation limits

### Production transport and scheduling fixes (2026-09-11)

- **HTTP response consumption is a production fix**, not a benchmark-only
  adjustment. Returning on 2xx without consuming a nonempty HTTP/1.1 body can
  prevent connection reuse, causing repeated TCP connections and additional
  HTTPS handshakes. The sink now drains successful responses with a 64 KiB
  threshold and the existing request timeout. Oversized/truncated responses
  may discard the connection, but never retry a POST already accepted with
  2xx. Empty/204 responses are not evidence that the old nonempty-body path
  reused connections. Pooling still depends on the peer. Encoded payloads
  are now owned once and shared across retries, not copied on every attempt.
- **MQTT keepalive is independent of inbound traffic.** The PINGREQ deadline
  survives each `select!` iteration and advances after sending a ping.
  Continuous PUBLISH/PINGRESP reception and full inboxes cannot reset it.
  Previously rebuilding `sleep(keepalive / 2)` could suppress pings forever.
  The default keepalive is 30 s; the protocol's 1.5x timeout is 45 s (broker
  enforcement timing can vary). Ten-second trials did not exercise the fault.
  Idle and continuous-traffic regression tests require multiple pings on the
  same connection. See [MQTT 3.1.1 §3.1.2.10](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html).
- **AppendOnly EOF no longer blocks checkpoint commands.** EOF returns a wait
  state; the source loop selects the 40 ms poll deadline alongside commands
  and cancellation. Checkpoint handling preserves the existing poll deadline.
  Blocking reads remain outside cancellable `select!` futures: a command must
  not abandon an in-progress read and lose its source cursor or rows.
- Live ingress shares an immutable schema and moves owned rows into batches.
  JSON batch encoding writes into the final buffer without per-row byte vectors.
  Type checks, credit limits, output order and failure semantics are unchanged.
- Ingest metrics count source batches once, including memory, ordered live and
  split-channel live input. Job completion/stop no longer adds live input a
  second time. This is processing-attempt ingress, not a unique-message delivery
  guarantee; snapshot/source-cursor counters are unchanged. `/v1/metrics` now
  exposes `io.mqtt_reconnects` and `io.http_retries`. `io_scope=running_attempts`
  makes clear that I/O totals cover currently supervised attempts, not durable
  history or a monotonic total across stopped/replaced jobs.

### MQTT burst handling

MQTT uses a bounded **wait-then-drop** policy when the inbox is full. Set
`source.inbox_wait_ms` in the pipeline JSON (MQTT only, integer 0..=1000):
omitted means **5 ms**; `0` restores the old immediate-drop behavior. Existing
stored specs remain readable and use the new default on their next start;
set `0` explicitly if immediate shedding is required. Embedders use
`MqttSourceConfig::inbox_wait_timeout`.
Older binaries reject an explicitly stored `inbox_wait_ms` field; account for
that when preparing a config/catalog rollback.

The fast path offers the current row without waiting. On Full, the single
source pump retains only that row and stops reading more packets. The raw-row
library path waits on a channel permit; the server also waits for byte credit
(a bounded 1 ms retry when shared credit is exhausted). A row stays outside
cancellable send futures and deadlines are not restarted by heartbeats. No extra
shadow queue or forwarding task is created. The encoded frame is freed before
waiting. See [Tokio Sender cancellation safety](https://docs.rs/tokio/latest/tokio/sync/mpsc/struct.Sender.html#method.send).

`io.mqtt_backpressure_waits` counts entries into this wait, and
`io.mqtt_backpressure_recovered` counts rows subsequently enqueued. Actual
timeouts (or immediate Full at 0 ms) increment `io.mqtt_dropped_full`, not
temporary fullness. Stop/connection failure may abandon the current row;
these counters are diagnostics, not delivery receipts. Their scope remains
currently supervised attempts.

`mqtt_dropped_full` has always counted only this connector's local full-inbox
drops, **not total loss**. Under sustained overload, stopping socket reads can
increase latency in TCP/broker queues; each expired wait drops only the current
row before reading continues. Broker-side queue limits may instead drop data or
disconnect the subscriber. Monitor broker queues/drops/disconnects and exact
end-to-end delivery/age, not just this counter. Immediate-drop mode also does not
promise that the newest event is retained or enforce an event-age limit.

TCP receive buffers predate this policy but may be used more heavily now. They
are external kernel memory, outside Sparrow's queue ledger and process RSS;
broker memory is external too. For the isolated Linux benchmark host the observed
`tcp_rmem` was `4096 131072 6291456` and `tcp_moderate_rcvbuf=1`: initial receive
buffer 128 KiB, autotuning ceiling 6 MiB **per socket**, not a preallocation or a
whole-system memory cap. Sparrow does not currently set `SO_RCVBUF`. Recheck the
target host/network namespace, inspect per-socket `ss -m` and deployment memory
limits, and multiply external limits by actual connections. Do not assume that
6 MiB is universal. See [Linux TCP buffer controls](https://docs.kernel.org/networking/ip-sysctl.html#tcp-rmem-vector-of-3-integers-min-default-max).

This trades a bounded enqueue wait for better microburst tolerance; **5 ms is
not an end-to-end latency bound** (broker/socket/downstream queues and scheduler
delays still exist). Sustained overload may still drop locally or at the broker.
It does not add MQTT ACKs, durable buffering, replay, or a lossless guarantee.

### MQTT byte-accounted ingress and optional QUICKACK

The server always uses `run_budgeted` + `with_budgeted_live_io`. Pipeline
`source.inbox_bytes` defaults to **256 KiB** of decoded-row Queue credit;
`inbox_capacity` is still an independent item bound (1..=4096, unchanged default
16 in stored-spec decoding). Row credit counts Vec capacity, scalar storage,
nested strings/bytes/arrays/objects, Arc/header allowances and envelope overhead;
shared payloads are conservatively charged in full per row. This is a memory
estimate, not wire JSON length or an allocator/RSS guarantee.

The Kernel separately reserves a conservative channel slot/block allowance for
the whole receiver lifetime, including empty retained blocks, and includes it
in job/process admission. Configured payload bytes **plus** this allowance must
fit the job Queue budget and static 4 MiB ceiling. A 4096-slot inbox may therefore
need a smaller payload limit than 2 MiB. Dynamic acquisitions bill inbox, job and
process ledgers; sibling pipelines cannot bypass the shared limit.

The one decoded working row takes Reservation credit before waiting. Queue
admission acquires Queue credit before releasing that working lease. The Kernel
then acquires batch Reservation credit before releasing Queue credit. EOF,
cancellation, invalid rows and failed handoffs release leases through ownership.
Wire records remain <=64 KiB; decoded rows larger than the configured inbox or
Kernel single-batch/mailbox byte limit are explicitly dropped, not retried
forever. `fail_on_decode` concerns decoding errors, not an overload guarantee.
Embedding callers of `run_budgeted` must use the matching accounted receiver
path/owner (or reserve channel overhead for their own receiver lifetime).

New I/O diagnostics (scope: currently supervised attempts):
- `mqtt_inbox_items/bytes`: admitted rows, including rows being transferred to
  a batch until destination credit succeeds; not all Kernel mailboxes.
- `mqtt_inbox_metadata_bytes`: fixed channel allowance, separate from row gauges.
- `mqtt_inbox_peak_items/bytes`: per-attempt high watermarks; the aggregate is
  the **sum of per-attempt peaks**, not a simultaneous process peak.
- `mqtt_pending_bytes`: Reservation-billed working row awaiting admission.
- `mqtt_accounted_sources`: attempts using the accounted path.
- `mqtt_dropped_oversize`: decoded rows exceeding the single-row bound.
- `mqtt_dropped_budget`: byte-credit timeouts or working-credit failures.
  Byte-credit timeouts also count in `mqtt_dropped_full`; do not blindly sum
  overlapping reason counters. Snapshots of atomics are not transactional.

`source.tcp_quickack` is **false by default**, opt-in on Linux; explicit true
is rejected on other server platforms. A TCP transport wrapper rearms QUICKACK
after successful nonempty **socket** reads, below TLS, not per MQTT message or
TLS plaintext read. Verification of certificates/hostnames is unchanged. A
failed setsockopt increments `mqtt_quickack_errors` and disables further attempts
for that session; it does not drop messages. `mqtt_quickack_calls` exposes the
attempt count. Reconnect creates a fresh transport. The option is temporary
kernel state, not a permanent per-segment guarantee; evaluate CPU/ACK traffic
and latency on the actual network before enabling it.
See [Linux TCP_QUICKACK](https://man7.org/linux/man-pages/man7/tcp.7.html).

Deployment note: byte accounting is enabled by default on the server MQTT
path; QUICKACK is not. In the measured Linux loopback/Mosquitto setup, 10k/s
with QUICKACK off had about 40 ms p99, versus about 0.8 ms with it on. These
trials used 32 inbox slots, not the stored-spec default 16, and are not a
default-configuration lossless guarantee. For a colocated broker and latency-
sensitive workload, evaluate `source.tcp_quickack=true` or the broker's
`set_tcp_nodelay true`, changing only one side at a time. Measured server CPU
increased by roughly 2.2/6.6 percentage points of one core at 10k/20k; this is
the whole profile's cost, not isolated setsockopt overhead or a WAN forecast.

### HTTP coalescing, bounded concurrency and flush

HTTP sink tuning is opt-in and does not change delivery/replay guarantees:

| Pipeline `sink` field | Default | Contract |
|---|---:|---|
| `batch_rows` | 1 | 1..65536 coalescing **target**; an existing larger upstream batch is sent whole, not split |
| `batch_bytes` | 262144 | 2..1048576, hard JSON request-body bound including brackets; an oversized upstream batch fails delivery |
| `linger_ms` | 0 | 0..1000, flush-readiness deadline from the first batch; not a hard dispatch or end-to-end age bound |
| `max_inflight` | 1 | 1..8; **values above 1 explicitly allow delivery/completion reordering** |

Defaults retain one POST per upstream batch and one in-flight request. They add
a strict 256 KiB body bound: deployments with larger encoded upstream batches
must choose an appropriate `batch_bytes` and budget. `(batch_bytes + 512) ×
(max_inflight + 2)` must fit the static 4 MiB working-buffer ceiling. This is a
conservative configuration bound, not a permanently reserved amount. Actual
delivery credit is **buffer capacity + 512 bytes**, acquired incrementally:
512 bytes before encoding, then capacity growth before each allocation. Small
requests can fit small job quotas even with a large `batch_bytes` ceiling;
large requests still need enough job/process headroom alongside their input
batches and other working buffers. `compact_kernel` defaults to a
1 MiB per-job Reservation quota; production host jobs default to 4 MiB.

The collector merges only compatible schemas/owners, never clones source rows,
and flushes at the row target, byte boundary, fixed linger deadline or EOF.
An expired deadline makes the collector ready to flush; saturated request slots,
queued input/carry and scheduler delays can still postpone dispatch.
It holds at most one collector plus one carry batch and bounded request tasks.
The encoder checks bytes before final-buffer growth; the request buffer takes
incremental Reservation credit before growth and retains it across shared-body
retries. Capacity starts at up to 256 bytes and grows geometrically, clamped to
the hard body limit; unused capacity remains billed. Merge keeps both buffers
billed while acquiring destination growth. If that acquisition is denied,
the collector flushes the two groups separately through its carry path, with
no failed receipt merely because coalescing lacked headroom. Encoding admission
failure instead fails that input receipt and counts `http_budget_drops` (not a
JSON syntax error); partial buffers and credit are released. Allocation failure
may retain a conservative growth reservation until that delivery is dropped.
This bounds the final body, not every temporary serde allocation or RSS page.
Reservation is separate from the Retention ledger used by window/dedup state.

An aligned barrier requests a collector flush through `InflightCounter`, even
if the collector has not received the last enqueued batch yet. Every constituent
input batch receives exactly one success/failure settlement; grouping changes
POST count, not the number of required ACKs. The barrier still waits for all
prior successes and refuses failed cuts. EOF flushes/drains; explicit stop
cancels pending/in-flight work, fails unresolved receipts and joins workers.
Cancellation after an ambiguous transport write is not a delivery guarantee.

`http_posted` counts successful requests, `http_acked_batches` acknowledged
upstream batches, `http_dropped` failed/abandoned upstream batches.
`http_encode_errors` and `http_budget_drops` explain pre-send failures;
`http_failed/retries` retain transport-attempt semantics. Error reason counters
overlap drops. Existing 2xx drain/no-duplicate-retry handling remains unchanged.

At 20 ms RTT, the serial ceiling remains about 50 POST/s. Coalescing changes
rows/POST; concurrency can increase POST/s but may increase downstream pressure,
reordering and duplicate exposure on retry. Neither solves sustained overload,
durable replay or event freshness by itself; validate remote RTT independently.

### Runtime mailbox observations (OBS-02 / AGE-01 initial slice)

Kernel jobs now prepare a bounded observer for every physical mailbox edge.
`GET /v1/pipelines/{name}/status` adds `mailboxes`; `GET /v1/metrics` adds
`mailboxes.jobs`, containing named per-attempt snapshots. No Prometheus exporter,
connection-health classification or end-to-end latency distribution is added in
this slice. Existing `queue_items` / `queue_bytes` and
`queue_metrics_available=false` remain legacy, whole-path-unavailable fields;
the new namespace explicitly covers **Runtime mailboxes only**.

Each edge reports its physical stage indexes/kinds, configured item/byte caps,
and the following distinct measurements:

| Field | Meaning |
|---|---|
| `queued` | Successfully published, not yet received envelopes; items/data_batches/rows/controls/accounted_bytes are separate |
| `consumer_held` | Received Envelopes not yet dropped; this is not all operator state, CPU activity, or a business completion count |
| `credits_reserved_bytes` | Logical byte permits held by pending senders, queued/consumer-held Envelopes and envelopes being drained on close; not queue depth |
| `metadata_bytes` | Conservative Queue-ledger allowance for the preallocated timestamp ring, observer/edge records and channel Envelope/block storage; not allocator-exact RSS |
| `peak_queued_items/bytes` | High-water marks of successful queue publication, not pending send attempts |
| `oldest_queued_age_us` | Monotonic residence age of the oldest queued envelope, including controls; null when empty |
| `oldest_queued_data_age_us` | Same clock, oldest queued data batch only; null if only controls or no data are queued |
| `send_attempts_total` | Calls that entered the send path; not received or delivered source messages |
| `enqueued_items_total/received_items_total` | Successful publication / mailbox receive transitions; both count data and controls |
| `discarded_on_close_total` | Envelopes still queued when the receiver closes; not all downstream/source losses |
| `aborted_sends_total` | Unpublished sends ending in rejection, close, cancellation or dropped future; not a delivery receipt |
| `blocked_sends_total/waiting_senders` | Sends that encountered insufficient byte/item capacity once per send / those whose capacity wait has not ended |
| `completed_waits_total/completed_wait_us_total/max_completed_wait_us` | Finished capacity waits, including cancelled/failed waits; duration runs from first observed capacity block to admission or abortion and includes scheduling delay; ongoing waits are excluded |

`Envelope::take()` can move payload beyond the Envelope lifetime. The batch's
own lease follows that payload; dropping the Envelope releases its mailbox byte
permit and consumer-held count, not the batch's backing allocation. Shared
payloads count logical bytes on each edge, without claiming multiple physical
allocations. Byte accounting uses `RowBatch::tracked_bytes().max(1)` or one
permit per control, not serialized HTTP size or wire bytes.

Queue timestamp storage is preallocated to `max_items`. Publish and channel pop
share a short **per-edge**, not global, critical section so a concurrent sender
cannot overrun the timestamp ring or change its FIFO association. No await is
held under that lock. Receiver destruction clears queued observations before
draining envelopes and closes the byte semaphore: a byte-blocked sender must
wake even while an external consumer still holds the last Envelope.

Metadata is charged before allocation and included in job/process queue
admission. Tight custom budgets that previously fit payload caps alone may now
be rejected; no quota is silently increased. An embedding caller retaining a
`JobMailboxObserver`/`MailboxObserver` keeps only that bounded metadata pool
billed until the last handle drops. Snapshot values do not retain payload or
owner handles. Additional standalone producer task/future storage remains the
embedder's responsibility. Legacy `mailbox::channel` is unobserved; embedders
can explicitly use `observed_channel` with a `MemoryOwner`.

The API uses `running_revision` for the active control-plane revision and
`runtime_attempt_id` for the Kernel execution ID, distinct from the catalog's
lifecycle-transition `actual.attempt_id`. `plan_revision` is the physical plan
revision (Server now binds it to the committed catalog revision; the saved authoring GraphSpec is not rewritten). Stage indexes are scoped to
that plan/attempt, not stable identifiers across revisions. No device/payload
labels or unbounded completed-job registry are created.

Initializing/busy/no-active-attempt states are explicitly unavailable, not
healthy empty queues. A finished handle may briefly expose its drained snapshot
until Supervisor reaps it; afterward no historical sample is returned. Each
mailbox snapshot is internally consistent, but different edges, catalog fields,
and API calls are not a transactional whole-system snapshot.

Age uses `std::time::Instant`, starts at successful mailbox enqueue, and is not
persisted. It is **not** time since broker publication, source ingestion, event
time, HTTP ACK or business completion. Coverage flags for source inbox, sink
outbox, transport and end-to-end age remain false. Connector inbox/HTTP counters
in `io` retain their separate existing scope. Full AGE-01 provenance/clock
handling, OBS-01 health and OBS-03 latency remain later slices; no expiration,
coalescing or message-loss policy changes are introduced here.

#### OBS verification (2026-09-12)

Final OBS code was tested on the same isolated x86_64 Linux host with
rustc/cargo 1.98.0. Core release tests: **430/0**; the separately built no-demo
Server combination: **21/0**. A selected set of **12 tests x 20 rounds** passed,
including concurrent producers, byte/item waiting, dropped send futures,
Receiver close with a consumer-held envelope, control cleanup, shared backing,
metadata admission, attempt isolation and API coverage. The first run's new
test waited for producer blocking but not for consumer acquisition; its fixture
synchronization was corrected without removing the consumer-held assertion.
Final metadata admission also includes channel Envelope/block storage, not only
the timestamp ring. Earlier logs/binaries are retained separately.

The actual production binary passed the existing clean-directory Count restore
smoke (10+20, checkpoint/restore, append 30 => one sum 60), including incompatible
upstream rejection and no-demo refusal. A normal MQTT trial at 2,000 events/s
with 10,000 inputs produced all expected 7,500 filtered outputs, without missing,
duplicate or invalid rows. It used an application response wait of 20 ms,
HTTP batch target 64, linger 10 ms, max-inflight 4 and QUICKACK enabled; this is
not a real-WAN RTT or keepalive/long-soak certification.

A deliberate pressure trial used the existing driver's 8-item MQTT inbox,
2,048 burst inputs, 8 ms application response wait and 4 s drain window. It
recorded 1,409/1,536 expected outputs and 170 input `mqtt_dropped_full`, not a
lossless success. Both Runtime edges reached 32 queued items; sampled maximum
oldest-data ages were 306,710/306,720 us, with 172/205 blocked sends. Across 45
samples per edge, queue conservation and configured caps held. End-of-observation
Runtime queues/held credits were zero. Queue residence is not end-to-end age;
connector errors/drops and capture arrival remain distinct observations.

An ABBA before/after screen used the same immutable driver and the BASE V2
production binary versus OBS: 80,000 File inputs, Count size 800, 2 measured
rounds per invocation, no periodic metrics sampling. All **8 measured + 4 warmup
trials** had complete, correct output; measured output hashes matched. Median
input rates were 476,595 vs 490,368 events/s; sampled peak RSS across measured
trials was 14,840 vs 15,012 KiB (+172 KiB). This did not trigger the predeclared
0.90 throughput-ratio / +1,024 KiB screening thresholds, but the overlapping
short-run ranges are **not evidence of a speedup or universal overhead bound**.

Evidence root: `/workspace/bench-compare/obs-artifacts-20260912/v3/`, including
test logs, `validation-plan.json`, `matrix.sh`, `healthy.sh`, `restore-smoke.sh`,
raw result/metrics timelines, `pressure-summary.jq`, summaries and exact binaries.
Test source: `/workspace/bench-compare/obs-source-20260912/`; tested patch
`source-v3.patch` SHA-256:
`18f337c046c5ad11a1ee169e595577fb1941e6b458cd5fc7d50e290725555989`.
Production binary SHA-256:
`aeb31d082db489534cea6a63244bad1d98e42982c7803223e4551003dbf26641`.
The 19 changed/new Rust files match the remote test source; documentation-only
result notes were added afterward. No local compilation or production service
replacement was performed. OBS-01/03, full AGE-01, Prometheus and release soak
remain open.

### Connector / runtime observation closure (2026-09-12)

`GET /v1/pipelines/{name}/status` now also exposes `observation`;
`GET /v1/metrics` exposes `observations.jobs`. These are authenticated, bounded,
**active-attempt** views, not durable history. The old `mailboxes` namespace still
describes only Runtime edges, and the old whole-path `queue_metrics_available`
remains false. Use the new boundary views rather than interpreting legacy zeroes.

#### Scope and interpretation

| Field | Meaning / limitation |
|---|---|
| `running_revision`, `runtime_attempt_id` | Actual running configuration and Kernel execution ID; not the latest stored revision or the catalog transition counter |
| `source`, `sink` | State, bounded reason/error code, state-change age and last-progress age; no device labels, payloads, credentials or arbitrary error text |
| `source_inbox`, `sink_outbox` | Actual channel-published items/rows/logical bytes, peaks, oldest enqueue age, capacity waits and queue residence distributions |
| `runtime_progress` | This attempt's ingested/final-output rows and transform-filtered rows; final Runtime output is not external delivery |
| `source_pending_accounted_bytes` | MQTT decoded working row while admission is pending; separate from published inbox data; null for other Source kinds |
| `delivery.active_input_*` | Dequeued upstream batches/rows and their logical input-byte size until terminal handling, including coalesced groups; not retained physical input memory |
| `delivery.encoded_credit_bytes` | HTTP encoded allocation credit, including fixed delivery overhead; retained through collection, carry, in-flight delivery and retry; not RSS |
| `http_active_delivery_groups` | Existing HTTP worker-group gauge, including backoff; distinct from `delivery.active_http_requests` |
| `delivery.http_attempts_*` | Started, finished and cancelled HTTP attempts, exposing in-flight/censored work instead of hiding it from latency reports |
| `delivery.completed_*`, `failed_or_cancelled_*` | Terminal outcomes for dequeued inputs. Shutdown-discarded channel entries appear separately in `sink_outbox.discarded_on_close_total` |
| `diagnosis_reasons` | Current evidence of admission, queue pressure, retry/failure, requests in flight or cancellation; not a guessed health score or automatic expiry policy |
| `legacy_io_counters` | Original I/O counters restricted to this attempt, still potentially combining Source and Sink roles; not a source-only conservation ledger |

File append EOF is `waiting_for_append`; finite EOF is `eof`. MQTT is `ready`
only after its handshake/subscription, with separate `connecting`/`reconnecting`
states. HTTP `ready` describes a request-ready client or its latest result,
**not a continuously connected TCP pool or a guarantee the next request succeeds**.
Log progress means a local write attempt, not durable storage. Source progress
units are MQTT publishes / HTTP push requests / decoded File rows; sink progress
units are successful HTTP response headers or written MQTT/Log rows. Do not sum
different units. A quiet, connected source is not marked failed just for silence.
For example, legacy `mqtt_decoded` includes MQTT Sink writes as well as Source
decodes, and `http_posted` can include HTTP Push input requests; their mixed-role
scope is explicit rather than presented as source-only outcomes. Budget/full
drop counters can overlap. Use the boundary/Runtime/delivery measurements and
their individual units; do not sum them into an invented exactly-once ledger.
Last error codes/counters remain visible after recovery; current state is separate.

Snapshots are coherent per component, not an atomic transaction across the
pipeline. Compare attempt IDs before correlating independently read status,
mailbox and metrics views. A reaped/stopped handle is `no_active_attempt`, registry
contention is `registry_busy`, and no sample is null/unavailable, never healthy
zero. Catalog lifecycle/failure fields remain the source of post-reap status;
absence from the active registry is not proof of downstream business completion.

#### Time and histogram contract

- All observation clocks use local monotonic `Instant`, independent of processing
  or event-time semantics. No observation timestamp is encoded in a checkpoint.
- MQTT/HTTP push origin starts at a complete locally received publish/request
  before decode. File origin starts when a decoded poll batch becomes ready;
  its progress counts decoded rows once per read batch, not channel admissions,
  while queue publication/ingest still count each item. Its separate
  `file_read_decode` histogram includes blocking-worker scheduling/read/decode.
  Neither origin includes upstream broker/device waiting.
- Batching merges earliest/latest origin bounds; share/detach and Filter/Project
  preserve conservative **input-batch** bounds. Filtering can remove the oldest
  input: this does not become an exact output-row age. Dedup/Lookup output carries
  current input bounds, not state/table age. Window aggregate output is explicitly
  unknown because complete input lineage is not retained. Unknown input bounds
  poison a merged bound rather than inventing a precise timestamp.
- `local_origin_to_sink` is the oldest known bound at a terminal delivery-group
  observation, not an exact per-row p99 or business acknowledgement. Unknown
  origins are counted separately. Restored aggregate state does not resurrect a
  pre-restart monotonic timestamp. Device event-time age, broker waiting, exact
  wire-send timing and business ACK remain explicitly unavailable.
- Queue occupancy, publication/pop counts and oldest age remain exact. Boundary
  queue residence histograms sample every 16th pop (`residence_sample_every=16`)
  to limit ingress overhead; capacity waits and Runtime mailbox residence are
  recorded without that sampling. Quantiles describe the sampled distribution,
  and the 100-sample minimum refers to recorded samples, not total input rows.
  Runtime compute histograms describe synchronous batch calls by operator kind,
  not every timer/checkpoint operation or a per-row CPU profile. Decode, encode,
  HTTP response headers, completed response body and delivery-group duration have
  separate histograms. Cancellation before an HTTP response is a cancelled
  attempt, not a completed header/body sample.
- Histograms are cumulative within an attempt, use 32 fixed exponential buckets
  (1 through `2^30` microseconds plus overflow), and store no event list.
  `p50_upper_us` / `p95_upper_us` / `p99_upper_us` are **bucket upper-bound
  estimates**, not exact quantiles. Fewer than 100 samples or an overflow quantile
  returns null. `samples`, buckets, sum and max are read together. Operation and
  coalesced-delivery samples are not interchangeable with row samples; failures
  and cancellations remain in outcome counters, not discarded from reports.

For HTTP, successful delivery retains the existing contract: 2xx headers followed
by a bounded response-drain **attempt**. A truncated/oversized/failed response body
increments `http_incomplete_response_bodies_total`, is not a completed-body sample,
and does not retry an already accepted POST. It is not a downstream business ACK.
Verified HTTPS has a fixture-based test with an explicit test-only CA; the default
production trust configuration still rejects that private fixture. This does not
introduce a production custom-CA or skip-verify option.

#### Ownership, queues and production fixes

`sparrow-io` now has an optional `observation` feature providing bounded observed
channel adapters; `sparrow-model` remains dependency-free. Existing Tokio handles
can be passed to the public run/JobRequest builder methods via `Into` and remain
unobserved. Direct users of public `JobRequest` I/O fields must adapt handle types.
Observed queues/flow handles must be initialized once, before traffic, for one
attempt; the Server does this through Kernel admission, not by sharing a global
observer between jobs.

R9 enforces the boundary-channel rule: first send/reserve/receive/close/wait
fixes an uninitialized channel in unobserved mode. Subsequent `initialize`
returns `InvalidArgument`, rather than attaching stamps halfway through traffic
and panicking on pop/drop. Initializing an observed channel with another owner
also fails. Read-only capacity/observer/snapshot queries do not start traffic.
Existing raw Tokio conversions remain unobserved. Flow-handle initialization
still belongs to Kernel admission; no dynamic observation toggle is introduced.

Timestamp rings and flow statistics acquire Queue metadata credit before their
sizeable allocations. The fixed allowance includes channel storage and is part
of admission; tight custom quotas may now reject a plan rather than allocate
unaccounted observation buffers. Queues count publication, not reserved slots;
publish/pop use a short per-channel lock with no await. Closing, cancellation,
outstanding permits and drain are accounted separately. Views can retain bounded
metadata credit after a handle ends, but never retain its payload as history.

This batch also fixes production issues exposed by the observation tests:

1. **HTTP worker capture:** Rust 2021 disjoint async-closure capture could move
   only `Delivery.bytes`/`receipts` into a task and drop its lease before the
   request finished. The worker now takes the **whole Delivery**. In-flight
   credit is tested while requests are pending, not only after completion.
2. **MQTT Sink liveness:** the sink now reads broker control/close events while
   idle, sends keepalive PINGREQ, bounds PINGRESP/handshake/write waits, and exposes
   reconnects. It is still QoS0, not replayable or broker-acknowledged delivery.
   The Source also validates SUBACK's packet ID and QoS0 success code: a denied
   subscription must not be presented as a ready subscribed connection.
3. **Terminal cleanup:** MQTT partial/failed/cancelled batches fail their receipt;
   unread queued batches are failed on shutdown. Log encode failures no longer
   acknowledge an entire failed batch. HTTP Push connection tasks are joined or
   aborted on shutdown instead of remaining detached beyond their source lifetime.

These fixes do not enable automatic expiry/merge/drop policies, stable business
IDs, reliable ingestion, full state lineage, a persistent metrics store, UI or
Prometheus export. Per-attempt observability is not a full-process RSS hard cap.

Verification evidence for this closure is recorded separately from the initial
OBS mailbox evidence above; implementation is not itself release certification.

#### Closure verification record (final v12)

Artifacts: `box@100.64.0.16:/workspace/bench-compare/obs-closure-artifacts-20260912/v12/`.
The final tested build uses Rust/Cargo 1.98.0 on Linux x86_64. There was no local
compilation, commit, tag, push, service deployment or 72-hour soak in this batch.

- Core/default-members release: **445 passed, 0 failed**. Independent
  `sparrow-server --no-default-features`: **21 passed, 0 failed**; these feature
  combinations are not 466 distinct tests. **28 selected tests × 20 rounds**
  passed without rebuilding the test binaries between rounds.
- Verified HTTPS connection reuse, rejection of the test CA by production-default
  trust, incomplete response bodies, denied MQTT subscriptions, MQTT reconnect/
  keepalive, cancellation, late permits, multi-Job isolation and bounded metadata
  are covered by focused tests. Actual Server APIs exercise a slow/failing HTTP
  Job alongside an idle/healthy File Job, revision changes and attempt reset.
- Fresh-directory production smoke again restored the Count aggregate to **60**;
  changed Map/earlier Filter were rejected without changing CURRENT. Demo CLI/API
  remained unavailable in the production build.
- Normal MQTT: **10,000 inputs → 7,500/7,500 expected filtered outputs**, no
  missing/duplicate/invalid rows; **463 POSTs over 3 TCP connections**. With a
  20 ms application response wait, capture-arrival p99 was **11,236 µs**—not ACK
  latency and not a 20 ms WAN test.
- Continuous measured MQTT: **120,000 inputs → 90,000/90,000 outputs**, no
  missing/duplicate/invalid rows over **120.006 seconds**; **19,010 POSTs over
  1 connection**. This covers repeated keepalive intervals, not multi-day leak
  freedom or a general capacity guarantee.
- Intentional best-effort overload: **1,413/1,536 outputs**, **123 missing**,
  no duplicates/invalid rows. This trial remains **invalid as a lossless run**.
  The observed source queue reached 8 items; sampled source/Sink waits reached
  399/233, with ~8.9/18.7 ms oldest boundary-queue ages. Missing outputs were not
  removed from throughput/latency evidence.
- Across normal/pressure/continuous timelines, **254 observable snapshots / 508
  boundary-queue snapshots** passed per-component count, histogram, capacity,
  delivery and HTTP-attempt invariants. These are sampled maxima and component
  consistency checks, not an atomic end-to-end receipt proof.

**Performance remains an explicit review item.** Final File ABBA has 8 measured
trials plus 4 warmups, all with the same normalized output hash
`1bdb251814aec266`. Median input throughput was **499,938 → 448,929 events/s**
(ratio **0.898**, about **10.2% lower**); maximum sampled engine RSS was
**15,096 → 15,564 KiB** (+468 KiB). These short host trials include configuration
submission through last sink arrival. Initial unsampled observation was about
14.3% lower in a separate comparison. Batched File progress, explicit 1/16
boundary-residence sampling and moving payload sizing outside the publication
lock reduced the observed cost, but **the final ratio still slightly crosses
the predeclared 0.90 investigation line**. No universal overhead bound or speedup
is claimed; OBS-02 performance acceptance stays open rather than changing the
threshold to manufacture a pass. Earlier candidate results are retained.
This v12 failure is historical; the R9 follow-up below separately closes the
File cost investigation for its tested configuration, without rewriting v12.

**Network-delay gate NOT RUN:** a new isolated network namespace was available,
but the kernel rejected `tc ... netem` with `Specified qdisc kind is unknown`.
`netem-status.json` records this limitation. HTTP application waits and the TLS
fixture are not substituted for this missing packet-delay test. Host loopback
qdisc configuration was unchanged; the existing benchmark Mosquitto PID 2657777
was not replaced or stopped. Only owned test children were used and cleaned up.

The early failed tests are retained: they exposed the HTTP lease's partial
capture, and a separate test assumed endpoint and outcome snapshots were atomic.
The latter now waits for both the explicit failure state and its outcome; it
does not weaken the delivery/credit assertions. Final source and build evidence
is indexed by `summary.json`, `source-files.sha256`, `source.patch`, test logs,
raw scenario folders, observation-check JSON and `SHA256SUMS`. The 30 changed/new
Rust files plus 5 Cargo/lock files match the remote tested source; documentation
result notes were added afterward.

Production binary SHA-256:
`e1349852598da381851f604e0a00a4314c5407f541865589cb34a1a1de7355c0`.
Frozen benchmark driver SHA-256:
`4d5ffd198c7ace2183237c30a0a4790127b45b52fca5d2f2b3a5a9f81af38c7a`.

### Other compatibility limits

- Top-level `queue_items` / `queue_bytes` are not currently updated by the
  production mailbox path (`record_queue` has no production callers). Treat
  these as unavailable (`queue_metrics_available=false`), not evidence of zero backlog. Ingest/emit/HTTP counters
  and connector loss/retry diagnostics remain separate observations; proper
  per-pipeline Runtime-edge instrumentation is now available in the scoped
  `mailboxes` namespace above; uncovered connector paths remain separate.
- Kernel `emitted_rows` counts rows reaching the final plan Sink once, not
  intermediate window emissions. A window followed by a Filter can emit fewer
  final rows; connector ACK/drop counters must still be checked separately.
  Earlier builds double-counted window output at both Window and Sink.

- Count windows expose `count_start` / `count_end`, replacing the former
  `window_start` / `window_end`. They are within-window arrival ordinals
  `[0, count)`, not timestamps. Update SQL/Graph/downstream mappings that
  explicitly name the old columns. PT/ET names are unchanged.
- Snapshot family magic remains `SPV1`, but the snapshot codec is now **version 2**;
  `MAN2` manifest version remains 1. V1 snapshots can be inspected, but cannot
  authorize restore or be silently re-encoded as V2: preserve the checkpoint and
  explicitly reset/replay from the source origin. Never seek to its saved offset
  with empty state. Floating-point value codecs preserve exact bits. Restoring
  duplicate canonical state keys still fails closed.
- The file library constructor defaults to `Immutable` for finite replay;
  Supervisor defaults to `AppendOnly` for tailing. Set `file_contract`
  explicitly when EOF behavior matters. AppendOnly retains partial lines;
  Sealed/Immutable deliver the final record even without a newline.
- Dynamic state keys remain unsupported by aligned snapshot codecs.
- Memory accounting uses size estimates, not a promised RSS limit or complete
  Scalar arena. Server MQTT ingress is byte-accounted; legacy raw-row library
  ingress retains static bounds. Other mailbox gauges are not implicitly wired
  by the MQTT-specific metrics above. TCP/broker buffers remain external.
- Default delivery remains `live_best_effort + restart_fresh`; no
  exactly-once or MQTT replay guarantee is added.

### Legacy R9/R10 aligned admission and semantic compatibility (BASE-01/02)

R9/R10 aligned Server jobs required a replayable file source and **exactly one** Count
or event-time window. Zero/multiple windows, processing-time windows, Dedup and
Lookup remain unsupported, not silently stripped. Validate, explain, start and
embedded Kernel admission share the plan gate; rejection precedes job quota
reservation. This does not implement a multi-participant checkpoint protocol.

V2 checkpoints compare the complete `SS02` typed, length-framed state descriptor,
not just hashes: window policies/aggregate inputs/COUNT-star, input field types
and nullability, and all upstream Filter/Map/Project expressions and schemas.
Bytes/Dynamic literals retain their contents, floats retain their exact bits,
and Dynamic object order/duplicate keys are not normalized away. Fusion grouping,
revision numbers and downstream sinks are not state semantics. Descriptors have
a 64 KiB encoded limit and nesting depth 64; overflow fails closed. Changes to
evaluation semantics require a descriptor version change, not silently reusing
old state. Standalone `PlanLayout::from_window` callers must attach the input
schema with `with_input_schema`; Server plans use `from_physical`.
Embedded callers can use `try_from_window` to preserve descriptor size/depth
errors. The legacy infallible builder still rejects incomplete semantics at
restore; its diagnostic now includes construction failure/missing schema, not
only legacy/hash-only snapshots.

`effective.aligned_eligible` uses the bound plan, not source kind alone. Status
labels its scope `stored_latest_revision`; it is not a statement about an older
running attempt or proof that a compatible committed checkpoint exists. Without
a bound plan eligibility is unknown (`null`), rather than optimistic `true`.

For an existing V1 aligned deployment, stop the job and preserve the old binary,
catalog and checkpoint before upgrading. Reset/replay uses a **new checkpoint
directory**, no restore claim, and the source origin; do this only if the needed
input history is retained and downstream replay/duplicate effects are acceptable.
Otherwise defer that job's upgrade rather than starting with partial state. A V1
binary cannot read V2 snapshots; rollback requires the preserved compatible set.

### Owner-carrying batch interfaces (BASE-03, bounded scope)

`RowBatch::detach` acquires destination credit before deep copying and retains a
conservative 2x resident-size estimate covering copy temporaries. It can reject
copies that the earlier payload-only estimate allowed. `into_rows` now returns
`OwnedRows`, not independently owned data and lease; shared batches do not clone
the row container. Use its borrowed `rows`/`schema`, `share`, or `into_batch`.
This is an intentional source-level API change (no in-tree raw-tuple callers).

`retain_value` returns `BatchValue`, pinning the complete batch and its existing
credit until the last handle drops. Its `value()` is borrowed; use handle `share`
for retained ownership. Public raw `Row`/`Scalar` clones and constructors remain
untracked escape hatches, **outside the hard-budget contract**. These changes do
not seal the Scalar arena, retroactively bill all allocations or impose an RSS
ceiling; that larger BASE-03 work is still a prerequisite for such claims.

### Verification record (2026-09-12)

On `box@100.64.0.16` (x86_64 Linux, rustc/cargo 1.98.0), isolated source based
on `3619676b4636dfce4bbd477ea79b028e6a6a4efb` passed:

- Core/default-members release tests: **419 passed, 0 failed**.
- Server `--no-default-features` release tests: **20 passed, 0 failed**
  (a separate feature combination, not 20 additional unique tests).
- Checkpoint/aligned, BASE-01/02/03 and final-output metric tests: **48 tests
  x 10 runs**, no failures, using already-built test executables.
- Actual no-demo production executable: two clean catalog/data directories;
  Count checkpoint after 10+20, restore, append 30 => exactly one logged sum 60.
  Changing the upstream Map or the earlier Filter while keeping the last Filter
  unchanged refuses restore before job admission; CURRENT remains unchanged.
  Explicit `--demo-io` boot and the demo API are refused.

Reproduction uses `CARGO_TARGET_DIR=/workspace/sparrow-runtime/target` and
`cargo test --release --locked --offline --quiet --no-fail-fast`; the production
combination adds `-p sparrow-server --no-default-features`. Source was staged in
`/workspace/bench-compare/base-gates-source-20260912/`, not the server checkout.
No local compilation was used. The first live smoke exposed the pre-existing
window/Sink double-count; its failed evidence is retained alongside the fixed
V2 run, rather than counted as a success.

Evidence under `/workspace/bench-compare/`:

- `base-gates-source-20260912/source-v2.patch` (tested patch, before these
  documentation-only results): SHA-256
  `fa5253cd2bbe74c88c8e517133e596462cd396c1af62a8f96ee4bc7bdeb27c62`.
- `base-gates-tests-20260912-v2.log` and
  `base-gates-production-tests-20260912-v2.log`.
- `base-gates-artifacts-20260912/v2/`: `repeat.log`, `smoke.sh`, `smoke.log`,
  `smoke-replay.log`, raw API/checkpoint/log artifacts in `smoke/` and
  `smoke-replay/`, and the exact test/production executables.
- `v2/sparrow-server-production` SHA-256:
  `5871c431e05b729cced0d65e52e7e228a82e7bb13848192b8bdfc451c9643cc6`.

This is recovery/admission/ownership validation, not a new Arrow/JIT benchmark,
WAN soak, target-device certification or proof of a complete hard-budget arena.

### Production release gate

Passing unit/integration tests and short host benchmarks is not a universal
production certification. Before rollout, record the actual device/CPU/RAM,
concurrent rules, payload/key/state sizes, steady and burst rates, downstream
RTT and permitted loss. Validate those workloads with sustained runs, resource
headroom and recovery drills; a one-job loopback run does not certify 16 jobs,
remote sinks, large state or multi-day leak freedom. QoS0 best-effort is not
suitable for a requirement of lossless ingestion without a separate durable
delivery design. Configure a persistent secrets key, strong API credentials,
explicit data/target allowlists and appropriate transport security; alert on
drops/retries/reconnects, failed/held pipelines, disk headroom and process RSS.
Pin the tested binary/config, retain a catalog backup and define a compatible
rollback procedure. Keep invalid/overload trials in the evidence instead of
turning their partial delivery into a successful throughput number.

The root-level `Sparrow_Code_Review_R4.md` through `Sparrow_Code_Review_R6.md` record
reviews of earlier workspace states, not necessarily current behavior. Runtime
contracts above and regression tests describe the implemented fixes; historical
build/test counts are not release guarantees.

## R9 observation follow-up (2026-09-14)

The v12 evidence above remains historical, not overwritten by the follow-up.
R9 changes these implementations without weakening queue/delivery guarantees:

- File poll chunks use multi-permit publication (at most 32 items, capped by
  inbox capacity) and Runtime drains ready ingress in bounded groups. Every row
  still consumes **one channel slot and one FIFO stamp**; a 32-slot inbox does
  not become a 1,024-row inbox. Decode errors and EOF split publication groups;
  checkpoint commands still run only after the preceding file drive completes.
  Cancellation refunds incomplete multi-permit reservations. Publication stays
  inside the per-channel lock, including for concurrent Sender references.
- A publication group shares its monotonic enqueue sample, taken **after**
  capacity admission, immediately before publishing under the lock. This is not
  decode-ready time and does not fold backpressure into residence. Per-item
  occupancy/bytes/FIFO origin and 1/16 residence sampling remain unchanged.
  There is still no unbounded per-event registry or Server observation toggle.
- Source, Sink and filtered progress counters have separate atomic cache lines.
  They are independent monotonic samples, not a simultaneous progress cut.
  Delivery/request/group accounting and mailbox queued/consumer-held/credit
  accounting retain coherent mutex snapshots; `Envelope::take()` does not
  release mailbox credit earlier. No unproven single-producer optimization is used.
- `JobStats.ingested_rows` now sums only Source-stage results. A new barrier
  regression exposed that Window output counts were previously added at task
  join (four inputs plus one window output incorrectly reported five inputs).
  Global ingest metrics already counted Source input; this fixes the returned
  per-Job statistic, not window/output semantics.
- Status binding results have a per-Store LRU cap of 64 entries / at most 4 KiB
  serialized effective fields per entry. Keys include pipeline revision, local
  stream-schema epoch and SQLite connection `data_version` for external writes.
  Schema changes, another Store connection, rollback and bind-error recovery
  are covered; a pipeline revision alone is not sufficient. Cache misses bind
  outside the catalog mutex; running-attempt views are never cached as latest spec.
- Status and metrics expose constant histogram boundaries/rules once in the
  top-level `histogram_contract`. Consumers of the pre-release observation API
  must read that object instead of each histogram's former `bucket_upper_us`,
  quantile rule, scope and overflow fields. Counts/buckets/quantile values keep
  their names and units. Mailbox `accounting_errors_total` and `accounting_valid`
  make a release-build underflow visible instead of wrapping; nonzero means
  invalid observation, not a silently repaired trustworthy zero. Debug builds
  still assert the invariant, and credit ownership is not changed by this guard.

Verification uses `scripts/obs-file-abba.sh` with frozen BASE-v2 / candidate
binaries and the v12 driver: 80k inputs × 2 measured rounds × ABBA, then 400k × 3
× ABBA. The throughput goal is predeclared as ratio >= 0.97 with RSS delta >
1,024 KiB flagged for investigation; all output hashes/counts must agree.
These are same-host end-to-end measurements, not CPU attribution for individual
locks, and do not replace real network-delay/long-soak certification.

### R9 verification record

Evidence directory: `/workspace/bench-compare/r9-artifacts-20260914/v4/` on
`box@100.64.0.16`; Linux x86_64, Rust/Cargo 1.98.0, locked dependencies, production
Server built separately with `--no-default-features`.

- Core/default-member release tests: **463 passed / 0 failed**. Separate no-demo
  Server tests: **22/0**. Frozen selected test executables: **48 tests × 20 rounds
  = 960/0**, including default and no-demo API coverage. The name filters also
  match existing `jobs_` tests; this is not a claim of 48 newly added tests.
- File ABBA, same frozen BASE-v2 binary and v12 driver, two independently sized
  matrices; **20 measured trials + 8 reduced warmups**, all valid:

  | Inputs / measured rounds per invocation | BASE median events/s | R9 median events/s | Ratio | Max sampled RSS delta |
  |---|---:|---:|---:|---:|
  | 80,000 / 2 | 484,361 | 566,333 | 1.169 (+16.9%) | +1,000 KiB |
  | 400,000 / 3 | 504,933 | 592,683 | 1.174 (+17.4%) | +464 KiB |

  Both exceed the predeclared 0.97 throughput goal and stay below the 1,024 KiB
  RSS investigation delta. The smaller RSS result is close to that line, not a
  universal memory-overhead guarantee. Hashes are `1bdb251814aec266` (80k) and
  `b4986ae79404b085` (400k). Faster Tokio bulk operations also benefit the original
  row-at-a-time path; the result is not proof that observation has zero cost or
  that every workload is 17% faster.
- Normal MQTT: **7,500/7,500 outputs**, no missing/duplicate/invalid rows, 2,000
  inputs/s with 20 ms application response waiting, 462 POSTs / 3 connections.
  Capture-arrival p99 **11.279 ms**, not HTTP completion or WAN RTT latency.
- Sustained MQTT: **90,000/90,000 outputs** at 1,000 inputs/s over **120.003 s**,
  no missing/duplicate/invalid rows; 19,069 POSTs / 1 reused connection,
  capture-arrival p99 **6.771 ms**. Not a 72-hour soak or capacity certification.
- Intentional best-effort overload: **1,412/1,536 outputs**, **124 missing**,
  zero duplicates/invalid rows. This remains an invalid lossless trial, not an
  optimization success. Source/Sink sampled queue maxima 8/2; over-capacity
  behavior was not hidden by widening File inbox item units.
- **254 observation / 508 boundary-queue / 508 Runtime-mailbox snapshots** pass
  component conservation, capacity and histogram checks, including zero mailbox
  accounting-error counters. These are not atomic end-to-end pipeline cuts.
- Fresh-directory production recovery: 10+20, checkpoint, restore, append 30
  yields one output with sum **60**. Incompatible Map/earlier Filter restores
  are rejected without replacing CURRENT. Demo boot/API are refused.
- Netem retried in an isolated namespace: **NOT RUN**, kernel qdisc unavailable.
  Host loopback qdisc is unchanged and pre-existing Mosquitto PID 2657777 remained
  running. Application waits/TLS fixture tests do not substitute for packet delay.

The patch replays from `3619676b4636dfce4bbd477ea79b028e6a6a4efb` in a clean
directory; all **36 changed/new Rust/Cargo/lock files** match the tested source.
The R9 verification run itself did not compile locally, deploy, push, tag or change
the version. Its code was subsequently committed as `4f70407`; formal documents
were excluded from that commit at the user's request.
Formatting checks cover the 13 Rust files touched by R9, not unrelated historical
workspace formatting. Earlier attempts are retained: v1 JSON-macro recursion
limit (split JSON construction rather than raising the limit); v2's new barrier
test exposed the JobStats count defect; v3 was missing SQL fixture files in the
isolated source copy, remedied without changing the fixture assertion or Rust.

Fingerprints:

- Production Server: `3ac57d2125ffb7c680fbe26c4de48e1e615910302ccbee730d913d57e7350b50`
- Frozen driver: `4d5ffd198c7ace2183237c30a0a4790127b45b52fca5d2f2b3a5a9f81af38c7a`
- Source patch: `a926696e2f9ad5699d73d2a34228029fb26df1b5547ee78a3a25793b9bf411bf`

Replay scripts and raw output: `obs-file-abba.sh`, `obs-repeat-tests.sh`,
`obs-closure-verify.sh`, `obs-check-snapshots.jq`, `restore-smoke.sh`, test logs,
File/IO `results.jsonl`, observation snapshots, source manifests and SHA256SUMS.

## Post-R9 production operations candidate (2026-09-14)

The next batch adds the fixed Rust 1.98.0 Linux x86_64 production package,
separate no-demo Server/HTTP-only `sparrowctl`, build/source manifests, CI and
deployment/rollback assets. The operational contract is maintained in
[`PRODUCTION.md`](PRODUCTION.md); historical R9 numbers above are not evidence
for the changed candidate.

- Periodic checkpoints are opt-in, attempt-local, single-flight with manual
  requests, skip missed ticks, and share a finite request deadline. A waiter
  timing out cannot release the actor's admission while a blocking commit runs.
- Stop/update/shutdown serialize transitions and join old actors before starting
  replacements. Startup failures cancel/join already-submitted jobs. Monitoring
  reads do not retain the checkpoint writer lock across replacement.
- CURRENT remains the commit boundary; bounded PUBLISHED proofs prevent corrupt
  CURRENT fallback from promoting a merely written/unpublished MANIFEST.
  Retention protects CURRENT and any numeric RestoreSpec dependency; a pin
  makes the effective minimum two generations, never an unbounded history.
- Periodic restore supports the existing File + single Count/ET tumble/ET hop
  aligned shapes only in this pre-K1 record. `resume_latest=false` remains the default. Missing,
  incompatible or unverified state never silently becomes a fresh start.
- File cuts refresh sampled identity over consumed bytes, including files that
  were empty at open. Unix active descriptor/path replacement is refused.
  Sampled prefix/middle/suffix identity is not a whole-file cryptographic proof;
  producer append-only discipline remains required.
- Current transform/window/dedup scratch is pre-admitted through output adoption;
  state container estimates include capacities; variable MIN/MAX updates admit
  a candidate before cloning and preserve the old value on failure. Raw helper
  APIs/third-party allocations are not a general hard-RSS/arena guarantee.
- Capabilities distinguish static implementation, plan eligibility and actual
  certification; Explain exposes checkpoint policy. `/checkpoints` and bounded
  allowlisted `/diagnose` expose explicit attempt/cache/age scopes without raw
  specs, SQL, destinations, secrets or free-form logs.

Pre-self-review evidence is in `PRODUCTION.md` under the 2026-09-14 v7 record:
490 core, 25 separate no-demo Server and 2 CLI tests; 79 selected tests ×20;
fresh-process restore/backup and File/periodic/MQTT profiles all recorded in v7.
Network/target-device and 24/72-hour release gates remain independent of short
tests. No automatic commit, tag, push or deployment is part of this batch.

### Self-review follow-up (2026-09-14)

The v7 measurements above are not relabelled as results for the changed source.
The follow-up fixes caller-cancellation safety by retaining the transition guard
and child joins in bounded admitted workers; atomically commits RestoreSpec and
its desired revision; reserves per-entry rebuild scratch rather than three whole
decoded snapshots; propagates the checkpoint deadline into sink flush and wakes
abandoned barriers; and explicitly selects the same Linux target for build,
dependency inspection and binary packaging. Lifecycle methods now require the
`Arc<Supervisor>` returned by `Supervisor::new`, not a bare borrowed Supervisor.

New evidence: 495 core / 26 separate no-demo Server / 2 CLI tests, 87 selected
tests ×20 across 11 frozen binaries, and a fresh process smoke with sums
60/150/240, actual backup read-back, 20 start/stop cycles (fd 12→12) and SIGTERM
lock release. A conflicting inherited `CARGO_BUILD_TARGET` was used for the
successful production package check. The pre-fix cancellation failure is kept.
Matching File ABBA against v7 (not R9) passed with ratios 0.9974 / 1.0212 for
80k / 400k; current-candidate periodic on/off ratio was 0.9825 with zero failed
checkpoints. Every warmup/measured trial passed output validation; existing
thresholds were unchanged. MQTT 120-second results were not rerun for this source.
Source/binary hashes and exact scope are in
[`PRODUCTION.md`](PRODUCTION.md#self-review-20260914). Network/device/soak gates
remain unfulfilled; this is a reviewed candidate, not production certification.

## OperatorId::WINDOW = 10 (layout fingerprints)

SQL/linear window nodes use `OperatorId::WINDOW` (**10**), assigned in R1 (A7).
A pre-R1 checkpoint that stored a different operator id (or a sequential
binder id) will fail closed on restore: `decide_state_reuse` rejects an
OperatorId mismatch. That is intentional — do not rewrite old CURRENT files.

## Secrets key (N12)

Catalog secrets are `enc:v2:` ChaCha20-Poly1305. The key is
`SPARROW_SECRETS_KEY` (32 raw bytes or 64 hex chars) or
`SPARROW_SECRETS_KEY_FILE`. If both are unset, the process uses a
**process-local random key** and logs a warning at store/server open
(dev only; sealed values will not survive restart). `--safe-mode` /
`SPARROW_SAFE_MODE=1` / `SPARROW_REQUIRE_SECRETS_KEY=1` refuse instead.

## Processing-time `now` (event-time future skew)

`WindowOperator::on_batch` / `WatermarkHub::observe_event` take `now` in
microseconds from `RuntimeClock`: host wall time on live jobs, or a
`SharedVirtualClock` in tests. That instant is **processing time**. It is
not event time and does not move the watermark.

When `max_future_skew` is set (ET windows default to one hour), a row
with `event_time > now + skew` is dropped, counted as
`RuntimeMetrics.future_dropped` (`GET /v1/metrics`), and must not advance
`max_et` (P0-5).

## R10：计费模型与生产边界修正

- Window/Dedup 不再在每批/控制消息上预留 `2 × 全部 retention`；state 与关闭/过期索引的已计费字节为 O(1) 累加值。输入 scratch 只与本次输入/被触及状态有关；timer/WM 按到期前缀分块关闭，不先物化全部 key。
- scratch 必须覆盖 raw 输出直到 owning RowBatch 接管；在异步 send 前退款。背压期间保留的是实际输出所有权，不是整份状态的虚构 Reservation。
- MIN/MAX 复用候选 Vec，只有获胜值 detach、增长才扩 lease；候选完整成功后替换并按实际大小 shrink。SUM/COUNT 等兄弟累加器仍正常更新，不能把“大小没变”当成“值没变”。父/子账本同步退款，共享 lease 不允许缩账。
- 表达式 bound 区分共享值大小与新分配：Column/Literal 仍携带下游 lower/upper/cast 所需大小，但不把融合投影按全行反复乘算。函数注册表提供实际分配类别，注册与 evaluator 有一致性回归。
- freeze 按 wire 长度估算，排序引用工作区单独计费；`encode_frozen` 返回不可拆开的 `EncodedSnapshot`，内部 `commit_prepared` 避免再次物化刚编码状态。嵌入调用改用只读 `bytes()`；`EncodedFreeze` 的 fields 不再对外可写。旧 CURRENT 仍校验 chunk/CRC/codec，绝不凭 PUBLISHED 跳过完整性检查；有效旧 proof 不重写，无删除不做 prune fsync。
- 数值恢复点在失败/进程重启后 held，需要显式 start（包括非 safe-mode）；仍恢复原 pin，不静默切 CURRENT。停机先 draining/停止 accept，再排空已准入请求与 join；新变更拒绝 503，鉴权先于 draining。

这些是现有路径的修复，不新增通用多状态恢复、DAG 或可靠 outbox。`max_state_keys` 不是任意宽状态必定可运行的保证；raw helper、第三方分配及全进程 RSS 硬限制仍按原边界声明。匹配代码的 R10 测试/多 key 性能证据见 [PRODUCTION.md](PRODUCTION.md)，不沿用历史 v7 或自审数字作为本轮通过证明。

## 重采样 profile v25/v26（开发 Preview）

- 单个线性 Resample，前后仅纯 Transform；File25 / JetStream26，使用已有 PTC1/TPD1 的持久暂停时间与 timer-before-input 顺序。旧 v14～24 目录不升级、不混写；输出 epoch 必须等于 state generation，完整计划身份包含模式、周期、wait/gap 与展开上限。
- IoT slot3/codec2 新增 kind13/14/15（Last/Mean/Interpolate）。每条状态是 key + `next_grid:Int64,pending_grid:Int64,sample_time:Int64,samples:UInt64,used:Bool` + 完整数值向量；缺失时间用 -1。前缀固定 38 字节，非物化扫描同样验证类型、数值完整性和 source cut，随后恢复再按绑定 schema/周期/等待期限检查并构造有界 timer。
- Last/Mean 只保留当前半开区间的一个向量/在线均值，空区间全 NULL；Last 不把 UInt64 绕经 Float64。Interpolate 只保留最近一个完整点及至多一个等待格，不预分配无限时间轴、不外推、不回写已发结果。过期等号先输出 missing，之后同刻输入只能影响仍未产出的格。
- 每次时间推进先算补格数量、时间溢出和 cap，再改变逻辑时钟。插值始终为随后可能产生的一个输入输出预留名额；不能先发满 timer 输出，再在输入处才发现超额。时间跳跃超过上限明确失败，不截断。
- 新机器放在冷 Box 内，不扩大原有 TimedState 的最大内联变体；条目、索引副本、工作区、输出和恢复候选由同一 Job owner 计费。恢复采用完整候选替换，输出入账后才消费 timer/改状态；取消时先释放分配、后归还额度。
- 诊断独立使用 `resample_*` 三类计数，不借用 Alarm 通知计数。每决策 fsync/full checkpoint 与 required HTTP 串行等待的限制不变，v25/v26 不是高吞吐批提交优化。

## 来源观测与静默 profile（验证候选）

- File23 / JetStream24 是独立线性 profile：Source → 首个 Silence 状态 → 可选纯 Transform → required HTTP；不混旧 PTC1/TPD1、ET、参考表或 DAG。旧版本和其他 profile 不得改写其历史。
- 顺序为 **持久 OFD1 decision → ProcessingTime → 可选输入 → FeedObservation → Barrier → required flush → CURRENT → 来源 ACK**。新 Silence 先更新当前输入的最近接收，再用当前健康事实判断；不改变旧 Alarm/Window 的 timer-before-input。
- `CaughtUp` 只是新鲜检查的来源前缀事实。积压、半行、在途、不可验证、慢观测、超出间隔的提交/背压或重启均打断覆盖，之后必须重新积累完整宽限。data-only decision 可保留未过期覆盖，但不能授权新静默事件。停机不计时，未提交后继按原时间/事实/输入摘要/输出身份重放，不重采一次 Ready。
- 静态登记与已观察 key 共同构成设备集合；从未见且未登记的设备不存在。`silent` 每 episode 一次，只有真实输入才 `resumed`；连接恢复不是设备恢复。输出为 key 加独立事件列，登记未见时 `last_seen=NULL`，不伪造原遥测行。
- kind12 状态、timer/key、副本、登记 JSON、工作区和输出同 Job 计费；覆盖重建是全局常数更新，不逐 key 重排；输出准入失败不能消费当前 episode。静默状态不 TTL 遗忘。宽限/时间溢出明确拒绝，不把溢出当成没有 timer。
- `checkpoint.observed_source` 只报告已成功提交的历史 cut，不表示实时健康；状态失败仍沿用原 Source 诊断。本 profile 每决策完整持久化，受 fsync/HTTP RTT 限制，不是高吞吐链路。匹配验证与未测范围见 [本批记录](PRODUCTION.md#silence-validation)。

## MQTT live 静默：非持久 FIFO 合同

- 与 File23/JetStream24 actor 分离；没有 OFC1/OFD1、SourcePosition、CURRENT 或来源 ACK。`SilenceClockPolicy::Live` 是 Silence 专用策略，不扩大其他 timing policy；paused 的 canonical tag 仍为 1，live 为 2，CheckpointPlan 一律拒绝 live 时钟。
- Connector 一次一个带绝对响应期限的 PINGREQ；只有及时 PINGRESP、无已知 ingress 队列/reader 预读积压且 epoch 未中断时，才入队候选健康事实。所有事实和预算化行共享同一个 FIFO；控制丢失通过后续事件上的 sticky epoch 表达，不能静默丢失失效通知。
- 独立 Kernel Source 一次消费一个事件，不经过旧批量预取；`LiveFeedStart → 可选一行 → LiveFeedEnd` 在同一 mailbox 排序，Silence 消费端重新验证新鲜度后才能排空 timer。控制不传播为下游遥测；输出仍经原 Kernel Transform/Sink，不绕开执行链。
- Box 内事实、QueuedRow、队列元数据、工作行与状态受 Job 额度；`mqtt_inbox_*` 的已计费行指标包括队列外最多一个已取出的工作事件，不能直接当作 FIFO occupancy。慢 HTTP 不会无界读入，也不会阻止取消；旧数据路径仍按原批量方式运行。
- 新尝试用新 generation；连接重建只打断覆盖，不伪造 resumed、不累加断连两侧的宽限。消息和事件均 live-best-effort，任意时刻崩溃可丢失；健康探测不能证明 broker 全局排空或 QoS0 无损。详见 [配置合同](IOT.md#mqtt-live-silence)。

## Action/File 与纯函数（2026-09-26 开发 Preview）

- HTTP/MQTT/Log/File 使用可选 v1 typed JSON tree；field 引用按最终 Sink schema 校验，显式 body:null 与缺省全行不同。HTTP 动态 query、MQTT单level变量限制最终目标，重试不重算 payload；不存在脚本或secret插值。
- 所有 Action/File 只支持 restart_fresh，拒绝 checkpoint/restore；已有 Alarm/可靠Silence/Resample 的原required HTTP协议不借此开放模板。多动作仍是原 DAG required/best-effort；部分成功不回滚。
- File输出独占已有allowlisted Linux目录，FD锚定、永久协作锁、有界新段、不覆盖旧文件、不自动删除；配额/坏尾明确失败。可选每批sync_data不冒充source重放/幂等/掉电保证。取消等待已开始的blocking写结束，不遗留旧writer，也不能承诺坏文件系统上的强制停机时限。
- 新18个纯函数复用SQL/Graph表达式与AllocationBound；明确Unicode scalar、严格数值转换、JSON pointer、UTC微秒与输出硬界。旧CAST、ASCII case、eager首错及semantics.VERSION=1不变；新增名称进入原canonical表达式身份。
- 生产Log不再留第二份ring；原显式调试capture API不作为Job预算内的生产存储。新File计数是写入/同步调用观测，不是可靠提交水位。
- 配置范围、预算、示例见[ACTIONS](ACTIONS.md)，候选与验证见[PRODUCTION](PRODUCTION.md#actions-validation)。性能门禁和长稳/存储故障仍独立判断。

## 可靠链路容量批（2026-09-26）

- 旧JSON编码的常见小型primitive行借用字段和值直接写入bounded writer，避免临时Map和字符串复制；≤16字段使用固定栈索引保持旧字典序。Bytes/Dynamic、宽表、重复字段名及serde_json/preserve_order保留旧路径；Float/UInt64/NULL/escaping及可靠envelope字节不变，不变更codec版本。
- 普通JetStream Actor新增可选空拉取退避上限，默认250ms不变；reset保留所配置的cap。time/observed profile明确拒绝。没有扩大默认pending/pull/队列/内存，也没有减少所有权、retention检查或改变Explicit ACK切点。
- 解码额度错误只将已验证的 `credit/used/request/cap` 与source sequence放入安全message/context，最终attempt诊断可见；不把任意connector文本或payload带入API。
- `/v1/metrics.process_credits` 是进程owner追踪的Job reservation/retention/queue/physical/handle及peak。多字段并非并发原子快照，也不覆盖第三方所有分配或等同RSS硬限制；只在停止后的稳定边界核对清零。
- 原门禁、有限容量和生产认证分开记录，见 [CAPACITY](CAPACITY.md)。过载后的完整排空不表示持续容量达标；拉取超时仍fail-closed，不通过静默跳过或延长期限隐藏问题。
