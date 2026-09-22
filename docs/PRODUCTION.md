# 当前生产化候选：安装、恢复与回退

本页说明当前实现合同，不是目标设备认证或正式发行公告。Linux x86_64、Rust 1.98.0、锁定 Cargo.lock；Row/prepared/fusion，支持线性和显式DAG Preview，恢复按独立profile准入。Arrow/JIT、HA、可靠MQTT、持久HTTP outbox均未开启。版本号仍为0.1.0，tag/push另行授权。最新时间型增量见 [时间 profile 验收](#paused-time-validation)，静态表/迟滞历史批次见 [组合证据](#k1-k4-reference-validation)；后文历史门禁不自动代表新增能力。

新增 K2 **可选 JetStream Preview**：`SPARROW_JETSTREAM=1` 仅为 Server 启用 SDK，默认构建及 HTTP CLI 不链接它。合同、v4 与 File/v3 的目录隔离、资源限制和未验证边界见源码 `docs/JETSTREAM.md`（启用 feature 的包内同时提供）。不要将 R11 的 File/MQTT 数据或下面的默认部署合同直接当成 NATS/TLS/WAN/长稳认证。

<a id="paused-time-validation"></a>
## 可恢复时间首批：2026-09-19 验收

**限定 Preview 已完成本批验证，不是完整 K1～K4 或生产认证。** 仅单个线性 HoldFor / Debounce，File append-only v14 或可选 JetStream v15，required HTTP、暂停的逻辑时间、CURRENT-only 恢复。每个输入行/空闲 tick 先持久决策再执行、每决策 checkpoint；不开放 PT Window、正 TTL、多状态时间组合、时间型 DAG、完整告警生命周期。合同与备份边界见 [IOT.md](IOT.md#paused-time-preview)。未 commit/push/tag/生产部署。

全部 Rust 构建在 `box@100.64.0.18` 集中执行，本机未编译。该节点为 Debian 13.6、Linux 6.12.94+、8 vCPU Intel Xeon；不直接继承原 `.16` 的性能数字。

### 功能、故障与兼容

- `frozen-v6`：747 passed / 18 ignored；独立 no-demo 44 passed。18 个 ignored 不计入普通通过数；本批 NATS 和既有 K2 等由专项明确启用。
- `paused-repeat-v6`：精确清单 22 项 × 20 轮 = 440 次通过，其中每轮 21 常规 + 1 隔离 NATS。覆盖配置、算子、完整行/NULL、timer/预算、codec、日志完整性、停机暂停、CURRENT 故障、慢 HTTP 重放和缺失日志拒绝。
- `process-v4` / Go driver-v4：File、JetStream 各执行 HoldFor、Debounce trailing、Debounce leading，共 6 种真实 SIGKILL 场景；核验真实信号退出状态、未提交输出 ID/内容完全一致、已提交后重启不重复、停机不计时及未提交输入不 ACK。上一批 package-v7 二进制（最高支持 snapshot v13）对 v14/v15 的两个拒绝反例保持完整历史、CURRENT 和输出不变。
- `process-default-v5` / Go driver-v5：默认关闭 JetStream 的生产二进制再跑上述 3 种 File 进程场景，全部通过。
- 旧矩阵：参考表/迟滞 26×20 + 9 种进程及旧 profile guards；B2 25×20、B1 38×20、Core-A 8×20、K4 50×20、K3 24×20及其进程；K2 专项（含真实 broker）、真实进程与 default/K1 smoke 均通过。
- 普通 release/all-targets/JetStream Clippy 退出 0，102 条 warning；不声称通过 `-D warnings`。

### 性能：相同节点重新跑原门槛

`performance-v3` 使用原冻结 K1 driver、原 K4 v5 baseline 和新 default package；三组预先声明的完整 ABBA **每组及合并均通过**。fresh 比值门槛仍为 ≥0.97，periodic on/off ≥0.90，RSS 增量 ≤2048 KiB；未降低门槛。

| 场景 | 合并吞吐比 | RSS 增量 | 测量样本 |
|---|---:|---:|---:|
| fresh / 0 state | 1.003825 | +820 KiB | 36 |
| fresh / 2 states | 0.990209 | +1016 KiB | 36 |
| periodic / 0 state | 1.005042 | +400 KiB | 24 |
| periodic / 2 states | 0.999532 | +56 KiB | 24 |

全部样本输出校验通过；periodic 提交成功数均为正、失败数为 0。这证明本轮原链路门禁未退化，不代表新时间型串行 profile 有同等吞吐。

新模式另做成本观察：File、单 key、Debounce leading-only、逐行 POST，各 100 行全部提交、100 个连续输出 ID；无人工 HTTP 延迟约 **139 行/s**，20 ms 模拟响应延迟约 **34.1 行/s**。这不是 WAN 或持续容量认证；大状态、多规则、磁盘写入量未外推。后续批处理/group commit 见 [OPT-012](OPTIMIZATION_BACKLOG.md#opt-012)，不能删除持久时间/ACK 保护换吞吐。

### 来源与保留的失败样本

产物根：`/workspace/bench-compare/paused-time-artifacts-20260919`；对应 `package-v7-default` / `package-v7-jetstream`、`frozen-v6`、`paused-repeat-v6`、`process-v4`、`process-default-v5`、旧矩阵 `*-v1` 和 `file-performance-v3-*`。测试源码、日志、原始失败和 checksum 均保留。

```text
default server  22f8be3f6ccb098da1b4e3ad076e1589b61b4a78be9c4ad956a16c5e1396b74a
JetStream       fc13de3e162fdb99aa533249a5fd9b3bbf7e04701b48de8a2281beb5ea505a9a
source manifest 4e49381c2159a7fbf7295635c25baa18c65fccdd3771512b3d38a4df8835e930
Go driver-v4    a629af3ba052f16b9b5d2a437144031608c9732c911619e36fa1c9961a7ea803
Go driver-v5    4539f567433c161ac92b20ddc1396208f0810f7d413732e27fb5d6b52ec8c21d
```

本地逐文件复核中所有 Rust 与生产包 source manifest 一致；两个 Go 文件是构建后独立冻结/验证的 driver 更新，不能称为原 manifest 的同一版本。Go 校验器现在包含 `main.go`、`stateful_dag.go`、`paused_time.go`，必须一起构建。原包保留构建时文档；此处最终记录是后续证据补充，不冒充重新编译。

保留：初始归档遗漏 workspace/打包目录、测试类型标注与不适用的 Cargo 参数失败；SSH 中断后的 `regression-v1.exit=0` **不作为全程通过证据**，K3/K2/smoke 后续阶段按实际完成日志补齐。Go driver-v3 在快提交时把 `CURRENT→PUBLISHED` 发布窗口的 ENOENT 当成失败；v4 仅在固定期限内重试 ENOENT，CRC/格式错误仍立即失败。性能 v1/v2 因新节点缺 Mosquitto/共享库而在测量前失败；在任务目录解包 Mosquitto 2.0.21 及依赖、校验 `ldd` 后运行完整 v3 三组，未安装/启动系统服务。原 driver 仅通过 wrapper 增加隔离 broker 路径；具体依赖 hash 见 `performance-v3-dependencies.sha256`。

**未运行/未覆盖**：真实 TLS/WAN、目标设备、24/72 h 长稳、断电/介质损坏、时间型大状态/多规则容量。上述项目仍是对应部署/发行门禁，不因短程通过变为已认证。

## 构建与追溯

```sh
bash scripts/production-build.sh /absolute/new/package
cd /absolute/new/package && sha256sum -c SHA256SUMS
```

脚本分别构建 `sparrow-server` 与 `sparrowctl`，显式 `--target x86_64-unknown-linux-gnu --no-default-features --locked --release`，不与 demo 包混合 feature。构建、依赖树与复制路径使用同一显式 target，不受继承的 `CARGO_BUILD_TARGET` 或 Cargo 默认 target 干扰而误打包旧 host 二进制。保存依赖树、工具链、逐文件源码指纹、二进制/部署文件校验和；包内 manifest 使用可迁移的相对路径。`source_commit` 不代表工作区干净，需同时检查 source manifest 和匹配测试证据；正式发行要求干净提交。历史示例使用默认 features，或 `--no-default-features --features examples`。CLI 生产依赖不含 Kernel/testkit/connectors。

CI 分核心回归、独立 no-demo 测试/真实进程 smoke、周期确定性重复。周期重复不等于 24/72 小时长稳。真实 RTT、介质/断电、目标硬件与长稳是独立发行门禁。

默认 `SPARROW_BUILD_MODE=candidate`。正式包使用 `SPARROW_BUILD_MODE=release`，脚本要求构建源码、README 与 docs 已提交且无未跟踪文件（不包括根目录 review/过程 MD）。默认不打包原始本地 diff；只有显式 `SPARROW_INCLUDE_SOURCE_PATCH=1` 才附带受限源码路径的 tracked patch，使用前人工检查凭据/私有配置，且该 patch 本身不包含新文件。源码指纹不是脱敏工具。

可复现冻结与重复（同一源码、同一工具链；两个 feature profile 不混用）：

```sh
bash scripts/production-freeze-tests.sh /absolute/new/test-evidence core
bash scripts/production-freeze-tests.sh /absolute/new/test-evidence production
bash scripts/production-repeat.sh /absolute/new/test-evidence 20
```

冻结脚本一次测试后保存 Cargo 返回的实际 executable、SHA256 和日志；重复脚本不编译、不覆盖旧证据。CI 定时任务使用同一脚本/筛选范围。`git diff --check` 比较 base 与 HEAD；clippy、包校验和、脚本语法检查为门禁。全仓 rustfmt 历史欠账暂单独输出 `format.log` 与 CI warning，**尚非阻断门禁**，不为本轮修复混入无关全仓格式变化。

## 运行目录与安全

- `/opt/sparrow/releases/<build-id>`：只读、校验过的完整包；`current` 指向选定版本。
- `/var/lib/sparrow/catalog.db`、`data/`、`checkpoints/`：专属 `sparrow` 用户，仅此目录可写，不与其他用户/程序共享写权限。
- `/etc/sparrow/production.env`、`secrets.key`：0600。systemd 读取的 env 文件可由 root 持有；**key 文件须允许实际服务用户读取**，例如 0600、owner=sparrow，父目录使用适当的 root:sparrow 0750。密钥为 32 原始字节或 64 个十六进制字符，长期保存，备份与 catalog 分开管控；缺失/错误密钥不得用“生成新密钥”掩盖。
- 模板见 `deploy/`；创建目录与服务用户后自行安装并 review systemd 单元。模板不会自动部署；必须替换 token，并使用 schema 与实际设备匹配的 stream。
- 管理端默认 loopback；跨机使用可信 TLS 反向代理或 SSH 隧道。Server 本身不提供管理 HTTPS 监听。`sparrowctl` 默认拒绝远端明文、URL 内凭据和重定向；明确 `--allow-insecure-http` 才允许远端 HTTP。
- `sparrowctl` 的服务 URL 必须挂在根路径 `/`，不支持代理子路径；HTTPS 当前使用 webpki 公共根，无自定义私有 CA 参数。私有 CA 环境可用 SSH 隧道连接 loopback，不能用跳过证书校验代替信任配置。非 demo Server 拒绝模板占位 token、少于 16 字节、全同字符或非可见 ASCII token；这不是随机性证明，仍应注入真正随机密钥。兼容保留的 Server `--token` 会出现在进程参数中，不推荐使用。
- Connector TLS 校验证书；网络目标仍须配置现有 allowlist，SecretRef 引用不能替代授权。文件/checkpoint 受 `SPARROW_DATA_ROOTS` 限制。safe-mode 缺少明确 root/持久密钥时拒绝。
- catalog 与 checkpoint 使用协作式文件锁；锁文件不删除。此合同面向可信本地文件系统目录，不提供对恶意同权限进程、硬链接别名或不可靠网络文件系统的隔离。
- SIGTERM/SIGINT 停调度并等待任务/阻塞提交退出。磁盘 I/O 永久阻塞时没有“必定有限时间优雅退出”的保证；systemd 最终强杀属于异常退出，不算 flush 成功。
- 收到停机信号先置 draining 并通知 HTTP 停止 accept，新到达的非 GET/HEAD/OPTIONS 请求在鉴权后返回 503；此前已准入请求仍可能完成。HTTP 排空后等待任务 join，超过 120 s 仅告警并继续等待，不 detach/abort 阻塞提交。systemd 的 `Restart=on-failure` 是进程层重启，与 Supervisor 的 pipeline 退避重试不同；固定恢复点保护同样适用于进程重启。

## 最小操作闭环

```sh
export SPARROW_URL=http://127.0.0.1:43180
# SPARROW_TOKEN 由受控环境注入，不放到命令行或 shell 历史。
sparrowctl health
sparrowctl capabilities
sparrowctl put-stream sensors stream.json
sparrowctl validate deploy/pipeline-aligned.json
sparrowctl explain deploy/pipeline-aligned.json
sparrowctl put-pipeline sensors deploy/pipeline-aligned.json
sparrowctl start sensors
sparrowctl status sensors
sparrowctl checkpoint sensors
sparrowctl checkpoints sensors
sparrowctl diagnose sensors --output new-diagnostic.json
sparrowctl stop sensors
sparrowctl restore sensors --snapshot-id 1
```

更新已有配置必须带服务器返回的 `--if-match ETAG`。start 的成功表示 desired 已提交，不等于实际 ingress 已激活；继续检查 status 的 revision/attempt/健康和进展。CLI：0 成功，1 本地/网络/超时错误，2 HTTP 拒绝。输入 JSON 上限 64 KiB，响应 2 MiB，诊断 128 KiB；诊断文件独占创建、0600，不覆盖旧证据。诊断只导出 allowlist 状态/有限审计摘要，不含 raw spec、SQL、目的 URL、SecretRef、自由文本错误/日志或任意宿主文件；不是完整日志归档。

restore 在一个 catalog 事务内同时发布含 RestoreSpec 的新 revision 与 desired=running；HTTP 处理器不再先 kill 再单独 start。返回成功仍是异步请求被接受，恢复点兼容性与实际启动需继续检查 status。Supervisor 统一执行旧任务 join 后再替换；已准入的启停/收敛操作由最多 128 个独立生命周期工作持有锁和子任务，即使 HTTP 或嵌入调用者取消等待，也不会提前释放转换锁、让新旧任务交叠。取消等待不等于撤销操作。

诊断事件是“全局最近 64 条审计中属于目标的最多 32 条”，响应明确给出窗口范围；空 events 不代表目标没有历史事件。反向代理返回非 JSON 的非 2xx 响应也按 HTTP 拒绝退出 2；成功响应仍须合法 JSON。`--output` 创建/写入失败时，已取得的诊断 JSON 回显 stdout 并退出 1；可能留下不完整的新文件，不能视为完整证据，也不会覆盖已有文件。

## 周期 checkpoint 与恢复

`checkpoint` 可省略，默认仅手动、不自动重放。配置项：

| 参数 | 默认 | 合法范围 |
|---|---:|---|
| interval_ms | null（关闭） | 100～86400000 |
| timeout_ms | 5000 | 100～120000 |
| retain_generations | 3 | 1～128 |
| max_store_bytes | 33554432 | 1 MiB～1 GiB |
| resume_latest | false | bool |

K1 支持单 File/replay → 线性计算 → 单 required Sink 的零状态、单 Count/ET tumble/ET hop，以及两个 Count 窗口串联。双状态混合时间策略、超过两个状态、PT/Dedup/Lookup、分支/多源/MQTT 入口恢复仍拒绝。进入 state 的 key 和 MIN/MAX 值须由现有 Scalar snapshot codec 支持，nested/Dynamic 状态值不开放；无状态计算或 COUNT(*) 的无关宽列不因此一概拒绝。每个 attempt 仍只有一个排队/进行中请求，手动和自动共用 gate。

每次切点必须收齐当前 attempt 的 Source、全部状态实例、required Sink ACK；零状态不是免除 Source/Sink 责任。实例以 operator/slot/shard（当前 slot=1、shard=0）区分，相同 ACK 幂等，冲突/未知 ACK 拒绝，旧 attempt/旧 checkpoint ACK 不补齐新切点。只有全部 freeze 和真实 Sink flush 成功才能发布 CURRENT；prepared ACK 集合及已编码快照不能由外部调用者任意构造为“可信”。

恢复前先校验完整参与者集合，再在同一 Job owner 下准备所有窗口；缺失/重复实例、错误 accumulator/key 类型或边界、预算不足均在 Kernel 输入激活前拒绝。`max_state_keys` 仍是**每个算子**的上限，不因两个参与者静默减半；最多两个参与者限定总条目工作量，完整 snapshot 总上限仍为 8 MiB，所有参与者共享 Job 的 reservation/retention，不各得一整份预算。

`append_only` 的空文件/暂无追加继续保持可 checkpoint；全过滤也推进真实 source cut。`sealed/immutable` 保留终止水位后完成 Job 的既有行为，不为 K1 改成永久 Running；需要 checkpoint 必须在活跃期间完成，不能把已经完成的有限作业宣称为仍可接收新 checkpoint 请求。

状态实例身份与 snapshot 序号不同：fresh/reset 在任何输出前，用 OS 安全随机源生成并持久化 128-bit `STATE_GENERATION`（`SG01`）；写入/同步失败不激活 Kernel。兼容恢复沿用快照里的 generation，配置 revision 和执行 attempt 可以变化。`checkpoint.state_generation` 为 32 位十六进制标识；随机碰撞概率约 2^-128，不是时钟/进程号拼接。该 marker **不是 CURRENT/提交证明**，不用于跳过源码、状态或输出完成校验；K1 尚不把它当作已经实现的业务输出 ID 或 exactly-once。

快照 v3 保持 `CPL1` 外层参与者清单，在旧 reader 可读取的 `CP01` 语义字段中使用 `RCP2` 封套保存完整诊断计算和状态依赖前缀；其余包含 source cut、各 state frame、attempt/revision/generation。恢复必须匹配 Source 身份/schema、每个状态实例及其全部上游计算（仍保留全局函数语义版本检查）；线性管道比较到最后一个窗口。末端下游 Filter/Map/Project 可以调整，零状态管道可调整过滤/投影而不重放文件头，状态中暴露 `downstream_semantics_changed`。两窗口之间的计算仍影响第二窗口，窗口参数/schema/参与者变化仍拒绝。改变输出逻辑不撤销已发送的 HTTP 副作用，也不提供 exactly-once。

历史 K1 plain `CP01` 快照仍可读取，但保持原来的**完整计算严格匹配**合同；需要下游更新时，先用原计算在本版本提交一个 RCP2 恢复点，再更新配置。旧 K1 可以完整解析新 CURRENT 的外层结构，但必须在计算兼容检查明确拒绝，不把未知 manifest 当损坏、偷偷回退到更老的 plain CP01。未发布的 R11 开发 CPL2 不属于支持格式，R12 移除该只读分支。R10 SPV1 v1/v2 不自动迁移，Server 在输入激活和 generation 写入前拒绝向含旧格式历史的目录写 v3，包括 `resume_latest=false`。不要靠删除旧代绕过检查；选择新目录 fresh 或恢复原二进制和同一备份时点的整套目录。

**R10 回滚限制：**旧二进制遇到混合 v2/v3 目录可能跳过 v3、恢复旧 v2 并重复输出；“不兼容即拒绝”仅在没有可回退旧代的单 codec 目录得到旧二进制保证。K1 的 guard 阻止正常 Server 创建这种混合目录，但不能修补外部工具或历史版本已经混写的目录。回滚必须先核对全部历史 codec，在副本上验证，不能只看 CURRENT。

切点捕获与 barrier 注入保持同一 Source 驱动内的 FIFO 顺序；之后 Source 可继续读取。单飞 checkpoint worker 独立收齐 ACK，再在 blocking pool 编码/提交，不要求 durable outbox，也不跳过 required Sink flush。停止、读失败和 EOF 都收尾该 worker；已经进入阻塞提交的工作不能因 API 超时/取消而 detach，持有 gate、lease 和目录锁直到结束。满队列时 worker 仍独立被调度，不依赖 Source 的批次 send 返回。

Source 不再停读也扩大了可能重复输出的窗口：Sink barrier 完成后可以发送 cut 之后的数据，而 CURRENT 仍在提交；此间崩溃会从上一提交点恢复，期间已发出的外部输出也可能重放。这没有改变 at-least-once 边界，不是 exactly-once；外部接收方需要幂等。

`checkpoints`/`diagnose` 同时展示 `STATE_GENERATION` marker 与各快照的版本/revision/attempt/generation，允许识别同一 v3 目录中的 fresh lineage。头部元数据仅作有界诊断，运行中缓存于启动/最近完成提交；不代替 CRC、MANIFEST、PUBLISHED 和兼容性校验，未知/损坏头部为 null。marker 不是恢复依据，兼容恢复相同 marker 不重复写盘。

**旧 codec 不自动迁移**：K1 Server 拒绝 R10 的 SPV1 v1/v2；R10 也不能恢复 v3。底层 Store 仍识别旧 codec 的完整性以安全管理旧历史，但不把格式不兼容当成损坏，偷偷回退成一个旧格式/旧位置。升级需保留原二进制和整套备份，或明确选择新目录 fresh；没有在线/离线自动转换工具。旧 `CheckpointSnapshot`/`PlanLayout` 和 legacy barrier helper 仅保留嵌入单窗口合同；Server 的零/单/双状态统一走 `PipelineSnapshot` / `PipelineRestore` / `wait_participants`。没有开通 DAG 或 broker 无损恢复。

嵌入 API 变更：已有 `AlignedJob` struct literal 需显式补 `pipeline: None` 才保留 legacy 路径；K1 使用 `pipeline: Some(PipelineRestore { plan, generation, restore })`，不同时填写 legacy `restore`。非 Server 的组合层必须自行持久化 fresh generation、提供有序 cut 和真实 Sink 完成计数；Kernel 不替外部来源伪造可重放保证。

HTTP/CLI 等待超时不撤销已开始的阻塞持久化；gate 持有到 source actor 真正完成。检查 `checkpoint.active/phase/last_success_id/timed_out_waiters_total` 再决定重试。周期不是硬 RPO，`last_success_age_ms` 是本 attempt 成功提交后的单调时钟年龄，恢复旧点时为 null，不伪造历史时间。磁盘信息是有界快照，不能当作跨组件事务快照。

Sink flush 共用该次 checkpoint 的端到端 deadline，不另设固定 5 秒截止；barrier 请求释放后立即解除其 flush 等待，不把已放弃的 barrier 留在数据通路上。此取消不会伪造 outbox ACK 或清除实际丢失，且不改变已进入阻塞提交后必须等真实完成的合同。

运行中的 `/checkpoints` 使用启动/最近完成提交的缓存，明确返回 `storage_sample`；不会为监控持有 writer 文件锁而阻止新 attempt。停止后返回实时磁盘列表。怀疑失败留下临时文件或外部磁盘变更时，先停止并重新列表，不把运行缓存当成实时磁盘扫描。

`CURRENT` 持久化是提交边界；`PUBLISHED` 记录已发布证明。损坏回退只选有证明的旧点，不能拾取未发布 MANIFEST。历史 CURRENT 在替换前补证明。GC 保留 CURRENT/最后有效点并限制代数与逻辑文件字节；失败后可能多留一代并报告 maintenance error。逻辑字节不含文件系统元数据，不等于磁盘配额，仍需独立文件系统空间告警。只删除规范 checkpoint 代目录，不删未知文件。列表不修改 CURRENT，也不宣称兼容；完整 CRC/codec/语义/身份在恢复时检查。列表与选择不是 pin 协议，选点前先 stop 并等待无活跃任务，避免自动 GC 与人工选点竞态。

`resume_latest=true` 明确允许启动/重启时从提交点恢复；仅真正空历史可 fresh。损坏/缺 CURRENT、有历史但无有效点、旧 codec、变更 schema/Filter/Map/窗口语义或 File 替换/截断都会拒绝，不静默重头读。`append_only` 文件不得 truncate/rename 替换，轮转需要停机建立新源/新 checkpoint 目录。sealed/immutable 文件不可修改；具体身份失败见结构化错误与本地受控日志。

数值 `snapshot_id` 是该配置的持续恢复依赖，后续 start 仍指向它，不等于“以后自动用最新点”。运行中 GC 会 pin 该点；代数至少保留 pin + CURRENT 两代，即配置 retain_generations=1 时有效下限为 2，字节上限仍有效。希望以后追随最新点时，发布去掉固定 restore 的配置并明确 `resume_latest=true`。每条 pipeline 使用独立 checkpoint 目录；不支持多条停启配置轮流共享目录的依赖管理。

R10 对数值恢复点采用 **fail-closed**：运行失败或 Server 进程重启后进入 held，即使非 safe-mode 也不自动从旧 pin 重放；操作员确认重复输出风险后显式 start 才解锁，仍使用原数值点。不静默改用 CURRENT，也不自动发布删掉 restore 的 revision。`explain` 提示固定点覆盖 `resume_latest`；仅设周期但未开自动恢复的配置也有提示。普通失败并非无限重试（连续失败上限 16，稳定运行才重置）；单次 checkpoint 提交失败本身不等于整个 Job 重启。

提交内部使用不可伪造的 `EncodedSnapshot`/`commit_prepared` 避免把刚编码的状态再物化一遍；公共 `commit_encoded` 仍校验不可信字节。替换 CURRENT 前仍读取旧 chunk 并检查 CRC/codec，只是不构造整份旧状态；**PUBLISHED 只证明发布，不证明 chunk 未损坏**。已有有效发布证明不重写；没有删除的 prune 不额外 fsync。只读 inventory 使用 `open_readonly`，不创建目录/锁文件。

File checkpoint 会在阻塞工作线程上重新采样**实际已消费 cut** 的身份，修复“空文件启动后追加的数据没有纳入身份”的缺口；Unix 下拒绝路径已替换而旧 fd 仍活跃的提交。当前身份为有界首/中/尾抽样，不是全文件密码学校验，不能保证发现任意位置的恶意/违规原地修改。可信 append-only producer 合同仍是前提。

## 升级、备份与回退

**核心增强 B1 的额外 catalog 门禁：** 新候选将 catalog schema 升为 **v3**，加入不可变参考表 revision 与历史 pipeline dependency。即使当前没有 Lookup，首次打开旧 catalog 也会事务升级；旧 v2 binary 将拒绝 v3 catalog。回退必须使用停机一致的升级前 catalog 备份，不能编辑 version 数字或删除 pin 表来绕过。表发布、固定绑定、GC 和验证边界见 [REFERENCE_TABLES.md](REFERENCE_TABLES.md)。Catalog 兼容拒绝与 checkpoint codec 拒绝是两个独立检查，不能把前者当作已经验证后者。

1. 保存配置 revision、capabilities、诊断与二进制/source manifest；先停 desired，再等待实际停止，关闭 Server，确认 writer lease 已释放。
2. 在服务停止时一起备份 catalog（含存在的 `-wal/-shm`）、原 File 数据、**整个 checkpoint 目录**和对应密钥；校验备份。不要只复制 CURRENT，也不要热复制 SQLite 文件当成一致性备份。
3. 在隔离目录用副本验证候选包启动、解密、validate/explain、指定恢复点和真实输出。只读列表不是备份可恢复验证。旧 snapshot codec 不就地升级，保留原件；不兼容时显式新 pipeline/fresh 或离线转换，不能编造恢复成功。
   **配置预检覆盖全部仍会使用的 revision**：包括当前 desired、数值恢复/人工固定版本以及预定回滚版本，不只检查 latest。用候选版本对这些 spec 执行 validate/explain；source inbox / sink outbox 必须在 `1..=4096`，HTTP outbox 另限 1024。历史超限配置也会被拒绝，不在启动时静默 clamp；升级前发布兼容配置并验证流量/背压。数值恢复点进程重启后的 held 需纳入值班操作清单。
4. 停服务后切换 `current` 到已校验的新目录再启动。确认实际 revision/attempt、输入计数与输出正确，保留旧包和升级前备份。
5. 回退必须停服务并切回旧包；若 catalog/checkpoint 新旧格式不兼容，使用**同一备份时点的整套副本**，不混用新 CURRENT 与旧 source。先在副本验证再重开真实 Sink。

配置/二进制回退不会撤销已发送 HTTP/MQTT；aligned 不等于 exactly-once，故障重放可能重复。没有持久 outbox，进程退出时尚未确认的输出不能被当作可靠提交。

## 故障处置

- `held`：先检查失败原因/目标可达性/凭据/身份，再显式 start 解锁；不要无限自动重试掩盖错误。
- 容量等待：检查 max-jobs 与队列/父 owner；不按增大队列来承诺不丢失。best-effort 过载丢失是失败样本/容量证据，不算无损通过。
- checkpoint 超时：先看 active 与最后提交点；文件系统提交不可强制取消。磁盘满/保留失败先停止输入与调度，检查配额和无关占用；不要删除 CURRENT 或仍需要恢复的代。修复后手动 checkpoint 并实际恢复验证。
- 不要在 checkpoint 目录手工创建 `chk-*`：非规范代名称或特殊文件会 fail-closed，错误包含相关路径。不要为“解锁”删除协作锁文件；真实锁占用为可重试容量错误，不支持锁等 OS 错误则单独报告为不可重试 Internal，先排查文件系统。
- 身份/codec/语义拒绝：核对源和备份，不编辑 snapshot 字节或跳过校验；明确选择合适版本/数据或显式 fresh。
- 诊断报告 registry_busy/unknown：重试采样，不解释为零占用/健康；没有成功年龄不等于刚成功。

## 本批资源合同

API 并发请求 128、阻塞 API 工作 64、catalog/冷启动工作 128。`API_REQUESTS` 在响应/504 返回后退款；`API_WORK` 跟随真实阻塞闭包，不因等待者超时而提前退款，二者不是同一 permit。source inbox/outbox 受配置上限，HTTP 另有更严 1024 outbox/4 MiB 聚合缓冲约束。File 64 KiB 记录/32 条读取批次，HTTP 编码在增长前获取信用，response drain/timeout 有界；超大输出明确失败，不静默拆分。

checkpoint 单份 8 MiB、MANIFEST 256 KiB、最多 4096 chunks；读取前和读取过程中均限长，目录扫描/递归有界，拒绝符号链接/特殊文件。checkpoint chunks 借用已编码切片，不再复制整份 snapshot。控制元数据与第三方 TLS/HTTP/SQLite/系统分配不宣称全部纳入 payload owner；全进程 RSS 硬上限、完整 Scalar arena 仍不是当前合同。

当前 Kernel 的 Filter/Project/Map eager scratch、Window/Dedup 暂存与关闭输出在生成之前先取得保守 Reservation，并保留到有 owner 的输出 batch 接手。StateKey 计入实际容器容量/嵌套值，MIN/MAX 使用先准入、后克隆候选的更新，额度/表达式失败保留原值；payload 先销毁再退 lease。保守额度可能更早拒绝低内存/大表达式配置，不能把它当作精确 RSS。公共 raw Row/Scalar/Vec 返回型 helper 与第三方解析器是单独的有限输入边界，不承诺任意库调用都自动计费；长期 arena 仍未实现。

R10 修正 scratch 公式：Window/Dedup 不再每批预留两倍全部 retention，保留量/关闭索引使用 O(1) 计数；本批输入与真正到期的有界输出分别准入。raw 输出转成 owning batch 后、异步发送前释放 scratch，不能在输出仍未接管时提前退款。MIN/MAX 复用候选容器，值未变化不 detach；只有增长才扩 lease，替换成功后按实际大小退款。融合表达式分别估计共享值大小与新分配，不再按列数/融合次数乘算整行。快照 wire 估算与 resident 估算分离，排序引用数组另行短期计费。

`max_state_keys` 是数量上限，不是任意宽度状态一定装得下的承诺。Server 每 Job 为共享的 4 MiB reservation / 4 MiB retention，key 上限为每 operator 1024；K1 双状态允许两者各 1024，但不各给一份 Job 字节预算。单 operator 4096 keys 并非默认支持档位。总预算、输入宽度、累加器/索引和并发输出仍共同约束准入，不能靠静默扩大测试预算宣称通过。

`/metrics.state_keys/state_bytes` 是最近一次 Window 采样，不是所有 Job 的总和；key 数现在取实际状态项，不再错误地使用 MemoryLease handle 数。`state_sample_scope` 明确这一限制。

R10 去掉了按 stage 深拷贝整个恢复快照的问题；K1 接手完整实例集合，在同一 owner 下准备各 Window，按实例重建后立即释放其 decoded handoff，阶段上下文只共享已准备的实例容器。重建按逐项最大 key/accumulator 暂存及有界 bookkeeping 预留，不额外预留三份完整快照。scratch/retention/index 准入失败则清理全部准备结果，不激活输入；仍不承诺任意快照或任意预算都可恢复，也不把 raw helper 宣称为事务式原地更新。

## 自审前 v7 验证记录（2026-09-14）

> 以下为历史证据；最新 K1 结果见 [K1 实现与验收](#k1-validation)，R10 单独保留。

以下数字和指纹仅对应自审前 v7，不是后续修复代码的新证据；自审后的复验单独记录，不覆盖原始失败或性能样本。

平台：`box@100.64.0.16`，Linux 6.12.94+ x86_64，rustc/cargo 1.98.0；本机未执行 Cargo 编译。原始记录：`/workspace/bench-compare/production-artifacts-20260914/v7/`。

| 验证 | 实际结果 |
|---|---|
| 核心 / 独立无 demo Server / CLI | 490/0、25/0、2/0；各 profile 单独执行 |
| 固定二进制重复 | 79 项 × 20 轮，1,580/0；10 个测试二进制 |
| 真实进程恢复 | 指定点 sum=60；kill -9 后显式自动恢复 sum=150；实际备份文件回读 sum=240；无多余输出 |
| 生命周期 | 20 次启停 fd 12→12，SIGTERM 退出与锁释放；配置不兼容恢复不改 CURRENT |
| 鉴权/危险操作 | demo、重复 catalog、错误 token、远端明文、覆盖诊断文件、不兼容恢复等负向用例通过；TLS/超时/响应体等连接器回归包含于核心测试 |
| File 80k ABBA 对 R9 | 554,601→563,685 rows/s，中位比 1.0164；RSS +440 KiB |
| File 400k ABBA 对 R9 | 584,175→577,289 rows/s，中位比 0.9882；RSS +392 KiB |
| 周期 100 ms 开/关匹配 ABBA | 9,258→9,221 rows/s，比 0.9961；开启的 4 个正式试次成功 13/13/13/14 次，失败均 0；RSS 差 -24 KiB 属采样噪声 |
| MQTT 2k 输入/s | 7,500/7,500 输出，0 缺失/重复；capture-arrival p99 11.178 ms；466 POST / 3 TCP |
| MQTT 120 s、1k 输入/s | 90,000/90,000 输出，0 缺失/重复；capture-arrival p99 6.700 ms；18,934 POST / 1 TCP |
| 刻意过载 | 1,412/1,536 输出，124 缺失、0 重复；无损判据下 **invalid**，不伪装成通过 |
| 观察守恒 | 267 个视图 / 534 个边界队列 / 547 个 Runtime mailbox 样本一致，计量错误为 0 |
| 网络/长稳 | netem qdisc 不可用，**NOT_RUN**；真实 WAN、目标设备、24/72 h、10,000 次循环未运行 |

File 共 20 个正式试次 + 8 个缩小输入 warmup，均 valid；同规模输出 hash 一致。周期开/关 8 个正式试次 + 4 个 warmup 均 valid。预先登记门槛：File ≥0.97、RSS 增量≤1,024 KiB；周期开/关≥0.90、RSS 增量≤2,048 KiB，最终均满足。当前结果不表示所有场景都更快；p99 是捕获端到达时间，不是远端业务确认或 WAN RTT。

v5/v6 的 400k 分别约 3.1%/3.3% 开销，未过预设门槛，原始样本保留。之后去掉固定宽度聚合的多余查表/重计费、修复 group key 被 detach 两次的分配浪费；保留 scratch/owner/恢复安全校验，没有改阈值或隐藏失败。历史 v1/v2 的编译/鉴权兼容问题与打包依赖检查误匹配 `sparrow` 为 `arrow` 也保留在独立日志中。

v7 代码证据（不包含根目录过程 MD）：

- 基线 `4f70407`；代码 patch SHA256：`f0eec40539e717436998d0b3d4701579689de4b4bad3e57d876e02e9f7a27d2e`。
- 231 个构建/源码/fixture/脚本文件清单 SHA256：`4cdf5a9379b6888342f6db1ab39eb4ece528d04517586b603718c826f066dcd4`；干净基线重放与服务器逐文件校验通过。
- Server SHA256：`d0b13b92248e38dbcda8478f74c9bf497f705dab48fbc2d0a22cdc225786a13e`。
- CLI SHA256：`4584cf502baf8edbfe3096baebade9ab7df2c5c42d7fc0b49b95ce17204798d4`。
- R9 固定 driver：`4d5ffd198c7ace2183237c30a0a4790127b45b52fca5d2f2b3a5a9f81af38c7a`；周期 driver：`1385f3de40af74ef2038e40257c57f7d03bd9b3f5d248d27d3c9b2828cf030a8`（v5 冻结，后续 Window 优化没有改 driver/采集协议）。

复用脚本：`production-build.sh`、`production-smoke.sh`、`production-repeat.sh`、`production-performance.sh`、`obs-closure-verify.sh`、`obs-check-snapshots.jq`。既有 Mosquitto PID 2657777 未被停止，host qdisc 未修改。systemd 文件仅为部署模板；实际安装/云端 CI/commit/push/tag 都没有执行。本轮可进入集中 review/候选冻结，不等于正式生产认证。

<a id="self-review-20260914"></a>
## 自审修复与复验（2026-09-14）

本节覆盖 v7 后的五项修复，原始证据独立保存在 `/workspace/bench-compare/production-self-review-20260914/`：

1. **生命周期取消安全**：旧启停 future 被取消会释放 transition 锁并脱离尚未结束的子任务。`before.log` 保留新增回归在修前的实际失败；现在锁/子任务跟随有界独立工作，kill、stop-all 与 converge 使用同一入口。
2. **restore 原子发布**：原来的配置写入→kill→desired start 存在取消/故障窗口。现在新 revision 与 desired 在同一事务提交；注入 COMMIT 失败时 revision、desired 和失败 hold 一并回滚。API 用例覆盖运行中旧版本到所选恢复点的替换。
3. **恢复预算过度预留**：原实现 decoded snapshot 计费后再预留三份完整副本。新回归构造同一 64 KiB 预算下可运行、可编码的状态，确认旧预留公式失败，而逐项重建通过；保留 decoded ownership、retention/index 和临时分配准入。
4. **flush deadline 与取消**：移除 sink 写死的 5 秒。虚拟时钟覆盖 8 秒策略下第 6 秒 ACK、100 ms 截止以及已放弃 barrier 立即解除等待；pending 与实际丢失不被取消动作清零。
5. **构建目标与来源**：显式 target 同时用于 build/tree/复制路径。实际在继承 `CARGO_BUILD_TARGET=aarch64-unknown-linux-gnu` 的环境构建 Linux x86_64 包，并执行版本检查、包校验和真实进程 smoke；相对 source-manifest 校验通过。

嵌入调用兼容说明：`Supervisor::converge_once/kill_named/stop_all/shutdown` 的 receiver 现为 `&Arc<Self>`，使用 `Supervisor::new` 返回的 Arc 调用；不能再仅持裸 `&Supervisor` 调用这些生命周期方法。HTTP 路由不变；取消调用者不再撤销已准入操作。

已复验：核心 **495/0**、独立 no-demo Server **26/0**、CLI **2/0**；固定 **11** 个测试二进制的关键 **87 项 × 20 轮 = 1,740/0**，包含跨 profile 重叠，不视作不同的独立用例。真实进程再次验证 sum=60/150/240、实际备份回读、20 次启停 fd **12→12**、SIGTERM 与锁释放，以及原有六类负向用例。测试/打包在服务器完成，本机未执行 Cargo 编译。

性能也使用本次实际生产二进制复测，仍沿用原脚本和预先登记门槛，未重新编译 driver：

| 本次比较 | 吞吐中位比 | RSS 差值 | 正确性/门槛 |
|---|---:|---:|---|
| File 80k，自审后 / v7 | 0.9974（551,845→550,385 rows/s） | +172 KiB | 通过 |
| File 400k，自审后 / v7 | 1.0212（572,753→584,898 rows/s） | -104 KiB | 通过 |
| 当前候选周期 100 ms，开启 / 关闭 | 0.9825（9,258→9,096 rows/s） | -196 KiB | 通过；开启试次提交 14/14/13/13，失败均 0 |

File 20 个正式试次 + 8 个 warmup、周期开关 8 个正式试次 + 4 个 warmup 全部 valid；同规模正式输出 hash 一致。File 门槛仍为 ≥0.97、RSS 增量≤1,024 KiB；周期门槛仍为 ≥0.90、RSS 增量≤2,048 KiB。这里 File 的分母是 **v7**，不是 R9；小幅增减不解释为所有场景都会提速，负 RSS 差值也不作为确定的节省承诺。

源码/产物指纹：

- 基线 `4f70407` + patch：`8f6d35f3f831cce88466efa59d33648e2c538470a39c669f45903c40c36e8972`。
- 排序的 231 文件清单：`577c064675368e7193ee5241aa974a0ab73a5b412e9ce9b1fe987264bd76b224`；干净基线重放和服务器逐文件校验通过。此清单与 build 脚本清单的排序不同，不能直接比较两个清单文件的 SHA。
- Server：`ce5e6112f32a56bd361780dd6fe2a7540d7eb7e3f960c6afe0e5f8f932845c61`。
- CLI：`d15573b920a48bd1360bc61e9944effc632a7809b396e83a2d564707fb8ec96a`。

本次未重跑 MQTT 120 s、真实 WAN、目标设备或 24/72 h 长稳；前节 MQTT 数据仍只对应 v7。没有 commit/push/tag/安装服务，阶段 3 发行门禁保持未放行。

<a id="r10-validation"></a>
## R10 修复与复验（2026-09-14）

范围是 review 指出的现有路径缺陷及其验证，不是下一阶段功能开发。全仓纯格式变化已撤回：65 个文件逐一确认只等于“修前源码经 rustfmt”才恢复，保留之前未提交的功能改动。全仓格式化/阻断式 fmt 门禁留作独立清理；没有按 review 的示例直接拆 commit，因为当前控制面依赖运行时新接口，须按依赖顺序另行验证可 bisect 性。

**修复对应关系**：§1 的 Window/Dedup scratch、O(1) 状态计费、输出接管再退款、MIN/MAX 复用/实际退款、融合表达式 bound、freeze wire/workspace 分离已落地；§2.1 用固定点失败/重启 held 代替自动旧点重放；§2.2 去掉重复物化/proof 重写/无删除 fsync，保留旧 chunk 完整性验证；§2.3 拒绝弱/占位 token；§2.4 先 draining 后 join；§2.5 补历史 revision 升级预检，不静默 clamp。其余锁、只读 inventory、arity/函数注册、CLI 错误、诊断窗口、脚本/CI/部署模板与运维说明同步处理。

`max_state_keys` 的旧“2×全部 retention”校验建议不再适用，不能把过时公式放进 validate；当前数量上限与动态内存上限的区别见资源合同。review 建议的“有 PUBLISHED 就跳过 chunk 验证”没有采用，因为它可能在 CURRENT 损坏且新提交失败时回收最后有效回退点。单独故障注入回归证明该保护仍然成立。

服务器证据目录：`/workspace/bench-compare/r10-artifacts-20260914/`，源码目录：`/workspace/bench-compare/r10-source-20260914/`。仍是 Linux x86_64 / Rust 1.98.0；本机没有执行 Cargo 编译。`before.log` 保留三个新增内存回归在**修前生产化候选**上的实际失败，分别触发全状态 scratch、MIN/MAX 幻影计费、融合投影乘算问题；修后通过。最初编译错误和测试 fixture 修正记录保留于 `core-first.log`、`core-second.log`、`validation-1/2/`，不覆盖失败记录，不把加大 Job 预算当修复。

| 验证 | 本轮结果 |
|---|---|
| 默认核心 profile / 独立无 demo profile | **513/0 / 30/0**；后者为 Server 28 + CLI 2，包含独立 feature 编译 |
| 固定二进制重复 | **107 × 20 = 2,140/0**，12 个二进制；跨 profile 重叠不视为独立用例 |
| 多 key 实际 Kernel | 1024 keys、MIN + SUM、宽输入；Count / PT / ET + 慢输出，输出唯一且精确，结束后 task/physical 清零；另含 MIN/MAX 无关列、宽融合投影、父子退款、状态计数缓存回归 |
| snapshot / 控制面 | 有界校验与 decode 对照、chunk 损坏+发布失败保留回退点、旧 proof 不重写、只读无创建、固定点非 safe-mode 失败 held、停机拒绝新变更 |
| 真实进程 smoke | 固定点重启 held → 显式 start 原点；sum=60/150/240、kill -9 自动 latest 恢复、实际备份回读；20 次启停 fd **12→12**；SIGTERM 与锁释放、七类负向用例通过 |
| MQTT 120 s 短程复验 | 1,000 输入/s，120,000 输入、**90,000/90,000** 预期过滤输出，缺失/重复/错误均 0；另有 12 s warmup（9,000/9,000），不是 24/72 h 长稳 |
| 构建/静态检查 | 两 profile 冻结、clippy 成功（保留 warning 日志）、脚本语法、包 SHA256 校验；缺 ss/脏树正式包/非法构建模式均拒绝且不创建目标目录。云端 CI 和 systemd 实际安装未执行 |

性能预先登记并保留全部试次：多 key 用新固定 driver（Sparrow-only）；小 key 沿用历史固定 driver，不重编译/替换采集逻辑。**三组 File 的分母均为同工具链新构建的干净 `4f70407`，不是 v7/自审候选或 eKuiper。**

| 形状 | 基线 → 本轮（输入 rows/s 中位数） | 比值 / RSS 差值 | 结论 |
|---|---:|---:|---|
| 1024 interleaved keys，MIN/MAX/SUM，131072 rows，window=128 | 291,300 → 297,151 | **1.0201 / +532 KiB** | 12 正式 + 4 warmup，全 valid，同一输出 hash |
| 小 key File 80k，window=800 | 550,378 → 560,328 | **1.0181 / −108 KiB** | 8 正式 + 4 warmup，全 valid |
| 小 key File 400k，window=800 | 584,098 → 567,886 | **0.9722 / +276 KiB** | 12 正式 + 4 warmup，全 valid；约 2.78% 开销，接近但未越过 3% 门槛 |
| 当前版本周期 checkpoint 100 ms，开启 / 关闭 | 9,278 → 8,902 | **0.9594 / +316 KiB** | 8 正式 + 4 warmup，全 valid；开启提交 14/14/13/14，失败均 0 |

门槛未调整：File/多 key 吞吐 ≥0.97，RSS 增量分别 ≤1024/2048 KiB；周期开关 ≥0.90、RSS 增量 ≤2048 KiB。只说明这些有限形状通过，不宣称普遍提速，也不从单机 capture-arrival 推断 WAN p99。多 key 采用真实 Server 的 1024-key 上限，没有偷偷改成支持 4096 keys；warmup 至少一个完整 keyed window 周期，因此该形状 warmup 也是 131072 rows。

源码/二进制证据（不含根目录过程 MD）：

- 干净 `4f70407` + 完整源码 patch：`92a5a0eeca5bd3e13ee1bbd826600d7a9d54ba2bf612d725314e9954a5be7624`。
- **233** 个构建/源码/fixture/脚本文件清单：`04098c538de938551305d1c9239051185ef0f244739f273fa5c42c8455051e29`；干净目录重放、本地/服务器逐文件核验通过，与包内源码清单零差异。
- Server：`5b0601051f193c3fcfcf2dff8542e148bc835409cee60f1004256f166132fcbe`。
- CLI：`ad06c0f9a71e219024799c9742995c0ff30257140a0e4d9d5571a5880433e55f`。
- 多 key driver：`a8129b5eeec8eb2b51ed1c89cc16be2f750ab22f2f5f6e8efb485a0ef97af734`；干净 HEAD Server：`82399108c9f4f06f2429bc7618d50a5fa8d6b8e02be568feac9befe6bb0ac385`。

目标设备、真实 WAN/netem、24/72 h 长稳、掉电/flash 失败模型仍未放行；没有 commit/push/tag/安装服务。R10 的有限范围修复验证不等于整个 MEM arena、K1 恢复协议或正式生产认证完成。

<a id="k1-validation"></a>
## K1 实现与验收（2026-09-14）

> 本节保留 R11 修复前 K1 v8 的历史数据与当时判断。RCP2、source 并行推进及增强 smoke 属于 R11，不能用本节旧数字为其背书；当前实现合同以上文为准。

基线为已提交/推送的 **`9a92527`**，功能位于独立分支 `feat/k1-checkpoint-participants`，尚未提交或推送。不是修改 R10 的历史证据，也不更改版本号/tag。实现范围、v3 迁移规则与有限文件 EOF 行为见本页当前运行合同。

**功能批次已闭合，性能按场景分别放行，不是所有门禁全绿。** 真实 Source → 零/单/双状态 → required Sink → 持久化 → 新进程恢复已贯通；没有新增 broker、outbox、DAG、UI 或 Arrow/JIT。Source/Sink 配置参数可调整不等于全计算语义都可复用，恢复仍要求匹配实例集合和 CP01 描述。

| 验证 | 本轮结果 |
|---|---|
| 默认核心 / 独立无 demo | **525/0 / 32/0**（后者 Server 30 + CLI 2）；各 feature profile 单独测试并冻结 |
| 固定二进制重复 | **120 × 20 = 2,400/0**；跨 profile 重复不视作独立用例 |
| 实际 Kernel 切点矩阵 | 零/单/双 Count，空输入/全过滤/部分/完全闭合切点；原始输入独立求和校验；单 ET tumble/hop 的水位/状态恢复 |
| 配额与故障 | 双实例各 **1024 keys** 编码/恢复通过，共享字节预算不倍增；部分 freeze 配额失败后可重试；慢 Sink 真确认、取消释放；缺失状态在输入前拒绝；坏 codec/未发布代不提交或不恢复 |
| 实际 API/进程 | 零状态/双 Count 的手动、周期、选点恢复；kill -9 后分别输出 **101 / 21**，各仅一条；generation 延续；generation 持久化失败时 `jobs_started=0`、`ingested_rows=0` |
| 旧格式 | 真实 R10→K1、K1→R10 都拒绝不兼容恢复，CURRENT 不变、无输出；不是自动迁移成功 |
| 原操作闭环 | sum=60/150/240、真实备份回读、固定点 held、20 次启停 fd **12→12**、SIGTERM/锁释放及原七类负向用例通过 |
| checkpoint API 延迟 | 零/双状态各 **240** 个正式样本；p50 **7.778 / 7.349 ms**，p99 **15.024 / 13.569 ms**，max **17.419 / 14.504 ms**；含对齐、Sink 和磁盘，不是逐行延迟/硬暂停上界 |
| 恢复到 Running | 零/双状态 **63 / 62 ms**，仅各一次真实进程样本，含 CLI 轮询/启动，不当作恢复 p99 或大状态保证 |
| MQTT 回归 | 2k 输入/s，10,000 输入 → **7,500/7,500** 输出，无缺失/重复；capture-arrival p99 **11.320 ms**；467 POST / 3 TCP；观察 66 个视图、132 个边界队列、132 个 mailbox 样本守恒 |
| 静态/构建 | `git diff --check`、脚本语法、独立 production build/包 SHA256 与 clippy 通过（保留 warnings）；本机未运行 Cargo 编译，云端 CI 未执行 |

### 性能：保留 100 ms 压力点失败，不靠改门槛过关

所有正式与 warmup 试次均按真实输出校验，同输入规模的正式 hash 一致；以下比值使用各组中位数。fresh 路径与 R10 固定二进制比较；周期 on/off **都运行 K1**，不把旧版无法运行的 aligned 零/双状态当作基线。普通路径目标 ≥0.97，周期目标 ≥0.90；RSS 阈值沿用各脚本预登记值，未降低。

| 形状 | 吞吐比 | RSS 差 | 结论 |
|---|---:|---:|---|
| 既有单窗口 File 80k / 400k，K1 / R10 | **0.9946 / 0.9962** | +52 / +36 KiB | 原 3% 门槛通过 |
| 零状态 fresh 32k，K1 / R10 | **0.9816** | −4 KiB | 通过；不是内存节省证明 |
| 双 Count fresh 131072，K1 / R10 | **1.0055** | +328 KiB | 通过，小幅差异不夸大 |
| 单窗口周期 100 ms，on/off | **1.0061** | +584 KiB | 通过；4 次正式 on 试次各成功提交 13 次、失败 0 |
| 双 Count 周期 100 ms，on/off | **0.9732** | +148 KiB | 通过；提交 10/12/12/12，失败 0 |
| **零状态 + 20 ms Sink + 100 ms 周期**，6400 输入 | **0.8443** | +144 KiB | **性能门禁未过**；正确性通过，不伪装为全绿 |
| 同一零状态压力配置，扩大至 25600 输入、100 ms | **0.8625** | −172 KiB | **仍未过**；提交 71/66/67/66，失败 0，保留复测反例 |
| 同一 25600 输入/慢 Sink，周期改为 **500 ms** | **1.0088** | +320 KiB | 该配置通过；4 次各提交 11 次、失败 0 |

按当前实现，aligned 切点需要等待 required Sink 完成并同步提交；高频切点会中断持续输出的流水执行。频率对照支持这是该负载下的重要开销来源，但未将 flush、文件轮询相位和 fsync 各自耗时完全分离。**500 ms 的通过不是 100 ms 已被代码优化**：本批保留安全切点语义，不通过跳过 Sink 确认、只在空闲时伪造周期成功或修改阈值提分。此慢 Sink/零状态组合不按 100 ms 性能目标放行；500 ms 是本机已测配置，模板 10 s 及真实目标环境仍需按 RPO/负载验收。并行前缀 ACK/持久输出协议留在后续可靠链路工作，不在 K1 偷换保证。

### 复现与来源

原始目录：`/workspace/bench-compare/k1-artifacts-20260914/`；源码：`/workspace/bench-compare/k1-source-20260914/`。最终用 `validation-v8/`、`package-v8/`、`smoke-v8/`、`k1-smoke-v8c/`、`regression-v8/`、`performance-v8/`、`performance-v8-two/`、`zero-interval-100/500/`、`manual-states-0/2/` 与 `mqtt-regression-v8/`，不覆盖旧版本数据。

```sh
bash scripts/production-k1-smoke.sh PACKAGE NEW_EVIDENCE_DIR OPTIONAL_R10_SERVER
bash scripts/production-k1-performance.sh NEW_EVIDENCE_DIR FROZEN_DRIVER R10_SERVER K1_SERVER
# 同输入规模，单独验证不同频率；默认 100 ms 的失败样本仍保留。
K1_PERF_STATES=0 K1_PERF_MODES=periodic bash scripts/production-k1-performance.sh NEW_DIR DRIVER R10 K1 500 25600
```

保留开发失败记录：旧测试把零状态当作错误的预期已换成新的正向验证/仍不支持 PT 的负向验证；保持 sealed EOF 完成的回归未放宽。`package-v5/v6` 曾命中共享 **显式 x86 target** 的旧依赖，虽然源码存在 `semantics`，旧 rlib 没有该模块；只清默认 target 不够。显式 target 的工作区包缓存清理后重建成功，第三方缓存保留。归档保留 mtime 时不能只信 Cargo 的 Fresh 提示；建议独立 checkout/target，并校验源码清单和实际二进制。smoke 首次端口碰撞、脚本误用不存在的 CLI metrics 命令也保留，随后固定非 ephemeral 端口、调用已存在 API 复验。手动延迟计划误写 chunk=800，实际 driver `CHUNK=400`；修正单独记录，240 个样本没有改动或删减。

- 源码基线 **9a92527** + 完整 patch SHA256：`abe92f9edd95b4dd99757d40439f542a858bf039e5e17e6ce06d4a4bdb64a641`。
- **240** 文件清单 SHA256：`70db97c096053b74800c0d7046850dbec5efa17c439085ef3f698608ad58dc32`；干净基线重放并逐文件校验。
- Server：`e989e1903bf7571de50d26617d3efb765843337d249163b9961c6fc3ba7dca77`。
- CLI：`5bc2ce61be23de013287d786acd2e92880eb8202d7a0a13bb420717e2f29115b`。
- K1 driver：`34330536f714268ed7482d4282041604fb9db5ec8744a54581aeabdba622f2f0`。

本节不等于正式发行或全配置性能放行。100 ms 零状态慢 Sink 压力点、真实 WAN/netem、目标设备、24/72 h 长稳和掉电介质验收仍未放行；未 commit/push/tag/部署。K1 可以进入集中 review，下一功能模块仍为 K2 可靠输入/输出。

<a id="r11-validation"></a>
## R11 收尾与独立自查（2026-09-15）

最终受测候选为 `package-v8`，源码基线 `9a92527` 加 K1/R11 补丁；以下不覆盖任意网络、设备和断电场景。原始目录 `/workspace/bench-compare/r11-artifacts-20260915/`，源码 `/workspace/bench-compare/r11-source-20260915/`。本机未 Cargo 编译，服务器分组测试、冻结二进制后重复执行；完整入口 `sparrow-r11-final-validation.sh`，`final-v8.exit=0`。v8 相对 v7 最后校正了 API 的 plain CP01 兼容提示和对应断言，仍重新执行全部最终门槛，不将 v7 数字冒充最终版本。

### 修复与自查闭环

- CI 事件 diff base 无效时回退 first parent，无 parent 则明确 warning/跳过，不再空树扫描；历史行尾空白修复。CI 单独 worktree/target 构建固定 R10 基线，旧 codec smoke 成为必跑项；缺基线为 PARTIAL、退出 4。没有把本机/服务器脚本执行说成 GitHub 云端 CI 已运行。
- File 在按序捕获 cut、注入 barrier 后继续读取，独立单飞 worker 收齐 ACK 后提交；取消/EOF/读错误等待 worker 收尾，不能 detach 正在执行的 blocking commit。多 chunk 的目录同步合并为发布 ACK/MANIFEST 前一次，文件同步及 CURRENT/PUBLISHED 顺序不变。
- **性能反例与根因：**只做 source 并行的 v5，零状态 100 ms 仍为 0.8413，25600 输入为 0.8466。发现 HTTP `force_flush` 让每个 Runtime batch 都变成小 POST，同一 6400 试次请求数 112→157；独立测试修前为 18 个单行 POST，修后为 `8+8+2` 三个 POST。现在合并已排队前缀，只让尾部跳过 linger，并按请求时 sent 计数结束强制状态；不改变 required Sink ACK 条件。
- **自查发现的回滚反例：**中间候选 CPL2 外层令旧 K1 在混合历史中静默回退到 checkpoint 1，重复输出 `6,6`，真实进程记录在 `k1-rollback-before/`。最终改为旧 Store 可完整解析的 CPL1 外层和 CP01-compatible RCP2 语义封套，在兼容检查明确拒绝。`k1-smoke-v8/` 验证 plain CP01 升级可恢复，生成新点后旧 K1 回滚拒绝、CURRENT 不变、只有一条 `6`；不是删旧代绕过问题。
- 状态依赖前缀允许最后状态之后的计算更新，旧 plain CP01 保持全计算严格匹配；窗口之间的变更拒绝。R10 v1/v2 目录在 fresh/restore 两条 Server 路径都拒绝混写。所有准备窗口缺失拒绝、去掉深拷贝 handoff 的 Clone、共用 FreezeHeader、按实例配额、freeze workspace 与精确 metadata Vec 容量均已处理。
- 补齐 key/accumulator/COUNT/ET 边界、未知/重复实例、同值/冲突 State ACK、owner 隔离、generation 初始化/写失败、64 实例、legacy reader 与多 chunk 诊断测试。失败用消息区分而非仅错误码/超时，freeze 失败当场检查退款。held 保留原错误，快照诊断展示版本/revision/attempt/generation。
- 辅助 retention/index 计数 release 下检查加减，错误计入 `state_accounting_errors_total` 并使 stage 失败，不静默 wrap/归零；锁采用 exclusive create、已有路径不带 create 重开，避免替换为 dangling symlink 后在目标处创建文件。旧 CRC/GC 验证不因缓存诊断头而省略。

### 最终匹配验收

| 检查 | package-v8 结果 |
|---|---|
| 核心 / 独立无 demo | **540 / 34** 通过；后者 Server 32 + CLI 2，`validation-v8/` |
| 重复与静态 | **140 × 20 = 2800** 通过；clippy 退出 0，历史 warnings 保留；脚本语法、diff check、CI 四种 base 输入通过 |
| 生产 smoke | 输出 60/150/240，20 次启停 FD **12→12**，7 项负向、固定点 held、SIGTERM join、实际备份恢复通过 |
| 强化 K1 smoke | kill 前已有输出，零状态总输出 **[200,101]**，双 Count **[21,57]**，不是 fresh 重放也能通过的 oracle；两个进程恢复观测均 63 ms（单次、含 CLI 轮询，非 p99） |
| 升级 / 回滚 | R10 双向 codec 拒绝、旧目录 fresh 拒绝且不激活 Job；历史 K1 升级/回滚反例修后通过。CI 的可复现基线为 R10，未提交 K1 历史包的进程测试是服务器附加项，Rust 另覆盖旧 reader 外层语法 |
| 手动 checkpoint | 零/双状态各 32000 输入 × 3 轮，CHUNK=400，各 **240** 个测量；p50/p99 分别 **7.411/14.596 ms**、**7.466/14.277 ms**，含对齐、Sink、API/磁盘，不是逐行延迟或硬停顿上界 |
| MQTT 回归 | 10000 输入、2000/s，过滤后 **7500/7500**，无缺失/重复/错误；845 POST、1 TCP，capture 到达 p99 **6429 μs**。仅短回归，不代替 keepalive/长稳认证 |

性能保持原阈值：fresh **≥0.97**、周期 on/off **≥0.90**，RSS 增量 **≤2048 KiB**；所有 warmup/测量输出正确且各匹配负载 hash 相同，无丢失/重复。最终候选预先登记三组 fresh ABBA，全部 36 条测量样本合并（每 variant 18 条），未删掉任意一次试次。

| 负载 / 比较 | 吞吐比 | RSS 增量 KiB | 结论 |
|---|---:|---:|---|
| 零状态 fresh 32000，R11/R10，三组 ABBA | **0.9870** | 224 | 通过 |
| 双 Count fresh 131072，R11/R10，三组 ABBA | **0.9928** | 232 | 通过 |
| 零状态 6400 / 20 ms Sink / 100 ms，R11 on/off | **0.9994** | 124 | 通过，强制 flush 不再拆碎排队前缀 |
| 双 Count 6400 / 20 ms Sink / 100 ms，R11 on/off | **0.9475** | 428 | 通过，有约 5.3% 开销，不宣称所有形状零成本 |
| 零状态 25600 / 20 ms Sink / 100 ms，R11 on/off | **1.0059** | 256 | 通过 |

周期 checkpoint 每个测量试次均实际成功且失败数为 0：零状态小规模 18/轮、双 Count 12/轮、零状态大规模 58/59/59/58。`performance-v8`、`fresh-v8-1/2`、`zero-100-v8` 均退出 0。v6 的首个 fresh 零状态比值 0.96938 曾略低于门槛，保留原失败及预先登记的两组复查；最终 v8 独立登记/执行完整三组，不把 v6/v7 结果混入最终汇总。

来源与复现：driver 为冻结 K1 v8（SHA256 `34330536f714268ed7482d4282041604fb9db5ec8744a54581aeabdba622f2f0`），R10 对照为已核对源码的固定生产包，均未在共享 target 重编译。脚本 `production-k1-performance.sh` 默认跑 fresh+periodic；额外 fresh 用 `K1_PERF_MODES=fresh`，大规模用 `K1_PERF_STATES=0 K1_PERF_MODES=periodic ... 100 25600`。原始计划、每组 summary、日志和 `.exit` 都在证据目录。

- 完整代码/脚本补丁 SHA256：`f2242a1a25004a4ddc5f6ade6b532a76d9d6a3b6c58dbad34bbf0ffc3b426df1`，从干净 `9a92527` 重放，**241 文件**逐一校验。
- 源码清单 SHA256：`e2501d41e4940f40cd9ea6015c28764ca67d48bc2d6cf0d2464624fc40777bc4`，与生产包 source manifest 相同。
- Server SHA256：`6feffdd99734885bf3777a5c3fd211bf5186284fbefb0ca594fc2b49d7d2abde`；CLI：`9d7df7ca6ce43988f55096a52fa75f7b7fc700046bdaeefa72b9aaa7c7ea2a19`。

保留开发过程反例：CPL1/CPL2 查找 fixture、API If-Match/allowlist/busy polling、直接 Window 测试需消费 pending emission 才推进 holdback，均在 `core-v1..v4` 与 direct API 日志中；没有改预算或放宽原断言来制造通过。R11 的 MIN/MAX 批内 key 缓存、上一代 CRC/GC IO 缓存和 held catalog 读缓存仍为后续低优先级优化，不冒充本轮已实现。真实 WAN/netem、目标设备、24/72 h 长稳和掉电仍未认证；本批不自动 push/tag/部署。

<a id="k1-k4-reference-validation"></a>
## 2026-09-17：K1～K4 静态表组合恢复与迟滞增量

本轮是用户授权的“补齐 K1～K4 剩余核心”中的完整可验收增量，**不是整个 K1～K4 完成**。停机暂停的确定性时间协议、HoldFor/Debounce、冷却、静默/离线、告警生命周期、Resample 和生产环境门禁仍未完成；K5/前端未启动。

### 实现范围与兼容

- 新 profile9：线性 File＋静态 Lookup＋1～2 Count/IoT(TTL0)。原无状态引用仍为v8，不在旧profile中扩写状态。
- 新 profile10：线性 JetStream＋静态 Lookup＋0～2 Count/IoT(TTL0)，保存可靠输出cursor，epoch必须等于state generation。
- 新 profile11：required File→HTTP DAG＋静态 Lookup＋最多16个 Count/IoT(TTL0)，完整图语义、全部来源切点及精确表revision/SHA/runtimeCRC一起验证。
- 新 Hysteresis 使用 Bool latch/kind6。无引用 File使用v12、JetStream使用v13；有引用时使用9～11。参考表沿用CPL3，其他迟滞沿用CPL1，新外层version/profile保护旧reader；旧3～8的语义和已支持编码不改变，不自动迁移。
- 正TTL、ET/PT/temporal/Dedup、side/lossy图及JetStream DAG均不因本轮而开放。校验也覆盖手工构造/解码的时间参与者、新旧profile篡改、错误拓扑和外部owner依赖。

### 匹配功能与故障证据

服务器 `box@100.64.0.16`，证据根目录 `/workspace/bench-compare/k1-k4-core-artifacts-20260917/`；冻结 `package-v7-default` / `package-v7-jetstream`、`frozen-v7`，最终功能脚本 **`regression-v7d.exit=0`**。

| 验证 | 结果与边界 |
|---|---|
| Rust reliable/default-members | **726 passed、17 ignored**；ignored不当作通过 |
| 独立无demo | **44 passed**，不同feature组合，不与前者相加声称独立测试数 |
| 新专项 | 人工清单**26×20=520**，Plan4/Runtime15/Control7；9类fake inventory/summary反例通过 |
| 新真实进程 | Go v11 oracle，**9种**独立场景，另含旧B2对新9～13及新无引用profile的拒绝检查 |
| 旧矩阵回归 | B2 **25×20**、B1 **38×20**、A **8×20**、K4 **50×20**、K3 **24×20**及各自进程通过；K2 **32项**含broker与独立进程、default/K1 smoke及旧codec/catalog回退通过 |
| 静态与构建 | Clippy普通模式退出0，**100条warning**，不是`-D warnings`；Go vet/build、两套生产包及SHA校验通过 |

9种场景为 File Lookup→Count→IoT、File Count→Lookup、JetStream Lookup→Count、JetStream Lookup→IoT、Lookup→Branch→两个required HTTP、双来源Lookup→Union、Lookup→Count→Hysteresis→双required HTTP，以及无引用File/JetStream迟滞。

- table r1在r2/r3发布和GC后保持固定；进程读真实CPL3依赖、state count、Source cut，以及MAN2长度/分块CRC/PUBLISHED证明。
- HTTP body已接收但响应被hold时，CURRENT和broker ACK不得提前。先SIGKILL再释放夹具，从旧切点重放；Union验证各Source子序列，不假设全局排序。
- `CURRENT.tmp`目录注入真实I/O失败。File路径验证精确API错误；JetStream可能先向API waiter返回`cancelled: job stopping`，必须从最终actual状态验证`checkpoint io:`和`is a directory`，不能把任意取消算作预期故障。旧CURRENT及其发布payload保持，broker pending未被ACK，恢复后相同业务数据/输出ID重放。
- Store可在尝试提交前回收不受保护的旧generation以满足retention。本轮检查受保护CURRENT/payload，**不承诺失败提交保留全部可回收历史**；旧reader/profile拒绝路径则检查完整复制历史不变。
- 迟滞独立golden为57,60,55,60,54；保留Active后重放输出55,60,54。混合DAG在cut=3保存pending Count＋Active latch；输入6,7,1后输出13，后缀7,1,1,6,7输出2,13，强杀恢复两required sink仍精确输出2,13。

```text
base commit      1dd17c8186b5f52f2cea85a5fae1a94d79722b22
source manifest  0a53e656817e135de9f727e7b9e877dbaddbf7d926c5bf9f387a89689f9bdc8f
default server   cd9611457cd4e6fd49ce4f6bc06b0ac8991f58c71a24664b942df1603d4b7843
JetStream server b6e5712cc44f419a070c2d925f6be16af8f54ca615b6fb53d29e69b66d102750
Go v11 oracle   e0190687488ce2ef7d70cabae49359680a2f7e78d70c04648610cb8d60c6b914
```

Rust/生产二进制固定为v7；Go v11源码和binary另在`driver-v11/`、`k1-k4-reference-process-v11`归档。v7原始source manifest没有伪装包含后来的Go夹具修正。失败v1～v6构建、v7a～v7c进程记录保留：包括helper/import/有效ET图fixture、旧拓扑诊断优先级，以及停止后volatile状态已不可用、5秒采样指标不能当作实时ACK事实、可靠commit失败先取消API waiter等。只修正对应原因，未通过放宽运行时恢复规则制造PASS；未在本地运行Cargo。

复用入口（先在服务器独立构建Go oracle；验证脚本不编译）：

```sh
go build -trimpath -o DRIVER tests/k1-k4-reference-process/*.go
bash scripts/production-k1-k4-completion-validate.sh NEW_EVIDENCE PACKAGE_JS FROZEN DRIVER OLD_B2_SERVER NATS_SERVER 20
```

### 本轮性能与生产门禁

原K4 v5为fresh对照；周期仍为同候选on/off（100ms checkpoint、20ms应用响应等待）。预定完整三组ABBA，保持fresh≥0.97、periodic≥0.90、RSS增量≤2048KiB。三组单组及全样本合并均通过，`performance-v7.exit=0`，未删除任何试次：

| 负载 | 合并吞吐比 | RSS增量 KiB | 实测样本 |
|---|---:|---:|---:|
| fresh 零状态 | 1.014472 | 844 | 36 |
| fresh 双Count | 1.002904 | 568 | 36 |
| periodic 零状态 | 0.999027 | 180 | 24 |
| periodic 双Count | 1.000047 | 108 | 24 |

全部输出完整、无缺失/重复/非法行且对应hash一致；周期checkpoint每次测量成功数为正、失败0。这是本候选的File回归门槛，不是新增参考表路径的极限容量、WAN/p99或eKuiper新对照；也不改写历史候选的失败样本。

真实TLS/WAN、目标设备容量、24/72h与介质掉电模型仍为NOT RUN。未commit/push/tag或部署；既有Mosquitto PID2657777未被停止，没有修改host qdisc。
