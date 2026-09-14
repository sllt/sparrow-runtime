# 当前生产化候选：安装、恢复与回退

本页说明当前实现合同，不是目标设备认证或正式发行公告。Linux x86_64、Rust 1.98.0、锁定 Cargo.lock；Row/prepared/fusion，线性执行。Arrow/JIT/DAG、HA、可靠 MQTT、持久 HTTP outbox 均未开启。版本号仍为 0.1.0，tag/push 另行授权。

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

只支持 File/replay + 单 Count/ET tumble/ET hop aligned。先验证完整状态语义与 File 身份；零/多状态、PT/Dedup/Lookup/MQTT 恢复被拒绝。每个 attempt 只有一个排队/进行中请求，手动和自动共用 gate，错过 tick 跳过，不积累无限任务。

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

`max_state_keys` 是数量上限，不是任意宽度状态一定装得下的承诺。Server 当前每 Job 为 4 MiB reservation / 4 MiB retention、1024 keys；4096 keys 并非默认 Server 支持的 benchmark 档位。总预算、输入宽度、累加器/索引和并发输出仍共同约束准入，过大配置应调整数据/资源，不靠静默扩大测试预算宣称通过。R10 的匹配验证记录独立于下方历史 v7/自审证据。

`/metrics.state_keys/state_bytes` 是最近一次 Window 采样，不是所有 Job 的总和；key 数现在取实际状态项，不再错误地使用 MemoryLease handle 数。`state_sample_scope` 明确这一限制。

恢复快照进入 Kernel 时只接手一份受计费的 decoded state，stage context 共享一次性 slot；唯一 Window 消费并重建后立即释放原件。修复了此前 `AlignedJob::clone` 把整个 WindowFreeze 复制到每个 stage、并持有至 Job 结束的问题。重建按逐项处理的最大 key/accumulator 暂存及有界 timer bookkeeping 预留，不再额外预留三份完整快照。scratch 不足在修改状态之前拒绝；逐项 retention/index 准入仍可能失败，此时启动失败并清理新任务，不承诺任意快照或任意预算都可恢复，也不把 raw helper 宣称为事务式原地更新。

## 自审前 v7 验证记录（2026-09-14）

> 以下为历史证据；本轮最新结果见 [R10 修复与复验](#r10-validation)。

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
