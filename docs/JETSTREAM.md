# JetStream 可靠输入/HTTP 输出（K2 Preview）

状态：实现与验证中，未发布、未部署；不能沿用 R11 的长稳或性能结论。设计决定见 [ADR-004](adr/004-jetstream-reliability.md)。默认构建仍不包含 NATS SDK。

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
- 保留 **Explicit** ACK：独立 worker 最多 16 个确认请求，共享有界队列/工作区，每条最多 3 次尝试；负响应不算确认。提交后仅调度 ACK，不在 source select 分支串行等待 N 个 RTT，停止时取消并 join worker；ACK 失败耗尽重试才结束 attempt。broker 断线仍 fail-closed，不承诺透明重连。
- `AckAll` 未采用。不能忽略重投递后的 consumer sequence 与旧 reply token，直接替换确认策略；以后改变需独立 conformance/恢复测试。
- `timeout_ms` 应覆盖目标负载下的正常 HTTP 排空时间。过小不会通过 ACK 丢弃输入，但会增加 checkpoint 失败与积压；持续慢端点仍需限流/容量规划。HTTP waiter 超时不取消已开始的 durable commit。

## 独占、恢复与回退

`JETSTREAM_OWNER` 私有文件与 canonical checkpoint 目录绑定。KV 记录逻辑 consumer 的 owner 及 attempt reader nonce；旧 reader 删除完成后才改写绑定，新 reader 从 **已提交 cut + 1** 创建，而非采用旧 consumer ACK floor。不同目录误用同一 consumer 会拒绝。

本地 `WRITER_LOCK` 持有到 actor、blocking commit 和实际 SDK I/O 关闭。`drain()` 返回仅代表命令入队；SDK event-loop 生命周期哨兵负责关闭确认、资源与锁保留。即使关闭 waiter 超时，旧 SDK 仍持有锁，不能重叠接管同一目录。这是**协作式单节点独占，不是分布式 fencing/HA**；禁止复制相同 owner/catalog 后在第二节点并行运行。

- snapshot v4 保存 Source cut、完整参与者状态、输出下一个 ordinal；File 仍写 v3。两种生产 writer 拒绝混写同一目录，旧格式不自动迁移。
- v4 恢复要求完整计算语义及来源/reader 绑定匹配。当前拒绝 downstream 语义变更、固定历史点 replay/reset 和移动目录；不能为允许更新而在内存里临时换 epoch，制造崩溃后的 ID 冲突。
- `sparrowctl checkpoints NAME`、`status NAME`、`diagnose NAME` 可检查恢复点/有效保证；列表不会改变 CURRENT。`checkpoint.reliable_source` 明确区分 published/committed cut、pending 条数/字节与采样年龄，pending 包含尚未确认的 ACK。
- `sparrowctl checkpoint NAME` 与自动/source-full checkpoint 共用单请求仲裁；busy 是可重试冲突，不是已提交。HTTP waiter 超时不取消已经开始的 durable commit。
- 故障排除后使用已有、带鉴权/审计的 `sparrowctl start NAME` 从最新提交点恢复。毒消息、过期或语义不兼容需先制定保留原始 broker/checkpoint 的修复或迁移方案；此 Preview 不提供危险的跳过/任意历史回放入口。

远端使用 `tls://` 和 `token_secret`（SecretRef），不得在 URL 放用户名/密码/token，不允许 skip-verify；信任根使用 SDK 的系统证书配置。仅连接 allowlist 中明确列出的地址并忽略自动发现的新节点。TLS/真实 WAN/故障时效尚须匹配构建验证，不以本机明文测试代替。

### 运维修复与清理

- 删除管线不会自动删除 stream/KV。停掉引用该逻辑 consumer 的全部 Job，确认 SDK 已退出后，保留 checkpoint/owner/KV 备份；由管理员核对绑定中的 attempt nonce，删除对应 reader。reader 另设 24 h inactivity 回收，**KV 绑定不自动过期**。仅确认永久弃用这个逻辑身份时才按 broker 的 KV 管理流程清理绑定；需要复用身份应优先恢复原目录，不强行覆盖。
- `JETSTREAM_OWNER` 损坏默认拒绝。只有确认是首次创建中断、没有 CURRENT/历史快照且 broker 尚无该绑定时，才可备份并移除这个未完成 marker 后重试。已有绑定或来源不明时必须恢复匹配备份/制定迁移方案，不能一概删除重建。

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
