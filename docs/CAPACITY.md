# 第5批：容量与尾延迟验证

本批在 `box@100.64.0.18` 的隔离进程/目录上测量，不改变现有服务、预算或broker持久性。基线为Action s6；旧File性能门禁仍保留原time-graph-v13二进制、驱动、3组ABBA、输入量和阈值。诊断大样本、CPU采样、容量阶梯不是替代门禁。

## 已固定的口径

`tests/capacity/plan.json` 明确每个案例的来源、Sink、记录数、请求速率、并行Job数、消息填充、窗口、响应延迟、空闲间隔及p99目标。默认每Job reservation/retention各4 MiB、queue 2 MiB，不增加队列/pending额度；JetStream默认pending128、pull8、73728 B、Explicit ACK，broker File存储、单副本、sync_interval=always。

- **正确性与容量分开**：输出数量、顺序、数值、来源/Job标识、可靠输出ID、最终durable cut/独立broker ACK floor必须匹配。高负载排空成功只说明此有限数据集没有丢重，不等于持续速率达标。
- **持续目标**：实际发布速率达到请求值95%以上，且p99达到案例预定目标；带恢复扰动/空闲逐条的案例单列，不标为稳态通过。当前默认p99目标100ms，仅为本参考矩阵目标，不是用户部署SLA。
- **时间**：paced输入从同一Go进程的实际网络写起点，到完整HTTP请求体到达接收端；Count使用窗口最后一条输入。包含client/broker排队与落盘成本，不伪造设备时间。HTTP响应delay在接收之后，ACK/提交完成时间另记。p50/p95/p99使用empirical nearest-rank；12条空闲样本的p99是最大值，不能当作长尾分布认证。
- **File**：预装文件不计生产者写入；HTTP量到实际末条请求到达，File Sink量到completed观测（含10ms状态轮询）。这些不是paced端到端p99。
- **采样**：每200ms记录本次Server进程CPU ticks、VmRSS/HWM/线程数、各Job发布/输出/提交/pending状态及broker持久消息数。不同系统的采样不是原子快照，不拿两个错位瞬时值断言ACK越界；最终静止边界单独核对。原门禁不启用本采样器。
- **诊断指标**：`/v1/metrics.process_credits` 是tracked Job reservation/retention/queue/physical/handle及峰值，不是RSS，也不是并发原子快照。停止后在无活跃Job的稳定边界验证余额/handle为0。allocator缓存导致RSS不立即回落，不凭RSS一项认定泄漏。
- **范围**：阶梯1k/5k/10k/20k、Count、双Job、20ms响应延迟、过载后恢复、消息8KiB/拒绝16KiB、File多Job/NDJSON，以及HTTP Push参考案例。MQTT用原冻结驱动另测；所有值只对实际成功的案例成立。

## 实现改动及不可变条件

1. 常见≤16字段、无Bytes/Dynamic的旧JSON输出直接借用标量编码，不再逐行克隆JSON树。保持旧字段顺序、Float/NULL/UInt64/时间戳/escaping与可靠envelope字节；wide/structured或serde_json启用preserve_order时走原路径。固定索引scratch，不放大body、批次或队列上限。
2. `source.jetstream.idle_backoff_max_ms` 可选5～250，缺省仍250。只适用于普通可靠Actor（零状态/Count、其已支持的TTL0 IoT/静态Lookup组合），持久time/observed profile明确拒绝，避免配置被忽略。调小可降低空闲首条等待，但增加空拉取/CPU/网络开销；不改变ACK cut、恢复语义、重连或健康授权。
3. JetStream解码额度失败仅保留安全的数字诊断 `source_sequence/credit/used/request/cap`，不透传原始payload、subject、credential或任意错误文本。

例如对已获准的普通JetStream管线，在现有 `source.jetstream` 配置中增加 `"idle_backoff_max_ms":20`。省略该字段仍为250ms；`effective.jetstream_execution.idle_backoff_ms.maximum` 随实际配置报告，不再固定显示250。该值不是端到端延迟承诺，HTTP linger、提交、broker和网络排队仍需相加。配置不能用于持久time/observed profile；这些管线会明确拒绝，而不是忽略配置。

回退旧版本前，须用当前版本的正常配置接口移除此新字段并按原流程停机/备份；旧 `JetStreamSpec` 的unknown-field检查会拒绝它。缺省None不写出字段，既有无此选项的配置不受影响。本参数不改变checkpoint计算语义，但其他SQL/算子/profile仍需独立满足原版本兼容合同，不能据此自动升级或重写checkpoint。

配额详情同时进入结构化错误与最终 `actual.last_error` 的安全消息部分：Supervisor保存message，不应仅在内部context补充后就宣称API诊断已修复。负例还必须命中 `source_sequence=1 (resource_exhausted)`、无输出且broker ACK floor为0，不能用任意任务失败冒充消息边界验证。

没有将Explicit ACK改成AckAll，没有删ownership/retention验证，没有降低fsync/HTTP flush保证，没有以增大默认预算换容量。新退避参数不是MQTT保活参数，也不是调节File EOF轮询。

## 验证与产物

驱动在服务器编译：`tests/k1-k4-reference-process/*.go`，通过`--capacity-plan ... --capacity-case ...`选择预声明案例。新产物目录必须不存在，旧失败不覆盖。运行产生case/spec、逐时采样、发布/输出/最终ACK、停止额度和独立容量结果；`valid`与`latency_target_met/sustained_target_met`含义不同。

```sh
go vet tests/k1-k4-reference-process/*.go
go build -trimpath -o DRIVER tests/k1-k4-reference-process/*.go
bash scripts/production-capacity-validate.sh NEW_ART BASELINE_JS_PACKAGE CANDIDATE_JS_PACKAGE FROZEN DRIVER NATS_SERVER 20
```

验证脚本本身不编译：先核对5项精确测试清单并重复20轮，再执行14案例各一组ABBA，加新低空闲参数仅候选两次，共58试次。每个案例的失败单独保留并继续采集，结束返回非零；不自动重跑、替换样本或把排空成功当作p99通过。旧API没有process credit诊断，报告 `credits_verified:false`；候选成功/预期拒绝案例必须验证停止后账本与handle清零。

`finite_input_rows_per_s` 以输入等价值计速（Count不是输出行速率）；`total_through_final_ack_ms` 不包含随后stop；HTTP请求数/连接数用于核对复用。采样/排空由轮询观测，不能当作精确kernel耗时。旧s2驱动曾把stop计入final-ACK耗时，字段还名为 `finite_http_rows_per_s`：历史文件不改写，不把它的该字段与修正后结果混算。

本批测试与数值在完成后写入[PRODUCTION](PRODUCTION.md#capacity-validation)。TLS/WAN、ARM/真实目标设备、24/72h、物理ENOSPC与掉电仍需独立第6批验证，不由短程矩阵自动放行。

## 2026-09-26：s4 有限容量结果

证据根：`/workspace/bench-compare/capacity-20260926-Jw3FQJ`；最终候选 `source-s4` / `package-s4-jetstream`，驱动 `capacity-driver-s4`。8 vCPU Linux x86_64参考服务器、同机loopback、上述默认预算和持久性。下表范围为候选两次试次的最小～最大值；双Job取全部Job/试次范围，不将两个p99平均。

| 案例 | 候选p99 | 实际结论 |
|---|---:|---|
| JetStream零状态1k/s，10k行 | 16.702～17.420 ms | 有限10s目标通过 |
| 零状态5k/s，50k行 | 19.426～25.456 ms | 有限10s目标通过 |
| 零状态10k/s，100k行 | 8.204～8.714 s | 无丢重且最终ACK一致；停止发布后还需7.269～7.443s排空，容量目标未达 |
| 零状态20k/s，200k行 | 无有效完整试次 | 新旧版本各两次均触发5s fetch reply期限并held，不能列为可用容量 |
| Count(3) 10k/s，99999行 | 8.193～8.750 s | 完整输出33333行/Job，最终ACK一致；容量目标未达 |
| 两Job，各5k/s、50k行 | 43.897～52.822 ms | 本有限双Job目标通过；不能外推任意规则数 |
| HTTP响应延迟20ms，1k/s | 49.201～49.436 ms | 150ms目标通过；响应延迟不是真实WAN RTT |
| 过载后去除20ms延迟 | 2.562～2.587 s | 完整恢复/排空，额外排空1.481～1.924s；未达到预声明1s尾延迟目标 |
| 每600ms一条，默认idle250 | 256.412～257.360 ms | 不满足100ms目标；默认策略未改 |
| 同负载，idle20 | 27.999～28.478 ms | 本12条小样本目标通过；p99实际上是该试次最大值 |
| 8KiB padding，96条@100/s | 18.967～19.162 ms | 指定三字段schema通过，不代表任意8KiB业务消息均可接纳 |
| 16KiB padding，一条 | 不适用 | 明确reservation拒绝、0输出、broker ACK floor=0 |
| 两Job File→HTTP，各32768行 | 不报告paced p99 | 数量、顺序和来源身份通过 |
| File→NDJSON，32768行 | 不报告paced p99 | 独立读盘oracle通过，开启sync_data；非掉电认证 |
| HTTP Push→HTTP，500/s、5000行 | 6.652～6.659 ms | 本有限10s目标通过 |

58试次中54个满足有限正确性（含预期配额拒绝），4个20k试次失败，`matrix-s4/exit=1`。10k、默认idle和过载恢复的p99未达项不能被54/58正确性计数掩盖。两次10k的实际发布速率均约9999.5～9999.8/s，并非生产者根本没发到10k；但这不能单独定位broker落盘、source拉取或提交中的根因。双5k成功也不是单Job10k已解决。

- 每个成功HTTP Job均只建立**1条输出TCP连接**，例如双File每Job512个请求、10k JetStream每Job2560～2815个请求；本配置inflight=1，不代表任意HTTP服务器/断连条件下永不重连。
- 候选28个成功/预期拒绝试次停止后，五类tracked余额/handle全部为0。成功试次Server VmHWM约19668～25364KiB；不是4MiB Job预算的违反证明，也不是RSS硬限制或长稳无泄漏认证。20k失败试次没有借用这些退款结论。
- idle250→20的12条试次，pull总数从101增加到364～365，Server CPU从12 ticks增加到15～17 ticks；这是可测取舍，不是“免费”降延迟。ticks按本机CLK_TCK=100解释，包含该试次控制成本。
- 16KiB负例最终API错误保留 `credit=reservation used=3785632 request=1055056 cap=4194304`，没有泄漏原始payload；不是把预算加大后重测通过。

本批明确改善了小型JSON编码和可选空闲唤醒，并补齐诊断/测量；**没有宣称解决10k/20k可靠持续接入**。高负载剩余项继续按OPT-001/004/012跟踪，真实目标设备和生产发行门禁保持独立。

### MQTT旧路径与时间型成本复查

`mqtt-capacity-s4` 复用原冻结K1驱动，MQTT Filter→HTTP、QoS0、broker TCP_NODELAY与source QUICKACK开启、inbox_wait_ms=20、HTTP64行/5ms linger/inflight4。每档固定before→after，每版本1次小规模warmup＋3次10s测量，共32试次（24次测量）；不是新的三组ABBA或eKuiper对比。所有试次输出按过滤后的独立oracle完整、0缺失/重复/非法行；hash按相同rate/input_events分组核对，不能要求小warmup与完整测量的hash相等。

| 请求速率 | 候选实际输入等价吞吐/s | 旧Action s6 p99 | 新s4 p99 |
|---|---:|---:|---:|
| 1k/s | 999.36～999.79 | 6.790～6.830ms | 7.036～7.051ms |
| 5k/s | 4997.66～4998.73 | 6.270～6.298ms | 6.252～6.287ms |
| 10k/s | 9992.26～9996.01 | 6.302～6.338ms | 6.329～6.355ms |
| 20k/s | 19985.45～19992.33 | 5.021～5.076ms | 5.028～5.066ms |

因此本轮**MQTT 10k/20k没有观察到丢重，但p99并未普遍领先旧版本**。上方未达的是默认预算下的JetStream持久链路，两种交付合同不能混为一谈。MQTT每试次输出连接1～2条，仍复用连接，不是每POST重连；QoS0有限loopback完整不能保证任意断网/崩溃下不丢失。

`timed-cost-s4` 使用旧独立paused-time oracle，6种File/JetStream时间场景及两组100行成本观察通过：单key、Debounce leading-only、逐行POST/决策完整提交，0人工响应延迟约**154.8行/s**，20ms响应延迟约**38.3行/s**。这是当前短程成本，不与2026-09-19数字直接作优化倍率比较；没有实现group commit，也不能给Alarm/Silence/Resample或大状态/多规则外推容量。
