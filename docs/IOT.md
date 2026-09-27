# K4：变化检测、告警、静默与重采样

**2026-09-23 时间型 DAG 已按限定 Preview 验收：** v18 将下方暂停时间算子组合扩展到 required File→HTTP 图；v19 单独承载 ET 窗口和 TTL0 IoT，不把 HoldFor/Debounce 解释为 event-time 模式。[合同](DAG.md#time-graph-recovery)、[匹配证据](PRODUCTION.md#time-graph-validation)。以下 v14～v17 的线性边界保持不变；新图支持不代表冷却、离线检测、告警生命周期或重采样已实现。

本次自查将 v14～v19 的输入指纹限制前移到准入：时间 journal 当前只编码一等标量源字段，不能让 nested/Dynamic 源列先通过检查、到第一条非 NULL 输入才失败；下游 Project 丢弃该列也不绕过完整输入指纹。live 模式及未使用时间 journal 的旧 profile 不受这项新增校验影响。

<a id="business-loop-plan"></a>
## 当前批：完整 IoT 业务闭环（IOT-06～09，开发中）

下面是本批范围；已实现范围以各 profile 的合同、capability 和匹配验证为准，不能把未覆盖组合当作已支持。沿用同一 Runtime、持久暂停时间和预算/恢复协议，不做前端，不把旧 Hysteresis 的原行输出偷偷改成告警事件。

1. **IOT-08 告警生命周期先行**：明确 Normal/Pending/Active/Recovering 和 enter/clear 条件、持续触发/恢复时长、等时到期先于输入；无样本保持最后有效条件。activate/resolve 带稳定 episode，使用独立输出 schema 与 codec。Active/Pending 不默认 TTL 淘汰；状态满明确失败，不能静默丢失告警。
2. **IOT-06 冷却/通知限频**：在状态转换之后控制通知，不冻结底层观察；明确恢复通知、等待通知的合并/丢弃策略、数量/字节/年龄上限。完整 episode、待通知、计时和输出身份同切点持久化，重放不产生新业务身份。
3. **IOT-07 静默/离线**：关联可重放的来源健康事实，区分设备静默、Source 断连和 pipeline 停止；仅对已观察或显式登记的设备判定，停机时间不推进。不能用 Graph idle 或缺失输入直接冒充连接断开/全体设备故障。
4. **IOT-09 Sampling/Resample**：逐个定义 last/mean/interpolate 的半开区间、缺样、输出时间、未来点需求和延迟上限；输入舍弃与缺值分别计数。key、展开、窗口/timer/保留点均有界，恢复后的剩余时间和输出身份可重现。
5. **整批交付门禁**：API/Graph bind→执行→恢复→诊断→可运行业务模板贯通，再自查和集中服务器测试；覆盖无输入到期、等时边界、异常输入、取消/退款、慢 Sink、journal/CURRENT 故障和真实 SIGKILL。新 schema/profile 与旧目录明确隔离；尚未接通/未测的组合继续拒绝。最后复跑旧功能和性能门槛，不借用 v18/v19 的成绩。

### Alarm + 通知冷却：开发候选合同（尚未整批验收）

新增 `kind:"alarm"`，`iot.fields` 的两个 Bool 字段依次是 **enter、clear**。共同参数要求 `emit_first:false`、`ttl_micros:0`、非 nullable keys、最多 48 个扁平 scalar 输入字段。`timing.kind:"alarm"`、`clock:"paused"`，显式设置 `activate_micros`、`resolve_micros`、`cooldown_micros`（均 ≥0，0 表示立即）和 `notification_max_age_micros`（>0）。NULL 或同时 enter/clear 按 `invalid` 处理；ignore 不更新 retained row、状态或 deadline。

- Normal + enter→Pending，连续 enter 满 activate 时长→Active；Active + clear→Recovering，连续 clear 满 resolve 时长→Normal。带内 `enter=false,clear=false` 取消 Pending 或 Recovering；无样本保持最后有效条件。等时先处理到期，再处理输入；跨多个期限只使用实际持久决策时间，不伪造中间观测。
- **生命周期不被冷却抑制**：activate/resolve 总是输出。首次 activation 可立即通知；上一通知的冷却未结束时，activate 输出 `sparrow_alarm_notify:false`，每 key 仅留一个待 activation 通知。冷却结束输出 `event:"notify"`，使用该 episode 的最新有效整行。重复样本不产生周期提醒，也不延长等待年龄。
- resolve 永远立即通知、取消该 key 等待的 activation 通知，并重新开始冷却；不会冻结底层状态或发出已恢复告警的迟到通知。等待年龄达到上限（含等号）先过期，计数而不补发；长 tick 不能把过期通知补成新通知。条件与通知 deadline 相同，条件转换先执行。
- 输出在原行后新增七列：`sparrow_alarm_event`、`sparrow_alarm_phase`、`sparrow_alarm_generation`（32 hex）、`sparrow_alarm_operator`、`sparrow_alarm_episode`、`sparrow_alarm_time`（逻辑微秒）、`sparrow_alarm_notify`。保留名称碰撞拒绝。完整 episode 身份是 **generation + operator + 配置的全部 key 值 + per-key episode**，不能只用序号跨设备去重；传输层仍另有 required HTTP 输出 ID。
- episode 只在真正 activate 时递增，resolve 沿用同一个 episode；Normal 也保留 counter，不做 TTL 淘汰。每 key 最多两个逻辑 timer，待通知不另存无界队列；整行/key/index 同 Job 计费。达到 key/timer/字节上限明确失败，不静默丢弃 Active/Pending。嵌入调用者必须在恢复/输入前绑定持久 generation；reset 后必须使用新的身份，不能复用旧 episode 命名空间。
- 独立状态 kind11，候选 outer profiles **File20 / JetStream21 / File图22**。继续要求暂停时间、required HTTP、独立 checkpoint 目录、CURRENT/TIME_PENDING；不混 ET、参考表或侧路，不自动升级旧目录。公共 Bound/Physical IoT 节点新增显式 output schema，手工构造嵌入计划也必须提供并校验。

模板 `deploy/pipeline-iot-alarm.json` 共用 `stream-k4-telemetry.json`：温度 ≥60 持续 1s 激活，≤55 持续 0.5s 恢复，通知冷却 5s、最大等待年龄 10s。模板先 Project 产生 enter/clear，**不要在 Alarm 前 Filter 掉正常值**，否则它无法看到恢复条件。需要仅发通知时在 Alarm 后 Filter `sparrow_alarm_notify`；保留完整生命周期则接收全部事件。

诊断新增 `alarm_notifications_deferred/expired/cancelled`，进程计数不作为持久业务账本。上述是本批开发合同和模板，仍待匹配全量/故障/性能验收；离线/重采样尚未因此完成。

开发验证记录：服务器 `iot-business-artifacts-20260923` 的 `frozen-v5` 为 788 passed / 18 ignored、no-demo 44 passed；`alarm-v6` 精确 14 项×20 轮、默认包 2 个及 JetStream 包 4 个真实进程场景通过，覆盖 activate/resolve 的 SIGKILL 重放、episode/输出 ID、停机暂停与旧版本拒绝。包 v6 仅补 runner/精确清单，与 v5 的 server/CLI 二进制逐字节相同；旧矩阵只完成部分。**随后 Review 修正了 AlarmIot 析构顺序：timer 索引必须先于为其计费的 entry leases 释放。此修复尚待最终匹配复验，不能把 v5/v6 结果冒充最终源码的通过证据。** v6 的剩余回归已停止并留 `.interrupted`，性能为 NOT RUN；源码另存 `iot-business-source-v5-archived-20260923`，不删除此前通过或失败证据。

**后续 v7 / final1：功能与完整旧矩阵已通过，性能未通过，因此整批未放行。** 析构修正已纳入 `frozen-v7`（788/18 ignored、no-demo 44）、14×20 Alarm 专项及 6 个真实进程场景，`validate-final1.complete` / exit0 证明完整旧矩阵完成；新 Alarm 图 v22 只有进程内恢复测试，独立二进制 SIGKILL 专项仍未做。普通 Clippy 退出0、有105条 warning。完整三组原门槛中，第三组 fresh 双 Count 比值 **0.908434**、合并 **0.964801**，未过 ≥0.97；其他场景通过，全部正确性/输出 hash 一致，失败样本保留，不能只挑前两组宣布通过。

定位记录：同一 v7 的另三组预先固定短程 AA/AB 均通过，但 8×输入的固定长程对照仍有一组候选比值 0.922794 未过；不能直接断言只是短测噪声。只调整新 metrics 字段位置的 `layout1` 尝试仍有 0.954049 的失败，已从工作区撤回，不作为修复保留。该实验包/源码、三组原样本、长短对照和 CPU profile 均在上述产物根保留。软件 CPU profile 的共同热点仍在分配/释放、预算操作、JSON 与窗口路径；尚未证明具体回退根因，不将单次 profile 吞吐/RSS 当门禁。当前编译源码恢复为匹配 v7，下一步先定位性能，不叠加 Offline/Resample，也不发布生产认证结论。

### 下一步静默检测的前置边界

2026-09-24 收尾候选 `alarm-closure-artifacts-20260924` / v1 已通过 795/18 ignored、独立 no-demo 44、Alarm 14×20、线性 6 个进程场景及完整旧矩阵。新增 v22 **双 Alarm→Union**、**单 Alarm→双 required Sink** 两种图，在 default/JetStream 两个包分别执行真实 SIGKILL；核验两端独立 ID/完整内容重放、activate/resolve 同 episode、停机暂停、提交后不重复及旧 v19 二进制拒绝。两个包测试的来源都是 File，不代表 JetStream 来源图已开放。验收脚本另通过 1 个合法 stub 与 8 个拒绝反例，stub 不作为真实进程证据。完整原三组 ABBA 的双 Count 已通过，但零状态第 2/3 组及合并未过，合并 0.885299；失败保留，**整批仍未放行**，不能用短程双 Count 探测代替。见 [本次证据](PRODUCTION.md#alarm-closure-validation)。

原 `paused_time` actor 的“没有读到行”同时可能来自空输入、pull 等待或 tick 唤醒，不等于健康观测；JetStream 的连接检查在 reader 路径，不能从某个旧的 Ready 指标反推整段时间都可观察。本批新增独立 `observed_time` actor 和**随时间决策持久化的来源观测合同**：来源失败/不可判断时不作新的设备静默判断，停机不计时，未提交后继使用原健康事实重放而不是重新采样指标。Pipeline stopped 与 Source unavailable 保留独立诊断；静默事件不是已证明的硬件故障。未见且未登记的设备不建立离线状态。不用现有 Graph idle/EOF 或 Alarm 的无样本保持条件替代；实现范围与验证状态见下文。

### IOT-07 实施边界（限定开发 Preview，功能验证通过）

**File/JetStream 的限定静默链路已通过功能、故障和旧矩阵验证，不代表 IOT-07 全范围或生产认证。** Connector 前置已提交 `cb097a4`；独立 OFC1/OFD1、File23/JetStream24、静默/恢复事件和模板已实现。静态 inventory 保持 `development_preview`，具体配置仍须通过 bind/admission，不能据此推导 MQTT、任意 DAG 或生产容量。本批 s8 的原性能门禁仍有一个单组失败，没有整批放行，见 [匹配证据](PRODUCTION.md#silence-validation)。

#### 来源事实与新鲜度

- 将**瞬时流前缀观测**与现有 `HealthState::Ready` 运维状态分开。记录来源位置、已观测尾部、观测种类；`CaughtUp` 只表示一次新鲜检查时无已知未消费输入，不证明硬件健康或整个历史区间没有短暂故障。冷 I/O 的开始/完成时间由 actor 的单调时钟测量，过慢观测不授予新鲜度。
- File 必须新鲜校验路径与已打开句柄、常规文件、截短和既有 append-only 身份约束；未读字节、BufReader 预读、未完成行、scan-budget `Pending` 均不能伪装排空。仅持续 append-only 来源可形成排空观测；sealed/immutable 结束不代表后续仍可收心跳。有限采样 fingerprint 不是全量内容认证。
- JetStream 保留所有 ownership、policy、range 检查；新鲜查询 stream 和本次 consumer，并与创建身份/配置比较。必须同时核对 stream 尾部、broker pending/delivered、已收到/已发布位置和未完成 pull，不能只看 `num_pending==0` 或 SDK connection state。控制面查询失败保持失败，不为了记录“不可用”而放宽 ACK 或所有权。
- 点观测没有自动计时权。后续 actor 必须在有界最大观测间隔内建立覆盖；来源积压、未完成记录、不可验证、断连、过长提交/背压及重启都会使覆盖中断或失效。重新建立覆盖后给予**完整静默宽限**，不把未知区间两侧的时长直接相加。定时器只有当前决策带有效排空事实才可产生新的 `silent`。

#### 设备状态与输出

- 设备集合由已接收的合法 key 与有界静态登记集合组成；登记沿用严格类型的 canonical key 编码，重复登记拒绝，登记数量/字节计入计划和 Job 上限。动态注册管理不隐式纳入首批。
- 首次输入建立最近接收时间，不凭空输出恢复。静默期达到阈值才输出一次 `silent` 并递增 episode；之后真正接收到该 key 的记录才输出一次 `resumed`，沿用该 episode。来源重新连接/排空自身不产生设备恢复；已 Silent 的设备在覆盖中断后仍保留原 episode，不批量重复告警。
- 登记但从未见过的 key 需等首次有效覆盖及完整宽限，才能发 `silent`；事件保留 `last_seen=NULL`，不得伪造原 telemetry 的 non-null 温度等字段。因此采用 **key + 独立静默事件字段**，不沿用 Alarm 的完整原行输出。时间字段明确是暂停的逻辑微秒，不是设备事件时间或 UTC。
- 完整身份包含 generation、operator、全部 key、episode，required HTTP 仍另有输出 ID。静默表示**所观测流缺少记录**；积压后收到旧遥测也只能证明收到一条流记录，不能据此声称设备此刻物理在线。
- 新 profile 明确采用**本决策输入先更新最近接收，再按本决策健康事实判断静默**；等时心跳不先制造一次 silent/resumed。这是新静默合同，不改变现有 Alarm/HoldFor/Window 的到期先于输入顺序。任何可能隐藏心跳的上游 Filter、采样或状态节点均不在首批准入范围。

#### 持久化、范围与验收

- 独立 cut、decision log 和 outer profile，不能给旧 PTC1/TPD1 或 v20～22 加一个缺省健康字段就改变旧语义。决策包含逻辑时间、输入摘要、完整来源观测及覆盖边界；先持久化，再发布输入/健康控制、required 输出、CURRENT，最后才允许来源 ACK。
- 未提交决策必须按原事实/时间/身份重放；不补采一次新的 Ready。恢复完未提交后继后，首个新决策显式打断观测覆盖，停机不计时，也不把重启当恢复通信。CURRENT 与 journal 缺失、版本/身份/序列矛盾必须拒绝。
- 首批可靠范围是 **线性 File append-only / JetStream → 首个静默状态 → required HTTP**，默认包和 feature 包分别验收；与 ET、参考表、其他时间状态及 DAG 的组合继续拒绝。这只是 IOT-07 的一个受限交付，不自动代表所有 Source 支持。
- MQTT live 使用下述独立接入，不继承本节的可持久恢复来源、排空前缀或输出重放保证。不能从 cached Ready 推导静默，也不能由 File/JetStream 的验证结果推导其通过。
- key、索引、timer、登记集合、观测/journal 和输出均受预算；覆盖重建不得无界遍历并重排全部设备。保持旧热路径布局与额度，不因新节点扩大所有旧任务的 future/control。
- 必测：从未登记、登记未出现、等时心跳、partial/Pending/backlog、慢观测/慢 Sink、路径替换/截短、broker/ownership 故障、断连不全体静默、重新覆盖宽限、静默与恢复的真实 SIGKILL/完整内容和 ID 重放、提交后不重复、旧版本拒绝、取消退款及原性能门禁。尚未执行的项不以 Connector 单测代替。

#### 配置与运维

模板 `deploy/pipeline-iot-silence.json` 共用 `deploy/stream-k4-telemetry.json`，将 `device_id` 用作 key，静默阈值 5 秒、最大观测间隔 1 秒、checkpoint tick 100 ms，额外登记 `device-registered`。`timing.kind:"silence"`、`clock:"paused"`；阈值至少是观测间隔的两倍，观测间隔至少容纳两个 tick。要求 `fields:[]`、`emit_first:false`、`ttl_micros:0`、`invalid:"error"`。

输出仅含 key 与 `sparrow_silence_event/generation/operator/episode/time/last_seen/never_seen` 七列，HTTP 仍为带稳定 `id` 的 `data` 包装。从未出现的登记 key 的 `last_seen` 为 NULL；首次实际记录无恢复事件，只有 Silent 状态收到自己的记录才 `resumed`。状态不 TTL 遗忘，达到 `max_keys`/字节/timer 上限明确失败。

`checkpoint.observed_source` 是最后成功提交（或恢复验证）的历史 cut；`view_age` 不是来源采样年龄，更不是连接当前健康。实时 Source 错误仍在原诊断中。File23/JetStream24 使用新的独立目录，不自动迁移旧时间 profile；恢复只读 CURRENT 及其一个未提交后继，旧版本拒绝该历史。仍是逐行/逐观测串行提交，受 fsync 和 required HTTP 等待限制。

<a id="mqtt-live-silence"></a>
### MQTT live 静默（独立开发 Preview）

模板 `deploy/pipeline-iot-mqtt-silence.json`；**MQTT QoS0 + clean_session → Silence → 可选纯 Transform → verified HTTP**。`timing.clock:"live"`、`recovery:"restart_fresh"`；拒绝 checkpoint、restore、上游 Transform、其他状态、DAG、参考表与 ET。普通不带 Silence 的 MQTT 接收路径保持独立。

- `max_observation_gap_micros` 为 100 ms～120 s；静默阈值至少为 gap 的两倍。探测周期为 `min(gap/4, keepalive/2)`，一次最多一个 PINGREQ；PUBLISH 不推迟探测，响应期限为 gap/4，迟到等号失效，超时重连。无请求的 PINGRESP 不授权覆盖。
- PINGRESP **不是 broker 排空证明，也不是 QoS0 送达证明**。它只验证响应连接；接收端另检查已知本地队列和 framed-reader 预读字节。MQTT 规范只把响应定义为 Server 存活指示，见 [MQTT 3.1.1 §3.13](https://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html)。事件含义始终是“此接收链路未观察到该 key 的新记录”，不是物理设备离线。
- 数据、探测与不可用事实共用一个有界 FIFO；每个事件带单调接收时间和 sticky discontinuity epoch。断连、队列积压/等待/丢弃、字节不足、坏 JSON 和 `RETAIN=1` 都中断覆盖；即使不可用控制本身塞不进队列，下个事件仍携带新的 epoch，不会误接旧宽限。记录只保留一个队列外工作事件，不预取整批健康事实。
- 消费端在所有上游排队之后再次校验：响应耗时 < gap/4，消费时间距请求开始 < gap/2。过期控制不给 timer 授权；长间隔和慢下游中断覆盖，必须重新积累完整宽限。timer 排空期间也重新检查期限。已发布到下游的事件仍可能因 HTTP 排队而迟到；事件时间是观测 cut，不是接收 HTTP 时的实时在线证明。
- 只有收到该 key 的合法、非 retained 记录才能 `resumed`；重连自身不会恢复设备。保留消息不登记 key、不更新 last_seen、不恢复 episode。收到过的 key 和静态登记 key 受原状态/字节/timer 预算约束，不 TTL 遗忘。
- 连接重建保留当前尝试的设备/episode，但中断覆盖；管线重启/进程强杀则清空已观察 key，重新加载登记集合、生成新 generation、重新等待完整宽限。输出仍为 key + 七个静默事件字段；live HTTP **没有可持久重放的输出 ID**。时间是当前尝试的单调微秒，不是暂停恢复时间、UTC 或设备事件时间。
- `fail_on_decode:false` 的丢弃有诊断且打断覆盖；`true` 直接失败/取消尝试。`mqtt_feed_probes/feed_breaks/ping_timeouts/retained_ignored` 与原丢弃/队列指标一起观察；probe 计数只是 connector 提供的候选事实，不保证消费端仍把它认定为新鲜，旧 `Ready` 仍不是计时授权。

验证入口：`scripts/production-mqtt-live-validate.sh`，精确测试清单及隔离 Mosquitto、真实 server SIGKILL、75 s 来包场景。当前结果与未测范围见 [匹配验证](PRODUCTION.md#mqtt-live-silence-validation)；不把短试次当成 24/72 h soak、TLS/WAN 或生产认证。

<a id="resample-preview"></a>
### IOT-09 Sampling/Resample（限定开发 Preview，功能/故障/回归通过）

三种模式已经接入 Plan、Runtime、检查点、控制面和 HTTP 输出；独立 v25/v26 的最终 s3 已通过本批功能、真实故障、旧矩阵和原三组性能回归门禁，仍不代表生产认证。下面是实际实现合同，证据见 [本批记录](PRODUCTION.md#resample-validation)。

- 首批是独立 File25 / JetStream26：线性 Source → 可选纯 Transform → 单个 Resample → 可选纯 Transform → required HTTP。复用暂停的有序 processing-time/TPD1 决策语义，完整计划与独立目录校验；不与其他状态、ET、参考表、DAG 或 MQTT live 混合。不是设备 event-time 对齐。
- 以逻辑零点为固定网格，`period_micros>0`。Last/Mean 使用半开区间 `[start,end)`，等边界先关闭旧区间再处理输入；Last 是**本区间**最后有效样本，保留整数类型/精度，不隐式无限前向填充；Mean 输出 Float64。已观察 key 的空区间明确输出 NULL/missing，未见 key 不生成数据。
- Interpolate 在网格点取值：精确命中的样本可直接使用，否则只用最近左点与首个后到右点做线性插值，不外推。`0<max_wait_micros<=period_micros` 限制每 key 最多一个等待网格；`max_gap_micros>0` 限制左右点跨度。等待期限等号处先过期，再处理同刻输入，已发出的结果不回写。重复同刻样本仅对尚未产出的网格按输入顺序取最后值。
- keys 与 value fields 分离；首批选择 1～16 个 numeric 字段，按完整向量处理：其中任一字段 NULL/非有限/类型不符时按 `invalid` 整行拒绝或忽略，不把不同时间的字段拼成一个点。Last 保持各数值字段原类型，Mean/Interpolate 明确转为 Float64；输入丢弃与输出缺值分别计数，聚合缩减不冒充丢失。
- 输出是 keys + nullable 采样值 + `sparrow_resample_mode/time/emitted_at/missing/samples/generation/operator`；`time` 是区间右边界或插值网格，`emitted_at` 是真正产出该结果的持久决策时间，不能混为一谈。Last/Mean 的 `samples` 为区间有效行数，插值为 0（缺值）、1（精确点）或 2（左右点）。稳定业务身份包含 generation/operator/全部 key/网格时间，HTTP 输出仍另有稳定 ID。
- keys、点/累加器、timer、字节和每决策展开量有界。`max_emissions_per_decision` 默认 256、最多 4096；须覆盖 `max_keys`，插值另预留一个输入决策产出的名额。长运行间隔造成过多补网格时明确拒绝，不静默截断，也不先物化无限空窗口；时间/计数/非有限运算溢出不得回绕。状态不 TTL 遗忘。
- 冻结包含剩余区间、等待点、使用标记与规范化状态；未提交决策重放原时间、输入和输出身份，停机不推进网格。恢复拒绝过期 pending、未来采样点、类型/模式/网格不一致及配额越界。验证覆盖三模式的独立黄金值、边界/缺样/插值超期与跨度、批量补格拒绝、OOM/取消退款、真实 SIGKILL/旧版本拒绝及原路径回归。

#### 配置与操作

模板 `deploy/pipeline-iot-resample.json` 复用 `deploy/stream-k4-telemetry.json`：每 1 秒取该区间最后一个有效温度，tick 为 100 ms。要求 `recovery:"aligned"`、File `append_only` 或启用 feature 的 JetStream、verified required HTTP、独立目录、`fail_on_decode:true` 和 `resume_latest:true`。周期必须不小于 checkpoint tick；这不保证慢 HTTP/磁盘下不会超出补格上限，超限会失败并保留恢复切点，不悄悄丢格。

- `timing.kind:"resample"`，`mode:"last"|"mean"|"interpolate"`，`clock:"paused"`。Last/Mean 必须省略或置零 `max_wait_micros/max_gap_micros`。Interpolate 例如周期 1 秒、等待 500 ms、左右跨度最多 2 秒；这三个配置都必须显式给出合法值。
- `keys` 与 `fields` 分离，`emit_first:false`、`ttl_micros:0`，不可混入 deadband/hysteresis。NULL 或非有限值按 `invalid:"error"|"ignore"` 处理整行，而非逐列拼接。来源格式解码失败仍由 `fail_on_decode` 独立控制。
- key 在收到第一个完整有效采样向量时才登记；`invalid:"ignore"` 不创建 key、不刷新已有点，也不消耗插值等待。所谓首个右点是首个后到的完整有效点。
- 已知 key 永久保留并在空网格输出 missing；这不是“设备离线”事件。需要停止空采样时应停止/重置规则，不应依赖隐式 TTL。
- 指标 `resample_discarded_inputs`、`resample_missing_outputs`、`resample_interpolated_outputs` 分别统计被忽略/替换的输入、空格输出、真正双点插值。Mean 多对一不算丢弃，exact 插值不算双点插值；指标是 attempt 内累计，不伪装为持久交付计数。
- `iot_input_rows` 与 `iot_emitted_rows` 是算子输入/输出的不同计量：聚合会缩减，空格补发又可使输出多于输入，不能用两者相减推断来源丢包，也不能套用一进一出的守恒公式。
- 运行验证入口为 `scripts/production-resample-validate.sh`，只使用冻结二进制，严格比较 24 项清单、逐项重复和两包真实 SIGKILL；不编译，不宣称长稳或性能认证。当前结果以 [验证记录](PRODUCTION.md#resample-validation) 为准。

<a id="linear-time-completion"></a>
## 线性时间组合 v16/v17（2026-09-22，限定 Preview 已验收）

在下方已验收的 v14/v15 单个 HoldFor/Debounce 之外，新增独立 **File v16 / JetStream v17**：

- PT tumbling window；ChangeDetect/Deadband 的正 TTL；最多两个状态的线性组合，允许 Count/PT、TTL0/正 TTL 值过滤、TTL0 Hysteresis、HoldFor/Debounce，在 schema 合法时串联。
- 至少包含一个 PT、正 TTL 或 HoldFor/Debounce 状态。只有旧 Count/TTL0 的计划仍选择旧 profile；只有一个 HoldFor/Debounce 仍选择 v14/v15。**不迁移、不混写旧目录**。
- 沿用独立 checkpoint 目录、File append-only、required HTTP、fail_on_decode、resume_latest、100～1000 ms tick、每决策提交和 CURRENT/TIME_PENDING 合同。File/JetStream 都输出稳定 ID 包装；它们仍可能在未提交时重发，不是 exactly-once。
- 不包含 ET、DAG、reference Lookup、side output、Dedup 或超过两个状态。Hysteresis 仍要求 TTL=0，避免静默遗忘 Active；HoldFor/Debounce 自己的 `ttl_micros` 也仍为 0。正 TTL 不自动代表完整告警生命周期。

### 时间边界与顺序

每个状态实例持有自己的同序逻辑时间，**仅消费源端已持久化的 ProcessingTime 控制，不采样宿主时钟**。算子先把新时间控制向下游传递，再排出本次到期产生的行，最后处理随后的输入及 barrier。这样下游先处理本次时间点已有的到期状态，再接收上游定时器的衍生行；输出行归当前决策时间，不携带未采样的中间墙钟时间。

- PT 窗口按逻辑零点对齐，使用 `[start,end)`；边界时先关旧窗，再将新行放入新窗。只为有数据的窗口输出聚合，不补空窗口；长 tick/背压跨越多个边界时，不伪造中间观测。
- `PT(1s) → PT(1s)`：第一级在 t=1s 输出的行进入第二级 `[1s,2s)`，不是已经到期的 `[0,1s)`。
- 同一算子到期项按 `(deadline, encoded key)` 稳定排出；各算子遵循上述 FIFO 边界，**不宣称跨算子全局 deadline 排序**。
- TTL=最后有效输入时间+TTL；有效但被抑制的输入也刷新 TTL，ignored NULL/无效输入不刷新。等于 expiry 时先淘汰，再按首次值策略处理输入。过期只删除基线，不生成 resolve/offline 事件。
- 停机、启动及未提交后继重放不推进时间。重放使用原有时间和完整状态/输出 cut，不能根据当前墙钟重新推算 TTL/窗口。

### 编码、预算与操作

PT 沿用 Window slot1/codec1、manifest kind0；正 TTL 使用 IoT slot3/codec2 的新 kind9/10，值前缀保存 `last_seen:Int64`。新 outer profile 与完整计划语义、Source cut、generation、next-output 联合校验。恢复前重建有界 timer/index，拒绝未来 last_seen、过期状态、窗口区间/聚合类型错误和预算越界；全部参与者准备成功后才发布输入。

状态、索引、工作区归同一 Job owner；timer/keys 仍受每实例上限及共享字节预算限制，扩大组合不扩大默认预算。到期输出按有界批次/单项让出执行预算。每决策 fsync/full checkpoint 的成本仍在 [OPT-012](OPTIMIZATION_BACKLOG.md#opt-012)，不是高吞吐优化。

模板：`deploy/pipeline-pt-recovery.json`（SQL）、`deploy/pipeline-iot-ttl.json`、`deploy/pipeline-time-combined.json`，共用 `deploy/stream-iot-timed.json`。JetStream 替换 Source、配置 `checkpointed_at_least_once` 及独立新目录；必须启用对应 build feature。

本批 759 常规、44 no-demo、25×20 与 38×20 专项（部分重叠）、24 种新增真实 SIGKILL、旧矩阵和三组 ABBA 已通过。对应源码、失败样本与性能证据见 [本批验收](PRODUCTION.md#linear-time-validation)，不继承下面的 747/44 或历史 ABBA 结论。未跑真实 WAN/TLS、目标设备、24/72 h 长稳和断电；新串行 profile 的大状态/多规则容量仍需部署前核定。

静态 capability 的 `iot.aligned` / `iot.jetstream` 节点保留旧 TTL0 profile 的范围；新 PT/TTL/双状态能力在 `paused_time_iot.linear_time_extension`。具体配置仍以 bind/validate、effective guarantees 和实际 snapshot profile 为准，不能把旧条目的 TTL0 约束套到 v16/v17，也不能绕过新 profile 的强制条件。

> 2026-09-19：可恢复时间 + HoldFor / Debounce 的首批线性 profile 已完成本批功能、故障、旧矩阵与性能验证。不是整个 K1～K4 完成或生产认证；见 [时间型 IoT](#paused-time-preview) 和 [匹配证据](PRODUCTION.md#paused-time-validation)。

<a id="paused-time-preview"></a>
## 可恢复时间、HoldFor / Debounce（限定 Preview）

以下只描述单个线性时间型算子的合同，未列出的组合不自动获得恢复资格。

### 首批边界

- 单个 HoldFor 或 Debounce，线性 File / JetStream → 可选纯 Filter/Project/Map → required HTTP；不混入其他状态、参考表、side output 或 DAG。
- File 必须 `append_only`；EOF 只表示等待追加，不能终止 timer。要求 `recovery:"aligned"`、独立 `checkpoint_dir`、`fail_on_decode:true`、`resume_latest:true` 和 `checkpoint.interval_ms:100..1000`。
- `interval_ms` 在新 profile 中表示空闲时间决策间隔；**每个输入行仍强制 checkpoint**。一个在途决策，一个输入行或空闲 tick，提交之前不开放下一个决策。不是通用高吞吐 WAL，也不是旧链路性能优化；会受 fsync、状态大小、HTTP RTT/重试限制。
- 不推进停机时间；启动和未提交后缀恢复期间不采样新的逻辑时间。正常运行期间的下游等待属于运行时间，完成提交后的下一次决策会反映该时长。tick 可能被背压推迟，不承诺硬实时。
- 逻辑时间以持久决策为准，不使用恢复时的墙钟差补算。故障前尚未记录的 elapsed 也不会被推算回去；这是可重放的逻辑时间合同，不是精确累计物理在线时长。
- 同一决策严格先触发所有到期 timer，再处理该输入，最后 barrier/flush。相同 deadline 按稳定编码 key 排序。输出沿用原行，File 和 JetStream 均使用 `[{"id":"…","data":{…}}]`，下游需要显式适配；不宣称 exactly-once。

### 配置与语义

共同 `iot` 参数仍需 keys/fields/max_keys/invalid；要求 `emit_first:false`、`ttl_micros:0`，不能混用 deadband/hysteresis。完整行最多 60 个扁平 scalar 字段；key 非空且非 nullable，状态和 timer 有明确配额，超限失败而非静默淘汰。

HoldFor 节点 `kind:"hold_for"`，观察一个 Bool 条件字段：

```json
"timing": {"kind":"hold_for","clock":"paused","duration_micros":1000000}
```

- true 首次出现开始计时；持续 true 更新保留行但不延长 deadline，到期只输出一次。
- false 撤销 Pending 或解除已触发的 latch；之后 true 才开始下一轮。无样本维持上次有效条件，不代表设备在线。
- `invalid:"ignore"` 不刷新或撤销条件；等于 deadline 的 false 输入会在到期输出之后处理。尚不是带 episode/activate/resolve 的完整告警生命周期。

Debounce 节点 `kind:"debounce"`：

```json
"timing": {"kind":"debounce","clock":"paused","quiet_micros":200000,
  "max_wait_micros":1000000,"leading":false,"trailing":true,"reset_on_repeat":true}
```

- quiet/max_wait 必须为正且 max_wait ≥ quiet；leading/trailing 至少开启一项，所有参数显式提供。
- leading 输出一轮 burst 的首行；trailing 输出最后有效行。两者同时开启且只有一次输入时，不额外重复 trailing。
- `reset_on_repeat:false` 时，observed fields 未变化的重复输入不延长 quiet，但仍更新保留的完整行，并可使 leading+trailing 的 trailing 生效。
- quiet 到期或首次输入起算的 max_wait 到期都结束当前 burst；持续来包不能无限推迟输出。

模板：`deploy/stream-iot-timed.json`、`deploy/pipeline-iot-hold-for.json`、`deploy/pipeline-iot-debounce.json`；生产候选包同时携带。修改路径、Schema/stream、allowlist 和 HTTP 地址后使用，不能混用旧 checkpoint 目录。

### 持久化与恢复

- 独立 snapshot v14（File）/v15（JetStream），kind7/8、slot3、codec2；保持旧 v3～v13 路径隔离，无自动迁移。Source cut 包含原连接器位置、逻辑微秒和决策序号；完整状态、timer、generation 与 next-output 一起提交。
- `TIME_PENDING` 记录一个先持久后发布的决策，带版本、SHA-256、generation/语义摘要、目标 Source cut 和可选输入行摘要。原子 temp 写入/fsync/rename/目录 fsync；仅在前一决策已提交后覆盖。
- 未提交决策恢复时，先验证原 Source 坐标和输入摘要，再按原时间发布；已被 HTTP 接收但未提交的 timer/输入输出可能重发，但 ID 与内容必须一致。
- 首次输入前提交 seq0/time0 的 bootstrap。日志丢失、校验失败、序号跳跃、generation/语义不符，或存在无法解释的无 CURRENT 历史，均拒绝恢复，不补采一个“现在”。
- 只支持 CURRENT 及其一个未提交后继，**不支持选择历史 snapshot**。备份必须在停止并 join 后完整保存 checkpoint 目录（含 `TIME_PENDING` / `STATE_GENERATION` / JetStream owner），不能只复制某个 `chk-*`。
- `TIME_PENDING` 与其临时文件各自最多 128 KiB，checkpoint 提交的目录配额统计会计入它们。日志先持久化、随后才检查 checkpoint 提交配额，因此磁盘还需预留最多 256 KiB 的瞬时日志空间；不能把 payload 大小当作整个目录的上限。输入保留、状态、编码和 SDK 仍分别受 Job 预算约束。

### 验收状态

服务器 `box@100.64.0.18`：`frozen-v6` 功能 747 passed / 18 ignored、独立 no-demo 44 passed；22 项专项（含显式启用的真实 NATS 用例）×20 轮全部通过。覆盖停机暂停、空闲触发、重复策略、max_wait、NULL、边界/预算、旧 profile 降级拒绝、慢 HTTP 未提交重放、CURRENT 故障、缺失日志。普通 Clippy 通过但有 102 条 warning，不是 `-D warnings`。

`package-v7-default/jetstream` 已构建；独立 Go driver-v4 的 File/JetStream × HoldFor/Debounce trailing/Debounce leading 共 6 种实际 SIGKILL 场景通过，并验证旧二进制对 v14/v15 保持完整历史/输出不变地拒绝。默认 feature-off 包另有 driver-v5 的 3 种 File 进程验证。旧矩阵与三组 ABBA 通过原门槛；新串行模式的 100 行成本观察约为 139 行/s（本机无人工 HTTP 延迟）或 34.1 行/s（20 ms 模拟响应延迟），不能按旧高吞吐链路选型。证据、失败样本及 hash 见 [验收记录](PRODUCTION.md#paused-time-validation)。PT Window/正 TTL、多状态、时间型 DAG、冷却/离线/Resample/完整告警仍未完成。

状态：2026-09-17，K4 首批功能、全局 Review 修复及下方限定矩阵复验已完成，随后随 `1dd17c8` 提交，未 push/tag。仍是 **Preview**，不是任意组合或全场景生产认证。

K2/K4 的 JetStream + IoT TTL0 独立 v7 组合已有匹配验证，见 [JETSTREAM.md](JETSTREAM.md)；下方 v6 矩阵和 K4 验收仍是原首批受测范围。2026-09-17 新迟滞和静态表组合已通过本轮匹配功能/进程回归，证据见 [K1～K4 组合验收](PRODUCTION.md#k1-k4-reference-validation)；性能单列，不将首批或这一增量写成整个 K1～K4 完成。

## K4 后续迟滞实现（Preview）

`kind:"hysteresis"` 复用 IoT 的 key、单个数值 field、`emit_first` 和 `invalid` 参数；新增 `iot.hysteresis`：

```json
{"direction":"high","enter":60.0,"exit":55.0}
```

- `high` 必须 enter > exit；Normal 遇到 value ≥ enter 转 Active，Active 遇到 value ≤ exit 转 Normal。`low` 要求 enter < exit，并反转比较方向。阈值有限且严格分离，等号规则固定。
- key 首次有效观察以 Normal 为初始状态再判断 enter；`emit_first` 仅决定是否输出这次初始化观察。后续只在状态转换时输出，带内抖动不输出。每个 key 独立保存 Bool latch。
- 输出保留原行/schema，不伪装成完整告警事件；尚无本节点专用 episode 或 activate/resolve schema。无效值按 `error/ignore`，忽略不得清除 Active、初始化 Unknown 或产生恢复通知。
- Int64/UInt64 输入不先转 f64，因此大于 2^53 的整数、负数和分数阈值不会因输入舍入改变进入/退出判断。
- 本节点暂要求 TTL=0，不能静默淘汰 Active。持久 latch 使用新 kind=6/slot=3，普通 File（线性/required DAG）使用新 profile12，JetStream 使用13；带静态表则使用9/10/11。旧v6/v7目录不混写，不以旧reader的损坏回退代替明确拒绝。
- 模板 `deploy/pipeline-k4-hysteresis.json`，输入/独立预期为 `k4-hysteresis.ndjson`、`k4-hysteresis.expected.json`；57,60,59,56,55,56,60,60,54 应输出57,60,55,60,54。

用户已确认时间型 IoT 在停机期间暂停计时，恢复后继续剩余时长。HoldFor/Debounce 已按上方独立 profile 验证；冷却、离线检测和完整告警生命周期仍未实现，不因迟滞或时间型首批而宣称完成。

### 时间状态的实现约束（首批已落实，其他组合继续受限）

原 v3～v13 的 PT Window 读取运行时钟、IoT TTL 使用 stage-local elapsed、UnionAll 按 ready 顺序合流，没有持久时间决策序号。因此仅添加 `remaining_ttl` 不能保证未提交后缀一致。新 v14/v15 已为单个线性时间型算子增加持久决策；并未把旧 PT/TTL/Union 自动迁入该协议。

- 首先建立 Job 级逻辑时间与有界、先持久后发布的输入/tick 顺序，恢复时重放未提交时间决策；同时间采用明确的 timer/input 顺序，不让每个算子各自采样当前时间。
- Source cut、逻辑时间/决策游标、状态、timer 和适用的输出 cursor 必须在同一个新 profile 中提交；CURRENT 失败或 HTTP 结果未知不得推进可靠 Source ACK。停机不推进逻辑时间；恢复完成之前不开放新输入。
- 旧 v3～v13 不改语义、不隐式升级；journal 的初始 generation、存储/工作量配额、断尾处理、保留/GC、丢失依赖拒绝和取消需要一起实现，而非只增加 freeze 字段。
- DAG 的 timer-after-Union 还需可重放的合流选择/进展协议。当前 per-source 有序不等于全局确定性；未实现之前必须继续拒绝该 aligned 组合。
- HoldFor 与 Debounce 只共享有界 keyed timer，不共用开始/重置语义；Cooldown 作用于通知，不阻止底层告警状态更新；Offline 必须区分设备静默、Source 断连和 pipeline 停止；Resample 的缺样、插值和丢弃有独立合同。
- 现有 Change/Deadband/Hysteresis 保持原 row schema。高级告警事件、episode 序号/生成代次、activate/resolve、重放身份使用独立 schema/codec；不能通过修改旧迟滞的输出偷偷引入。

每个新组合仍须验证无新输入时 timer 触发、跨 deadline 的 SIGKILL/停机暂停、相同未提交后缀输出与身份、CURRENT/journal I/O 故障、慢 Sink、预算/取消退款，以及原 TTL0 路径不退化。首批证据只授权上面的 v14/v15 矩阵，不授权其余组合。

## 首批 Change/Deadband 范围（历史 v6 合同）

本批实现 IOT-01 变化检测、IOT-02 Deadband，以及相应状态、资源、恢复、配置、诊断和模板。复用现有 GraphSpec → BoundPlan → PhysicalPlan → Kernel → Connector 路径，不另造执行引擎。

- 新 Graph 节点：`change_detect`、`deadband`，一入一出，输出保留完整原行、schema 和非聚合来源 metadata。
- 可用于线性计划或已有 DAG；不合并不同 key 的状态，不承诺跨输入的全局排序。
- 本批不增加同名 SQL 函数／SQL 语法；配置入口是现有 PipelineSpec 的 Graph。已有 SQL 阈值、窗口统计和 Lookup 继续复用。
- 不做 Web 前端；不包含 Hysteresis、HoldFor、Debounce、离线检测或完整告警生命周期。

## 公共配置

每个节点的 `iot` 配置显式声明以下字段：

```json
{
  "keys": ["device_id"],
  "fields": ["temperature"],
  "emit_first": true,
  "ttl_micros": 0,
  "max_keys": 1024,
  "invalid": "error"
}
```

| 字段 | 合同 |
|---|---|
| `keys` | 1～16 个不重复、非空、已声明且 non-null 的字段；Bool/Int64/UInt64/Utf8/Bytes/Timestamp。拒绝 Float key、Dynamic、嵌套类型 |
| `fields` | 变化检测观察 1～16 个不重复字段；支持 Bool/Int64/UInt64/Float64/Utf8/Bytes/Timestamp。Deadband 必须恰好一个 Int64/UInt64/Float64 字段 |
| `emit_first` | 每 key 首个有效值是否输出；为 false 时仍建立参考值，不是忽略这条状态观察 |
| `ttl_micros` | 0 明确禁用时间淘汰，仍受 key/bytes 上限约束；正值为本次运行的 processing-time 空闲 TTL，不是设备时间或事件时间 |
| `max_keys` | 正数上限，还受 Job 的 max_state_keys、内存和进程总准入限制；到限明确失败，不偷偷淘汰仍有效的 key |
| `invalid` | `error`：无效观察值使 attempt 失败；`ignore`：整行不输出、不更新参考值、不刷新 TTL，并计数。自然 TTL 仍独立到期 |

NULL、缺失后解码为 NULL 的值、NaN/Infinity 属于无效观察值。Source 自身的解码错误仍按既有 decode 策略处理，不能误当已经进入 IoT 算子的 `ignore`。key 的缺失／NULL／错误类型不属于可忽略的传感器值。

## 变化检测

`kind: "change_detect"`，不允许附带 `deadband` 配置。任意一个观察字段与该 key 上个有效输入不同，就输出当前完整行；相同则抑制。比较保留声明类型，整数不先转换成浮点；有效 Float 的 `+0` / `-0` 相等。只观察指定 `fields`，其他业务列改变不会独自触发输出。

## Deadband

`kind: "deadband"`，在公共配置内增加：

```json
"deadband": {
  "mode": "absolute",
  "baseline": "last_output",
  "threshold": 2.0
}
```

- `absolute`：只有 `abs(current - reference) > threshold` 才输出；**等于阈值不输出**。
- `relative`：只有绝对变化超过 `abs(reference) × threshold` 才输出；0.05 表示 5%，不是 5。零参考值下，任何非零变化满足相对变化条件。
- `last_input`：每个有效输入都更新参考值，含被抑制的输入。
- `last_output`：只有被选中输出的输入更新参考值；首个有效值始终负责初始化，即使 `emit_first=false`。
- threshold 必须有限且非负；不接受隐式类型转换、无限阈值或负值。

例：输入 `10, 11, 12, 13`，绝对阈值 2、输出首值。`last_input` 只输出 10；`last_output` 输出 10、13。后一种不会把逐步漂移永久吞掉。

## 状态、TTL 与取消

状态仅保存 detached 的 key 和观察值，不长期引用完整输入 batch。候选替换、变量长度值、索引与输出同时存活时先做预算准入；失败不能留下半更新的 key 状态。max_keys 不代表任意长字符串都能接纳。

HashMap bucket 容量按保守高水位单独计费：部分 key 到期后容量信用仍保留，空表或 cleanup 实际释放容器后才退还。正 TTL 的每 key 到期项同时受 `max_timers` 限制；底层复用一个唤醒 timer，不代表可以绕过逻辑待到期项上限。

正 TTL 在有效输入时刷新。到期后相同值再次到来按新 key 的首值策略处理；TTL 清理有定时触发，不依赖下一条业务消息。实时运行以单调计时，测试使用虚拟时钟；它不利用上游 watermark，也不能用墙钟回拨延长本应结束的等待。停止、失败和取消都释放状态、索引、timer、输出与持有信用。

## 恢复与兼容

K4 使用独立 **v6 snapshot profile**，IoT participant 使用独立 codec/slot，不伪装窗口状态。v6 与 File/v3、JetStream/v4、DAG/v5 必须不同目录，旧 binary 不自动迁移新状态。

声明的初始 aligned 矩阵：

- File 输入，IoT `ttl_micros=0`；线性计划最多两个 Count/IoT 状态参与者。
- 既有 required File→HTTP DAG 可组合 Count/IoT 状态，仍受图级参与者与总预算限制。
- 不开放正 TTL、ET/PT、Lookup、Dedup、Source time、side output、有损图或 JetStream 与 IoT 的 aligned 组合；不自动降级为 fresh。

所有状态和 required 输出在同一 checkpoint 切点冻结／flush。恢复前验证完整计算语义、实例、codec、schema 与源身份；修改字段、阈值、首值策略等不能复用旧状态。IoT profile 不使用旧线性计划的下游语义放宽。

`restart_fresh`／显式 reset 会丢失前值连续性，重新执行首值策略；不能把它当作业务恢复事件。这里是值过滤，不输出稳定告警 episode，也不声称外部 HTTP exactly-once。未提交的输出在故障重放中可能重复。

## 诊断与验收

`iot_input_rows`、`iot_emitted_rows`、`iot_filtered_rows`、`iot_invalid_rows`、`iot_expired_keys` 为算子处理累计计数；`iot_state_keys` / `iot_state_bytes` 为活跃 IoT 实例汇总。一个输入经过多个 IoT 节点会被各节点分别计数，不能当作 Source 行数或外部送达证明。`invalid_rows` 是被过滤行中的无效值子集。

上述字段同时进入 metrics JSON 和结构化运行日志。旧 `JobStats.state_keys/state_bytes` 仍是窗口口径，不能用它们替代 IoT 专项指标；内存总准入仍统一由 Job/Process owner 负责。

匹配源码的测试／故障／性能证据和全局 Review 结论见下方验收记录。

## 可运行模板与操作

以下模板是 K4 的最小可复现场景，不表示端口、目录或 HTTP 目标已经在
机器上自动创建。先准备一个能返回 HTTP 2xx 的本机 fixture，再使用模板；
`18081`、`18082` 只是模板默认端口，不能当作“开箱即用”的服务。

### 1. 准备 schema、数据目录和 HTTP fixture

先注册 [`stream-k4-telemetry.json`](../deploy/stream-k4-telemetry.json)：

```sh
export SPARROW_URL=http://127.0.0.1:43180
# 由 secret manager 或受控环境注入；不要写入命令行参数或 Git。
export SPARROW_TOKEN='从受控环境注入的随机管理 token'

sparrowctl put-stream telemetry deploy/stream-k4-telemetry.json
```

这个 schema 的 `device_id` 是非空 key，`temperature` 是可为 NULL 的
`float64`；因此 change 模板可以演示 `invalid=ignore`，而 deadband 模板的
`invalid=error` 仍只接收有效数值。

服务进程必须设置受控的数据根目录。例如生产服务使用：

```sh
export SPARROW_DATA_ROOTS=/var/lib/sparrow/data:/var/lib/sparrow/checkpoints
```

把 fixture 复制到 `/var/lib/sparrow/data/`，并保证运行用户可读；不要把
`/tmp` 或整个用户目录加入 allowlist。开发环境也应使用明确目录，例如
`$PWD/deploy:/tmp/sparrow`，同时把模板中的绝对路径改为该目录下的副本。

HTTP 输出端口需要由操作者自备 2xx fixture 并加入目标 allowlist。当前
`sparrowctl` 没有 allowlist 子命令，使用现有认证 API 配置（或由部署初始化
脚本写入 catalog）：

```sh
curl --fail --silent --show-error \
  -X PUT "$SPARROW_URL/v1/allowlist" \
  -H "Authorization: Bearer $SPARROW_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"host":"127.0.0.1","port":18081}'
curl --fail --silent --show-error \
  -X PUT "$SPARROW_URL/v1/allowlist" \
  -H "Authorization: Bearer $SPARROW_TOKEN" \
  -H 'Content-Type: application/json' \
  --data '{"host":"127.0.0.1","port":18082}'
```

若修改模板中的 URL，必须把修改后的 host/port 加入 allowlist；不要使用
`skip_verify` 或未授权的旁路代理。管理 token 使用 `SPARROW_TOKEN`；启用
safe-mode 时还应设置持久的 `SPARROW_SECRETS_KEY_FILE`（32 字节原始密钥或
64 位十六进制）及 `SPARROW_REQUIRE_SECRETS_KEY=1`。K4 模板本身不包含
`SecretRef`，外部认证信息不要写进 JSON。

### 2. 变化检测：aligned v6

模板：[`pipeline-k4-change.json`](../deploy/pipeline-k4-change.json)，输入和
golden：[`k4-change.ndjson`](../deploy/k4-change.ndjson)、
[`k4-change.expected.json`](../deploy/k4-change.expected.json)。6 条输入中有 1 条
NULL 行被 `invalid=ignore` 丢弃，不更新 edge-b 的参考值；最终输出 3 行：

```json
[
  {"device_id":"edge-a","temperature":20.0},
  {"device_id":"edge-b","temperature":5.0},
  {"device_id":"edge-a","temperature":21.5}
]
```

实际 CLI 命令如下，`start` 只提交 desired 状态，必须继续检查 status：

```sh
sparrowctl validate deploy/pipeline-k4-change.json
sparrowctl explain deploy/pipeline-k4-change.json
sparrowctl put-pipeline change deploy/pipeline-k4-change.json
sparrowctl start change
sparrowctl status change
sparrowctl diagnose change --output change-diagnostic.json
sparrowctl checkpoint change
sparrowctl checkpoints change
```

模板使用 `ttl_micros=0` 和 `recovery=aligned`，checkpoint profile 是 v6。
File 使用 `append_only`，读完这份 fixture 后等待追加，保留周期／手动
checkpoint 入口；不要为这个恢复演练换成读完即结束的 `sealed`。
确认 checkpoint 已提交后，异常重启或 `kill` 演练结束，再执行
`sparrowctl start change`；`resume_latest=true` 会从兼容的 v6 CURRENT 恢复。
也可以先用 `sparrowctl checkpoints change` 查看数字 ID，再用
`sparrowctl restore change --snapshot-id N` 请求指定恢复点。恢复请求是新的
catalog revision，实际结果仍以 status 的 revision/attempt、checkpoint 和
诊断为准。

不要直接复用旧 v3/v4/v5 目录。需要显式 fresh/reset 时，复制模板并使用新
`checkpoint_dir`（或新 pipeline 名称），去掉固定 restore 后再用
`sparrowctl put-pipeline NAME FILE --if-match ETAG` 更新；不要删除仍需回退的
旧目录来“解锁”。fresh 会丢失前值连续性，status/explain 的 effective IoT
字段会标明这一点。

### 3. Deadband：restart_fresh

模板：[`pipeline-k4-deadband.json`](../deploy/pipeline-k4-deadband.json)，输入和
golden：[`k4-deadband.ndjson`](../deploy/k4-deadband.ndjson)、
[`k4-deadband.expected.json`](../deploy/k4-deadband.expected.json)。配置是
`absolute + last_output + threshold=1.0`；5 条输入输出 2 行（20.0、21.6），
等于或小于阈值的变化被抑制：

```sh
sparrowctl validate deploy/pipeline-k4-deadband.json
sparrowctl explain deploy/pipeline-k4-deadband.json
sparrowctl put-pipeline deadband deploy/pipeline-k4-deadband.json
sparrowctl start deadband
sparrowctl status deadband
sparrowctl diagnose deadband --output deadband-diagnostic.json
```

这是 `restart_fresh` 模板，未声明 aligned checkpoint。要演示 fresh reset，
先执行 `sparrowctl stop deadband`，确认 status 已停止，再执行
`sparrowctl start deadband`；文件从头读取，前值状态重新建立，第二轮输出会
再次得到 20.0、21.6。它不是告警恢复事件，也不是外部 HTTP 去重证明。

### 4. 有界失败演示

复制一份模板，把 IoT 配置的 `max_keys` 改成 `1`，再输入两个不同的
`device_id`。`validate` 可通过 schema/参数检查，但第二个 key 在运行时会
得到有界的 state quota failure；检查 `status`/`diagnose`，应看到失败而不是
隐式淘汰旧 key。修复配置或扩大受控预算后，用新 revision 重启；不要通过
无限增大队列或关闭 owner 计费来绕过限制。

### 5. 复用既有模板（TPL-02）

K4 不重建已有能力；下面只说明仓库中现有示例实际做什么，以及它们能否沿用
当前 Server 的控制面。TPL-02 不是新的 IoT 状态或恢复承诺。

#### 5.1 `pipeline-k1-zero.json`：线性 SQL 过滤

文件中的 SQL 是：

```sql
SELECT device_id, v FROM sensors WHERE v > 100
```

它是无状态的逐行过滤：输入

```json
{"device_id":"d1","v":50}
{"device_id":"d1","v":200}
{"device_id":"d2","v":101}
```

只会向 `log` sink 输出后两行（日志前缀通常为
`sparrow-log `）。它没有 Count/ET/PT 窗口、没有 watermark，也没有 IoT
前值；`v` 不是 IoT Deadband 的阈值配置。

模板使用 `source.file_contract=append_only`、`recovery=aligned`、显式
`checkpoint_dir` 和周期 checkpoint。实际操作前，应把模板的绝对路径改到
已配置的 `SPARROW_DATA_ROOTS`，注册与 SQL 一致的 schema（例如
`device_id` 非空 `utf8`、`v` 非空 `int64`），然后执行：

```sh
sparrowctl put-stream sensors /path/to/sensors-stream.json
sparrowctl validate /path/to/pipeline-k1-zero.json
sparrowctl explain /path/to/pipeline-k1-zero.json
sparrowctl put-pipeline zero /path/to/pipeline-k1-zero.json
sparrowctl start zero
sparrowctl status zero
sparrowctl checkpoint zero
sparrowctl checkpoints zero
```

`append_only` 到达 EOF 后会等待追加，不代表作业失败或已经完成；有限文件若要
正常结束，应明确改成 `file_contract=sealed`，并在结束前完成需要的 checkpoint。
这个过滤计划走 File/v3 的 zero-state aligned profile：切点仍需 Source 和
required Sink 的真实 ACK，不能因为没有 state 就跳过 barrier。恢复会从兼容
checkpoint 的 source cut 继续，而不是静默从文件头重放。

将同一配置改为普通 `restart_fresh` 时，不创建持久 checkpoint/generation，
每次启动都会从文件头重新执行，外部日志可能重复。aligned 模式即使当前没有
可恢复快照，fresh/reset 也会在受控 checkpoint 目录激活新的
`STATE_GENERATION`；它表示新的 lineage，不保留旧 state 连续性。v3/v4/v5
目录或损坏/缺失 CURRENT 不会被改成 fresh 绕过。

#### 5.2 `pipeline-k1-two-count.json`：线性 GraphSpec 的 Count 窗口

该文件用 GraphSpec 表达一条**线性**链路，因此 physicalize 后仍是线性
File/v3 计划，不需要 `graph_io`。它先按 `device_id` 每 3 行计算 `SUM(v)`，
再把这些 `s` 结果每 2 个计算一次 `SUM(s)`，最终列名为 `total`。例如单个设备
输入 `1..12` 时，第一层输出 `6,15,24,33`，第二层 `total` 输出 `21,57`；
输入只有 `1..7` 时先输出 `21`，第 7 行留在第一层 Count state 中等待后续两行，
第二层已经消费两个 `s` 并完成一次输出。日志行还会带有 Count 窗口的
`count_start/count_end`，它们是 0-based 的到达序号，不是时间戳。

Count 窗口按记录数关闭，不依赖设备时间、wall clock 或 watermark；未满窗口在
checkpoint 中保留，故恢复后不能把它当作 fresh 重跑。该模板的 `source` 是
`append_only` File、`sink` 是 `log`，所以不需要 HTTP fixture；EOF 后默认等待
追加。要让有限输入正常结束，可明确改成 `file_contract=sealed`。

实际操作与 zero-state 模板相同，先准备同样的 `sensors` schema 并把绝对路径改到
`SPARROW_DATA_ROOTS` 下：

```sh
sparrowctl put-stream sensors /path/to/sensors-stream.json
sparrowctl validate /path/to/pipeline-k1-two-count.json
sparrowctl explain /path/to/pipeline-k1-two-count.json
sparrowctl put-pipeline two-count /path/to/pipeline-k1-two-count.json
sparrowctl start two-count
sparrowctl status two-count
sparrowctl checkpoint two-count
sparrowctl checkpoints two-count
```

这是 zero/双 Count 的同一 File/v3 aligned 矩阵：Source、两个 state participant
和 required Sink 都必须在同一切点完成；恢复保留未满窗口和 source cut。改成普通
`restart_fresh` 时不持久化 checkpoint/generation，每次从文件头重新计算；aligned
fresh/reset 则在受控 checkpoint 目录建立新的 `STATE_GENERATION`，不延续旧窗口
状态。Lookup、Dedup、ET/PT 或带分支/多源的真正 DAG 不由这个线性 fixture 代表，
应按各自的支持矩阵单独 validate。

#### 5.3 `v02_static_table`：静态 ReferenceTable

这是 `crates/sparrow-cli/src/bin/v02_static_table.rs` 的 embedding 示例，
不是 SQL 或 Server API。它建立 `sites` 的两个内存快照：v1 将 `a` 映射到
`west`，v2 映射到 `east`；分别用两个新 Job 运行同一条
`device_id=a, temperature=21.0` 输入，预期输出：

```text
job1 (running keeps v1) site=west
job2 (new job uses v2) site=east
v02_static_table: ok
```

可用 examples feature 运行（这只是开发/embedding 命令，不是生产 Server
构建的一部分）：

```sh
cargo run --locked -p sparrow-cli --no-default-features --features examples --bin v02_static_table
```

静态快照在 Job 建立时绑定；运行中的 Job 不会因为另一个新快照自动切换。表名、
key/schema、keep 字段或快照 CRC 不匹配会拒绝构造/运行；示例没有 checkpoint、
`STATE_GENERATION` 或恢复连续性，也没有时间语义。当前 Server 没有参考表发布和
pipeline 绑定工作流，不能只把这个 binary 的 `ReferenceTable` 当作 Server
可运行配置。

#### 5.4 `v03_versioned_lookup`：按事件时间 as-of 的版本表

这是 `crates/sparrow-cli/src/bin/v03_versioned_lookup.rs` 的 embedding 示例。
`sites` v1 从 `valid_from=0` 起为 `west`，v2 从 5 秒起为 `east`；输入的
`ts=1_000_000` 和 `ts=6_000_000` 分别命中：

```text
as-of t=1s site=west
as-of t=6s site=east
v03_versioned_lookup: ok
```

运行命令：

```sh
cargo run --locked -p sparrow-cli --no-default-features --features examples --bin v03_versioned_lookup
```

这里的 `ts` 只用于 `FOR SYSTEM_TIME AS OF` 等价的版本选择，不是 IoT TTL、
watermark 或窗口结束时间。版本表必须有有效区间和可解析的 event-time；缺失或
错误类型会在 lookup 时失败。该示例同样是内存 Job，默认 fresh，未接入
Server 的表发布、依赖 pin 或 checkpoint；当前 aligned checkpoint matrix
明确拒绝 Lookup，因此不能宣称恢复时自动保留外部表版本。

参考表发布、依赖 pin、外部 Lookup 的恢复与运营属于后续 TAB-01～03；K4 只复用
这些已有能力，不扩大其 Server 或生产保证。

本页命令依赖已构建的 `sparrow-server`、`sparrowctl`，以及操作者提供的
HTTP 2xx fixture。K4 不提供 Web 页面，不自动启动目标端口，不自动写入
allowlist，也不将上述本地演示升级为生产认证。

<a id="k4-validation"></a>
## K4 全局 Review 与匹配验收（2026-09-17）

基线 HEAD 为 `d52b15c`，工作区包含未提交的 K3/K4。测试在
`box@100.64.0.16` 完成，本机未编译；产物根为
`/workspace/bench-compare/k4-artifacts-20260917/`，源码副本为
`/workspace/bench-compare/k4-source-20260917/`。未修改既有 Mosquitto 服务，
只使用独立目录、子进程和 loopback fixture。

### Review 后落地的修复

- **数值与状态原子性**：整数相对阈值移位溢出、浮点极端差值溢出；输出准入先于基准更新，候选/旧值复制先取得预算。新 key 的 table/index/state 准入失败回滚容量信用。
- **资源与时间**：per-key TTL 受逻辑 timer 上限约束；bucket 高水位持续计费，空表/cleanup 真正释放；恢复使用独立 replacement map，失败不改旧状态。输入结构 schema 与 owner 在状态变更前校验。
- **恢复边界**：IoT 独立 codec，拒绝空 key、错误 arity/profile 和 RCP2 宽松前缀；v5/v6 encode/decode 拓扑检查一致；无法分类的残缺历史 fail-closed，保留原目录。
- **图执行**：aligned 图只接收有序 events 输入；Union 的关闭输入不能替代 barrier ACK，active alignment 中关闭会失败，普通逻辑 EOF 不伪造切点。
- **控制面**：使用每个物理 Source 的 schema 校验 connector，Source/Sink 分开校验；HTTP-only/目录要求仅收窄新增 IoT aligned，不破坏旧线性 Count/Log。多源 replay、fresh 连续性、能力类型清单和有限错误归因与实际执行一致。
- **交付模板**：测试实际加载两份 K4 JSON、NDJSON 和完整 golden；change 使用 append_only，避免 sealed 读完即结束导致后续 checkpoint 演练失效。

### 测试结果

- `frozen-v5`：reliable/default-members **630 通过、0 失败、16 ignored**；独立 no-demo 生产入口 **36 通过、0 失败**。两种生产包均构建通过，默认包不带 NATS SDK。
- `validation-v5c`：**50 项 K4 × 20 轮 = 1000 次通过**。覆盖计划、算子、Kernel、codec/store、控制面、API、两个实际模板；精确清单为 `expected-tests-v5b.txt`。缺 crate、缺测试、改名、多余、重复、ignored 六个反例均在执行 driver 前拒绝，完整清单通过。
- 两种算子的真实进程 oracle 均通过：周期 v6 提交、SIGKILL 后未提交后缀重放、恢复后的等值抑制、HTTP 响应被阻塞时不能发布 checkpoint、CURRENT.tmp 提交失败不移动 CURRENT、语义变化拒绝旧状态、显式 fresh 新 generation、旧 K3 binary 拒绝 v6 且不改历史。
- `k3-regression-v5`：**24 项 × 20 轮 = 480 次通过**，另有零/双状态多源多 Sink 进程故障回归。
- 默认生产/K1 smoke、**32 项 K2（含独立真实 NATS）**及 K2 零/双 Count 进程故障回归通过。
- Go driver `vet/build` 通过。Clippy 正常命令退出 0，但仍有 **87 条 lint 提示**；`-D warnings` 未通过，不能称为零告警。后续清理见 [OPT-009](OPTIMIZATION_BACKLOG.md#opt-009)。

### 旧 File 路径性能回归

预先声明三组完整 ABBA，三组单独及全部样本合并均通过；没有编译与测量并行，
没有丢弃慢样本。fresh 为 K3 v14 → K4 v5，periodic 为同一 K4 binary 的周期
on/off（100 ms checkpoint、20 ms 慢 HTTP、原批处理参数）。fresh 每组形状合并
36 个测量试次，periodic 24 个；warmup 同样核对正确性。

| 形状 | 吞吐中位数比值 | peak RSS 差值 | checkpoint |
|---|---:|---:|---|
| fresh 零状态，K3 → K4 | 0.989576 | +72 KiB | 不适用 |
| fresh 双 Count，K3 → K4 | 1.006209 | +312 KiB | 不适用 |
| K4 零状态，周期 on/off | 1.004484 | −44 KiB | 12 个 on 试次各成功 18 次，失败 0 |
| K4 双 Count，周期 on/off | 1.005210 | −136 KiB | 12 个 on 试次各成功 12 次，失败 0 |

原门槛仍为 fresh ≥0.97、periodic ≥0.90、RSS 增量 ≤2048 KiB。全部试次输出
无 missing/duplicate/invalid，各组 normalized hash 一致。结果不表示每个指标
都提升，也不是远端 RTT/p99 或 eKuiper 新对照；本轮没有作这些推论。
完整脚本为 `sparrow-k4-regression-v5c.sh`，最终 `regression-v5c.exit=0`；
原始试次与四份合并 JSON 位于 `file-performance-v5-*`。

### 构建指纹与失败记录

```text
default server: 747580ab99f252cccf4ce99bd3888678a5a06a6f00b909415c72e089ff614cf7
JetStream server: 23f7b73ac0ab12626c7b3ca5e8b650faeb822c3c0895198bfb126d65c9f36a2d
K4 Go driver v2: 96e74b176264de4dcf4d8c449a80b162e01f8641defe66cdd974851076f5af75
50-test manifest: 3dd5a5a4bd4a8aa046864023921e05645102fe1c821c56cd028dd0d97c8f5d1f
v5 build source manifest: d662dcee748d9716d99c889cc43571a439f6c31c5ccee8766ede2ef1ae5859d4
```

产物不覆盖：早期编译错误、两处 fixture 合同错误、跨源全局顺序误断言、
49/50 精确清单拒绝，以及旧 binary 仅在 status 而非 stderr 报错导致的 driver
失败均保留。修正的是测试 oracle/清单时，没有改运行语义来迁就测试。
v5 冻结后只更新两项**不参与 Rust 编译**的验证输入：显式测试清单和 Go driver；
`build-to-validation-source-v2.diff` 单独记录差异，最终源码逐文件校验见
`local-remote-source-verify-v2.log`。包内源码指纹保持构建时原值，未伪造重编译。
本页和 TODO 的最终文档更新不改变已测试 binary，候选包中保留当时的开发说明。

**放行边界不变**：K4 首批功能完成不等于稳定发行或全部设计完成。TTL aligned、
IoT+JetStream、Hysteresis/HoldFor/完整告警生命周期、Server 表发布和 Web/K5
未开放；目标设备、TLS/WAN、24/72 h 长稳、掉电/介质故障仍需对应部署验收。
