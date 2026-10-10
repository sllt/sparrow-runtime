# 窗口能力与边界

更新：2026-09-27。开发顺序第6批（WIN-01 / ANA-01）的实现合同。

**当前状态：限定Preview实现、自查和本批统一验证完成，尚未发布或整体生产放行。**
旧窗口已有证据不自动证明新窗口；本批不恢复暂停的JetStream高负载专项。

## 支持范围

| 类型 | SQL GROUP BY窗口项 | Graph `window.kind` | 本批恢复范围 |
|---|---|---|---|
| PT跳跃 | `HOP(PROCESSING_TIME, slide, size)` | `hop_pt` | restart_fresh；第11批子批2c起线性单窗口 aligned paused File v34 / JetStream v35（见PRODUCTION矩阵） |
| 滑动计数 | `COUNT_WINDOW(size, step)` | `sliding_count` | restart_fresh；第11批子批2a起线性单窗口 aligned File v31 / JetStream v32（见PRODUCTION矩阵） |
| PT逐事件滑动 | `SLIDING(PROCESSING_TIME, size[, delay])` | `sliding_pt` | restart_fresh；第11批子批2c起 v34/v35（同上） |
| ET逐事件滑动 | `SLIDING(ts, size[, delay])` | `sliding_et` | restart_fresh；第11批子批2b起线性单窗口 aligned File v33（见PRODUCTION矩阵） |
| PT会话 | `SESSION(PROCESSING_TIME, gap, max_duration)` | `session_pt` | restart_fresh；第11批子批2c起 v34/v35（同上） |
| ET会话 | `SESSION(ts, gap, max_duration)` | `session_et` | restart_fresh；第11批子批2b起线性单窗口 aligned File v33（见PRODUCTION矩阵） |

保留已有PT/ET tumbling、ET hopping、单参数`COUNT_WINDOW(size)`及其原有恢复profile。
新窗口支持COUNT/SUM/AVG/MIN/MAX，沿用NULL和整数溢出报错规则。不产生空窗口。
本表不是eKuiper SQL/配置逐项兼容声明，也不是任何Source/状态/恢复组合都可用。

SQL时间整数的单位是**微秒**，也支持整数`INTERVAL`的MICROSECOND、MILLISECOND、SECOND、MINUTE、HOUR。
单位不识别、值非法、转换溢出、额外参数、DISTINCT/OVER/filter等修饰不能静默忽略。
一个GROUP BY只允许一个窗口分配项。

## 精确语义

### 跳跃和滑动计数

- HOP是固定网格`[start,end)`，`0 < slide <= size`；重叠数按原`max_overlap`限制，默认8。
  PT使用处理时钟，ET使用事件时间；它们都不是逐事件SLIDING的别名。
- 双参数COUNT_WINDOW是按key到达顺序滑动。第n条到达时，仅在`n >= size && n % step == 0`输出最近size条。
  `1 <= step <= size`；例如size=5、step=2，第6条首次输出第2～6条。
  `count_start/count_end`是该key的零起点绝对序号区间`[n-size,n)`；EOF丢弃不满/未到步长的尾部，不补部分窗口。
  单参数旧COUNT_WINDOW仍是原滚动计数，不改变旧输出及快照语义。

### 逐事件SLIDING

- 每条接纳的输入产生一个触发点T；同时间戳的多条输入是多个触发，不合并成一次。
- 成员范围为`(T-size,T+delay]`。离散微秒输出列统一表示为`[T-size+1,T+delay+1)`。
  因而T=10、size=10、delay=5的输出边界是`[1,16)`，包含时间15，不包含0或16。
- PT零delay：立即输出**当时已到达的前缀**，同一微秒随后到达的行不修改已输出结果。
  PT正delay：exclusive end到期时输出；到期输出先于这个时间点的新输入。
- ET：等待watermark到达exclusive end再final，避免在T本身就关闭而遗漏同时间戳后续事件。
  正delay会等待将来时间范围，不是简单把相同结果延迟发送。
- ET首版L=0，时间戳小于已推进水位的行走late路径，不修正已发结果。
  未来偏移超限计入future-drop，不推进水位；未初始化/all-idle水位不擅自推进。

### Session

- 每key事件按`(timestamp, arrival_sequence)`排序。首条时间first，末条时间last；
  会话结束为`min(last+gap, first+max_duration)`，半开区间，等于结束点的事件开启新会话。
- ET未关闭会话允许乱序桥接，也允许更早事件改变first后因max_duration而重新分段。
  不能仅合并累加器：更早事件可能需要把既有会话拆开；首版保留有界聚合输入来正确重算。
- PT在处理时钟到达结束点时final，ET等水位到达结束点时final。
  L=0、final-only；已输出会话不重开，迟到撤回/更正仍是V2-02。
- gap与max_duration必须显式为正；max_duration可以小于gap，此时最大时长先切断会话。

## 时钟、结束和资源

- 新PT窗口在同一attempt内对处理时钟回退做单调钳制，不能重新打开已关闭窗口。
  不是paused durable clock；重启不保持同一处理时间或未输出状态。
- 有限ET输入的明确永久EOF关闭未完成窗口；普通网络断连不能伪装EOF。
  Graph通道无EOF标记关闭会失败。PT有限输入在自然deadline排空后结束，不因文件读完提前关闭会话。
- `max_buffered_rows`仅用于SlidingCount/SLIDING/Session：每key最多1024条（默认），Graph可配1～16384。
  包含未完成触发和仍需供后续窗口使用的历史输入。SQL当前使用默认值，滑动计数size不能超过默认上限。
  单次候选和输出还必须先获得独立临时额度；该上限不是“保证能装入这么多条”。
- 保存的是detached聚合表达式值，不是原始RowBatch；没有pin住大输入批次。
  Job的retention/reservation、max_state_keys和PT timer上限同时生效。数值上限不代表内存豁免。
  到达上限明确失败，不通过丢数据或扩大无界队列继续运行。
- 有界重算的单次工作与该key保留行数×聚合数相关；输出逐窗口计费、背压和取消。
  这是正确性优先的Preview，不承诺与旧增量Count/Tumble相同的吞吐。
- 每key一个deadline索引；处理到期项不扫描/复制整个key空间。内部顺序按deadline、编码key；同key事件以时间、到达序号稳定排列。

## Graph配置例子

下面是一个窗口节点，接在带`ts`的Source后面；ET Source可显式配置out_of_orderness。

```json
{
  "id": 2,
  "kind": "window_agg",
  "keys": ["device_id"],
  "event_time_field": "ts",
  "window": {
    "kind": "session_et",
    "gap_micros": 1000000,
    "max_duration_micros": 10000000,
    "max_buffered_rows": 4096
  },
  "aggs": [{"fn": "count", "alias": "samples"}],
  "out": [3]
}
```

其他参数：hop_pt使用`size_micros/slide_micros`；sliding_count使用`size/step`；
sliding_pt/sliding_et使用`size_micros/delay_micros`，delay缺省0。
不用歧义的`window.kind="sliding"/"session"`推断时间域；PT不接受event_time_field。
冲突字段和不适用于所选新类型的参数拒绝，不静默忽略。

## 恢复、验收与后续

**第11批子批2a：**滑动计数（`COUNT_WINDOW(size, step)`）作为线性管道唯一状态时，可用 aligned 恢复：
participant codec 4（`BWF1`，保存每key到达序号和最近size条已求值聚合输入），File v31 / JetStream v32，
完整语义严格匹配；格式、限制、恢复额度规则和验证状态以 [PRODUCTION.md](PRODUCTION.md) 矩阵为准。
滑动计数key不过期，仅受max_keys约束。

**第11批子批2b：**ET滑动/ET会话作为File线性管道唯一状态时，可用 aligned 恢复：participant codec 4
（`BWF1` kind 7/9，保存每key到达序号、按(事件时间,序号)排序的已求值聚合输入及滑动的pending标记，
外加单输入水位生成器状态），File v33，完整语义严格匹配；JetStream + ET 仍拒绝。timer/deadline不单独持久，
恢复时由状态按在线规则重建，并校验“切点时无到期项”。线性ET的out_of_orderness恒为0（只有图source_time可设置），
因此比水位更早的乱序行一律late，ET会话的“乱序桥接”只发生在图路径；恢复后同样的乱序行仍判late。
线性File没有idle生成器：**ET窗口只在新事件推进水位或EOF时关闭**，恢复后无新输入不会输出。
future-skew用重放时墙钟比较（已知限制，与v3/v29一致）。会话保持final-only、L=0。

**第11批子批2c：**PT跳跃/PT滑动/PT会话及PT滚动+新聚合作为线性管道唯一状态时，可用 aligned 恢复：
paused File v34 / JetStream v35，时钟为v16/v17持久逻辑钟（每次决策一个ProcessingTime + 至多一行，随后提交；
不读墙钟，停机与pending重放不推进时间，无追赶）。PT跳跃窗口start可为负；PT滑动/会话的BWF1尾部保存冻结时逻辑钟，
恢复时必须等于快照切点。恢复后无新输入时tick照常触发窗口。不与TTL/HoldFor等第二状态组合；PT滚动+旧聚合仍为v16/v17。

以下段落适用于其余新族的 restart_fresh 行为。

其余新族尚未发布checkpoint codec/profile。Control拒绝aligned、restore、checkpoint、checkpoint_dir；
CheckpointPlan、旧PlanLayout复用判定和Kernel也分别拒绝，不能绕过控制层直接创建假恢复承诺。
新的buffered族通过Kernel执行；旧WindowOperator raw freeze helper不是这些新族的公共执行/恢复接口。
旧StateSemantics和旧窗口freeze编码不改；给新类型编码参数不等于已支持快照。
重启只能显式fresh，状态和计数序号从头开始；停机前待发窗口不会在恢复后继续。

本批统一验收范围固定为：

1. SQL/Graph绑定一致，参数/溢出/资源反例，恢复准入拒绝。
2. 独立golden及有限数学reference：重叠、首窗/步长、边界相等、重复时间戳、延迟、乱序桥接/重分段、NULL/AVG/MIN/MAX。
3. Kernel线性/Graph、虚拟时钟、永久EOF、取消/owner退款、late/future和水位行为。
4. 真实Supervisor→sealed File→HTTP的六种新族有限oracle。
5. 一次默认成员回归，配合旧窗口/恢复门禁；必要时只重跑失败的相关组。

### 本批验证结果（2026-09-27）

- 服务器：`box@100.64.0.19`，Linux x86_64，Rust/Cargo 1.98.0；本机未编译。
- 默认12成员Release回归（含JetStream feature）：**937 passed、0 failed、21 ignored**；未纳入Arrow实验workspace成员。
- 生产入口Server/CLI独立`no-default-features`：**44 passed、0 failed**；不由默认demo构建代替生产入口校验。
- 新Runtime 14、SQL 3、Control 2项，用冻结测试二进制各重复5轮：**19×5＝95 passed**。
  Control包括六种新窗口的真实Supervisor→File→HTTP有限oracle，不是只调用内部helper。
- 已核对源文件清单与本机一致、测试后源码未变化。`git diff --check`、新增文件rustfmt及打包脚本语法检查通过。
- 首轮Runtime有1个测试夹具失败：取消后测试仍持有mailbox observer的37,712 B元数据额度，却先断言owner归零。
  修正为先检查队列credit/accounting再释放测试持有的observer；最终整套及5轮重复均归零。原失败日志保留，不伪报首轮全绿。
  自查同时补了保守Vec容量估算、Count与时钟独立、旧状态复用入口拒绝、ET字段类型/名称准入；不新增恢复承诺。

主要命令：

```sh
cargo test --locked --release --quiet --no-fail-fast --features sparrow-server/jetstream
cargo test --locked --release --quiet --no-default-features -p sparrow-server -p sparrow-cli
# 冻结的Runtime/SQL/Control测试程序，各执行5次（不重复编译）：
./runtime-tests window_completion_tests:: --quiet
./sql-tests window_completion_tests:: --quiet
./control-tests window_completion_tests:: --quiet
```

证据目录：`/workspace/bench-compare/windows-20260927-etFWDh/`。
最终源码在`source-r2/`，以`source-final.sha256`而不是目录后缀识别；清单SHA-256为
`046ab1ee36188271ff07db019b14a2b2d36efa91266dd47a7c8312d9298e09d0`。
`tests-final.log`、`no-demo-final.log`、`repeat-final.log`及对应`.exit`保存结果；
`source-final-verification.log`、`frozen.sha256`保存源码/二进制核验，`tests-r1.log`保留初次失败。
基线HEAD为`e3bc558`，存在此前未提交改动，不能把它当作本批候选的精确源码身份。

不重复上一批58档容量试次，不恢复JetStream 10k/20k专项，不把有限测试称为长稳/生产认证。
21项ignored未伪算通过；本批未做新窗口容量/跨引擎性能对照、Clippy零告警或生产打包发布。
新族持久恢复仍为后续功能；重算/增量移除等优化统一记在[OPT-015](OPTIMIZATION_BACKLOG.md#opt-015)，由负载证据驱动。
下一批按第7批函数/SQL/有界分析执行；插件仍按第8批的原生/脚本/WASM三条线实施。
