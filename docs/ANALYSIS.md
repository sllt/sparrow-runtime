# Bounded analysis Preview

第 7 批增加集合/编码函数、多行展开、两路事件时间关联、补充聚合和独立有限查询。它们不是完整 SQL 引擎，也不代表与 eKuiper 的全部语义/配置兼容。

**状态：2026-09-27 限定实现、自查与服务器集中验证完成，尚未发行。** 新 UNNEST、Join 和补充聚合只开放 `restart_fresh`；aligned/checkpoint/restore 显式拒绝，不改旧状态 codec。无新的吞吐、长稳或生产认证结论。

## 纯函数

SQL 与 Graph 共用类型检查、函数 registry、NULL/错误和分配额度：

- 数组：`array_length`、`array_get`、`array_contains`、`array_append`、`array_slice`、`array_concat`、`array_join`。
- 对象：`object_keys`、`object_values`、`object_get`、`object_has_key`、`object_remove`、`object_set`。
- 字符串与编码：`split`、`base64_encode`、`base64_decode`、`hex_encode`、`hex_decode`、`sha256`。

集合最多 1024 项，每个输入/输出文本、字节或动态值最多 64 KiB；运行 Job 的内存预算仍可先拒绝。数组下标从 0 开始，越界或不存在的字段返回 SQL NULL；`array_slice(array,start,length)` 使用非负起点和长度，超出部分截到数组末尾。`append`/`set` 可以写入 NULL，其它操作按函数合同传播 NULL。数组元素比较是带类型的结构比较，动态对象字段顺序也参与比较；不是 SQL 数值隐式转换比较。

对象 keys/values 保留输入键顺序。`split(text,separator)` 只接受非空字面量分隔符。Base64 使用标准带 padding 编码及严格解码；hex 解码要求偶数个十六进制 ASCII 字符。解码结果为 Bytes，SHA-256 输出小写 hex；不是加密或密码存储函数。

## UNNEST

```sql
SELECT s.device, to_int64(u.item) AS value, u.ord
FROM events s CROSS JOIN UNNEST(s.items) WITH ORDINALITY AS u(item, ord)
```

首版一个数组表达式、一个展开节点。NULL/空数组产生 0 行，非数组 Dynamic 在运行时失败。输入列保留，追加元素列与 `unnest_source`、`unnest_input`、`unnest_ordinal`；名称冲突拒绝。元素顺序不变，ordinal 从 1 开始。`unnest_input` 是本算子按来源标签计数的已接收输入序号，不是 broker/file offset，重启归零；未知 source 标签为 NULL。展开全部完成后才传递后续控制消息。

Dynamic 元素转整数推荐 `to_int64`，支持解码为 UInt64 的正 JSON 整数并检查范围；旧 `CAST` 的受限转换语义未在本批改写。

Graph 节点：`{"id":2,"kind":"unnest","unnest":{"expr":{"k":"col","name":"items"},"as_field":"item","max_rows":1024,"max_bytes":1048576}}`。`expr` 遵循通用 ExprSpec。默认每输入最多 1024 行/1 MiB 展开估算，硬上限 4096 行/16 MiB；上限不是预留保证。保留原数组列也计入每个输出行，过宽记录可能远未到行数上限就被拒绝。输出逐行续行、接受背压和取消，不一次分配全部展开结果。

## 两路事件时间 Join

```sql
SELECT a.device, a.value AS left_value, b.value AS right_value
FROM readings a LEFT JOIN readings_aux b
ON a.device = b.device
AND INTERVAL_MATCH(a.ts, b.ts, 3000000, 5000000, 1000000)
```

`INTERVAL_MATCH(left_time,right_time,before,after[,out_of_orderness])` 参数单位微秒，也接受已有 interval 字面量。匹配条件是 `right_time ∈ [left_time-before,left_time+after]`，两端包含。省略乱序界限时为 0；输入晚于声明界限会失败，不能靠调度顺序掩盖。

`WINDOW_MATCH(left_time,right_time,size[,out_of_orderness])` 使用同一从 0 对齐的事件时间滚动网格 `[start,end)`；不是 HOP 多窗口关联。支持 inner 和 left，1～8 个不同的、同类型标量等值 key；NULL/NaN key 不匹配。一次 SQL 仅一个 Join/UNNEST，普通 Lookup JOIN 仍走原功能；新 Join 后的分组组合应使用明确 Graph 而非假定任意 SQL 都已开放。

用于长期管线时，SQL Join 同样必须提供 `graph_io`：两个 Source ID 为 `1`/`2`，Sink ID 为 `6`；旧 `source`/`sink` 字段必须与各自最低 ID 的配置一致，不允许隐藏的第二份配置。普通线性 SQL 不能借此挂载多余 I/O，绑定阶段仍核对精确端口和图拓扑。有限查询只需 inputs，不启动上述连接器。

- 两侧必须是两个直接 Source，且分别有非 NULL Int64/TimestampMicrosUTC 事件时间绑定；不接受任意上游 Transform/窗口改写后沿用旧水位。
- 每对匹配在第二行到达时输出一次。跨两路总顺序不保证；每侧接收 ordinal 可观测但不是持久身份。
- Left 仅当**右侧真实水位严格超过包含式匹配上界**（Window 为到达窗口末端）或永久 EOF 后发 unmatched；绝不收到左行就先发 NULL。
- Idle 不证明无匹配，不据此清理或提前输出。无水位前进时，状态可能达到额度并明确失败；不能无界等待/缓存。
- 输出 `join_time=max(left_time,right_time)`，unmatched 为左时间；`join_left_ordinal`/`join_right_ordinal` 保留局部序号。Left 的右字段可 NULL。未决 unmatched 左行会约束输出水位。
- L=0，无 retract/迟到更正。低于本侧已发水位的行报错；超未来时钟容差计入 `future_dropped`。时间溢出报错。
- 默认每侧 1024 行、每次输入最多 1024 个匹配/1 MiB 输出估算；Graph 可调到 4096 行/匹配、16 MiB。key/state/输出另受 Job 预算限制；超限失败而非静默截断。
- Graph 使用 `interval_join`/`window_join` 和 `stream_join` 对象：`left_input/right_input`、`left_keys/right_keys`、`left_time/right_time`、`mode`、`before_micros/after_micros` 或 `window_size_micros`、可选前缀和上述额度。默认输出前缀 `left_`/`right_`；SQL 按别名重写字段。

这些是有界小状态 Preview，不是高吞吐哈希 Join 认证；限额失败前已经发出的旧结果不回滚，也不提供跨 Sink 原子事务。

## 补充聚合

`FIRST`、`LAST`、`VAR_POP`、`VAR_SAMP`、`STDDEV_POP`、`STDDEV_SAMP` 支持一个表达式，不支持 DISTINCT/星号。First/Last 跳过 SQL NULL，遵循所属窗口的既定顺序：增量 Count/PT/滚动窗口按接收顺序，有缓冲 ET Sliding/Session 按事件时间再按到达序号；聚合函数本身不另外排序。全 NULL 返回 NULL。动态值里的 null 不等同 SQL NULL。

方差/标准差使用顺序 Welford f64 算法，人口口径空集返回 NULL，样本口径不足 2 个非 NULL 数值返回 NULL；拒绝非有限数或累积溢出。Int64/UInt64 转 f64 在超过 2^53 时可能失精度。没有新增并行 merge、近似聚合、UDAF ABI 或恢复 codec，旧聚合/旧 codec 保持原合同。

## 独立有限查询

认证后的 `POST /v1/query`，或 `sparrowctl query request.json`：

```json
{"sql":"SELECT array_length(items) AS size FROM events","inputs":[{"stream":"events","rows":[{"items":[1,2,3]}]}],"limits":{"input_rows":1024,"output_rows":256,"output_bytes":524288,"work_units":1000000,"timeout_ms":2000}}
```

SQL 使用已登记的 Stream schema；也可改用 `graph`（二选一，允许嵌入 catalog）。inputs 必须精确匹配 1～4 个 Source 名称。每个查询单独 Kernel/内存额度/工作额度/生命周期，不占用 live Job slot；只支持内存输入、纯转换、新分析、ET/Count 窗口和单 Capture 输出，不启动外部 Source/Sink/Action、不修改流水线。历史数据应由客户端准备成有限 rows；**不提供打开任意服务端历史文件的 API**。

请求最多 64 KiB（SQL 最多 8 KiB），解码输入最多 2 MiB/4096 行，计划最多 32 个 stage。上例为默认限额；输出硬上限 4096 行/1 MiB，工作额度上限 1000 万，执行超时 10～30000 ms。额度以运行时估算和编码字节共同约束，不能承诺这些上限同时可达。

`work_units` 是运行时抽象步骤计数，不是 CPU cycle/操作系统硬配额；查询仍与服务共享进程和机器资源。

全进程最多两个 admitted query；超额立即拒绝，不建立无限队列。响应字节持有内存额度和 admission，直到发完/丢弃，慢客户端不能无限积存结果。客户端取消/超时会取消并等待全部查询 task 退出后再释放 worker admission。超时覆盖 Kernel 执行，不承诺抢占任意解析/本地存储调用；请求/解析另有上述体积限制。

成功返回 rows、input_rows、output_rows、future_dropped、complete，以及 `execution=independent_bounded_kernel`、`side_effects=false`、`recovery=none`、`certified=false`。超限/错误不返回部分成功结果；`complete` 不意味着输入未来时间丢弃为 0，必须同时检查计数。它是有限 Preview，不是无界历史数据库或资源隔离进程。

## 验证范围

已验证：独立 Join 双重循环 oracle（两种到达顺序/inner/left/interval/window）、边界水位/unmatched、NULL key、fan-out/额度退款、UNNEST 顺序/空值/typed timestamp/取消、不同时间字段的 Join→ET Window、聚合独立数值答案、SQL/Graph 拒绝矩阵、有限查询限额/响应 admission、真实双 File→required HTTP 与 API 认证隔离。最终默认12成员/JetStream Release **957 passed / 21 ignored**，无 demo Server/CLI **45 passed**，新增20项重复5轮 **100 passed**。源码指纹、命令、初轮失败和未测项见[匹配证据](PRODUCTION.md#analysis-validation)。

后续仍有：Join/新聚合恢复、任意上游/多 Join 组合、近似聚合/UDAF、服务端历史文件查询和本批容量/长稳。这些不由“函数/SQL 可调用”自动视作完成。
