# ADR-004：K2 JetStream 可靠链路

日期：2026-09-15。状态：实施中，尚未发布可靠保证。

## 决策与交付边界

用户已批准 NATS JetStream 路线。基线为 K1/R11 提交 `6fb20c2`，独立分支 `feat/k2-jetstream-reliability`；不引入 Kafka 或并行建设本地输入日志，也不改动现有服务。

- SDK 固定 `async-nats = 0.50.0`，关闭默认 features，仅启用 ring、JetStream/KV 和已核验服务端配置字段。NATS SDK 不进入 `sparrow-runtime`/`sparrow-model`，生产 HTTP-only CLI 不链接它。新增 `jetstream` 构建 feature，关闭时明确报告不可用。
- 首个实测服务端固定 NATS Server **2.14.6**；仅在服务器创建隔离数据目录/监听端口。该版本修复 standalone 恢复后 stream created 时间保存，来源身份不能依赖会随重启变化的字段。[官方发行说明](https://github.com/nats-io/nats-server/releases/tag/v2.14.6)
- 保证目标为：约定 required 输出确认 + durable checkpoint 后逐消息 ACK；不因已读、已进入内存或最大 sequence 而 ACK。HTTP 成功不等于业务幂等，不声明 exactly-once。
- 优先使用 broker 的保留/重放；本批不默认另做独立 HTTP outbox，因而不承诺 broker/下游断开时仍能本地持久接纳并离线续送。HTTP-07 保留为未实现的条件分支。
- Source 必须验证 stream 代次、保留策略和实际 reader 可读范围。reader 按 attempt 隔离；消费者所有权仅声明经过验证的单节点协作式独占，不把它扩写为跨节点 HA/fencing。
- 输出 ID 按逐行序号而非 HTTP batch 生成；恢复保存输出序号和语义 lineage，下游语义切换须避免“同 ID、不同内容”。窗口/时间形状按确定性重放的实测矩阵开放，不因 K1 File 可恢复就默认 NATS 的任意形状均可恢复。
- Poison 默认明确失败/有限重试后 held；选用 DLQ 时，原始消息和失败身份必须先达到有界持久点，再允许对应 source cut 前进。满/损坏/过期和人工重放分别验证，不静默 ACK。

## SDK conformance 前置

不能直接采用 SDK 默认的自动拉取或缓冲设置：默认 subscription capacity 为 65536；`read_buffer_capacity` 只控制初始容量，不是硬上限。需显式限制 pull 条数/字节/expiry、全 consumer 的 MaxAckPending、订阅/命令队列与 retained ACK 集合；slow-consumer/gap 不能转为正常处理。[ConnectOptions](https://docs.rs/async-nats/0.50.0/async_nats/struct.ConnectOptions.html)

`Client::drain()` 源码只排入 Drain 命令，并不等待后台任务退出。SDK 资源 lease 需要跟随实际生命周期，stop/replacement 要等待关闭；不得因为 drain 返回就退款或启动新 reader。进行中的 pull 也不能因 checkpoint select 被随意丢弃，否则可能丢掉 SDK 已接收但尚未提交给 Kernel 的消息。[SDK 源码](https://github.com/nats-io/nats.rs/tree/main/async-nats/src)

连接仅使用批准的端点，忽略自动发现的新服务器，认证只用 SecretRef，TLS 不提供跳过校验选项。数据预算与协议/控制开销分开记录，实测 SDK 内部缓冲和关闭后资源回收，不能把配置值当作测量值。

## 整批验收顺序

1. SDK/服务端行为、保留策略、bounded pull、ACK/progress/redelivery、关闭和 reader 独占 conformance。
2. 稳定来源身份/连续交付证明/有界 pending 去重、同一 Job owner 下的 source bootstrap 和输入接纳。
3. 完整参与者 checkpoint、保存切点后 ACK、未提交重放、checkpoint 后 ACK 丢失、consumer 重建与 retention 拒绝。
4. 稳定输出身份/required 确认、有限失败与 DLQ/重放；核对零状态与开放窗口组合的完整输出 oracle。
5. API/CLI、capabilities、配置模板与操作文档；真实进程、broker 重启、拒绝/磁盘/网络故障、资源和性能回归。
6. 独立自查并修复后再交付；未完成项不勾选，不以 conformance helper 代替完整链路。

## 首批收口与自查决定

R12（2026-09-16）补充：保留 Explicit ACK，采用 16 并发/3 次有限确认尝试的独立、有预算且可 join worker；不把 AckAll 枚举替换视为已证明等价。Source 合并已就绪记录，区分非空批次完成与空闲退避。正常 checkpoint busy/对齐排空超时不再结束 attempt；存储/真实输出/协议错误仍 fail-closed。KV policy 也在运行期复核，stream、KV policy、binding 共三个控制查询的成本明确保留。验证入口与新证据见 JETSTREAM.md。

首批选用 fail/held（不选 DLQ）、broker 保留/重放（不选独立 outbox）；只有全 stream 顺序输入和零/单 Count/双 Count + HTTP。语义切换和固定历史 replay 在有 durable lineage fork 前拒绝，不能在 RAM 临时换输出 epoch。完整 REL 与发行门禁仍保留，当前为 Preview。

自查落实：SDK `Context` 的 publish-ACK 队列另限 4 / concurrency 1；实际 source ACK 使用轻量 reply token 并验证响应，不沿用 SDK `Some(_)` 即成功的判断。headers 展开、原始 Bytes pin、稀疏 schema 与独立输入 Box 均有预占/释放反例。Source bootstrap 必须先占真实 Job quota，guard 关联 SDK 实际关闭与目录锁；旧 reader 删除成功前不丢其 KV 绑定。

源错误不能被后续 Cancelled 覆盖；required HTTP 失败立即结束 attempt，不等长周期 checkpoint。重试计数恢复必须有 durable 输入进展，不能靠运行 30 秒循环重置。默认 File 性能曾因新 `IngressEvent` 内联批次回归，已改为薄的、有信用覆盖的间接变体，保留未过门槛的原始样本。最终受测证据与边界统一记入 `docs/JETSTREAM.md`。
