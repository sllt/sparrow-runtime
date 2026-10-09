# ADR-006：Local DataBus（进程内 topic 总线）

日期：2026-10-09。状态：Preview，已实现，未生产认证。

English summary: pipelines inside one runtime are chained through named
in-process topics. Each subscriber has a bounded buffer whose limit is charged
up front to its job's reservation, and an explicit overflow policy that
defaults to `drop_oldest`. The bus does not replay, so it only claims
`live_best_effort`. Aligned, checkpoint and restore are refused instead of
inventing a replay point.

## 背景

用户希望一条规则的输出能直接作为另一条规则的输入，相当于 eKuiper 的
memory source/sink。现在只能绕道 MQTT/HTTP，代价有三个：一次网络往返、一个外部 broker，
以及两套预算账本。

## 决策

1. **一个 runtime 一个 `DataBus`**，由 Supervisor 持有。订阅和发布注册都是 RAII 句柄，
   注销时 topic 自动消失，没有显式的 topic 生命周期，也就不会泄漏。
2. **每个订阅一个有界缓冲**，同时受条数和字节两个上限约束。上限在订阅时一次性计入
   订阅方 job 的 reservation，不按单条消息补账，因此不会出现先分配、后补账。
   订阅不下则 `BoundExceeded`。
3. **溢出策略显式配置**：`drop_oldest`（默认）、`drop_newest`、`block`（等待有上限）。
   每种策略有自己的计数器。`block` 订阅者排在所有非阻塞订阅者之后才等待，
   所以它只拖慢发布方，不拖慢其他订阅者。
4. **完成点 = 投给所有匹配订阅缓冲**。Sink 据此回执 outbox。没有订阅者时计数并丢弃，
   不阻塞上游。
5. **语义只声明 `live_best_effort`**。内存总线没有可重放的位置：订阅者缓冲在停止时丢弃，
   新订阅者看不到历史。如果把上游 checkpoint 推进与"已投入缓冲"绑定，下游崩溃后就会丢数据，
   却仍然声称是 at-least-once。因此拒绝 aligned、restore 和 checkpoint，
   且 Source 和 Sink 两端都拒绝。
6. **注册失败 fail closed**。publisher 达到上限时，`databus_sink_fatal` 使 job 失败，
   不会静默变成一个"什么都不发布"的 Sink。
7. **不引入依赖、不加 cargo feature**。实现只用 `tokio::sync::Notify`、`std::sync::Mutex`
   和标准容器。

## 后果

- 链式规则没有网络开销，消息体在扇出时共享。代价是每个订阅者都要预留缓冲上限的内存。
- 不检测跨 pipeline 的环，只拒绝同一 pipeline 内的自反馈环。
- 将来若需要可靠的链式传递，应该经过 JetStream 这类持久化中间层，而不是给内存总线加
  checkpoint。
