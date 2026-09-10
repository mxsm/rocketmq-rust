---
title: "Glossary"
---

This glossary keeps English identifiers, Chinese terminology and project semantics together. Configuration keys, Rust types and protocol fields retain their exact spelling in both languages. The same word can name different progress or ownership boundaries; use the definition belonging to the operation being discussed.

## Terms

| English | Chinese | Meaning in this project |
| --- | --- | --- |
| Topic | 主题 | A named message stream whose routes contain queues; it is not a single ordered log. |
| MessageQueue | 消息队列 | The canonical identity includes topic, broker name and queue ID; a queue ID alone is not globally unique. |
| Producer / consumer | 生产者 / 消费者 | The sender / receiver role; an application can own several clients with explicit lifecycles. |
| Consumer group | 消费者组 | A logical subscription and consumption identity; clustering consumers coordinate queue ownership within the group. |
| Rebalance | 再平衡 | Recompute assignments after membership or routing changes; it can expose redelivery and in-flight work races. |
| Tag / SQL filter | 标签 / SQL 过滤 | Subscription selection criteria, not an authorization policy; SQL filtering has Broker configuration conditions. |
| Queue offset | 队列偏移量 | A logical position within a queue; it is not a CommitLog byte address. |
| Physical offset | 物理偏移量 | A byte position in the primary message log; compare only within the correct storage identity. |
| Consumer offset | 消费偏移量 | Recorded consumption progress; local advancement, remote persistence and business completion are distinct. |
| Message ID / business key | 消息 ID / 业务键 | Transport/storage identity versus application identity; retries need a stable business key for deduplication. |
| At-least-once | 至少一次 | Delivery semantics that permit duplicates; applications must handle repeated business work. |
| At-most-once | 至多一次 | Delivery semantics that permit loss while avoiding repeated delivery within the stated scope. |
| Exactly-once | 精确一次 | A scope-specific end-to-end property requiring explicit evidence; a send status or offset commit alone does not establish it. |
| Idempotent | 幂等 | Repeating the same logical operation preserves its intended business effect. |
| ACK / invisible time | 确认 / 不可见时间 | POP uses a receipt to acknowledge delivery; expiry can make an unacknowledged message visible again. |
| Dead letter queue | 死信队列 | A destination for messages whose normal retry path is exhausted; inspect the cause before redriving. |
| Request correlation ID | 请求关联标识 | Matches one pending request with its reply; it is not a durable business transaction ID. |
| CommitLog / ConsumeQueue | 主消息日志 / 消费队列索引 | Primary records versus derived queue lookup state; rebuilding a derived index does not recreate missing primary records. |
| Appended watermark | 追加水位 | An exclusive boundary of appended primary-log bytes; it need not already be durable. |
| Durable watermark | 持久水位 | An exclusive boundary of durable bytes; a record is covered only when its end is at or before that boundary. |
| Durability | 持久性 | The storage persistence property under the stated failure model; local and replicated acknowledgement policies differ. |
| Store cursor / epoch | 存储游标 / 纪元 | A progress token scoped by source or engine identity and generation; it is not a portable migration offset. |
| ISR / SyncStateSet | 同步副本集合 | The replicas participating in the configured HA acknowledgement policy; membership is separate from master authority. |
| Master epoch / fencing | 主节点纪元 / 隔离旧主 | Authority generation and rejection of stale authority; ordinary NameServer routing is not Controller consensus. |
| Ownership / lifetime | 所有权 / 生命周期 | Who controls a value or service, and how long its access remains valid; shared access does not imply shared shutdown ownership. |
| ServiceContext / TaskGroup | 服务上下文 / 任务组 | Scoped resources and owned task lifecycle; shutdown cancels and awaits owned work. |
| Resource budget / backpressure | 资源预算 / 背压 | Bounded admission and pressure propagated to callers; task ownership and budget ownership are separate trees. |
| Cancellation / timeout | 取消 / 超时 | Stop waiting or request work to stop; neither proves that an accepted remote or blocking operation was undone. |
| Frame / protocol | 帧 / 协议 | A bounded encoded exchange unit / its interpretation rules; TCP packet boundaries do not define message frames. |
| Authentication / authorization | 身份认证 / 授权 | Establish identity / decide allowed operations; Cargo features and tool discovery are neither substitute. |
| Partial result / freshness | 部分结果 / 新鲜度 | A bounded observation can omit unavailable sources or rows; age and warnings affect interpretation. |
| Availability / consistency | 可用性 / 一致性 | Ability to serve requests / agreement of observed state under a defined model; both need a stated scope. |
| Latency / throughput | 延迟 / 吞吐量 | Time for the stated operation / completed work per unit time; compare equivalent workloads and percentile definitions. |

## Read symbols with their unit and scope

- A queue position of 42 and a CommitLog byte position of 42 are not interchangeable. Include the queue or storage identity and unit with an offset.
- A durable watermark of 4096 covers bytes before 4096. An appended record ending after that boundary is not yet covered by that watermark.
- A request timeout of 3000 ms says how long the caller waited under that API's deadline rules. It does not prove that the business action did not execute.
- “Committed” needs an object: local client progress, remote consumer-offset persistence, transaction decision, log persistence and Controller state-machine commitment have different meanings.

## Follow the concept

| Area | Explanation |
| --- | --- |
| Identity, offsets and records | [Message model](../architecture/message-model.md), [storage](../architecture/storage.md) |
| Delivery, retries and business deduplication | [Delivery and retry](../guides/delivery-and-retry.md), [request/reply](../guides/request-reply.md) |
| POP and consumption progress | [POP](../consumer/pop.md), [LitePull](../consumer/pull-consumer.md) |
| Ownership and resource limits | [Runtime](../architecture/runtime.md), [Rust API](./rust-api.md) |
| Authority and acknowledgement | [HA and Controller](../architecture/ha-controller.md) |
| Identity and bounded observations | [Security](../architecture/security.md), [read-only MCP](../ecosystem/mcp.md) |

When adding a term, describe the owning boundary and a likely confusion, then update both languages and the relevant conceptual page. Keep symbols and executable examples unchanged during translation.
