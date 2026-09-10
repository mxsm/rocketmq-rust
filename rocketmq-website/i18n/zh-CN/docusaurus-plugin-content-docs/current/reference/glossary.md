---
title: "术语表"
---

本术语表统一英文标识、中文术语与项目语义。配置键、Rust 类型及协议字段在两种语言中均保留精确拼写。同一词语可能涉及不同的进度或所有权边界；应使用当前操作所属的定义。

## 术语

| 英文 | 中文 | 本项目中的含义 |
| --- | --- | --- |
| Topic | 主题 | 具有名称的消息流，其路由包含多个队列；主题不是单一有序日志。 |
| MessageQueue | 消息队列 | 规范标识包含主题、Broker 名称和队列 ID；队列 ID 本身不具备全局唯一性。 |
| Producer / consumer | 生产者 / 消费者 | 发送方 / 接收方角色；一个应用可以拥有多个生命周期明确的客户端。 |
| Consumer group | 消费者组 | 订阅与消费的逻辑标识；集群消费模式下，同组消费者协调队列所有权。 |
| Rebalance | 再平衡 | 成员或路由变化后重新计算分配；可能涉及重复投递和在途工作的竞态条件。 |
| Tag / SQL filter | 标签 / SQL 过滤 | 订阅选择条件，不是授权策略；SQL 过滤具有 Broker 配置前提。 |
| Queue offset | 队列偏移量 | 队列内的逻辑位置，不是 CommitLog 字节地址。 |
| Physical offset | 物理偏移量 | 主消息日志中的字节位置；只能在正确的存储标识范围内比较。 |
| Consumer offset | 消费偏移量 | 记录的消费进度；本地推进、远端持久化与业务完成彼此不同。 |
| Message ID / business key | 消息 ID / 业务键 | 传输或存储标识与应用标识的区别；重试去重需要稳定的业务键。 |
| At-least-once | 至少一次 | 允许重复的投递语义；应用必须处理重复的业务操作。 |
| At-most-once | 至多一次 | 在指定范围内避免重复投递、但允许丢失的投递语义。 |
| Exactly-once | 精确一次 | 需要明确证据支持、范围特定的端到端性质；发送状态或偏移量提交本身不能证明它。 |
| Idempotent | 幂等 | 重复执行同一逻辑操作仍保持预期的业务效果。 |
| ACK / invisible time | 确认 / 不可见时间 | POP 使用回执确认投递；不可见时间到期后，未确认消息可能重新可见。 |
| Dead letter queue | 死信队列 | 正常重试路径耗尽后的消息去向；重新投递前应检查原因。 |
| Request correlation ID | 请求关联标识 | 关联一个待完成请求与其应答；不是持久化业务事务 ID。 |
| CommitLog / ConsumeQueue | 主消息日志 / 消费队列索引 | 主记录与派生队列查询状态的区别；重建派生索引不能恢复缺失的主记录。 |
| Appended watermark | 追加水位 | 已追加主日志字节的排他边界；这些字节不一定已经持久化。 |
| Durable watermark | 持久水位 | 已持久化字节的排他边界；只有记录结束位置不大于该边界时，记录才被覆盖。 |
| Durability | 持久性 | 指定故障模型下的存储持久化性质；本地与复制确认策略不同。 |
| Store cursor / epoch | 存储游标 / 纪元 | 受来源或引擎标识及代际约束的进度令牌；不是可跨系统迁移的偏移量。 |
| ISR / SyncStateSet | 同步副本集合 | 参与所配置 HA 确认策略的副本集合；成员关系与主节点权限相互独立。 |
| Master epoch / fencing | 主节点纪元 / 隔离旧主 | 权限代际及对过期权限的拒绝；普通 NameServer 路由不是 Controller 共识。 |
| Ownership / lifetime | 所有权 / 生命周期 | 由谁控制值或服务，以及访问在多长时间内有效；共享访问不代表共享关闭所有权。 |
| ServiceContext / TaskGroup | 服务上下文 / 任务组 | 具有作用域的资源和受所有权管理的任务生命周期；关闭时取消并等待所拥有的任务。 |
| Resource budget / backpressure | 资源预算 / 背压 | 有界准入与向调用方传播的压力；任务所有权和预算所有权属于不同的树。 |
| Cancellation / timeout | 取消 / 超时 | 停止等待或请求停止工作；两者都不证明已接受的远端或阻塞操作被撤销。 |
| Frame / protocol | 帧 / 协议 | 有界编码交换单元 / 其解释规则；TCP 数据包边界不定义消息帧。 |
| Authentication / authorization | 身份认证 / 授权 | 确认身份 / 决定允许的操作；Cargo feature 和工具发现都不能替代它们。 |
| Partial result / freshness | 部分结果 / 新鲜度 | 有界观察可能省略不可用来源或数据行；数据年龄和警告影响结果解释。 |
| Availability / consistency | 可用性 / 一致性 | 提供请求服务的能力 / 指定模型下观察状态的一致程度；两者都需要明确范围。 |
| Latency / throughput | 延迟 / 吞吐量 | 指定操作所需时间 / 单位时间内完成的工作；比较时保持负载与分位数定义一致。 |

## 连同单位和范围理解符号

- 队列位置 42 与 CommitLog 字节位置 42 不能互换。记录偏移量时，同时注明队列或存储标识及单位。
- 持久水位 4096 覆盖 4096 之前的字节。结束位置超过该边界的已追加记录尚未被该水位覆盖。
- 请求超时 3000 ms 表示调用方按该 API 的截止时间规则等待的时长，不能证明业务动作未执行。
- “已提交”必须有对象：客户端本地进度、远端消费偏移量持久化、事务决议、日志持久化和 Controller 状态机提交具有不同含义。

## 深入阅读

| 领域 | 说明 |
| --- | --- |
| 标识、偏移量与记录 | [消息模型](../architecture/message-model.md)、[存储](../architecture/storage.md) |
| 投递、重试与业务去重 | [投递与重试](../guides/delivery-and-retry.md)、[请求应答](../guides/request-reply.md) |
| POP 与消费进度 | [POP](../consumer/pop.md)、[LitePull](../consumer/pull-consumer.md) |
| 所有权与资源限制 | [运行时](../architecture/runtime.md)、[Rust API](./rust-api.md) |
| 权限与确认 | [HA 与 Controller](../architecture/ha-controller.md) |
| 身份与有界观察 | [安全](../architecture/security.md)、[只读 MCP](../ecosystem/mcp.md) |

新增术语时，说明其所属边界和容易混淆的概念，同时更新两种语言及相关概念页面。翻译过程中保持符号和可执行示例不变。
