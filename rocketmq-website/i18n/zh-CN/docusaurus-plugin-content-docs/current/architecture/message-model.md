---
title: "消息模型与偏移量语义"
---

RocketMQ Rust 将应用创建的消息与路由、存储、投递阶段补充的元数据分开。这使生产者、消费者、协议适配器和存储后端能够共享领域类型，而不让模型 crate 承担套接字或后台任务的所有权。

建议先阅读[基本概念](../getting-started/basic-concepts.md)。本章解释当前源码中的模型，并不是用 Rust 结构体描述 CommitLog 的持久化格式。

## 领域对象与职责

| 对象 | 职责 | 不能由此得出的结论 |
| --- | --- | --- |
| `Message` | 生产者提供的主题、消息体、标志和属性 | Broker 已接纳、已分配队列或已持久化 |
| `MessageBody` 与 `MessageProperties` | 消息使用的消息体表示和属性操作 | 业务结构有效或调用方拥有权限 |
| `MessageExt` | 消息及投递、存储元数据，包括队列和物理偏移量、时间戳、主机及重消费次数 | 业务处理成功 |
| `MessageEnvelope` | 将消息、路由和存储信息分离后组合的表示 | 使用了另一种线协议，或所有现有 API 都已被它替代 |
| `MessageQueue` | 主题、Broker 名称、队列 ID 三元组 | 套接字地址、消费者分配结果或偏移量 |

队列标识的规范类型位于 `rocketmq_model::message::MessageQueue`。`common::message::message_queue` 下的兼容路径重新导出同一个类型。相等比较和哈希包含全部三个字段：Broker A 的队列 0 与 Broker B 的队列 0 是不同队列。Broker 地址可以改变，而逻辑 Broker 名称仍是队列标识的一部分。

公开消息 API 提供构造器和访问方法；应用文档不应自行拼出类似 `struct Message { topic: String, body: Vec<u8> }` 的近似定义。当前实现使用专用消息体、属性类型和紧凑字符串。成功构造模型值也不能替代生产者、Broker 或存储层的校验。

## 消息体与属性

消息体是应用数据。RocketMQ 不会根据其字节推断业务结构或去重策略。需要兼容演进或幂等业务效果时，应用应选择明确的结构版本和稳定业务键。

Tag、Key、重试元数据、事务标记和定时元数据都可能通过消息属性传递，但其管理方不同。用户属性用于过滤及业务元数据；系统保留属性会影响处理流程，应通过对应 API 设置。`TagA || TagB` 这样的 Tag 表达式属于订阅选择器，不是单条消息的 Tag 值。

SQL 过滤针对消息属性求值；消息体中的 JSON 字段不会自动变成 SQL 属性，详见[消息过滤](../consumer/message-filtering.md)。Key 用于查询和关联；两次发送使用相同 Key，并不会让 Broker 自动去重。

共享字节缓冲区和 `Arc<MessageExt>` 可以减少复制，但引用存活期间也会保留内存。零拷贝轮询 API 改变的是所有权和分配行为，不会免除消费者完成处理、记录进度的责任。

## 必须区分的四类位置

| 位置 | 范围与单位 | 典型用途 |
| --- | --- | --- |
| 队列偏移量 | 一个主题/Broker/队列中的逻辑消息位置 | 拉取请求和消费进度 |
| CommitLog 偏移量 | 主日志中的物理字节位置 | 存储查找、恢复和复制 |
| 持久化水位 | 在相应契约下已持久化的物理排他边界 | 判断某次追加范围是否被覆盖 |
| 派生游标 | 特定引擎、源 epoch 对主日志排他边界的处理进度 | 重建或推进 ConsumeQueue、索引 |

消费者的下一个偏移量不是 CommitLog 字节地址。应结合拉取结果和处理策略使用它，不能按消息体大小累加，也不能简单按返回消息数推算。过滤以及无效偏移量修正，都可能在未返回同等数量消息的情况下推进结果位置。

例如，一条记录可以占用物理字节 `[4096, 4224)`，同时对应逻辑队列偏移量 `17`。持久化水位 `4224` 覆盖了该追加范围；下一个逻辑队列位置则按消费协议解释。这两个数字都不是跨 Broker 的全局消息序号。

消息 ID、业务 Key 和 POP receipt handle 也具有不同职责。receipt 标识一次投递及其确认上下文，不是永久消息标识。续期返回新 receipt 后，不应继续使用旧值。

## 编码与校验分别发生在哪里

`rocketmq-model` 提供不依赖运行时的值类型及模型契约错误。`rocketmq-protocol` 负责 Remoting 请求头、命令体、线协议编码器和消息编解码兼容性。`rocketmq-store` 及其实现 crate 负责持久化分帧、恢复和持久性。因此，修改 Rust 字段、Serde 名称、协议编码、存储记录，需要分别分析兼容性。

模型有效的消息仍可能因主题不存在、属性组合不受支持、调用方无权限或存储不可用而失败。模型校验错误与运行错误是不同层次的问题。同样，设置事务状态属性不会使消息与应用数据库形成原子事务；[事务协议](../producer/transaction-messages.md) 需要持久化业务决策和事务回查。

## 阅读实现

- [模型导出与边界](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/README.md)、[队列标识](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/message.rs)。
- [生产者消息](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/message_single.rs)、[扩展消息](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/message_ext.rs)、[消息封装](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/message_envelope.rs)。
- 继续阅读[存储契约](storage.md)、[协议与传输](protocol-transport.md)及[投递与重试](../guides/delivery-and-retry.md)。
