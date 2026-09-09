---
title: "LitePull：轮询、处理与提交"
---

LitePull 让应用显式编写轮询循环，由客户端管理路由发现、拉取和订阅队列分配。本页保留已有 `pull-consumer` URL，介绍 `DefaultLitePullConsumer`；Classic Pull 兼容接口是另一套 API。

通过[快速开始](../getting-started/quick-start.md)运行完整应用。以下片段使用相同的 `DocsFirstMessage` 主题和 `docs_first_message_consumer` 消费者组。

## 使用应用运行时构造消费者

在应用持有的运行时内部，使用名为 `client` 的现有 `Arc<ClientRuntime>`：

```rust
let consumer = DefaultLitePullConsumer::builder(client)
    .consumer_group("docs_first_message_consumer")
    .name_server_addr("127.0.0.1:9876")
    .consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset)
    .auto_commit(false)
    .poll_timeout_millis(1_000)
    .build()?;
consumer.subscribe("DocsFirstMessage").await?;
consumer.start().await?;
```

使用客户端公开导出和 Model 的 `ConsumeFromWhere` 类型，具体导入见[完整源码](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/src/main.rs)。LitePull builder 可能返回错误。启动和订阅失败时，也应清理应用持有的运行时。

`subscribe(topic)` 选择全部消息。需要表达式时，使用 `subscribe_with_expression(topic, expression)` 或 `subscribe_with_selector`。所选过滤方式必须受 Broker 支持，同组订阅应保持一致。

订阅模式由客户端分配队列。显式 assignment 是另一条控制路径，不能把 assignment 和 subscription 当作彼此独立的过滤条件混用。

## 轮询并完成批次

```rust
let messages = consumer.poll_with_timeout(1_000).await;
for message in &messages {
    println!("RECEIVED id={}", message.msg_id());
}
if !messages.is_empty() {
    consumer.commit_all().await?;
}
```

普通轮询返回消息值组成的向量。空向量可能表示没有可消费数据、超时，或存在需要诊断的客户端状态；它不是结构化的网络错误报告。生产循环还应观察客户端诊断与队列分配，不能把每次空轮询都解释为主题为空。

示例将打印消息 ID 视为工作完成。实际应用中应替换为成功的业务处理，失败时不能继续执行无条件批次提交。并发处理时，按队列跟踪完成情况，只推进连续完成的前缀；后面的消息成功，不能成为跳过前面失败消息的理由。

`poll_with_timeout_zero_copy` 返回应用持有的 `Arc<MessageExt>`。保留这些值会延长消息数据的存活时间。无论选择哪种轮询变体，都需要限制应用批次和下游并发。

## 提交的实际含义

当前实现中，`commit_all` 将已分配队列的偏移量更新到客户端偏移量存储，**不会**立即执行远端持久化。部分队列级失败在实现内部记录日志，而不会作为外层失败返回。因此，返回 `Ok(())` 不能证明每个队列均已推进，也不能证明 Broker 已持久保存该位置。

周期持久化和正常关闭路径会单独执行偏移量存储工作。当前 `commit_sync` facade 也委托给 commit 路径，其名称不承诺同步、持久的 Broker 确认。map/set 操作提供显式持久化选择，但将其作为正确性边界前，需要理解返回值和日志行为。

应区分以下位置：

| 位置 | 含义 |
| --- | --- |
| 已拉取/可用数据 | 客户端后台工作已取得的数据 |
| 应用处理完成 | 业务代码实际完成的工作 |
| 客户端已提交偏移量 | 已交给客户端偏移量存储的位置 |
| 已持久化组偏移量 | 可通过相应偏移量持久化路径取得的进度 |

偏移量表示单个队列内部的进度。崩溃后，应用完成情况与持久化进度可能不同。应通过业务幂等容忍重放，不能把偏移量提交描述为外部事务。

## 起始位置与重复运行

`ConsumeFromFirstOffset` 是针对没有适用已存储进度的组的起始位置策略，不会回退已有组。教程中正常重复运行时，保持组不变并发送新消息。

手动重置偏移量会改变可重放或跳过的数据，并影响组内成员。这是具有业务后果的维护操作，不是空轮询的常规修复方式。应先检查路由、分配、订阅表达式和当前进度。

## 保留生命周期所有权完成关闭

示例等待五条消息、60 秒截止时间或 Ctrl+C。每条路径都会执行 `consumer.shutdown().await`，随后关闭共享客户端运行时、运行时所有者和遥测。应用工作任务也需要所有者；关闭消费者，不能替应用完成已经在其他执行器上脱离管理的业务任务。

重复与失败窗口见[投递与重试](../guides/delivery-and-retry.md)，轮询未得到预期数据时参见[首次诊断](../operations/first-diagnosis.md)。

来源：[LitePull 公开 facade](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_lite_pull_consumer.rs)、[提交与关闭实现](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/default_lite_pull_consumer_impl.rs)、[远端偏移量存储](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/store/remote_broker_offset_store.rs)。
