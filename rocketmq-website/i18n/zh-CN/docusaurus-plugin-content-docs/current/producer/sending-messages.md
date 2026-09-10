---
title: "发送消息"
---

# 发送消息

根据应用如何观察完成结果、如何选择目标队列来选择发送方法。所有方法均使用应用持有的 `ClientRuntime` 和已启动的生产者。完整运行时、Broker、主题及关闭设置参见[快速开始](../getting-started/quick-start.md)。

## 选择结果契约

这里的“同步发送”表示等待 Broker 响应；Rust 调用仍是需要 await 的异步 Future。

| API 类型 | 结果通道 | 适用场景 |
| --- | --- | --- |
| `send`、`send_with_timeout` | `ClientResult<Option<SendResult>>` | 观察发送响应后再继续 |
| `send_with_callback`、`send_with_callback_timeout` | 方法直接返回值，以及回调结果或错误 | 保持有界在途工作并关联完成结果 |
| `send_oneway` | `ClientResult<()>`，不包含 Broker 响应 | 应用明确不要求远端确认的场景 |
| `send_batch` 及超时变体 | `ClientResult<SendResult>` | 显式发送符合约束的批次 |
| `send_to_queue` 及队列变体 | 相同完成方式，目标队列明确 | 保持已知目标 |
| `send_with_selector` 及选择器变体 | 相同完成方式，由应用在可用队列中选择 | 用稳定业务键路由相关事件 |

仅有 `Ok` 包装不足以统计一次已确认发送成功。应检查 `SendResult.send_status`，在允许可选返回值的 API 中显式处理 `None`。直接超时发送路径预期获得响应；响应缺失不等于 Broker 成功确认。

`SendStatus::SendOk`、`FlushDiskTimeout`、`FlushSlaveTimeout` 和 `SlaveNotAvailable` 含义不同。后三种状态可能出现在追加已被接纳之后。状态名沿用协议术语；对应持久性与副本条件参见[存储设计](../architecture/storage.md)。即使返回 `SendOk`，也不表示消费者已完成业务处理。

## 发送一条消息并检查状态

以下完整函数接收快速开始应用创建的共享运行时。在该运行时上调用它，返回后关闭共享 ClientRuntime 和 RuntimeOwner。调用前创建 `DocsFirstMessage`。

```rust
use std::{io, sync::Arc};
use rocketmq_client_rust::{ClientRuntime, DefaultMQProducer, SendResult, SendStatus};
use rocketmq_model::common::message::message_single::Message;

async fn send_one(
    client_runtime: Arc<ClientRuntime>,
) -> Result<SendResult, Box<dyn std::error::Error>> {
    let mut producer = DefaultMQProducer::builder(client_runtime)
        .producer_group("docs_sending_group")
        .name_server_addr("127.0.0.1:9876")
        .build();
    let outcome: Result<SendResult, Box<dyn std::error::Error>> = async {
        producer.start().await?;
        let message = Message::builder()
            .topic("DocsFirstMessage")
            .key("order-1001:event-1")
            .tags("created")
            .body("order created")
            .build()?;
        let result = producer.send_with_timeout(message, 3_000).await?
            .ok_or_else(|| io::Error::other("send response is absent"))?;
        if result.send_status != SendStatus::SendOk {
            return Err(io::Error::other(format!(
                "send policy not satisfied: {}", result.send_status
            )).into());
        }
        Ok(result)
    }.await;
    producer.shutdown().await;
    outcome
}
```

消息键用于业务关联；设置键不会使 Broker 自动去重。如果业务事件标识必须跨重试或进程重启保留，应独立持久化。上述函数在操作出错后仍会执行清理。

## 使用回调时完整处理失败

回调接收 `Option<&SendResult>` 和 `Option<&ClientError>`。这些引用只属于当前回调调用；后续处理需要的数据应有界复制。避免在完成记录中保留完整请求或消息体。

两个结果通道都要检查：方法可能在提交前失败，完成结果也可能通过回调报告失败。当前 `send_with_callback` facade 会将部分底层提交错误交给回调，然后返回 `Ok(())`。因此，只统计方法返回成功会高估实际发送成功数。

在应用持有的完成记录得到结果，或显式截止时间到达前，应保持生产者和运行时存活。限制在途数量、字节数以及回调工作。回调不应阻塞运行时工作线程，也不应启动无人持有的后台任务。关闭流程不能代替对每次业务操作结果的关联。

单向发送没有可检查的发送结果回调或 Broker 确认。本地提交成功不能证明远端持久化。当后续业务步骤要求确认 Broker 已接纳事件时，不应使用单向发送。

## 发送合法批次

显式批次不能为空，必须使用同一主题和一致的 `waitStoreMsgOK` 值，不得包含重试主题消息或延迟、定时消息。当前 `MessageBatch` 校验器检查延迟级别、相对毫秒或秒延迟、绝对投递时间属性。不要将事务语义与普通批量路径组合。

以下片段在生产者启动后执行：

```rust
let messages = vec![
    Message::builder().topic("DocsFirstMessage")
        .body("batch event 1").build()?,
    Message::builder().topic("DocsFirstMessage")
        .body("batch event 2").build()?,
];
let result = producer.send_batch_with_timeout(messages, 3_000).await?;
```

与单条发送一样，应检查批次的 `send_status`。批量发送不是跨消费者数据库的事务。编码后的总大小、属性和帧开销必须满足当前客户端与 Broker 限制；仅限制消息条数无法保证这一点。提交前拆为有界批次，并为每个业务事件保留独立重放标识。

自动累积批量与显式 `send_batch` 是不同选项。启用累积器后，当前 `send` 以及部分回调和队列 facade 可以经过累积器，而显式 `send_with_timeout` 直接调用带超时的发送实现。不能假定所有重载具有相同缓冲行为。

## 明确选队列与重试策略

从主题路由获取可发布队列，不要虚构 Broker 名称或队列 ID。选择器接收候选队列、消息和参数，返回可选队列。应处理候选集合为空的情况；对零个队列执行取模无效。

相关事件需要稳定映射，要求顺序时还应串行发送。路由或队列数量变化可能改变简单取模映射。详见[顺序消息](../guides/ordered-messages.md)。

超时限制客户端等待，不会取消远端副作用。区分接纳前拒绝与发送后结果不确定，采用有界重试截止时间，并考虑所选发送路径已有的内部重试。不要悄悄把失败业务事件改投其他主题，这会改变订阅、顺序和恢复语义。

## 示例目标与观察结果

在 `rocketmq-example/` 使用其独立清单，提前创建各示例的实际主题：

| 目标 | 主题 | 观察重点 |
| --- | --- | --- |
| `producer-basic-send` | `BasicSendTestTopic` | 响应、回调和单向发送的区别 |
| `producer-batch-send` | `BatchSendTestTopic` | 批次状态与所选队列 |
| `producer-send-to-queue` | 阅读其常量 | 显式路由选择 |
| `producer-send-with-selector` | 阅读其常量 | 选择器参数与队列映射 |

```bash
cargo check --example producer-basic-send --example producer-batch-send
cargo run --example producer-basic-send
```

这些示例使用回环 NameServer 常量，没有统一的命令行地址选项。它们演示 API，其中的调试输出不是生产日志规范。已验证的首条消息流程和完整清理见网站配套教程应用。编译成功不能证明回调丢失、故障转移或崩溃恢复行为。

源码依据：[生产者 facade](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/producer/default_mq_producer.rs)、[发送实现](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/producer/producer_impl/default_mq_producer_impl/send.rs)、[批次校验](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/message_batch.rs)、[统一发送结果](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/result.rs)。
