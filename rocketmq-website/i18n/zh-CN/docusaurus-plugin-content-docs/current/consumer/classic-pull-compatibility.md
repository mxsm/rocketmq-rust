---
title: "Classic Pull 兼容接口"
---

# Classic Pull 兼容接口

`DefaultMQPullConsumer` 已不推荐用于新开发，但仍具有可运行的运行时兼容路径。新轮询应用应使用 [LitePull](./pull-consumer.md)。迁移需要为每次请求显式选择队列和偏移量的旧代码时，可以保留 Classic Pull。

## 可运行构造与 detached 构造

| 构造方式 | 运行行为 |
| --- | --- |
| `DefaultMQPullConsumer::builder(client_runtime).consumer_group(...).build()?` | 使用给定共享客户端运行时构建可运行 facade |
| `new()`、`default()`、`with_consumer_group(...)` | 保留 detached 兼容值，运行操作返回初始化错误 |
| detached 实现标记 | 不会创建后备运行时 |

仅修改 detached 值的组名不会接入运行时，应在应用边界替换构造方式。builder 使用 Classic 手动模式下的 LitePull 基础设施，关闭自动提交；它不会把显式拉取转为普通 LitePull 后台轮询。

builder 要求组名并校验时长。默认普通拉取超时为 10 秒，Broker 挂起时间为 20 秒，客户端挂起请求超时为 30 秒。长轮询的客户端超时必须大于 Broker 挂起时间。

## 对显式分配的队列拉取一次

以下兼容片段包含完整 facade 清理。调用方提供自己持有的队列及拟读取的下一偏移量，并保持共享运行时存活。函数将消息返回调用方，不推进业务进度。

```rust
use std::sync::Arc;
use rocketmq_client_rust::{
    ClientResult, ClientRuntime, DefaultMQPullConsumer,
    MessageSelector, PullOptions, PullResult,
};
use rocketmq_model::common::message::message_queue::MessageQueue;

#[allow(deprecated)]
async fn pull_once(
    runtime: Arc<ClientRuntime>,
    queue: MessageQueue,
    next_offset: i64,
) -> ClientResult<PullResult> {
    let consumer = DefaultMQPullConsumer::builder(runtime)
        .consumer_group("docs_classic_group")
        .name_server_addr("127.0.0.1:9876")
        .build()?;
    let result = async {
        consumer.start().await?;
        let options = PullOptions::new(
            queue, MessageSelector::by_tag("*"), next_offset, 16,
        )?;
        consumer.pull_with_options(options).await
    }.await;
    let shutdown = consumer.shutdown().await;
    let result = result?;
    shutdown?;
    Ok(result)
}
```

长期应用应复用已启动 facade，在循环结束后关闭；每次拉取创建消费者不是吞吐场景的推荐模式。此处拉取出错仍执行关闭。示例不表示自动获得输入队列的排他所有权。

## 移动游标前解释结果

| `PullStatus` | 含义与后续操作 |
| --- | --- |
| `Found` | 处理返回消息，目标批次成功后再使用 `next_begin_offset` |
| `NoNewMsg` | 本次请求没有新的可用数据，采用有界等待或长轮询 |
| `NoMatchedMsg` | 已扫描数据但未匹配，返回的下一偏移量可以越过已扫描的不匹配位置 |
| `OffsetIllegal` | 请求位置不在合法范围，应检查保留策略、重置以及返回的 min/max/next 位置 |

不要用收到的消息数量直接累加偏移量，过滤与空洞可能使结果错误。也不能把 `OffsetIllegal` 当作可以静默丢弃业务恢复范围的许可。

`PullOptions` 校验队列主题和 Broker 名称、非负偏移量、正消息数和响应大小限制，以及合法超时。在普通默认值上开启 block-if-not-found 时，还需要将客户端超时提高到挂起时间以上。facade 专用的 block-if-not-found 方法使用 builder 中对应设置。

## 队列分配与进度所有权

`fetch_subscribe_message_queues` 返回已知队列，不表示当前进程独占全部队列。注册的 `MessageQueueListener` 回调区分全部队列和分配给当前消费者的队列。拉取前应使用该分配结果，或明确的外部所有权策略。

facade 提供 `update_consume_offset`、`fetch_consume_offset`、`min_offset`、`max_offset` 和 `search_offset`。偏移量更新与远端持久化应按底层偏移量存储路径理解。拉取结果本身不会提交业务进度。

完成业务后再推进下一读取位置，保留用于重放的持久业务标识，停止拉取已撤销的队列。包装层状态机拒绝重复启动、启动失败后重启及关闭后重启；重新启动时应创建新 facade。关闭本身具有幂等性。

## 有意识地迁移到 LitePull

| Classic 职责 | LitePull 选择 |
| --- | --- |
| 每次请求显式指定队列与偏移量 | 选择订阅分配或显式 `assign`，仅在有意改变位置时使用 `seek` |
| 手动拉取循环 | 替换为有界 `poll` 或零拷贝轮询与处理 |
| 每请求选择器 | 轮询前配置等价主题订阅或选择器 |
| 应用持有进度 | 明确设置自动提交，保留先处理后推进进度的顺序 |
| 队列所有权回调 | 在目标模式中保留分配与撤销处理 |

没有交接计划时，不要让新旧消费者同时操作同组进度。记录最后完成的业务位置，停止旧所有者，启动新模式，再检查重复、缺口和组进度。仅替换 builder 不构成偏移量迁移。

当前 LitePull `commit_all` 更新客户端偏移量存储状态，并可能在内部记录逐队列错误；它不是立即持久化全部队列的提交。迁移时应保留这一认知。

源码依据：[Classic facade](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_pull_consumer.rs)、[builder](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_pull_consumer_builder.rs)、[生命周期与分配适配](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/default_mq_pull_consumer_impl.rs)、[拉取结果](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/pull_result.rs)。
