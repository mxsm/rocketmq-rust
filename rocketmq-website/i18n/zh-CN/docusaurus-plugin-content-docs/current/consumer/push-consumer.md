---
title: "Push 消费者"
---

# Push 消费者

`DefaultMQPushConsumer` 将消息交给已注册的监听器。在普通 Pull 模式中，客户端完成路由发现、队列分配、后台拉取和长轮询，然后调度回调。“Push”描述应用接口，不表示 Broker 无条件主动推送流。

需要应用手动轮询时，使用 [LitePull](./pull-consumer.md)。需要 Broker 管理不可见时间和回执确认时，参见 [POP](./pop.md)；其监听器成功返回后的确认路径不同。

## 启动具有完整生命周期的消费者

按照[快速开始](../getting-started/quick-start.md)创建 `DocsFirstMessage`，再将组创建命令的参数替换为 `-g docs_push_group`，为本页消费者创建独立组。以下函数使用教程中的共享 `Arc<ClientRuntime>`。监听器仅把统计消息数作为处理演示；实际应用应在返回成功前完成有界、幂等的业务处理。

```rust
use std::sync::Arc;
use rocketmq_client_rust::{
    ClientResult, ClientRuntime, ConsumeConcurrentlyContext,
    ConsumeConcurrentlyStatus, DefaultMQPushConsumer,
    MessageListenerConcurrently, MQPushConsumer,
};
use rocketmq_model::common::message::message_ext::MessageExt;

struct CountListener;

impl MessageListenerConcurrently for CountListener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> ClientResult<ConsumeConcurrentlyStatus> {
        println!("received={}", messages.len());
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

async fn consume_until_interrupt(
    client_runtime: Arc<ClientRuntime>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut consumer = DefaultMQPushConsumer::builder(client_runtime)
        .consumer_group("docs_push_group")
        .name_server_addr("127.0.0.1:9876")
        .consume_message_batch_max_size(1)
        .build();
    let outcome = async {
        consumer.subscribe("DocsFirstMessage", "*").await?;
        consumer.register_message_listener_concurrently(CountListener);
        consumer.start().await?;
        tokio::signal::ctrl_c().await?;
        Ok(())
    }.await;
    consumer.shutdown().await;
    outcome
}
```

在持有它的 RuntimeOwner 上调用该函数，之后关闭共享 ClientRuntime、RuntimeOwner 和遥测设施。启动前注册监听器和订阅。学习默认新组行为时，先启动消费者再发送；已有存储偏移量可能覆盖初始位置设置。

## 选择并发方式与消费模型

| 选择 | 含义 |
| --- | --- |
| 并发监听器 | 不同批次可以并行执行，完成顺序可能不同于队列顺序 |
| 顺序监听器 | 在对应队列所有权和锁边界内串行消费 |
| 集群消费 Clustering | 组成员分担工作，重试遵循该组配置路径 |
| 广播消费 Broadcasting | 每个实例接收独立副本；普通并发实现对失败投递记录日志并丢弃，不使用集群式发回重试 |

监听器方法是同步的。客户端通过受管理的阻塞边界调度，但下游调用仍需要自己的有限超时和容量。把工作交给无人持有的后台任务后立即返回成功，可能使进度先于业务提交推进。

同一逻辑组内，不应混用并发与顺序监听器契约，也不应使用不一致订阅。组成员应在主题、选择器、消费模型和顺序要求上保持一致。另一个独立业务应用通常应使用自己的消费者组。

## 从监听器结果理解进度

`ConsumeSuccess` 表示成功处理的前缀，`ReconsumeLater` 表示批次未成功。默认并发上下文在成功时确认全部消息。使用部分确认时，其含义是前缀索引，不是批次中的任意子集。

普通集群并发消费中，失败消息进入发回路径。发回失败的消息继续待处理，等待后续本地尝试。完成或成功移交的消息可从处理队列移除，使下一个安全偏移量推进；偏移量持久化是另一步。

因此，业务完成、监听器成功、进度更新和消费者组进度持久化是不同事件。应用应容忍这些步骤之间崩溃造成的重放。广播模式的失败行为不同，需要显式应用恢复策略。

顺序消费使用 `MessageListenerOrderly` 和 `ConsumeOrderlyStatus`。当前队列失败时可以暂停并重试，以队列进度为代价保护局部顺序。发送侧映射和故障边界参见[顺序消息](../guides/ordered-messages.md)。

## 再平衡与内存控制

路由或组成员变化可能撤销并重新分配队列。队列所有权是临时的；应停止属于已撤销所有权的工作，并使业务幂等在转移到其他进程后仍然有效。

| 配置类别 | 限制或影响的内容 |
| --- | --- |
| `pull_batch_size` | 网络请求批次大小 |
| `consume_message_batch_max_size` | 单次监听器调用的消息数量 |
| `consume_thread_min` / `consume_thread_max` | 受管理的消费并发控制，不代表允许无限阻塞 |
| `pull_threshold_for_queue` / `pull_threshold_size_for_queue` | 队列缓存数量与大小压力 |
| 主题阈值及 `pull_interval` | 主题级压力与拉取节奏 |
| `consume_from_where` | 没有可用存储进度决定起点时的初始位置 |

网络批次与回调批次不同。更大的缓存可能保留消息体并延长关闭；增加线程不能修复数据库饱和。根据真实处理成本选择限制，同时观察积压、待处理数量、保留字节和重试率。

暂停会按消费者路径暂停接入，但它不是证明全部回调完成的事务屏障。退出服务生命周期时应显式关闭。

## 排查运行中的消费者

| 观察结果 | 后续检查 |
| --- | --- |
| 没有回调 | 主题路由、组配置、监听器注册、起始偏移量、选择器 |
| 增加实例但吞吐提升有限 | 队列数量、分配及下游瓶颈 |
| 重复投递 | 监听器失败、发回或 ACK 失败、再平衡和持久化进度 |
| 回调成功但积压增加 | 处理延迟、队列分配、持久化，以及指标实际对应的集群和组 |
| 广播失败消息消失 | 广播分支不提供集群式重试语义 |

在 `rocketmq-example/` 中，`consumer-cluster` 展示并发集群消费，`consumer-orderly` 展示顺序接口。创建资源和运行前，应查看各自的主题、组常量。SQL 与 Tag 示例也有独立主题。集群侧检查命令参见[首次诊断](../operations/first-diagnosis.md)。

源码依据：[Push facade 与配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_push_consumer.rs)、[Push 生命周期](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/default_mq_push_consumer_impl.rs)、[并发处理](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/consume_message_concurrently_service.rs)、[顺序处理](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/consume_message_orderly_service.rs)。
