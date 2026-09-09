---
title: "生产者生命周期与发送结果"
---

生产者通过 NameServer 发现可写队列，再向 Broker 发送消息。`DefaultMQProducer` 提供普通发送 API；事务和请求/响应用法还包含各自的状态与失败处理。需要完整应用及本地服务时，从[快速开始](../getting-started/quick-start.md)入手。

## 显式管理运行时所有权

当前客户端 API 要求由应用持有 `Arc<ClientRuntime>`。在 `RuntimeOwner` 下使用子服务上下文和遥测句柄创建它，再传入 `DefaultMQProducer::builder`。构造生产者不会启动它，也不会创建主题。

```text
RuntimeOwner
  child service context + telemetry handle
    Arc<ClientRuntime>
      DefaultMQProducer
        start → send operations → shutdown
    ClientRuntime shutdown
  RuntimeOwner shutdown
Telemetry shutdown
```

上图表示：应用创建运行时所有者、子服务上下文及遥测句柄，由客户端运行时支撑生产者；生产者完成启动、发送和关闭后，才关闭客户端运行时、运行时所有者和遥测。

同一应用中的兼容客户端 facade 可以共享客户端运行时。它持有客户端基础设施和后台工作，不是可以在进程退出时直接遗弃的全局单例。关闭前停止接收新发送请求，等待应用持有的在途操作，再先关闭 facade，最后关闭它们依赖的运行时。

[完整收发示例](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/src/main.rs)在成功和失败路径中均保留清理逻辑。其独立 [manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/Cargo.toml)给出了准确的路径依赖。

## 构建与启动

以下片段在应用持有的运行时内部执行，使用已有的 `client`。完整示例包含导入、错误处理和清理：

```rust
let mut producer = DefaultMQProducer::builder(client)
    .producer_group("docs_first_message_producer")
    .name_server_addr("127.0.0.1:9876")
    .build();
producer.start().await?;
```

生产者组标识生产者角色，不会创建消费者组，也不决定主题。启动前配置 NameServer，并创建具有可写队列的目标主题。教程配置已关闭主题自动创建。

## 构造并发送消息

使用 Model 消息 builder：

```rust
let message = Message::builder()
    .topic("DocsFirstMessage")
    .body("documentation message".to_owned())
    .build()?;
let result = producer.send_with_timeout(message, 3_000).await?;
```

超时单位是毫秒。该 API 返回 `ClientResult<Option<SendResult>>`，需要同时检查错误、结果是否存在以及 `send_status`。最外层 Rust 结果不能表达 Broker 的全部返回情况。

| 发送状态 | 含义 | 应用处理 |
| --- | --- | --- |
| `SendOk` | 所选发送路径按配置的存储/复制策略报告成功 | 保存诊断所需结果，按业务契约继续 |
| `FlushDiskTimeout` | 要求的磁盘刷盘等待未在规定时间内完成 | 持久化结果存在不确定性，直接重试可能产生重复 |
| `FlushSlaveTimeout` | 要求的副本等待未在规定时间内完成 | 诊断副本进度并应用业务重试策略 |
| `SlaveNotAvailable` | 所需副本不可用 | 重试前检查拓扑与可用性要求 |
| 错误或缺少结果 | 未获得可用的成功结果 | 保留操作上下文，不能据此断定消息未存储 |

规范的结果类型位于 `rocketmq-model::result`。发送成功不能证明消费者已经处理消息，也不会让数据库更新与发送组成原子操作。

## 明确选择发送操作

[发送消息](sending-messages.md)说明发送变体与队列选择。需要响应的发送可以返回 Broker 结果；单向发送则主动放弃该响应。事务消息需要本地事务决策与检查行为，详见[事务消息](transaction-messages.md)。

在第一次发送前定义业务事件标识，在重试中保持该标识，并使消费者的副作用具备幂等性。客户端重试次数与超时应受应用总截止时间约束；在客户端重试外再叠加无限应用重试，会放大集群故障。

## 关闭与诊断

发送结束后始终调用 `producer.shutdown().await`，包括启动或后续操作失败的路径。随后关闭共享客户端运行时并检查报告，再关闭运行时所有者与遥测。不要仅为了在异步代码中调用关闭操作而创建另一个 Tokio 运行时。

启动成功但发送失败时，先检查主题路由、可写队列数和 Broker 公布地址，再调整超时。参见[首次诊断](../operations/first-diagnosis.md)和[投递与重试](../guides/delivery-and-retry.md)。

来源：[生产者 facade](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/producer/default_mq_producer.rs)、[发送结果](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/result.rs)、[消息 builder](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/message_builder.rs)。
