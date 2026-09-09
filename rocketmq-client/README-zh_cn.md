# RocketMQ Rust Client

[English](README.md) | [简体中文](README-zh_cn.md)

`rocketmq-client-rust` 提供生产者、事务生产者、Push Consumer、Lite Pull Consumer
和可选 Admin API。消息领域类型位于 [rocketmq-model](../rocketmq-model)，协议类型位于
[rocketmq-protocol](../rocketmq-protocol)，网络通信由 [rocketmq-transport](../rocketmq-transport) 实现。

## 依赖与运行时所有权

以下配置适用于仓库中与这些 crate 同级的应用；外部项目需要调整路径：

```toml
[dependencies]
rocketmq-client-rust = { path = "../rocketmq-client", default-features = false }
rocketmq-model = { path = "../rocketmq-model" }
rocketmq-runtime = { path = "../rocketmq-runtime" }
rocketmq-observability = { path = "../rocketmq-observability" }
tokio = { version = "1", features = ["signal"] }
```

应用创建 `RuntimeOwner`，将 `ChildServiceContext` 和 `TelemetryHandle` 注入
`ClientRuntime::try_new`。生产者、消费者和 Admin 会话共享 `Arc<ClientRuntime>`；
客户端不会自动创建回退运行时。先关闭各个客户端门面，再关闭共享 ClientRuntime，
最后关闭 RuntimeOwner。关闭报告需要由应用检查。

公开类型从 crate 根或 `prelude` 导入；内部模块不是受支持的导入路径。

## 生产者示例

准备运行中的 NameServer、Broker 和 `TopicTest`，然后运行：

```rust,no_run
use rocketmq_client_rust::{ClientResult, ClientRuntime, ClientRuntimeConfig, DefaultMQProducer};
use rocketmq_model::common::message::message_single::Message;
use rocketmq_observability::TelemetryRuntimeGuard;
use rocketmq_runtime::RuntimeOwner;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let owner = RuntimeOwner::new()?;
    let telemetry = TelemetryRuntimeGuard::noop();
    let client_runtime = ClientRuntime::try_new(
        owner.root_context().component("example-client"),
        ClientRuntimeConfig::default(),
        telemetry.handle(),
    )?;

    let (result, client_report) = owner.block_on(async {
        let mut producer = DefaultMQProducer::builder(client_runtime.clone())
            .producer_group("example_producer_group")
            .name_server_addr("127.0.0.1:9876")
            .build();
        let result: ClientResult<()> = async {
            producer.start().await?;
            let message = Message::new("TopicTest", b"Hello RocketMQ");
            let result = producer.send_with_timeout(message, 2000).await?;
            println!("send result: {result:?}");
            Ok(())
        }.await;
        producer.shutdown().await;
        (result, client_runtime.shutdown().await)
    });
    let runtime_report = owner.shutdown_runtime_blocking()?;
    result?;
    if !client_report.is_healthy() || !runtime_report.is_healthy() {
        return Err(std::io::Error::other("client shutdown did not complete cleanly").into());
    }
    Ok(())
}
```

## Push Consumer 示例

下面的完整异步函数接收应用拥有的客户端运行时。调用方应在所属 RuntimeOwner 上执行它，
并在函数返回后关闭共享 ClientRuntime 和 RuntimeOwner。

```rust,no_run
use std::sync::Arc;
use rocketmq_client_rust::{
    ClientResult, ClientRuntime, ConsumeConcurrentlyContext, ConsumeConcurrentlyStatus,
    DefaultMQPushConsumer, MessageListenerConcurrently, MQPushConsumer,
};
use rocketmq_model::common::message::message_ext::MessageExt;

struct Listener;

impl MessageListenerConcurrently for Listener {
    fn consume_message(
        &self,
        messages: &[&MessageExt],
        _context: &ConsumeConcurrentlyContext,
    ) -> ClientResult<ConsumeConcurrentlyStatus> {
        println!("received {} messages", messages.len());
        Ok(ConsumeConcurrentlyStatus::ConsumeSuccess)
    }
}

async fn consume_until_interrupt(
    client_runtime: Arc<ClientRuntime>,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut consumer = DefaultMQPushConsumer::builder(client_runtime)
        .consumer_group("example_consumer_group")
        .name_server_addr("127.0.0.1:9876")
        .build();
    let result = async {
        consumer.subscribe("TopicTest", "*").await?;
        consumer.register_message_listener_concurrently(Listener);
        consumer.start().await?;
        tokio::signal::ctrl_c().await?;
        Ok(())
    }.await;
    consumer.shutdown().await;
    result
}
```

## 消费 API 与消息所有权

- Push Consumer 支持并发或顺序监听器，以及 Tag 和 SQL92 订阅；Broker 必须支持所选过滤能力。
- Lite Pull Consumer 提供轮询和位点管理。零拷贝轮询返回拥有所有权的
  `Arc<MessageExt>`，可以保留到下一次轮询之后；消息存活期间底层数据仍被引用。
  返回独立消息值的普通轮询路径会克隆消息。
- 事务生产者、请求应答和队列选择的可运行示例位于 [rocketmq-example](../rocketmq-example)。

## Cargo features

| Feature | 行为 |
| --- | --- |
| `admin-full` | 包的默认 feature，组合 Admin 查询和修改能力。根工作区依赖会禁用默认 features，具体消费者显式选择。 |
| `admin-read` / `admin-mutation` | 分别启用查询或修改 API；编译可用性不代表运行时权限。 |
| `observability` | 启用客户端跟踪集成。 |
| `observability-metrics` | 启用客户端指标集成。 |
| `otlp-traces` | 启用 OTLP 跟踪导出支持，仍需运行时配置。 |
| `nameserver-dns-discovery` | 启用可选 DNS 发现支持。 |
| `test-support` | 提供测试和基准所需的辅助 API。 |

本 crate 没有名为 `tls` 的 feature。TLS 由传输 crate 的编译 feature 和连接配置共同控制；
需要时应用必须确保依赖图启用了 `rocketmq-transport/tls`。默认客户端传输依赖启用 SOCKS，
不会单独启用 TLS。

## 验证

从仓库根目录按修改范围选择：

```bash
cargo fmt -p rocketmq-client-rust -- --check
cargo check -p rocketmq-client-rust
cargo test -p rocketmq-client-rust <test_name>
cargo bench -p rocketmq-client-rust --bench client_hot_path_benchmark --features test-support
```

网络示例需要真实集群。编译通过不能证明消息投递、重试、事务或故障恢复行为。

## License

[Apache License 2.0](../LICENSE-APACHE).
