# RocketMQ Rust Client

[English](README.md) | [简体中文](README-zh_cn.md)

`rocketmq-client-rust` implements producers, transaction producers, Push
Consumers, Lite Pull Consumers and optional Admin APIs. Message domain types
live in [rocketmq-model](../rocketmq-model), wire types in
[rocketmq-protocol](../rocketmq-protocol), and networking in
[rocketmq-transport](../rocketmq-transport).

## Dependencies and runtime ownership

For an application beside these crates in this repository, use the following
dependencies. External projects must adjust the paths:

```toml
[dependencies]
rocketmq-client-rust = { path = "../rocketmq-client", default-features = false }
rocketmq-model = { path = "../rocketmq-model" }
rocketmq-runtime = { path = "../rocketmq-runtime" }
rocketmq-observability = { path = "../rocketmq-observability" }
tokio = { version = "1", features = ["signal"] }
```

The application creates a `RuntimeOwner` and injects a `ChildServiceContext`
and `TelemetryHandle` into `ClientRuntime::try_new`. Producers, consumers and
Admin sessions share `Arc<ClientRuntime>`; the client never creates a fallback
runtime. Shut down client facades first, then the shared ClientRuntime, then
the RuntimeOwner. The application must inspect the shutdown reports.

Import public types from the crate root or `prelude`; internal module paths
are not supported import paths.

## Producer example

Provision a running NameServer, Broker and `TopicTest` before running:

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

## Push Consumer example

This complete async function accepts the application's client runtime. Invoke it
on the owning RuntimeOwner, then close the shared ClientRuntime and RuntimeOwner
after it returns.

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

## Consumer APIs and message ownership

- Push Consumers support concurrent or orderly listeners and Tag/SQL92
  subscriptions. The Broker must support the selected filtering capability.
- Lite Pull Consumers provide polling and offset management. Zero-copy polling
  returns owned `Arc<MessageExt>` values that may outlive a poll call; the
  underlying data remains retained while those messages live. The ordinary
  polling path that returns independent message values clones messages.
- Runnable transaction, request/reply and queue-selection examples live in
  [rocketmq-example](../rocketmq-example).

## Cargo features

| Feature | Behavior |
| --- | --- |
| `admin-full` | Package default; combines Admin reads and mutations. The root workspace dependency disables defaults so consumers select their required surface explicitly. |
| `admin-read` / `admin-mutation` | Enable query or mutation APIs; compile-time availability does not grant runtime permission. |
| `observability` | Enables client tracing integration. |
| `observability-metrics` | Enables client metrics integration. |
| `otlp-traces` | Enables OTLP trace export support; runtime configuration is still required. |
| `nameserver-dns-discovery` | Enables optional DNS discovery. |
| `test-support` | Exposes helpers for tests and benchmarks. |

There is no client `tls` feature. TLS depends on the transport crate's compiled
features and connection configuration; applications requiring TLS must enable
`rocketmq-transport/tls` in their dependency graph. The normal client transport
dependency enables SOCKS and does not independently enable TLS.

## Validation

Select checks from the repository root for the changed area:

```bash
cargo fmt -p rocketmq-client-rust -- --check
cargo check -p rocketmq-client-rust
cargo test -p rocketmq-client-rust <test_name>
cargo bench -p rocketmq-client-rust --bench client_hot_path_benchmark --features test-support
```

Network examples require a real cluster. Compilation alone does not verify
delivery, retry, transaction or recovery behavior.

## License

[Apache License 2.0](../LICENSE-APACHE).
