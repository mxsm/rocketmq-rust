---
title: "Producer lifecycle and send results"
---

A producer discovers writable queues through the NameServer and sends messages to a Broker. `DefaultMQProducer` provides ordinary send APIs; transaction and request/reply usage add their own state and failure handling. Begin with [quick start](../getting-started/quick-start.md) for a complete application and local services.

## Own the runtime explicitly

The current client API requires an application-owned `Arc<ClientRuntime>`. Create it under a `RuntimeOwner` with a child service context and telemetry handle, then pass it into `DefaultMQProducer::builder`. Constructing a producer does not start it or provision its Topic.

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

Share the client runtime across compatible facades within an application. It owns client infrastructure and background work; it is not a global singleton to abandon at process exit. Stop admitting new sends before shutdown, await application-owned in-flight operations, then close facades before the runtime they depend on.

The [complete first-message application](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/src/main.rs) preserves cleanup on both success and failure. Its standalone [manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/Cargo.toml) shows the exact path dependencies.

## Build and start

This fragment belongs inside the application's owned runtime and uses its existing `client`. The complete example includes imports, error handling and cleanup.

```rust
let mut producer = DefaultMQProducer::builder(client)
    .producer_group("docs_first_message_producer")
    .name_server_addr("127.0.0.1:9876")
    .build();
producer.start().await?;
```

The Producer Group identifies the producer role; it does not create a Consumer Group or determine the Topic. Configure the NameServer before startup and provision the target Topic with writable queues. In particular, the tutorial disables automatic Topic creation.

## Construct and send a message

Use the Model message builder:

```rust
let message = Message::builder()
    .topic("DocsFirstMessage")
    .body("documentation message".to_owned())
    .build()?;
let result = producer.send_with_timeout(message, 3_000).await?;
```

The timeout is in milliseconds. In this API, the send call returns a `ClientResult<Option<SendResult>>`; inspect both the error/result presence and `send_status`. The outer Rust result alone does not express every Broker outcome.

| Send status | Interpretation | Application response |
| --- | --- | --- |
| `SendOk` | The selected send path reported success under its configured storage/replication policy | Record the result needed for diagnosis; continue according to the business contract |
| `FlushDiskTimeout` | The requested disk-flush wait did not finish in time | Treat persistence outcome as uncertain; a blind retry can duplicate data |
| `FlushSlaveTimeout` | The requested replica wait did not finish in time | Diagnose replica progress and apply the application's retry policy |
| `SlaveNotAvailable` | The expected replica was unavailable | Check topology and availability requirements before retrying |
| Error or missing result | No usable successful result was obtained | Preserve operation context; do not infer that no message was stored |

The canonical result types live in `rocketmq-model::result`. Send success does not prove that a consumer processed the message, nor does it make a database update atomic with sending.

## Choose the operation deliberately

[Sending messages](sending-messages.md) describes the send variants and queue selection. A response-bearing send can report a Broker result; a one-way send intentionally gives up that response. Transaction messages require a local transaction decision and checking behavior, described in [transaction messages](transaction-messages.md).

For a stable retry policy, define a business event identity before the first attempt. Keep that identity across retries and make the consumer's side effects idempotent. Client retry counts and timeouts belong inside the application's overall deadline; layering unbounded application retries over client retries can amplify a cluster failure.

## Shut down and diagnose

Always invoke `producer.shutdown().await` after finishing sends, including when startup or a later operation fails. Then close the shared client runtime and inspect its report; close the runtime owner and telemetry afterward. Do not introduce another Tokio runtime just to call shutdown from asynchronous code.

If startup succeeds but sending fails, check Topic routes, writable queue counts and the advertised Broker address before changing timeout values. See [first diagnosis](../operations/first-diagnosis.md) and [delivery and retry](../guides/delivery-and-retry.md).

Sources: [producer facade](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/producer/default_mq_producer.rs), [send result](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/result.rs), [message builder](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/message_builder.rs).
