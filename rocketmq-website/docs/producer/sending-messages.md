---
title: "Sending messages"
---

# Sending messages

Choose a send method by how the application observes completion and selects a destination. All methods use an application-owned `ClientRuntime` and a started producer. Begin with [quick start](../getting-started/quick-start.md) for the complete runtime, Broker, topic, and shutdown setup.

## Choose the result contract

“Synchronous send” here means waiting for a Broker response. The Rust call is still an asynchronous future and is awaited.

| API family | Result channel | Suitable use |
| --- | --- | --- |
| `send`, `send_with_timeout` | `ClientResult<Option<SendResult>>` | Observe the send response before continuing |
| `send_with_callback`, `send_with_callback_timeout` | Immediate method result plus callback result/error | Keep bounded application work in flight and correlate completion |
| `send_oneway` | `ClientResult<()>` without a Broker response | Cases where the application deliberately does not require remote acknowledgement |
| `send_batch` and timeout variants | `ClientResult<SendResult>` | Send an explicit eligible batch |
| `send_to_queue` and queue variants | Same completion style, chosen queue | Preserve a known destination |
| `send_with_selector` and selector variants | Same completion style, application selects from available queues | Route related events using a stable business key |

An `Ok` wrapper is not sufficient to count a successful acknowledged send. Inspect `SendResult.send_status`, and handle `None` explicitly where the API permits it. The direct timeout path expects a response; absence is not a successful Broker acknowledgement.

`SendStatus::SendOk`, `FlushDiskTimeout`, `FlushSlaveTimeout`, and `SlaveNotAvailable` have different meanings. The latter statuses can follow an accepted append. The status names retain the protocol's terminology; [storage design](../architecture/storage.md) explains the corresponding durability and replica conditions. Even `SendOk` does not mean a consumer completed its business work.

## Send one message and inspect its status

This complete function accepts the shared runtime created in the quick-start application. Call it on that runtime and close the shared ClientRuntime and RuntimeOwner afterward. Provision `DocsFirstMessage` before calling it.

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

The key supports business correlation; setting a key does not make the Broker deduplicate sends. Persist the business event identity independently if it must survive retries or process restarts. The cleanup runs after an operation error too.

## Use callbacks without losing failures

Callbacks receive `Option<&SendResult>` and `Option<&ClientError>`. Those references belong to the callback invocation; copy the bounded information needed for later processing. Avoid retaining a whole request or message body in a completion record.

Inspect both channels: a method can fail before submission, and completion can fail through the callback. In the current `send_with_callback` facade, some underlying submission errors are delivered to the callback and the method returns `Ok(())`. Therefore, counting only successful method returns will overcount successful sends.

Keep the producer and runtime alive until the application's outstanding completion records settle or its explicit deadline expires. Bound in-flight count/bytes and callback work. A callback should not block a runtime worker or launch unowned background tasks. Shutdown is not a substitute for correlating each business operation's outcome.

One-way sending has no send-result callback or Broker acknowledgement to inspect. Successful local submission cannot establish remote persistence. It should not be used when the next business step requires confirmation that the Broker accepted the event.

## Send a valid batch

An explicit batch is non-empty, uses one topic and a consistent `waitStoreMsgOK` value, and excludes retry-topic messages and delayed/timer messages. The current `MessageBatch` validator checks delay level, relative millisecond/second delay, and absolute delivery timestamp properties. Do not combine transaction semantics with the ordinary batch path.

The following is an excerpt after producer startup:

```rust
let messages = vec![
    Message::builder().topic("DocsFirstMessage")
        .body("batch event 1").build()?,
    Message::builder().topic("DocsFirstMessage")
        .body("batch event 2").build()?,
];
let result = producer.send_batch_with_timeout(messages, 3_000).await?;
```

Inspect the batch's `send_status` as for a single send. A batch send is not a transaction spanning a consumer's database. Encoded aggregate size, properties and framing overhead must fit the active client/Broker limits; a message-count limit alone cannot ensure that. Split into bounded batches before submitting, and give every business event its own replay identity.

Automatic accumulation is a separate option from explicit `send_batch`. Current `send`/some callback and queue facades can route through the accumulator when enabled, while the explicit `send_with_timeout` path directly invokes its timed send implementation. Do not assume all overloads have identical buffering behavior.

## Select queues and retry deliberately

Fetch publish queues from the topic route rather than inventing a Broker name or queue ID. A selector receives candidate queues, the message and its argument, and returns an optional queue. Handle an empty candidate set; a modulo operation on zero queues is invalid.

Keep mapping stable for related events and serialize their sends if order matters. Route changes or queue-count changes can change a simple modulo mapping. See [ordered messages](../guides/ordered-messages.md).

Timeouts bound a client's wait, not the remote side effect. Separate pre-admission rejection from an uncertain post-send result, keep a bounded retry deadline, and account for retries already performed by the selected send path. Do not silently redirect a failed business event to another topic: that changes subscriptions, ordering and recovery semantics.

## Example targets and observations

From `rocketmq-example/`, use its standalone manifest and provision each example's actual topic first:

| Target | Topic | What to inspect |
| --- | --- | --- |
| `producer-basic-send` | `BasicSendTestTopic` | Response, callback and one-way differences |
| `producer-batch-send` | `BatchSendTestTopic` | Batch status and selected queue |
| `producer-send-to-queue` | Read its constants | Explicit route selection |
| `producer-send-with-selector` | Read its constants | Selector argument and queue mapping |

```bash
cargo check --example producer-basic-send --example producer-batch-send
cargo run --example producer-basic-send
```

These examples use loopback NameServer constants rather than a universal command-line address option. They demonstrate APIs; their debug prints are not a production logging policy. For a verified first-message sequence and full cleanup, use the website's paired tutorial application. Compilation does not establish callback loss behavior, failover, or crash recovery.

Sources: [producer facade](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/producer/default_mq_producer.rs), [send implementation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/producer/producer_impl/default_mq_producer_impl/send.rs), [batch validation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/message_batch.rs), [canonical send results](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/result.rs).
