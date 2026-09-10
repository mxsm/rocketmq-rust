---
title: "Migrate Rust application APIs"
---

# Migrate Rust application APIs

Migrate imports, runtime construction and message-completion semantics together. Fixing an import can make code compile while leaving a detached consumer or an incorrectly owned runtime. This guide targets the current source API; use the matching source and dependencies for your release rather than combining snippets from unrelated versions.

## Locate the owning crate

| Responsibility | Current owner | Integration guidance |
| --- | --- | --- |
| Client facades, builders, client configuration and typed results | Cargo package `rocketmq-client-rust`, Rust import `rocketmq_client_rust` | Prefer crate-root public exports such as `ClientConfig`, `ClientRuntime`, `DefaultMQProducer` and `DefaultLitePullConsumer`. |
| Messages, queue identity and runtime-neutral domain types | `rocketmq-model` / `rocketmq_model` | Use canonical model types rather than copying structs or keeping obsolete common/remoting imports. |
| Remoting headers, request/response codes and codecs | `rocketmq-protocol` / `rocketmq_protocol` | Only depend on these when the integration actually implements a protocol boundary. |
| Connections, transport admission and TLS execution | `rocketmq-transport` / `rocketmq_transport` | A business client normally uses its facade; direct transport use adds lifecycle and protocol responsibilities. |
| Runtime owner, service contexts and task ownership | `rocketmq-runtime` / `rocketmq_runtime` | Establish ownership at the application boundary and pass child contexts. |
| Telemetry owner and handles | `rocketmq-observability` / `rocketmq_observability` | Keep the owner alive; cloned handles do not become independent shutdown owners. |
| Canonical errors and stable descriptors | `rocketmq-error` / `rocketmq_error` | Match typed descriptors/retry hints rather than parsing human-readable error strings. |

This is a responsibility map, not a mechanical rename of every symbol formerly located under `rocketmq-common` or `rocketmq-remoting`. Consult [module ownership](../architecture/module-map.md) for library work. Private `base`, `producer` or `consumer` implementation paths are not a stable alternative to the public reexports.

Generate the selected package's public API from your checkout when resolving an import:

```bash
cargo doc -p rocketmq-client-rust --no-deps
```

For a source integration, the [first-message manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/Cargo.toml) shows consistent local dependencies and `default-features = false` for the application client. Package defaults and workspace-selected features may differ. Enable `admin-read` or `admin-mutation` only when the application uses those APIs; see [features](../reference/features-platforms.md).

## Replace implicit construction with explicit ownership

The following is a conceptual before/after map, not code to compile:

```text
Before: each library constructs a client and assumes background execution exists
After:  application owns RuntimeOwner
        -> creates a service context and shared ClientRuntime
        -> passes Arc<ClientRuntime> into facade builders
        -> stops facades, shuts down the shared client and its runtime owner
```

`ClientRuntime::try_new` receives a service context, `ClientRuntimeConfig` and a telemetry handle. An `Arc` shares one client runtime; it does not create a fallback runtime for each facade. Keep application runtime creation outside an already running asynchronous task. A library should accept the required runtime/context instead of introducing another top-level runtime or a nested `block_on`.

The complete [first-message main](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/src/main.rs) demonstrates startup, Ctrl+C/bounded consumption, facade cleanup and final shutdown reports. Preserve cleanup on error paths; an early `?` before cleanup can abandon the intended shutdown sequence. For telemetry exporters with their own shutdown work, follow [runtime ownership](../architecture/runtime.md) and [observability ownership](../architecture/errors-observability.md); the example uses no-op telemetry.

## Current producer construction and result handling

This self-contained function requires a caller-owned live client runtime, a configured NameServer/Broker, and the `DocsMigration` topic. It sends one real message when called. Its code was checked for compilation; this migration probe was not executed against a cluster.

```rust
use std::sync::Arc;
use rocketmq_client_rust::{ClientRuntime, DefaultMQProducer};
use rocketmq_model::common::message::message_single::Message;
use rocketmq_model::result::SendStatus;

async fn send_once(client: Arc<ClientRuntime>) -> Result<(), Box<dyn std::error::Error>> {
    let mut producer = DefaultMQProducer::builder(client)
        .producer_group("docs_migration_producer")
        .name_server_addr("127.0.0.1:9876")
        .build();
    let outcome = async {
        producer.start().await?;
        let message = Message::builder()
            .topic("DocsMigration")
            .body("migration probe")
            .build()?;
        let result = producer.send_with_timeout(message, 3_000).await?;
        let result = result.ok_or_else(|| std::io::Error::other("send returned no result"))?;
        if result.send_status != SendStatus::SendOk {
            return Err(std::io::Error::other("send did not return SEND_OK").into());
        }
        Ok(())
    }.await;
    producer.shutdown().await;
    outcome
}
```

The producer builder returns a facade directly, while other builders can return a `Result`. Follow each signature rather than adding or removing `?` uniformly. `send_with_timeout` takes milliseconds and returns an optional send result; inspect both absence and `send_status`. An operation error still reaches `shutdown` in this function. A sustained workload should start one producer and reuse it, rather than create one per message.

`SEND_OK` is not proof of exactly-once business execution. Other send statuses may describe accepted data whose requested flush/replication condition was not met. Preserve idempotency and the intended durability interpretation when replacing earlier Boolean or unwrap-based handling; see [sending messages](../producer/sending-messages.md).

## Keep Classic Pull only through its runnable builder

| Earlier construction pattern | Current migration |
| --- | --- |
| `DefaultMQPullConsumer::new()` / `default()` / `with_consumer_group(...)` followed by operational calls | These create detached compatibility values. Replace construction with `builder(client_runtime)` for runnable Classic behavior. |
| Explicit queue, selector, offset and batch size | Preserve those inputs with `PullOptions`. Queue ownership remains the application's responsibility. |
| Implicit cursor increment by returned message count | Interpret `PullStatus` and the returned next offset; filters and gaps invalidate that arithmetic. |
| Reusing a stopped consumer object | Create a new facade after shutdown or failed startup; shutdown is not a reset-to-new operation. |

The following runtime-backed compatibility function retains an explicit queue and next offset. The caller processes the result and decides when to advance business progress. It does not automatically commit that progress.

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

The deprecation allowance is confined to this compatibility example. Do not use broad warning suppression as the migration strategy. For long polling, keep client timeout greater than Broker suspension and preserve the chosen request budget. [Classic Pull compatibility](../consumer/classic-pull-compatibility.md) explains assignment, status and lifecycle behavior.

## Move to LitePull as a behavior change

For new polling applications, construct LitePull with an explicit commit decision:

```rust
use std::sync::Arc;
use rocketmq_client_rust::{ClientResult, ClientRuntime, DefaultLitePullConsumer};

fn build_polling_consumer(client: Arc<ClientRuntime>) -> ClientResult<DefaultLitePullConsumer> {
    DefaultLitePullConsumer::builder(client)
        .consumer_group("docs_migration_consumer")
        .name_server_addr("127.0.0.1:9876")
        .auto_commit(false)
        .poll_timeout_millis(1_000)
        .build()
}
```

This function constructs the consumer only. The caller subscribes or assigns queues, starts it, runs a bounded poll loop, processes business work, updates progress and always shuts it down. Use the complete [LitePull guide](../consumer/pull-consumer.md) and first-message application for that surrounding lifecycle.

| Classic workflow | LitePull replacement decision |
| --- | --- |
| Pull a selected queue at an explicit offset | Choose `subscribe` with group assignment or explicit `assign`; use `seek` only for an intentional position change. |
| Per-request selector | Configure equivalent subscription/filter semantics before consumption. |
| Own next-read cursor after every result | Process first, then update progress under the selected commit policy. |
| Application assignment callbacks | Stop work on revoked queues and respect the selected allocation mode. |
| One successful pull means work is complete | Separate fetching, business completion, local commit, remote submission and Broker persistence. |

Current `commit_all` updates local offset-store state and can internally log per-queue errors; it is not an immediate durable all-queue transaction. The periodic/shutdown path and Broker persistence remain separate. Do not let a method name turn into a stronger completion contract during migration.

For a handover, record the last completed business range, stop the old queue owner, start the new mode with deliberate group/position settings, and inspect replay and gaps. Using the same group is not a substitute for coordinating ownership. Starting at “first offset” does not necessarily override a group's existing stored progress.

## Validate the application change

1. Update manifests/imports to actual public owners and compile the application's selected feature graph, including any standalone manifest.
2. Review startup and shutdown on success, timeout, failed startup and cancellation. Cancellation does not undo a request already accepted remotely.
3. Exercise the application's message mode and its result handling, then inspect assignment and progress across restart/rebalance.
4. Retain business idempotency and error descriptors in the integration; avoid exposing raw causes or secret-bearing configuration through diagnostic logs.

The functions here are compile-checked excerpts, not a new end-to-end migration test. The existing first-message application supplies a complete runnable lifecycle. See [Java migration](./java-to-rust.md) when the service implementation also changes, and [upgrade/rollback](../operations/upgrade-rollback.md) when persisted state changes.

Sources: [public client exports](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/public_api.rs), [crate exports](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/lib.rs), [Classic builder](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_pull_consumer_builder.rs), [model](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-model/src).
