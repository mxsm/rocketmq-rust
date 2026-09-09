---
title: "Send and consume your first messages"
---

This tutorial connects the [local Rust services](local-source.md) to one matched producer/LitePull application. You will create a Topic and Consumer Group, send five messages, process them, and commit the group's offsets. All commands run from the repository root.

## Before sending

The NameServer and Broker must be running with the tutorial configuration. Confirm `DocsCluster` and `docs-broker` using `cluster clusterList` as shown in local setup. Build the example before starting its one-minute consumer window:

```bash
cargo build --manifest-path rocketmq-website/examples/first-message/Cargo.toml
```

| Setting | Value |
| --- | --- |
| NameServer | `127.0.0.1:9876` |
| Topic | `DocsFirstMessage` |
| Consumer Group | `docs_first_message_consumer` |
| Producer Group | `docs_first_message_producer` |
| Read/write queues | Four of each on the tutorial Broker |
| Subscription | All messages in the Topic |

The executable uses these constants in [its complete source](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/src/main.rs). Both commands use the same checkout and Topic. The larger `rocketmq-example` collection has independent demonstrations with different built-in Topics.

## 1. Create the Topic and Consumer Group

Set the NameServer in this terminal before running the admin commands. In PowerShell:

```powershell
$env:NAMESRV_ADDR = "127.0.0.1:9876"
```

In a Unix shell:

```bash
export NAMESRV_ADDR=127.0.0.1:9876
```

The current `clusterList` and `updateSubGroup` subcommands use the environment and do not accept `-n`. The Topic and progress commands below accept their own `-n` option; it is not a global CLI flag.

These two commands change cluster metadata. Use them against the dedicated local cluster:

```bash
cargo run -p rocketmq-admin-cli -- topic updateTopic -t DocsFirstMessage -c DocsCluster -r 4 -w 4 -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- consumer updateSubGroup -g docs_first_message_consumer -c DocsCluster
```

`updateTopic` creates or updates Topic configuration. `updateSubGroup` creates or updates Consumer Group configuration. Do not reuse existing application resource names: rerunning an update is a configuration operation, not merely a query.

Inspect the resulting route with this read-only command:

```bash
cargo run -p rocketmq-admin-cli -- topic topicRoute -t DocsFirstMessage -n 127.0.0.1:9876
```

The route should identify `docs-broker` and advertise `127.0.0.1:10911` with readable and writable queues. If no route is returned immediately, allow the Broker's route registration to propagate and query again. An unreachable returned address is a Broker advertisement problem, even if the NameServer is reachable.

## 2. Start the consumer

Open a third terminal, leaving both services running:

```bash
cargo run --manifest-path rocketmq-website/examples/first-message/Cargo.toml -- consume
```

Wait for `CONSUMER_STARTED`. The application subscribes before startup, polls with a one-second timeout, and waits up to 60 seconds for at least five messages. Empty polls are normal while no eligible data is available.

For a new group without stored progress, `ConsumeFromFirstOffset` allows reading existing data. For an existing group, stored offsets take precedence. The setting does not force historical replay.

## 3. Send from another terminal

Within the consumer's 60-second window:

```bash
cargo run --manifest-path rocketmq-website/examples/first-message/Cargo.toml -- produce
```

The producer sends five small messages with a three-second timeout per call. It prints `SEND 0` through `SEND 4`, the returned status, message ID and queue offset. Missing results and non-`SendOk` statuses are treated as errors. An error terminates the command after cleanup.

The consumer prints `RECEIVED id=...` and `OFFSET_COMMIT_REQUESTED received=...`. Message IDs, batching and receive order vary. Count can exceed five if earlier messages remain on the Topic. After processing a nonempty batch, the application commits progress and exits when its total reaches at least five.

This output demonstrates the tutorial's application path; it does not establish global ordering, exactly-once business execution, replication, or crash durability. The example prints message IDs and counts instead of logging message bodies.

## 4. Understand the result

The producer's return concerns sending. The consumer's print concerns application processing. `commit_all` concerns the group's consumption progress. None of those operations is automatically part of a transaction with an external database. In current LitePull, `commit_all` updates client offset-store state separately from persistence and can log per-queue errors internally. `OFFSET_COMMIT_REQUESTED` therefore reports the call returning, not a durable Broker acknowledgement. See [the commit semantics](../consumer/pull-consumer.md).

The sample treats printing each ID as completed processing, then commits. Replace that step with successful business processing before using the pattern in an application. If processing fails halfway through a batch, do not blindly commit the entire batch; define retry, idempotency and the contiguous progress you can safely advance.

The application owns one `RuntimeOwner`, creates an `Arc<ClientRuntime>` beneath it, and passes that client runtime into the facade builder. It closes the producer/consumer, the shared client runtime, the runtime owner, and telemetry before returning. Keep that lifecycle when adapting the example.

## Repeat or diagnose

For another run, start the consumer and then send another five messages. Reusing the group normally resumes its committed progress. If old unconsumed data exists, the consumer may finish from that data before the new producer runs; this demonstration does not correlate a unique run ID.

If the consumer times out, use [first diagnosis](../operations/first-diagnosis.md): check the returned Broker address, matching Topic/group, queue assignment and stored progress. Do not delete the store or reset offsets as the first response.

Read [producer overview](../producer/overview.md), [LitePull consumption](../consumer/pull-consumer.md), and [delivery and retry](../guides/delivery-and-retry.md) before expanding the example.
