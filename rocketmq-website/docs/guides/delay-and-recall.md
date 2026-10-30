---
title: "Delayed delivery and recall"
---

# Delayed delivery and recall

Delayed messages become eligible for delivery after a configured delay or timestamp. Eligibility is separate from actual consumer processing: dispatch backlog, consumer availability and business work add latency. A timer is not a real-time deadline guarantee.

## Select one scheduling form

| Message builder option | Meaning | Server-side condition |
| --- | --- | --- |
| `delay_level(n)` | Index into the configured delay-level table | The Broker/store's level scheduler and current `messageDelayLevel` mapping |
| `delay_secs(n)` | Relative delay in seconds | Timer normalization and selected timer engine |
| `delay_millis(n)` | Relative delay in milliseconds | Timer normalization and configured precision |
| `deliver_time_ms(timestamp)` | Absolute Unix-epoch milliseconds | Clock interpretation, future timestamp and admission horizon |

Use one scheduling form per message. Current timer-property precedence is seconds, then milliseconds, then absolute delivery time; relying on that precedence makes mixed-property messages harder to reason about. Do not combine delay-level and timer properties.

The default level table starts at 1 second, 5 seconds and 10 seconds, so level 3 is 10 seconds with that table. The mapping is configurable. The current default timer precision is 1,000 ms and the standard maximum delay is three days; the compatible precision set is 100, 200, 500 or 1,000 ms. These defaults are source configuration, not a universal deployment guarantee.

## Send and observe a timer message

Use the full producer lifecycle from [sending messages](../producer/sending-messages.md). Provision `DelaySendTestTopic` and a consumer subscribed to it. The following excerpt runs after producer startup:

```rust
let message = Message::builder()
    .topic("DelaySendTestTopic")
    .key("reminder-1001")
    .delay_secs(30)
    .body("reminder")
    .build()?;
let send_result = producer.send_with_timeout(message, 3_000).await?;
```

Inspect the optional result and `send_status` before treating the send as acknowledged. Record the business event ID and, when present, the returned `recall_handle`. A message key is not itself a recall handle.

For an absolute timestamp, calculate Unix-epoch milliseconds with checked arithmetic and synchronize the participating hosts' clocks. Normalization rejects malformed values, overflow, past timestamps and requests outside the configured horizon. The wire representation's millisecond unit does not imply millisecond delivery accuracy.

The relevant standard TOML keys belong in the existing `[store]` table:

```toml
[store]
timerWheelEnable = true
timerPrecisionMs = 1000
timerMaxDelaySec = 259200
```

This is an excerpt, not a complete Broker configuration. A configured timer also requires its selected backend and service lifecycle to be active. Extended timeline mode has separate RocksDB/feature and admission-horizon conditions; do not infer its availability from the standard timer example.

## Recall using the returned handle

Recall is intended for eligible timer messages before they become due. The current handle-generation path does not give ordinary delay-level messages the same recall handle. Require the actual `SendResult.recall_handle` instead of fabricating one from the message ID.

This excerpt assumes `result` is a successfully inspected `SendResult` and the producer is still started:

```rust
if let Some(handle) = result.recall_handle.as_deref() {
    let recalled_message_id = producer
        .recall_message("DelaySendTestTopic", handle)
        .await?;
}
```

The client checks lifecycle, topic and a non-empty decodable handle, then resolves its Broker. Retry/DLQ topics are not supported by this facade. The Broker checks recall enablement, role/availability, write permission policy, topic existence, matching topic/Broker in the handle, and the remaining time window.

The Broker's `recallMessageEnable` currently defaults to true, but the actual deployment's authorization still applies. Remaining time must be positive and strictly below the selected maximum recall horizon. A valid-looking handle is not authority to bypass topic permissions.

## Interpret recall according to the engine

In the standard path, recall appends a timer deletion marker. Its response is evidence about the marker write path, not a synchronous scan proving that no consumer ever saw the original message. Delivery and deletion processing can race.

In ExtendedTimeline mode, the current processor uses a typed cancellation operation. `Cancelled` and `AlreadyCancelled` map to success; `TooLate`/`NotFound` map to an illegal operation; `Retry` maps to service unavailable; `Quarantined`/`Unsupported` map to a system error. That explicit state result is different from the standard marker path.

With `timerStoreMode = "native"`, the Broker registers the recall with the native engine before it writes the deletion marker, so a delivery that starts in between still sees it. The answer is typed the same way: the recall succeeds only while the timer is not yet due, and a recall of a timer already due is refused as an illegal operation without writing a marker. If the marker cannot be written, the registration is withdrawn and the recall reports a system error.

In any mode, recall does not undo a consumer's completed external side effect. Keep business cancellation state authoritative so a late reminder can be recognized and ignored or compensated according to the application's rules.

## Failure and recovery decisions

| Observation | Interpretation |
| --- | --- |
| Send times out | Timer admission may be uncertain; reconcile before creating a second business reminder |
| Send result has no recall handle | Do not infer recall support from a delay setting alone |
| Recall reports topic/Broker mismatch | Check the exact original handle and target; do not edit handle fields |
| Recall is too late | Delivery may have become eligible; use the application's cancellation policy |
| Recall times out | The remote cancellation/marker may have succeeded; preserve the original identity when reconciling |
| Delivery is later than requested | Inspect timer dispatch, clock, storage and consumer backlog separately |

Batch validation and the transaction producer reject delayed/timer combinations in their respective paths. A delayed send also does not impose ordering relative to ordinary messages sent later to the same business topic.

## Timer files and undeliverable timers

The standard timer keeps its index in the Apache RocketMQ 5.x `timerlog`, `timerwheel` and `config/timercheck` files by default (`timerFileFormat = "java"`). A Rust Broker and a Java 5.x Broker can therefore take over the same store directory without losing pending timers. `timerFileFormat = "rust_v2"` selects the segmented Rust files instead. After the setting changes, timers still held in the previous format are delivered from it.

Two `[store]` keys decide what happens to a timer that cannot be delivered. They keep the names of their Apache RocketMQ counterparts, but the Rust defaults never drop a timer whose payload is still in the CommitLog:

| Key | Rust default | Java default | Effect |
| --- | --- | --- | --- |
| `timerSkipUnknownError` | `false` | `false` | `true` quarantines and skips such a timer: one whose record cannot be read or routed, or whose write the Store rejects. With `false` it is quarantined and the timers behind it wait for an operator. |
| `timerEnableRetryUntilSuccess` | `true` | `false` | `false` drops and logs a delivery whose write has failed three times for a transient reason, as Java does by default. With `true` the write is retried until it succeeds. |

A timer or recall whose payload CommitLog cleanup has deleted, for example under disk pressure, is skipped with a warning whatever these keys say, as in Java: nothing can bring the payload back, and waiting for it would hold back every later timer. A skipped recall no longer cancels its timer.

With `timerSkipUnknownError = false`, Java also drops a payload it cannot read and a rejected message; Rust keeps them. Quarantined timers are recorded in `timer-v2/quarantine.jsonl` under the store root.

## Long delays with the native engine

`timerStoreMode = "native"` admits timers of up to `timerNative.maxDelayDays` (400 days by default) to the native engine, a tiered log of time buckets under `timer-native/` in the store root. A timer due within the mid horizon points into the CommitLog; a later one is copied once, so CommitLog cleanup cannot remove it before it is due. Deadlines are rounded to `timerPrecisionMs` as in the standard engine, and the engine needs `timerWheelEnable = true`.

```toml
[store]
timerWheelEnable = true
timerStoreMode = "native"

[store.timerNative]
maxDelayDays = 400
```

| Key under `[store.timerNative]` | Default | Meaning |
| --- | --- | --- |
| `maxDelayDays` | `400` | Longest delay admitted |
| `laneCount` | `16` | Delivery lanes; fixed once `timer-native/` exists |
| `nearWindowSec` | `600` | Timers due within this many seconds are kept in memory |
| `nearWheelMemoryBytes` | 256 MiB | Memory those timers may use |
| `midHorizonHours` | `48` | Timers due later are copied; the engine uses at most `fileReservedTime` minus six hours |
| `payloadSegmentBytes` | 256 MiB | Size of one segment of copied timers |
| `checkpointIntervalMs` | `5000` | How often indexed timers and delivery progress are made durable |
| `deliveryBatchMessages` | `512` | Due timers one lane delivers per batch |
| `deliveryInflightWrites` | `32` | Writes one batch keeps in flight |

Each timer is delivered by the engine that admitted it, so `timerStoreMode` can be switched in both directions:

- After a switch to `native`, the standard engine keeps delivering the timers it admitted before.
- After a switch away from `native`, the native engine keeps delivering its timers. Once a clean shutdown finds that it holds none, the store no longer runs it; switching back to `native` starts it again.
- A recall reaches the engine that admitted its timer, whichever mode is active. Typed answers come only from the native mode.
- A store that still holds native timers does not load in ExtendedTimeline mode.

Broker runtime info describes the native engine in three entries: `timerNativeRole` (`active` or `draining`), `timerNativeMetrics` and `timerNativePipelineMetrics`. `timerNativeMetrics` holds what the engine keeps, such as `wheelTimers`, `hourBuckets`, `minuteRuns`, `payloadDays`, `recalls` and `sourceBehind`, and what it has done since the Broker started, such as `indexed`, `written`, `recalled`, `dropped` and `skipped`, plus `followed` on a slave. The OpenTelemetry timer enqueue and dequeue totals count native timers as well.

In a master/slave group, a slave indexes native timers and follows its master's deliveries through the messages the master writes. A slave promoted to master therefore writes only the timers its old master had not written; as with the standard engine, a timer can still be delivered twice around a failover.

Once a checkpoint makes their delivery durable, the files of delivered timers and their payload copies are deleted, and the CommitLog files they referred to are released for cleanup. A recall is dropped from memory once the timer it names is delivered, but the recall log on disk is not yet compacted.

## Example and validation scope

From `rocketmq-example/`:

```bash
cargo check --example producer-delay-send
cargo run --example producer-delay-send
```

The example sends four messages to `DelaySendTestTopic` using level, relative seconds, relative milliseconds and absolute timestamp forms. It uses the loopback NameServer and then closes its producer. Pair it with a consumer on that exact topic and compare requested eligibility with actual receive time.

The example does not invoke recall, test a near-deadline race, or demonstrate timer recovery after a crash. Those scenarios require separate observation against the chosen engine and persistence configuration.

Sources: [delay example](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-example/examples/producer/delay_send.rs), [timer normalization](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/timer_request.rs), [store configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/src/config/message_store_config.rs), [handle generation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/processor/send_message_processor/message_builder.rs), [recall processor](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/processor/recall_message_processor.rs).
