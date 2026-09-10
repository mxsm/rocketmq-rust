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

In either mode, recall does not undo a consumer's completed external side effect. Keep business cancellation state authoritative so a late reminder can be recognized and ignored or compensated according to the application's rules.

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

## Example and validation scope

From `rocketmq-example/`:

```bash
cargo check --example producer-delay-send
cargo run --example producer-delay-send
```

The example sends four messages to `DelaySendTestTopic` using level, relative seconds, relative milliseconds and absolute timestamp forms. It uses the loopback NameServer and then closes its producer. Pair it with a consumer on that exact topic and compare requested eligibility with actual receive time.

The example does not invoke recall, test a near-deadline race, or demonstrate timer recovery after a crash. Those scenarios require separate observation against the chosen engine and persistence configuration.

Sources: [delay example](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-example/examples/producer/delay_send.rs), [timer normalization](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/timer_request.rs), [store configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/src/config/message_store_config.rs), [handle generation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/processor/send_message_processor/message_builder.rs), [recall processor](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/processor/recall_message_processor.rs).
