---
title: "Message filtering"
---

# Message filtering

Filtering selects messages for a subscription. It does not remove messages from the primary log, replace authorization, or guarantee that all business conditions were checked. Use tags for coarse event categories and supported SQL expressions for predicates over message properties.

## Choose the layer

| Selection | Input | Boundary |
| --- | --- | --- |
| Tag expression | Message tag and `TagA || TagB` or `*` | Subscription filtering with a simple category model |
| SQL92-style selector | Named string properties and a supported expression | Requires Broker property-filter support and valid subscription metadata |
| Application predicate | Delivered message and business state | Consumes network/client capacity; application decides whether processing succeeded |

Keep group members' topic subscriptions and selectors consistent. A filter change is a change to what the group considers relevant; it is not a request to replay previously skipped records.

## Set producer metadata

This excerpt runs after the producer has started and the topic exists:

```rust
use rocketmq_model::common::message::message_single::Message;

let message = Message::builder()
    .topic("SqlFilterConsumerTestTopic")
    .tags("order_created")
    .raw_property("region", "cn")?
    .raw_property("priority", "3")?
    .body("order event")
    .build()?;
let result = producer.send_with_timeout(message, 3_000).await?;
```

Inspect the returned send status as described in [sending messages](../producer/sending-messages.md). Property values are strings on the message; SQL evaluation applies its supported coercion rules. A JSON field inside the body does not automatically become a filterable property.

Treat tags as exact categories. `TagA || TagB` is a subscription expression, not a recommendation to put that entire expression into one message's tag.

## Subscribe with a Tag or SQL selector

For a started-later `DefaultMQPushConsumer` with its listener registered:

```rust
consumer.subscribe("OrderEvents", "order_created || order_paid").await?;
```

For SQL, use the public `MessageSelector`:

```rust
use rocketmq_client_rust::MessageSelector;

consumer.subscribe_with_selector(
    "SqlFilterConsumerTestTopic",
    Some(MessageSelector::by_sql("region = 'cn' AND priority >= 3")),
).await?;
```

Complete the lifecycle from [Push consumers](./push-consumer.md), including startup and shutdown. LitePull has its own `subscribe` and selector methods; do not copy Push method signatures into a LitePull loop.

The Broker's `enablePropertyFilter` defaults to false. For the canonical TOML configuration, merge this field into the existing `[broker]` table and restart the configured Broker using the normal setup procedure:

```toml
[broker]
enablePropertyFilter = true
```

This is a configuration excerpt, not a complete Broker file. The non-Tag pull path rejects requests when this support is disabled, and client subscription checks compile the requested expression. All Brokers that can serve the subscription need compatible support; one enabled Broker does not configure the others.

## Use the implemented expression language

The current SQL runtime supports logical `AND`/`OR`/`NOT`, comparisons, `IS NULL`/`IS NOT NULL`, `IN`/`NOT IN`, `BETWEEN`/`NOT BETWEEN`, and supported string predicates such as `CONTAINS`, `STARTSWITH` and `ENDSWITH`. It is a predicate language over properties, not a database `SELECT` statement with joins or arbitrary functions.

Strings use single quotes and escape a quote by doubling it. Missing properties evaluate as `NULL`; the three-valued logic means a missing value is not automatically equivalent to false in every intermediate expression. Final Broker matching requires a true Boolean result. Validate missing, malformed and boundary values, not only one matching message.

Numeric-looking strings can be coerced by the evaluator where required. Keep producer property schemas stable so a changed representation does not silently change matches. Do not assume every Java client or another Broker version supports identical expression extensions.

## Understand prefiltering and final evaluation

ConsumeQueue tag codes or optional Bloom metadata can reject candidates before loading full properties. Bloom hits are candidates, not proof of a final SQL match. The current filter falls back to later evaluation when relevant prefilter metadata is absent or unsuitable.

For SQL, the Broker evaluates the compiled expression against message properties from the read path. Subscription versions and compiled filter metadata must agree. A stale subscription or missing filter metadata can fail before any message reaches a listener.

Application-side filtering remains useful for decisions that require current business state. If the application intentionally ignores a delivered message and returns success, it has chosen to advance that consumer's progress. To process it later, define a replay or separate subscription rather than relying on rejection by application code.

## Diagnose mismatches

1. Confirm the target cluster, topic, group, starting offset and producer send result.
2. Inspect the actual tag/property values using bounded authorized tooling; do not infer them from a message body.
3. Check `enablePropertyFilter` and expression compilation errors for SQL.
4. Compare the subscriptions of all group members and their versions.
5. Test one matching, one non-matching and one missing-property event in an isolated group.
6. Observe progress separately from delivered count: scanning filtered records can advance the next position without returning messages.

From `rocketmq-example/`, `consumer-tag-filter` and `consumer-sql-filter` demonstrate their respective selectors. The SQL example uses `SqlFilterConsumerTestTopic` with `region = 'cn' AND priority >= 3`. Provision that topic/group and send matching properties; running an unrelated simple producer is insufficient.

Sources: [selector API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/message_selector.rs), [SQL language](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-filter/README.md), [Broker expression filter](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/filter/expression_message_filter.rs), [pull validation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/processor/pull_message_processor.rs), [Broker configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/config/broker_config.rs).
