---
title: "Message model and offset semantics"
---

RocketMQ Rust separates the message an application creates from the metadata assigned during routing, storage, and delivery. This separation lets producers, consumers, protocol adapters, and storage backends share domain types without making the model crate own sockets or background tasks.

Read [basic concepts](../getting-started/basic-concepts.md) first. This chapter describes the current source model; it is not a Rust struct definition of the persisted CommitLog format.

## Domain objects and responsibility

| Object | Responsibility | What it does not establish |
| --- | --- | --- |
| `Message` | Topic, body, flags, and properties supplied by a producer | Broker acceptance, queue assignment, or durability |
| `MessageBody` and `MessageProperties` | Body representation and property operations used by the message | Application schema validity or authorization |
| `MessageExt` | Message plus delivery/storage metadata, including queue and physical offsets, timestamps, hosts, and reconsume count | Successful business processing |
| `MessageEnvelope` | Composed representation separating message, routing, and storage information | A different wire protocol or a universal replacement for every existing API |
| `MessageQueue` | The tuple of Topic, Broker name, and queue ID | A socket address, consumer assignment, or an offset |

The canonical queue identity lives in `rocketmq_model::message::MessageQueue`. The compatibility path under `common::message::message_queue` re-exports that same type. Equality and hashing include all three fields: queue 0 on Broker A and queue 0 on Broker B are different queues. Broker addresses can change while the logical Broker name remains part of queue identity.

Public message APIs expose constructors and accessors; do not reproduce an approximate `struct Message { topic: String, body: Vec<u8> }` in application documentation. The current implementation uses dedicated body/property types and compact strings. Creating a model value does not replace producer, Broker, or storage validation.

## Body and properties

The body is application data. RocketMQ does not infer a business schema or deduplication policy from its bytes. Choose a schema version and a stable business key when applications need compatible evolution or idempotent effects.

Tags, keys, retry metadata, transaction markers, and timer metadata travel through message properties, but have different owners. User properties support filtering and application metadata. Reserved system properties affect processing and should be set through the appropriate API. A Tag expression such as `TagA || TagB` belongs to a subscription selector, not to the Tag value assigned to one message.

SQL filtering evaluates message properties; a JSON field inside the body does not automatically become a SQL property. See [message filtering](../consumer/message-filtering.md). Keys help lookup and correlation; assigning the same key to two sends does not make the Broker deduplicate them.

Shared byte buffers and `Arc<MessageExt>` can reduce copies. They also retain memory while references survive. A zero-copy polling API changes ownership and allocation behavior, not the consumer's obligation to finish processing and record progress.

## Four positions that must stay distinct

| Position | Scope and unit | Typical use |
| --- | --- | --- |
| Queue offset | Logical message position in one Topic/Broker/queue | Pull requests and consumer progress |
| CommitLog offset | Physical byte position in the primary log | Storage lookup, recovery, and replication |
| Durable watermark | Exclusive physical boundary known durable under the relevant contract | Deciding whether an append range is covered |
| Derived cursor | Engine/source-epoch progress through an exclusive primary-log boundary | Rebuilding or advancing ConsumeQueue/index state |

A consumer's next offset is not a CommitLog byte address. It must come from the pull result and processing policy, not from adding the body size or simply counting returned messages. Filtering and invalid-offset correction can advance the returned position without returning that many messages.

For example, a record may occupy physical bytes `[4096, 4224)` while appearing at logical queue offset `17`. A durable watermark of `4224` covers that append range; the next logical queue position would be interpreted through the consumer protocol. Neither number is a global message sequence across Brokers.

Message IDs, application keys, and POP receipt handles also serve different purposes. A receipt identifies a delivery attempt and its acknowledgement context; it is not a permanent message identity. Do not reuse an old receipt after a renewal returns a replacement.

## Where encoding and validation happen

`rocketmq-model` provides runtime-neutral values and model contract violations. `rocketmq-protocol` owns remoting headers, command bodies, wire encoders, and message codec compatibility. `rocketmq-store` and its implementation crates own persisted framing, recovery, and durability. Changes to Rust fields, Serde names, protocol encoding, and stored records therefore need separate compatibility reasoning.

A valid message model can still fail because the Topic does not exist, a property combination is unsupported, the caller lacks permission, or a store is unavailable. Model validation errors and operational errors are distinct. Likewise, a transaction state property does not create an atomic transaction with an application's database; the [transaction protocol](../producer/transaction-messages.md) requires durable business decisions and transaction checks.

## Reading the implementation

- [Model exports and boundaries](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/README.md), [queue identity](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/message.rs).
- [Producer message](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/message_single.rs), [extended message](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/message_ext.rs), [envelope](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/message_envelope.rs).
- Continue with [storage contracts](storage.md), [protocol and transport](protocol-transport.md), and [delivery and retry](../guides/delivery-and-retry.md).
