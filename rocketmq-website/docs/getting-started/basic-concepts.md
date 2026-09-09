---
title: "Messaging concepts"
---

A message system separates producing an event from processing it. The Broker stores and serves messages while clients discover routes and track consumption. Use the concepts below to reason about placement, progress, and failure before choosing an API.

## Topic, queue and message

A **Topic** is a logical stream such as `DocsFirstMessage`. A Topic can have several message queues on one or more Brokers. A **message queue** identifies a Topic, Broker, and queue ID; offsets are meaningful within that queue. Queue 0 on Broker A is not the same sequence as queue 0 on Broker B.

A message has a body and metadata such as tags, keys, and properties. A **tag** supports a subscription filter; a **key** can help locate a message or carry an application identifier. Neither a tag nor a message key automatically establishes business deduplication.

Consider an order-created event. The body contains the event, the Topic groups this class of events, and an application business identifier lets the downstream database recognize repeated processing. The Broker message ID is useful for diagnostics but should not replace an explicit business idempotency strategy.

## Producer Group and Consumer Group

A **Producer** discovers writable queues and sends messages. Its group identifies the producer context; transaction messaging adds specific group and callback requirements. A send call and a local business transaction are separate operations unless your application deliberately coordinates them.

A **Consumer Group** represents a consumption subscription/progress identity. In clustering mode, consumers in the group share work through queue allocation and coordination. A different group can consume the same Topic independently. Adding consumers to an existing group is therefore different from giving every consumer a new group name.

Use consistent subscriptions within a group. Changing Topic, filter, or consumption mode on just one member can produce surprising delivery or coordination behavior. Broadcast consumption is a distinct mode with different progress and retry assumptions.

## Routes are metadata, not the message payload

A NameServer receives Broker registration and answers route queries. Clients use the returned Broker addresses to communicate with Brokers. The payload does not flow through the NameServer.

The address a Broker advertises must be reachable from its clients. A container can successfully register a private address while a host client cannot connect to that address. Checking only the NameServer port cannot diagnose this second hop.

## Consumption position and acknowledgement

| Concept | Meaning | It does not prove |
| --- | --- | --- |
| Queue offset | Position in one queue's sequence | Global order across all queues |
| Consumer position | Where a consumer is reading or has advanced locally | That progress is durably committed to its shared store |
| Committed offset | Progress recorded for later continuation | An atomic commit of the application's database transaction |
| Send acknowledgement | Broker response under the configured write policy | That every consumer has processed the event |
| POP receipt/ACK | Delivery receipt and completion for a POP attempt | The same operation as an ordinary LitePull offset commit |

With LitePull manual commit, process a batch successfully before committing its progress. If a process fails after the business write but before progress is recorded, it may process the event again. Committing first creates the opposite failure window: progress can advance before the business operation succeeds.

`ConsumeFromFirstOffset` is an initial-position policy. It does not reset an existing group's stored progress on every restart. Reusing a Consumer Group normally resumes its established position.

## Ordering, retries and delayed delivery

Ordering is scoped to the queue and processing model that preserve it. Multiple queues provide parallelism; they do not establish one global sequence. A retry can delay later processing or expose duplicates depending on the selected model.

Delayed delivery asks the system to make a message available later; it is not a guarantee of execution at an exact wall-clock instant. Consumer scheduling, load and failures still affect processing time.

For all models, distinguish “accepted,” “durable under a policy,” “visible to reads,” “delivered,” and “business processing completed.” [Delivery and retry](../guides/delivery-and-retry.md) expands those boundaries.

## Apply the concepts

The [first-message tutorial](quick-start.md) uses one Topic, one explicit Consumer Group, and manual LitePull commits. Keep those names aligned before troubleshooting connectivity. [First diagnosis](../operations/first-diagnosis.md) follows the route from process configuration to Topic metadata and consumer progress.

Source definitions: [message and queue models](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-model/src/common/message), [consumer APIs](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-client/src/consumer), and [protocol heartbeat types](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-protocol/src/protocol/heartbeat).
