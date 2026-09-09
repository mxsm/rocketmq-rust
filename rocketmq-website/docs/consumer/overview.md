---
title: "Choose a consumer model"
---

Choose a consumer by how the application receives work, owns concurrency and acknowledges progress. The models share discovery and transport infrastructure, but they do not have interchangeable confirmation semantics.

## Compare the models

| Model | Application interface | Progress or acknowledgement | Suitable starting point |
| --- | --- | --- | --- |
| LitePull | Explicit polling loop | Queue offsets, automatic or application-controlled commit | An application that owns batching and processing flow |
| Push | Client invokes a concurrent or orderly listener | Listener outcome feeds the consumption/retry path | Callback-oriented applications |
| POP | Receive messages with receipt/invisibility state | Receipt-based ACK and visibility handling | Applications designed for POP's retry and receipt lifecycle |
| Classic Pull compatibility | Explicit queue/offset pull requests | Application-managed queue position and compatible offset operations | Maintaining an existing Classic Pull integration |

Use [LitePull](pull-consumer.md) for the first-message tutorial. [Push consumption](push-consumer.md) describes callbacks, and the [client source](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-client/src/consumer) contains the POP and Classic Pull surfaces.

Push is an application-facing programming model. In the ordinary Push path, the client performs pulling/long polling and dispatches messages; the name does not mean that a Broker opens an unsolicited connection to invoke your code.

Classic Pull's facade is deprecated. Its runtime-backed builder provides a compatibility path, while detached construction does not initialize a runnable client. New applications should choose the supported programming model they need rather than copying older constructors.

## Consumer Groups and queue assignment

In clustered offset-based consumption, members of a Consumer Group cooperate on queue assignments. Increasing consumer instances beyond available assigned queues does not create more queue-level parallelism. Independent applications that each need the full stream should use different groups.

Members of the same group should use consistent subscriptions and a compatible consumption model. A group name is not a per-message filter. Topic, namespace, group, expression and queue ownership all affect which data an instance can see.

Rebalance changes assignments as membership or routes change. An application may finish work near an ownership transition and later see a message again. Make side effects idempotent and distinguish currently assigned work from old in-flight processing.

## Processing success and consumption progress

Offset-based consumers track positions per queue, not a single total order across the Topic. A new group's initial-position policy applies when there is no usable stored position; it does not override an existing group's progress.

For LitePull, polling transfers messages to application code. It does not prove that an external database update completed. Automatic commit follows client progress, so asynchronous business work launched after polling can remain unfinished when progress advances. Start with explicit processing followed by deliberate commit if that distinction matters.

For Push, return a successful listener outcome only after the work represented by that callback is complete. Acknowledging and then dispatching untracked work to another executor separates the acknowledgement from its business effect.

For POP, retain and use the receipt associated with the delivery. Invisibility expiry can make an unacknowledged message eligible for redelivery. Extending visibility, retrying the business operation and acknowledging are separate actions; a stale receipt is not a general-purpose message identifier.

These models can support retries and duplicate delivery. None, by itself, creates an atomic transaction with your application's storage. See [delivery and retry](../guides/delivery-and-retry.md).

## Runtime, pressure and shutdown

Inject an application-owned `Arc<ClientRuntime>` into the chosen builder. Configure subscriptions and listeners before startup, then keep the application running while it owns work. Bound worker concurrency and retained batches so a slow dependency does not turn the client into an unbounded memory queue.

The zero-copy LitePull path returns owned `Arc<MessageExt>` values. Keeping those values alive retains their underlying data. Zero-copy changes copying and ownership costs; it does not remove memory accounting or business backpressure.

When stopping, stop accepting new business work, resolve the work already accepted according to its retry policy, and close the consumer before the shared client runtime and runtime owner. An interrupt should not silently translate unfinished work into successful consumption.

## Next steps

Follow [quick start](../getting-started/quick-start.md) for a complete matched application, then [LitePull](pull-consumer.md) for offsets and polling. Use [message filtering](message-filtering.md) when selecting only part of a Topic. If data is not arriving, [first diagnosis](../operations/first-diagnosis.md) starts with routes, subscriptions and assignment.

Sources: [client API overview](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md), [Classic Pull facade](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_pull_consumer.rs).
