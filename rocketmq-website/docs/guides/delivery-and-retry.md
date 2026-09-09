---
title: "Delivery, acknowledgements and retries"
---

Reliable messaging requires an explicit answer to two questions: what has completed when an operation returns, and what may happen if that return is lost? The answer depends on the send path, storage policy, consumer model and business transaction.

## Separate the completion boundaries

| Boundary | What happened | What remains independent |
| --- | --- | --- |
| Accepted | The selected Broker write path accepted the message | Required disk/replica waits and visibility |
| Durable under a policy | The relevant persistence/replication condition completed | Stronger failure models outside that policy |
| Visible | Reads can discover the message through the selected path | Delivery to every consumer |
| Delivered | The client obtained the message | Business-side effects |
| Processed | The application completed its work | Consumption progress persistence |
| Progress recorded | The chosen progress/ACK path advanced | An atomic transaction with an external database |

The main message log and its derived structures serve different purposes. Building a ConsumeQueue, index or timer view cannot strengthen an earlier write acknowledgement. Likewise, a healthy replica connection is different from an acknowledgement that waits for replica progress.

## A send timeout is an unknown outcome

```mermaid
sequenceDiagram
    participant P as Producer
    participant B as Broker
    participant C as Consumer
    participant D as Business database
    P->>B: Send event with stable business ID
    B->>B: Accept according to configured write policy
    alt Response reaches producer
        B-->>P: Send result
    else Response lost or late
        Note over P,B: Producer times out; stored outcome may be unknown
        P->>B: Retry with the same business ID
    end
    C->>B: Poll eligible messages
    B-->>C: One or more deliveries
    C->>D: Apply effect and deduplicate in one transaction
    D-->>C: Business transaction complete
    C->>C: Update client consumption progress
    C->>B: Separate progress persistence or ACK path
    Note over C,D: Failure after the business commit can cause replay
```

The diagram shows failure windows rather than a universal storage sequence. A Broker may have stored a message before the producer's deadline expires. Retrying can therefore create another delivery even if the first attempt returned an error.

Inspect [send statuses](../producer/overview.md), including disk or replica timeout outcomes. A one-way send intentionally has no Broker response to inspect. “No exception” cannot be used as a common durability definition across every send variant.

## Retry at the correct layer

| Layer | Trigger | Responsibility |
| --- | --- | --- |
| Transport/client send | Connection failure, timeout or selected retryable response | Bound attempts by a total deadline; account for uncertain writes |
| Application send | Business workflow still needs publication | Preserve event identity and avoid multiplying client retries without a limit |
| Consumption | Processing failure or incomplete acknowledgement/progress | Apply the selected consumer model's retry behavior |
| Business effect | External dependency or transaction failure | Use idempotency, transactional state and an explicit failure policy |

Do not treat a retry counter as a delivery guarantee. Retries can exhaust, resource permissions can reject requests, stored data can expire, and a consumer can commit progress before its business work completes.

For LitePull, the application owns the polling loop and processing decision. `commit_all` updates client offset-store state; current implementation details and persistence limits are explained in [LitePull consumption](../consumer/pull-consumer.md). A successful outer return is not a durable per-queue acknowledgement.

For Push, a listener's result participates in the client/Broker retry flow. For POP, the receipt and invisibility window determine acknowledgement and eligibility for redelivery. Offsets, listener outcomes and POP receipts should not be substituted for one another.

## Make the business operation idempotent

Give the business event a stable identity, such as an order event ID plus its event type/version. Within the database transaction, conditionally record that identity and apply the intended effect. A duplicate should observe the previously committed result rather than repeat the effect.

For example, a payment-recording consumer can use a unique event ID in the same transaction that records the payment state. If the process fails after that transaction but before progress persistence, replay finds the event ID and avoids applying the effect twice.

An in-memory set is insufficient across process loss, and checking for an ID before starting a separate transaction leaves a race. The deduplication record needs a retention policy that covers possible replay. A Broker message ID or a message key does not automatically implement this database invariant.

Publishing alongside a database update has a separate dual-write problem. An outbox or a suitable transaction-message design can coordinate that workflow, but the chosen design must explain its own recovery and duplicate behavior. Merely selecting the transaction producer does not make all external systems participate in one transaction.

## Order and retries

Queue-level ordering requires consistent queue selection and a processing model that preserves the intended order. Parallel queues and concurrent business workers do not create a global sequence. A failed earlier message creates a decision: block later work, retry within the ordering constraint, or apply a documented business exception policy.

Delayed or retry delivery also does not promise an exact business execution time. Broker eligibility, polling, consumer availability and downstream capacity all contribute to the observed delay.

## Decide before production use

Write down the failure model the application must tolerate, its business event identity, the point at which work is considered complete, the progress/ACK path, and what happens after retry exhaustion. Keep the actual storage and topology configuration beside those decisions.

The [local tutorial](../getting-started/quick-start.md) exercises ordinary sending and LitePull processing on one Broker. It does not establish replication, failover or exactly-once end-to-end effects.

Sources: [send outcomes](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/result.rs), [store contracts](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-store-api), [LitePull progress implementation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/default_lite_pull_consumer_impl.rs).
