---
title: "Client coordination and request ownership"
---

The Rust client connects application-facing producer and consumer APIs to route discovery, Broker connections, retries, and consumption coordination. Its shared machinery belongs to an application-supplied `ClientRuntime`; creating a producer does not authorize a hidden fallback runtime.

## Ownership and shared state

The application creates a `RuntimeOwner`, gives a child service context and telemetry handle to `ClientRuntime::try_new`, and shares the resulting `Arc<ClientRuntime>` with client facades. Public imports come from `rocketmq_client_rust` or its `prelude`. Internal factory and processor modules describe implementation, not supported application import paths.

`MQClientInstance` brings together producer/consumer registration, Topic route caches, Broker address state, heartbeat coordination, transport callbacks, and scheduled refresh/rebalance work. Shared runtime ownership does not mean every facade has the same group, subscription, or client identity. Identity and group configuration still determine how a Broker observes the client.

## From Topic to request

1. Resolve configured NameServer endpoints, optionally through the explicitly selected discovery implementation.
2. Obtain or refresh Topic route data and derive publish/subscribe queue views.
3. Select a queue and resolve its Broker endpoint using the operation's routing policy.
4. Submit the request through managed transport with the remaining deadline and applicable admission limits.
5. Interpret both transport outcome and Broker result; update routing/fault information and choose any allowed retry.

Route caches reduce discovery traffic, but can be stale after Broker movement or a role change. The route-update coordinator and scheduled refresh keep local views current; an individual operation still needs to handle missing routes and unreachable advertised endpoints. NameServer connectivity and Broker connectivity are separate dependencies.

The ordinary producer retry loop carries a shared deadline, checks it before work, and passes a derived attempt deadline into the send kernel. It does not restart a full timeout budget for every attempt. Retry policy considers route failures, send status, remaining attempts, and communication mode; it can refresh a route or select another Broker. A selector or explicitly targeted queue has its own semantics, so do not assume every send overload applies identical failover behavior.

## Completion is layered

| Observed result | What the application knows | What remains uncertain |
| --- | --- | --- |
| Local validation/admission rejection before sending | This attempt did not reach the Broker through that path | Whether an earlier attempt succeeded |
| Local write completion | Transport completed the local write operation | Broker processing and persistence |
| Valid send response | Broker returned a particular send status | Business consumption and any durability beyond that status |
| Response timeout or disconnect after dispatch | No conclusive response was observed | Whether the Broker accepted or completed the request |
| Cancelled client work | Local waiting/work has stopped according to its owner | Whether a remote operation can still complete |

Callback and one-way APIs expose different completion contracts from a response-bearing send. An `Ok` outer result is not always a `SendOk` acknowledgement; inspect the selected API's return/callback path. See [sending messages](../producer/sending-messages.md).

Application retries must include idempotency and an overall deadline. Wrapping the client's bounded retries in an unbounded application loop defeats that bound and can create duplicate effects.

## Consumption coordination

| API | Coordination and completion |
| --- | --- |
| Push concurrent | Client pulls/receives and schedules callbacks; acknowledgement and retry follow callback outcome |
| Push orderly | Queue ownership/locking and ordered callbacks constrain processing within a queue |
| Lite Pull | Client coordinates subscribed queues and polling; the application chooses when processed progress is committed |
| Classic Pull compatibility | Caller supplies queue/offset operations through a deprecated facade backed by the current client |
| POP path | Broker delivery invisibility and receipt-based acknowledgement supplement client session/assignment state |

Rebalance changes queue ownership; it does not make a previous handler's side effects disappear. Group members should use compatible subscriptions and processing semantics. Commit only progress that the application can safely resume from. Lite Pull's local commit state and its remote persistence are separate steps; inspect [Pull consumption](../consumer/pull-consumer.md).

Shared message references can outlive a poll and retain memory. Bound application work as well as the client's queues. Synchronous Push listener code uses the managed blocking execution boundary; long business operations can still occupy that capacity until they return.

## Closing the client

Stop new application work, finish or deliberately abandon in-flight business operations within the chosen deadline, and shut down producer/consumer/Admin facades. Then await `ClientRuntime` shutdown and inspect its report. Close telemetry after instrumented service work and close the owning runtime outside its async execution boundary.

Dropping one facade cannot stand in for closing the shared runtime. Likewise, a cancelled async wait cannot forcibly stop a blocking callback that already started. The shutdown report and outstanding business work must both be considered.

## Source map

- [Client entry points and features](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md).
- [Client instance](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/factory/mq_client_instance.rs), [route coordinator](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/factory/route_update.rs).
- [Producer retry loop](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/producer/producer_impl/default_mq_producer_impl/retry.rs).
- [Runtime ownership](runtime.md), [ordered messages](../guides/ordered-messages.md), [POP](../consumer/pop.md).
