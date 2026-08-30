# Remoting V2 processor migration and release notes

## Release classification

The removal of the V1 processor compatibility layer is source breaking and
belongs in a major-version release. Wire-level RocketMQ request and response
fields remain compatible; the break is in Rust processor composition,
response ownership, deferred handling, and transport capability access.

This release removes the processor-facing legacy adapter/bridge, legacy
connection-handler context and wrapper, direct response-write facade,
ambiguous `Ok(None)` response convention, and the corresponding processor
re-exports. `Channel` remains available where it is a legitimate client or
transport primitive; ordinary request processors no longer receive it.

## Processor migration

Implement `rocketmq_transport::api::v2::RequestProcessorV2`. A processor owns
one mutable `RemotingRequest` for the duration of the call and must return one
exhaustive `HandlerOutcome`:

```rust
use rocketmq_transport::api::v2::{
    HandlerOutcome, RemotingRequest, RequestProcessorV2, ResponsePlan,
};

#[derive(Clone)]
struct HealthProcessor;

impl RequestProcessorV2 for HealthProcessor {
    async fn process(
        &mut self,
        _request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        Ok(HandlerOutcome::Reply(ResponsePlan::empty_response(0)))
    }
}
```

Use the typed request projections instead of a legacy context:

- immutable ingress identity, original code/opaque, one-way state, origin,
  authentication, deadline, and session facts come from `RemotingRequest`;
- request-local cancellation, arbitrary session close, raw writer access, and
  complete `Channel` access are intentionally unavailable;
- service-specific push/request/close operations use the narrow capabilities
  injected by that service's composition root.

## Response migration

Replace direct writes and optional command returns with one explicit outcome:

| Legacy shape | V2 shape |
|---|---|
| return a response command | `HandlerOutcome::Reply(ResponsePlan)` |
| write through context, then return `Ok(None)` | return an owned `ResponsePlan`; the canonical writer performs the only write |
| one-way inferred from a mutable command after hooks | immutable ingress one-way identity suppresses all response writes |
| nullable “no response” | a sealed protocol no-response marker for an audited request code |
| retain a context/channel for long polling | take a deferred responder, complete a typed registration, and return `HandlerOutcome::Deferred` |

Construct a body-free response head, then transfer exactly one body owner with
`ResponsePlan::command`, `bytes`, `segments`, or `file_regions`. Do not attach a
body to the head before constructing the plan. The transport corrects response
type and original opaque at its private binding boundary and encodes the head
once. Segment and file-region bodies stay separate through the writer.

`Reply`, `Deferred`, and protocol no-response are affine terminal choices.
Duplicate response attempts are rejected and observed. A write result with
`PossiblyPartial` progress is terminal and must never be retried; only a
`NotStarted` result may be considered for a separately bounded, idempotent
retry policy.

## Deferred migration

Long-poll and other deferred paths must complete the following ownership
sequence:

1. validate typed request/origin/session facts and the effective deadline;
2. reserve global/per-key count and retained-byte capacity;
3. reserve a deferred wait permit;
4. finish all recoverable work before taking the responder;
5. register the affine responder and request/session provenance;
6. return `HandlerOutcome::Deferred`;
7. claim and resume through the session-owned execution/writer path;
8. release wait/execution permits, indexes, leases, and responder state on
   success, timeout, cancellation, session close, or shutdown.

Do not retain a channel, context wrapper, raw cancellation token, or detached
task. Producers and scanners must be owned by an injected `TaskGroup`; shutdown
seals new work and drains accepted handler/writer work before reporting health.

## Embedded calls

Embedded Broker/Proxy calls use the V2 embedded dispatcher with an authoritative
principal. The transport constructs a channel-free embedded session; command
headers cannot forge the principal. No loopback socket, `ChannelInner`, pending
owner, writer task, or encoded frame is created for the local response handoff.

## Rollout and observation

Before the source-breaking release, enable the V2 composition one service at a
time in this order:

1. transport test service;
2. Namesrv and Controller;
3. Broker, including Pull/Pop/Notification/PopLite deferred paths;
4. Proxy and embedded Broker calls.

At each step, compare a stable traffic window with the preceding service
baseline and observe:

- `rocketmq_transport_response_total` by result/mode;
- `rocketmq_transport_response_duplicate_total` and
  `rocketmq_transport_response_abandoned_total` (no unexplained increase);
- `rocketmq_transport_deferred_inflight` and
  `rocketmq_transport_deferred_retained_bytes` (bounded in steady state and
  zero after shutdown);
- `rocketmq_transport_response_queue_wait_seconds` and
  `rocketmq_transport_request_duration_seconds` P50/P99;
- service throughput and CPU;
- typed session/Broker shutdown reports, writer queued items/bytes, deferred
  invariant failures, and task leaks.

Pause promotion on a duplicate/abandoned response increase, unbounded deferred
gauges, persistent queue-wait regression, unhealthy shutdown report, or
throughput/P99 regression beyond the reviewed budget. Investigate by request
code class, response mode, body kind, and stable failure category; never add
credentials, message bodies, complete requests, or high-cardinality identities
to telemetry.

During the pre-release migration window, rollback is performed by restoring
the previous service composition as a whole. Do not introduce a per-request
dynamic boolean that selects V1 or V2. After this source-breaking release the
V1 processor adapter is absent, so rollback means deploying the preceding
release, not activating an implicit in-process fallback.

## Release-note summary

- **Breaking:** ordinary processors now implement `RequestProcessorV2` and
  return `HandlerOutcome`; the processor-facing connection context, adapter,
  bridge, direct-write methods, and nullable response convention are removed.
- **Response semantics:** one affine response owner reaches the canonical
  writer; original one-way state is immutable; write completion means local
  socket write/flush, not peer processing; possibly-partial writes are not
  retried.
- **Deferred semantics:** a responder is taken once, registration is
  provenance-bound, and all terminal paths release bounded permits and leases.
- **Capability restrictions:** processors receive typed request/session facts
  and service-specific narrow capabilities, not raw channel, close,
  cancellation, or writer authority.
- **Operations:** V2 response/deferred/writer metrics and typed shutdown reports
  are the rollout signals. Benchmark results are engineering-review evidence,
  not a custom CI gate.
