# Remoting Processor V2 Migration Ledger

This ledger records the final MIG-07 inventory for removing production
dependencies on the V1 `RequestProcessor` contract,
`ConnectionHandlerContext`, and raw `Channel` parameters. It is an
implementation and review aid, not a source fingerprint, generated baseline,
or release gate. It describes the completed ownership model and records the
Transport compatibility surface removed by FIN-05.

## Status and ownership model

| Status | Meaning |
|---|---|
| `V2 production` | The live application route uses a typed V2 processor, typed response outcome, and explicit session/capability ownership. |
| `V2 infrastructure` | V2 transport, deferred-response, capability, or lifecycle infrastructure and its tests; not itself an application route. |
| `Historical frozen compatibility` | Transport-only V1 code that was retained during MIG-07 and removed by FIN-05. It is a historical classification, not an available composition choice. |
| `Removed` | A production V1 leaf, aggregate adapter, raw-channel manager, or legacy long-poll ownership seam has been deleted. |

Migration ownership remains grouped by the implementation batches defined by
the connection-handler-context plan:

- `MIG-02`: Transport, Client, Namesrv, and Controller processor foundations.
- `MIG-03`: ordinary Broker leaves and session identity registries.
- `MIG-04`: response-heavy Broker leaves and deferred-response seams.
- `MIG-05`: Broker aggregate/router production cutover and bounded V1 drain.
- `MIG-06`: Admin nested handlers, Auth, and Proxy foundations.
- `MIG-07`: final production V1 removal, typed server callbacks, and
  compatibility cleanup.

MIG-07 is complete: every application production consumer listed below is
`V2 production`, all non-Transport V1 ownership seams are `Removed`, and
FIN-05 has removed the previously frozen Transport processor compatibility
surface.

This cutover is an intentional source-breaking API migration. Application
composition roots do not preserve V1 registration or lifecycle shims because
doing so would retain the production authority this stage removes. The former
Transport-only compatibility API and its tests were removed by FIN-05.
Downstream source consumers must migrate to the typed V2 contracts before
adopting this release, and the release
must be classified and communicated as breaking by the release process.
Wire-level RocketMQ framing, request codes, headers, and valid-input response
semantics are unchanged. Controller heartbeat leases are now bounded
to 24 hours so untrusted values cannot pin the generation-fencing table
indefinitely. Broker Controller-mode configuration enforces the same shared
limit at startup; deployments with a larger
`controllerHeartbeatTimeoutMills` value must lower it before upgrading.

## Final production inventory

| Area | Production owner | MIG-07 state | Completion evidence |
|---|---|---|---|
| Transport | V2 dispatch, response plans, deferred registration, session registry, and `TransportServerV2` | `V2 production` plus `V2 infrastructure` | Application composition roots select V2 server/dispatcher owners; no production route depends on a V1 context callback. |
| Client | V2 inbound client ownership and typed outbound/session capabilities | `V2 production` | Client callback helpers use typed outcomes and capability senders; raw `Channel`/`ConnectionHandlerContext` is not an application callback contract. |
| Namesrv | Typed request processor, broker-session registration, and typed session lifecycle | `V2 production` | Registration, heartbeat refresh, and cleanup are bound to the current `SessionId`; a superseded session cannot refresh or remove its replacement. |
| Controller | Typed request processor and generation-fenced `BrokerSessionId` heartbeat/close lifecycle | `V2 production` | Only an admitted current generation reaches the Raft heartbeat write. Superseded sessions skip replication, bounded-capacity rejection is explicit, invalid identities/timeouts consume no fencing capacity, and tombstones outlive the accepted heartbeat lease. |
| Broker | V2 aggregate/router, V2 leaves, typed deferred services, and typed client callback capabilities | `V2 production` | Production route is V2; legacy leaves, aggregate adapters, long-poll owners, and raw-channel manager tables are removed. |
| Auth | Typed request/session/auth provenance | `V2 production` | The production remoting path derives provenance from the typed V2 request and no longer recovers identity by downcasting a raw context or channel. XCT-01 owns the later single-security-boundary consolidation. |
| Proxy | V2 request ingress, typed server callbacks, and atomic session binding | `V2 production` | Proxy ingress uses V2 dispatch; heartbeat admission and session binding are atomic and generation-safe. |
| Proxy-local | Affine V2 embedded-response boundary for local broker calls | `V2 production` | Local query/send/recall/transaction/pop/pull/ack/offset paths consume `EmbeddedResponse` directly; segments retain their owners and no V2 outcome is reconstructed as a legacy command. The old `ProcessRemoting` and `ProcessRemotingV2Compatibility` actor variants and wrappers are removed. |

## Typed capability and lifecycle boundaries

The V2 design keeps request processing separate from connection authority. A
processor receives a typed request and may return a typed response plan; it
does not receive an arbitrary `ConnectionHandlerContext` or `Channel` merely
to perform a later callback.

- `ServerPushSender` is the typed one-way/server-push capability. Its command
  allow-list covers the supported broker-to-client notifications and callback
  messages.
- `ServerRequestSender` is the typed request/response capability. It uses a
  per-session pending-request owner, correlates responses within that owner,
  applies an absolute deadline, and fails pending requests when the session
  closes. An opaque value from another session cannot complete the request.
  Cancellation after a request may have reached the socket retires the exact
  pending owner and aborts its session, preventing a late response from being
  reused by a later request.
- `SessionCloseHandle` is the typed close capability. The public capability
  exposes a close operation with a typed `SessionCloseReason`; composition
  roots retain the internal immediate-close operation. Session retirement is
  cancellation-safe: dropping retirement while accepted work drains aborts the
  exact session instead of leaving an unowned live connection.
- Broker `ClientSessionTransport` groups the session identity and the permitted
  push, request, and close capabilities. Proxy `RemotingSessionCapability`
  retains only the push and close capabilities it needs. Neither registry
  exposes a raw channel.
- Proxy heartbeat admission validates identity, header/body/client identity,
  and authorization before atomically committing the session binding. A
  generation-aware reverse index prevents an old connection's close event
  from removing a newer replacement session.

These boundaries preserve real response delivery, callback correlation,
disconnect failure, and deadline behavior without restoring V1 write authority
through an extension or downcast.

## Transport FIN-05 removal record

The following items formed the frozen MIG-07 compatibility unit. FIN-05 removed
them together, so no partial legacy processor authority remains:

| Former compatibility item | FIN-05 state |
|---|---|
| `rocketmq-transport` V1 `RequestProcessor` and processor-facing `ConnectionHandlerContext` types | `Removed`; V2 processors own `RemotingRequest` and return `HandlerOutcome`. |
| `RemotingGeneralHandler<RP>` and the V1 `TransportServer<RP>` processor launch/connection path | `Removed`; `TransportServerV2` is the processor server boundary. |
| V1 `AuthorizedCommandDispatcher` bridge, `LegacyRequestBridge`, and `LegacyProcessorAdapter` | `Removed`; no dispatcher can reconstruct legacy processor authority. |
| Transport V1 processor/adapter tests and local materialization fixtures | `Removed` or rewritten as V2 contract/wire tests; historical ADR text remains documentation only. |

No V1 compatibility item is retained in Broker, Client, Namesrv, Controller,
Auth, Proxy, Proxy-local, or Transport processor ownership. Removing this unit
does not change the V2 application contract or RocketMQ wire format.

## Source-breaking migration map

The left-hand surfaces are no longer exported. Migrate each capability as a
design change rather than mechanically renaming a parameter:

| Deprecated V1 surface | V2 replacement |
|---|---|
| `RequestProcessor` / `LocalRequestProcessor` | `RequestProcessorV2` / `LocalRequestProcessorV2` |
| `AuthorizedCommandDispatcher` | `AuthorizedCommandDispatcherV2` |
| `ConnectionHandlerContext` / `ConnectionHandlerContextWrapper` | the owned `RemotingRequest` aggregate |
| `ConnectionHandlerContext::channel()` / `connection_ref()` | `RemotingRequest::session()` and `RequestControlView` for read-only facts; composition-owned `ServerPushSender` or `SessionCloseHandle` only when that authority is required |
| swallowing `write_response*` / `write*` methods | return `HandlerOutcome::Reply(ResponsePlan)` or claim one `DeferredResponder` and call `respond` |

An inline processor moves response ownership into its return value:

```rust,ignore
impl RequestProcessorV2 for Processor {
    async fn process(
        &mut self,
        request: &mut RemotingRequest,
    ) -> rocketmq_error::RocketMQResult<HandlerOutcome> {
        let response = RemotingCommand::create_success_response_command();
        Ok(HandlerOutcome::Reply(ResponsePlan::command(response)?))
    }
}
```

A deferred processor reserves and transfers exactly one response capability;
it does not retain a channel or handler context:

```rust,ignore
let responder = request.take_deferred_responder()?;
let registration = registry.register(request, responder)?;
Ok(HandlerOutcome::Deferred(registration))
```

Server-initiated pushes and closes are resolved from the session registry by a
composition root and stored only where those explicit capabilities are needed.
Ordinary processors cannot recover either capability from `RemotingRequest` or
its extension store.

## Removed production seams

MIG-07 removes the following classes of non-Transport V1 ownership. The
checklist below records the accepted final tree rather than preserving an
intermediate dual implementation:

- Broker V1 aggregate/router adapters and production `RequestProcessor`
  projections.
- Broker legacy leaf wrappers that only existed to reconstruct a context.
- Broker legacy Pull, Pop, Notification, and PopLite long-poll waiter/service
  owners that stored or forwarded `ConnectionHandlerContext`.
- Broker producer/consumer raw `Channel` callback tables and duplicate V1
  session registries.
- Proxy-local `ProcessRemoting` actor and `process_remoting` wrappers.
- Namesrv and Controller raw-channel registration, heartbeat, and close
  callbacks.
- Auth and Proxy context/channel downcasts used to recover source identity.

The old names remain useful in historical migration notes, but a match in a
production path is not an accepted compatibility exception.

## Historical responsibilities and exclusions

The earlier migration batches remain useful for ownership and regression
triage:

- `MIG-02` established typed processor foundations and session lifecycle ports.
- `MIG-03` and `MIG-04` moved Broker leaves to typed response/deferred plans.
- `MIG-05` switched Broker production composition to the V2 aggregate and
  drained accepted legacy work.
- `MIG-06` migrated nested Admin handlers, Auth provenance, and Proxy ingress.
- `MIG-07` removes the remaining production V1 consumers and makes server
  callbacks capability-based.

The following are not production V1 inventory and should not be mechanically
converted:

- Business methods named `process_request` that implement neither remoting
  processor trait.
- The typed Transport `SessionProcessor` contract used by local/network test
  servers.
- V2-only response-plan, provenance, deferred-registration, deadline,
  lifecycle, and wire-delivery fixtures.
- Outbound uses of Transport `DefaultRequestProcessor` that do not select a
  server-side V1 route.

## Re-scan and acceptance checklist

Run these simple path-based searches manually while reviewing the final tree.
Classify every match as V2 infrastructure, historical documentation, a V2 test
fixture, or ordinary same-name business code. A production match for the
removed processor compatibility types is a defect. Do not save hashes, create
a fingerprint file, or turn these searches into a custom gate.

```powershell
rg -n --glob '*.rs' `
  'ConnectionHandlerContext|ConnectionHandlerContextWrapper|impl.*RequestProcessor' `
  rocketmq-transport/src rocketmq-broker/src rocketmq-client/src `
  rocketmq-namesrv/src rocketmq-controller/src rocketmq-proxy/src `
  rocketmq-auth/src

rg -n --glob '*.rs' `
  'ctx\.channel\(\)|try_write_response\(|write_response\(|send_frame_segments|send_file_regions_response' `
  rocketmq-broker/src/processor rocketmq-broker/src/long_polling

rg -n --glob '*.rs' `
  'process_remoting(_with_timeout)?\(|process_remoting_v2_compatibility|LocalBrokerCommand::ProcessRemoting(V2Compatibility)?\b' `
  rocketmq-proxy-local/src
```

- [x] Transport, Client, Namesrv, Controller, Broker, Auth, Proxy, and
  Proxy-local production consumers are V2.
- [x] Typed server push, request/response, and close capabilities are used at
  callback boundaries.
- [x] Proxy heartbeat admission and session binding are atomic and
  generation-safe.
- [x] Broker legacy leaves, long-poll owners, raw-channel callback managers,
  and V1 aggregate adapters are removed.
- [x] FIN-05 removed the complete frozen Transport V1 processor compatibility inventory.
- [x] Historical fixtures and same-name business methods have an explicit
  classification.
- [x] No fingerprint, baseline, source hash, or custom gate is introduced.
- [x] Compatibility callers and tests were retired before the source-breaking
  FIN-05 removal.
