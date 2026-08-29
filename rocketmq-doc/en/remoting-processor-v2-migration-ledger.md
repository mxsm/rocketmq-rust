# Remoting Processor V2 Migration Ledger

This ledger records the final MIG-07 inventory for removing production
dependencies on the V1 `RequestProcessor` contract,
`ConnectionHandlerContext`, and raw `Channel` parameters. It is an
implementation and review aid, not a source fingerprint, generated baseline,
or release gate. It describes the completed ownership model and the remaining
explicitly frozen Transport compatibility surface.

## Status and ownership model

| Status | Meaning |
|---|---|
| `V2 production` | The live application route uses a typed V2 processor, typed response outcome, and explicit session/capability ownership. |
| `V2 infrastructure` | V2 transport, deferred-response, capability, or lifecycle infrastructure and its tests; not itself an application route. |
| `Frozen compatibility` | Transport-only V1 code retained deliberately for compatibility coverage or a bounded migration window. It must not be selected by an application production composition root. |
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

MIG-07 is complete when every application production consumer listed below is
`V2 production`, all non-Transport V1 ownership seams are `Removed`, and the
only remaining V1 inventory is the explicitly listed Transport compatibility
surface.

This cutover is an intentional source-breaking API migration. Public
application-facing V1 registration, lifecycle, and processor entry points are
not preserved as deprecated shims because doing so would retain the production
authority this stage removes. Downstream source consumers must migrate to the
typed V2 contracts before adopting the release that contains MIG-07, and that
release must be classified and communicated as breaking by the release
process. Wire-level RocketMQ framing, request codes, headers, and valid-input
response semantics are unchanged. Controller heartbeat leases are now bounded
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
| Proxy-local | V2 compatibility boundary for local broker calls | `V2 production` | Local query/send/recall/transaction/pop/pull/ack/offset paths use V2 compatibility; the old `ProcessRemoting` actor and wrappers are removed. |

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

## Transport frozen compatibility inventory

The following items remain intentionally available only as compatibility
coverage or a future FIN-05 removal unit. They are not production application
consumers and must not be expanded or used as a migration shortcut:

| Compatibility item | State and rule |
|---|---|
| `rocketmq-transport` V1 `RequestProcessor` and `ConnectionHandlerContext` types | `Frozen compatibility`; preserve only the public/legacy contract needed by existing compatibility callers and tests. |
| `RemotingGeneralHandler<RP>` and the V1 `TransportServer<RP>` launch/connection path | `Frozen compatibility`; no application composition root may select it after the V2 cutover. |
| V1 `AuthorizedDispatcher` bridge and `LegacyProcessorAdapter` | `Frozen compatibility`; do not add new callers; remove during FIN-05 after compatibility users are retired. |
| Transport V1 server/adapter tests | `Frozen compatibility`; they verify the compatibility surface and are not evidence of a production V1 route. |

No V1 compatibility item is retained in Broker, Client, Namesrv, Controller,
Auth, Proxy, or Proxy-local production ownership. FIN-05 may remove the
Transport inventory as a separate compatibility cleanup without changing the
V2 application contract.

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
production path is not an accepted compatibility exception unless it is one of
the four Transport items listed above.

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
Classify every match as V2 infrastructure, frozen Transport compatibility,
test fixture, or ordinary same-name business code. Do not save hashes, create
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
  'process_remoting(_with_timeout)?\(|LocalBrokerCommand::ProcessRemoting\b' `
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
- [x] Only the explicitly listed Transport V1 compatibility inventory remains.
- [x] Historical fixtures and same-name business methods have an explicit
  classification.
- [x] No fingerprint, baseline, source hash, or custom gate is introduced.
- [ ] FIN-05 removes the frozen Transport compatibility inventory after its
  compatibility callers and tests are retired.
