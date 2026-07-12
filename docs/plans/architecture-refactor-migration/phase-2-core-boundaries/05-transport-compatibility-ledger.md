# M05 transport compatibility and ownership ledger

This ledger freezes the M05 canonical ownership boundary, the legacy compatibility envelope, admission policy,
and the decision on the unused parallel connection implementation.

## Canonical ownership

| Surface | Canonical owner | Compatibility path |
|---|---|---|
| Pending request table and owner-scoped guards | `rocketmq-transport::base` | `rocketmq_remoting::base` exact re-export |
| Codec and bounded frame limits | `rocketmq-transport::codec` | `rocketmq_remoting::codec` exact re-export |
| Connection and active socket shutdown | `rocketmq-transport::connection` | `rocketmq_remoting::connection` exact re-export |
| TLS runtime and client connector | `rocketmq-transport::tls` | Connector functions are exact re-exports; runtime adapter preserves the legacy standalone task owner/report name |
| `ServerConfig`, `TlsConfig`, and `TlsMode` | `rocketmq-transport::config` | Common and Remoting legacy paths exact re-export |
| Adaptive encode buffer | `rocketmq-transport::smart_encode_buffer` | Remoting legacy module exact re-export |
| Transport error constructors | `rocketmq-transport::error_helpers` | Remoting legacy module exact re-export |
| Low-level request client/server and admission | `rocketmq-transport` | Available through `rocketmq_remoting::transport`; high-level client and Broker composition remain owner adapters |

Remoting retains `default = [tls]`. Its `tls`, `simd`, and `observability` features forward to transport, and
transport forwards SIMD to protocol and observability to its optional owner. Public wire types remain canonical
protocol types, so the move does not introduce a schema conversion.

## Lifecycle and overload policy

Every pending request reserves an opaque before send and owns count plus retained-byte capacity. Response,
timeout, send failure, owner close, and guard drop compete through one completion path. A retired owner cannot
reuse an opaque; reconnect creates another owner, preventing a late response from completing a new request.
Callback and batch response timeouts await retirement of the same canonical session before the pooled connection
can be reused. Queued sends hold a Tokio lifecycle read guard across state validation, admission, enqueue, and
writer completion. Retirement takes the matching write guard, sends a writer-close command, and waits for the
socket close acknowledgement before marking the shared state closed and cancelling the session group. Its
five-second absolute deadline covers both gate acquisition and writer completion; expiry marks the session closed
and aborts the owned writer task, including a writer blocked in socket I/O. The synchronous `Connection::close`
facade remains an immediate compatibility operation and does not hold any synchronous lock across an await. A
closed queued connection rejects later sends before enqueueing.

Admission is hierarchical across global, per-IP, optional per-tenant, and per-session budgets. Connection and
handshake exhaustion closes/rejects the connection before work is admitted; inflight, queued, and processor
exhaustion returns an explicit rejection policy. Control traffic has a bounded reserve. Metric events omit scope
identifiers and use non-blocking `try_send`, so a missing or slow collector cannot block the data plane.
Idle scoped budgets are reclaimed at the cardinality boundary only when the map owns the last reference and all
permits are released. Decoded admission bytes cover length prefix, serialized header, and body through a
transport-only envelope; the protocol command and its wire/JSON/debug/equality shape remain unchanged. The public
`CompositeCodec` decoder therefore continues to yield `RemotingCommand`; retained-byte metadata is available only
through the crate-private canonical session codec. Queued responses inherit the originating request class so
control responses can consume only the bounded control reserve. The production Remoting adapter rebinds each
processor context to that request's actual response connection. Its session map retains a stable connect-time
context for lifecycle events; it is never rebound to a later command. Every command instead receives an
independent Channel/ConnectionHandlerContext snapshot whose queued connection permanently carries that request's
admission class. A real HeartBeat exchange verifies the control reserve while the data writer budget is
saturated, and a delayed Data response on the same socket proves it cannot inherit the HeartBeat class or borrow
the reserve.

The default values preserve the current permissive envelope and remain explicitly configurable. They are not
presented as production tuning recommendations; deployment-specific profiling is required before changing them.

## Security and dependency closure

Transport borrows protocol commands into `SecurityRequestView` and accepts injected `RequestPolicy` and
`OutboundSigner` interfaces. It does not select or depend on an authentication provider. Its normal dependency
closure contains protocol, security API, runtime, error, and optional observability owners, and excludes Common,
Remoting, Broker, Store, the high-level Client, legacy DTO crates, and authentication providers.

Production Remoting client/server builders expose additive optional transport-security injection. Defaults are
`None` and preserve the previous no-op behavior. The outbound signer runs immediately before canonical send;
the server installs the policy/principal on `TransportListener`. `PeerInfo.tls` records the actual connection
result, including plaintext accepted in permissive mode. Initial certificate/acceptor construction and reload
filesystem work both run through the injected `BlockingExecutor` during asynchronous startup.

## `connection_v2` decision

Delete. Repository search found no production consumer or feature that selected `connection_v2`; it duplicated
connection responsibilities without a complete request lifecycle, compatibility facade, or independent release
boundary. Consequently it could not pass the compatibility and maintainability prerequisites for a meaningful
V1/V2 request benchmark. Keeping it would retain 1,549 lines of a second unowned stack with no evidence that it
improves compatibility, throughput, p99, RSS, and maintainability together. Reintroduction requires a separately
versioned experimental proposal and benchmark harness.

## Removal conditions

Exact legacy paths remain through the documented compatibility window. They may be removed only in the next
major version after downstream canonical-import evidence. High-level Remoting client/server adapters stay with
their composition owners until their own migration milestone; M05 does not move Broker processors, Store logic,
route selection, authentication providers, or complete high-level Client implementations into transport.
