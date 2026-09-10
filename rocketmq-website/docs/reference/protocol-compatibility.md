---
title: "Protocol and compatibility reference"
---

# Protocol and compatibility reference

Compatibility is a relationship between two endpoints, versions, configurations, and operations. A matching request code or successful ordinary send does not establish compatibility of every client API, administrative command, consensus protocol, or on-disk format. Use this page to define the surface you depend on before selecting a deployment or migration path.

## Choose the correct endpoint

| Endpoint | Protocol surface | Typical operation | Boundary |
| --- | --- | --- | --- |
| NameServer remoting listener, normally `9876` | RocketMQ remoting | Broker registration and route/KV queries | A route service, not the Proxy's gRPC `MessagingService`. |
| Broker remoting listener, normally `10911`; fast port normally `10909` | RocketMQ remoting with registered Broker processors | Send, pull/POP, offsets, metadata and administration | A defined request enum is not proof that the selected processor/mode handles it. |
| Broker HA listener, normally `10912` | HA replication path for the selected mode | Primary-log replication and progress | Not a client send port, and not a generic interchangeable Java/Rust HA contract. |
| Proxy gRPC listener, normally `8081` | `apache.rocketmq.v2.MessagingService` | Route, send, assignment, receive/ACK, pull/offset, transaction and telemetry methods | Requires matching API schema, client metadata, selected backend, and operation support. |
| Optional Proxy remoting listener, normally `8080` | Proxy remoting adapter | Supported remoting operations through Proxy | Disabled by default; separate from gRPC and from directly connecting to a Broker. |
| Controller remoting listener | Broker-facing control and administration | Controller metadata, role/election and sync-state operations | Separate from the Controller's internal Raft transport. |
| Controller Raft endpoint | Rust OpenRaft gRPC | Consensus between configured Rust Controller nodes | Not Java JRaft/DLedger membership or persisted-state compatibility. |

These are normal defaults, not discovery instructions: inspect the actual configuration and advertised addresses. Controller examples intentionally configure explicit, separate remoting and Raft ports. See [service configuration](service-configuration.md) and [deployment overview](../deployment/overview.md).

## Remoting wire contract

The frame carries a total length, a combined serialization/header-length field, header bytes, and an optional body. The high 8 bits of the combined field identify serialization; the low 24 bits carry header length. JSON and RocketMQ binary header encodings are distinct. Numeric request/response codes, correlation fields, flags, extension-key names, field widths, and message-body codecs are compatibility surfaces.

`RemotingCommandFactory` owns immutable defaults for version and serialization. Initializing application defaults does not retroactively modify a previously constructed factory. The runtime language tag `RUST` is metadata, not a different framing protocol. Message compression, CRC handling, v1/v2 topic encoding, and body/property decoding must also match the chosen path; header compatibility alone is insufficient.

Consult [request codes](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/src/code/request_code.rs), [response codes](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/src/code/response_code.rs), and [transport design](../architecture/protocol-transport.md). Unsupported requests and invalid encodings should produce the relevant explicit outcome; reconnecting indefinitely does not make an unsupported protocol supported.

## gRPC contract and business status

The repository's [service schema](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/proto/service.proto) defines `QueryRoute`, `SendMessage`, `QueryAssignment`, streaming `ReceiveMessage`/`PullMessage`, acknowledgment and offset APIs, transaction completion, client telemetry, recall, and Lite subscription methods. The schema defines messages and RPC signatures; the Proxy backend and the selected Broker capability determine whether a particular request can execute.

Inspect the response's RocketMQ status and any per-entry statuses, even when the gRPC transport call succeeded. In particular, `SendMessageResponse` contains both an overall status and repeated result entries. Streaming methods can carry status records as well as messages; treating every received item as a business message loses failure information.

Receipt handles and invisible durations belong to POP-style delivery. They are not interchangeable with classic pull's queue offsets. A successfully accepted end-transaction request is not proof that all transaction recovery/checking behavior has completed. Client identity metadata, authentication, TLS, body limits, batching and gzip admission are independent request conditions. Use the concrete [Proxy examples](../deployment/proxy.md) and [error reference](errors.md).

## Compatibility by surface

| Surface | What remains recognizable | What must be checked separately |
| --- | --- | --- |
| Rust source API | Public concepts such as producer, consumer, message, route and result | Current crate imports, feature flags, method signatures and runtime injection. Wire compatibility cannot keep a private Rust module import compiling. |
| Ordinary client remoting | RocketMQ framing, typed headers, codes and message semantics in implemented paths | Exact client/server versions, serialization, compression, TLS/ACL, topic type and operation. |
| Advanced message semantics | Transaction, order, filtering, delay/recall, POP and Lite concepts | Broker configuration, current operation support, group/request mode, retry and commit behavior. |
| Administrative APIs | Familiar topic/group/configuration/offset operations | Actual CLI hierarchy, flags, registered command, permissions, partial-result handling and runtime-versus-persisted changes. |
| Configuration | Some Java-compatible field names and explicit Broker properties conversion | Canonical TOML sections, types, defaults, rejected fields and conversion report. A copied Java file is not automatically a valid Rust file. |
| Primary and derived storage | CommitLog and queue/index concepts | Segment sizes, record versions, derived backend, source epoch, timer/POP/compaction/tiered metadata and recovery ownership. |
| Default HA | Master/replica concepts | The exact replication implementation and acknowledgment/failure scenario; do not infer mixed-implementation qualification from the concept. |
| Controller HA | Master election and sync-state functional outcomes | Rust authority/lease contracts, OpenRaft membership, internal transport and persistence. Java internal-protocol and mixed-quorum compatibility are excluded. |
| Operations products | Cluster/resource vocabulary | Dashboard, MCP and SRE API/auth/state schemas have their own versions and are outside the core messaging release scope. |

For SQL filtering, both the client selector and Broker property-filter support matter. For LitePull, local offset commit, remote submission, Broker persistence, and completed business effects are separate. For replication, acceptance, local durable progress, and replica acknowledgment are separate. These are behavioral conditions, even when two implementations accept the same bytes.

## Recorded core scope and explicit exclusions

The [1.0 capability manifest](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/v1-capability-manifest.json) records Apache RocketMQ `5.5.0` as its Java comparison baseline. Each capability has its own profile, compatibility mode, implementation status, evidence status, and referenced tests. That declaration is not a blanket assertion of parity with every Java version or deployment.

The manifest explicitly excludes OpenMessaging, BrokerContainer runtime/admin operations, DLedger CommitLog, and Java Controller internal protocols. In particular, Controller parity is described through pure Rust functional outcomes, not Java DLedger, JRaft, AutoSwitch wire, or mixed quorum compatibility. Broker rejects DLedger configuration instead of quietly selecting a different HA implementation.

The [core release scope](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/core-release-scope.json) classifies packages and identifies core services. It excludes the Dashboard, MCP and SRE products from that core release scope; those products still exist and need their own documentation and validation. Long-running security/concurrency and capacity qualification entries marked deferred with no evidence must not be represented as passed because short component tests exist.

## Read evidence at its actual scope

| Repository evidence | What it can establish | What it cannot establish alone |
| --- | --- | --- |
| [Header compatibility tests](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/tests/request_header_java_compatibility.rs) | The header cases and fixtures asserted by that test | Live Java server interoperability for all registered requests. |
| [Remoting business compatibility tests](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/tests/remoting_command_java_business_compatibility.rs) | Specific extension-field, language-code and key-length behavior | Full end-to-end message durability or HA failover. |
| [Message codec tests](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/tests/message_codec_compatibility.rs) | Fixed-frame decoding, truncation, CRC/compression and v2 topic cases | Arbitrary historical store directories or complete data migration. |
| [Client batch-admin API test](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/tests/batch_admin_delete.rs) | Public typed API availability in its feature selection | That a live remote server accepted the batch or preserved its authorization semantics. |
| A recorded `component` or `interop` capability state | The scenario described by its referenced evidence | That the same scenario was rerun during this documentation change or covers an unlisted profile. |

When evaluating your own compatibility requirement, record both sides' versions, the endpoint and serializer, build features, backend/mode, topic/group configuration, credentials policy, and the observed operation result. Include failure/retry behavior when your requirement depends on it. For example, “ordinary send and LitePull against the configured Rust LocalFile deployment” is a narrower and more useful result than “all RocketMQ clients compatible.”

## Storage and rollback decisions

Do not point a different backend or older binary at a live data directory to test compatibility. Stop the owning service, preserve a recoverable copy, and use the applicable [upgrade and rollback](../operations/upgrade-rollback.md) procedure. The offline downgrade inspection reports only the persisted formats it checks; it does not establish Controller membership compatibility or business-state correctness.

Changing `storeType` is not migration. RocksDB-derived structures still relate to the primary log and their recorded progress; tiered state is a separate secondary path. Controller's internal state directory is not a Java consensus snapshot import format. A rollback that needs an older layout may require a compatible backup or a supported migration rather than just replacing an executable.

See [storage backends](../architecture/storage-backends.md), [HA and Controller](../architecture/ha-controller.md), and [backup and recovery](../operations/backup-recovery.md) for the ownership and recovery contracts behind those decisions.
