# Proxy ingress and Store logical reads

The Proxy business core can build without Tonic runtime bindings or Transport.
`rocketmq-proxy` owns gRPC ingress, socket metadata extraction, and transport status
projection. `rocketmq-proxy-core` owns request metadata, message DTOs, business
contracts, payload status mapping, admission accounting inputs, and session state.

Core still generates the shared protobuf message types with Prost. Its optional
`grpc-bindings` feature adds generated Tonic client/server bindings; the Proxy
facade enables that feature. It does not put network orchestration back in Core.
Local and Cluster adapters select their remoting response type through
`ProxyRemotingBackend::Response` and own their Transport dependency directly.

## Rust API migration

| Previous entry | Current entry |
| --- | --- |
| `rocketmq_proxy_core::ingress::grpc` (including the deprecated `grpc` alias) | `rocketmq_proxy::ingress::grpc` |
| `rocketmq_proxy_core::RemotingConfig` | `rocketmq_proxy::RemotingConfig` |
| `rocketmq_proxy_core::GrpcTransportContext` | `rocketmq_proxy::GrpcTransportContext` |
| Core context constructors taking a Tonic or Transport request | Import `rocketmq_proxy::ProxyContextExt`; neutral callers use `ProxyContextWithPrincipal::from_metadata` |
| `ProxyStatusMapper::to_tonic_status` | Import `rocketmq_proxy::ProxyStatusMapperExt` alongside the mapper |
| Core-generated gRPC client/server modules enabled implicitly | Enable `rocketmq-proxy-core/grpc-bindings`, or use the existing Proxy facade exports |
| Core reexports of `EmbeddedDispatchOutcome` / `RemotingResponse` | `rocketmq_transport::api` |

Network metadata does not create an authenticated principal. The facade attaches
the existing verified principal after authentication. Deadlines, request identity,
session identity, TLS identity, and retained-message accounting retain their
existing ownership and behavior.

## Store backend reads

`BackendReadOps` contains logical message reads and queue metadata. `BackendOps`
composes this trait with the remaining write, lifecycle, and administration
operations. The Broker capability delegates logical reads through that contract;
it does not downcast a queue store or access CommitLog for timestamp/cold-area
queries. Physical access belongs to the LocalFile and RocksDB implementations.

LMQ lookup first checks the lightweight index, then falls back to the ordinary
queue index of the selected backend. RocksDB continues to use its own ordinary
queue lookup during that fallback. Public Broker capability methods are unchanged.

## Compatibility and validation

This is an explicit Rust source API migration for ingress consumers. Protobuf
schemas, Java request/response codes, metadata names, message bodies, and persisted
layouts are unchanged. The protobuf contract tests and moved ingress tests cover
the wire mapping; Store's logical-read regression covers live LMQ updates and queue
statistics. Core is also validated without the optional gRPC bindings.
