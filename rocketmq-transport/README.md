# rocketmq-transport

Bounded TCP/TLS transport for RocketMQ Rust. Consumers import the curated `rocketmq_transport::api` or `prelude` surface; connection, writer, session, and dispatch implementation modules remain private.

## Integration model

`TransportClientBuilder` / `RemotingClientBuilder` construct managed clients, and `TransportServer` owns server transport work. Applications supply runtime capabilities from [rocketmq-runtime](../rocketmq-runtime/README.md), configure admission and frame limits, install processors/security hooks, and await shutdown reports. Protocol models remain in [rocketmq-protocol](../rocketmq-protocol/README.md).

The [`public API`](src/public_api.rs) includes:

- `TransportClientConfig`, `ServerConfig`, `FrameLimits`, socket/TLS configuration, and NameServer endpoint types.
- `AdmissionController` and resource limits for bounded request/connection work.
- `RequestDeadline`, request outcomes, and send/response receipts. Local write completion does not establish remote application processing.
- `RequestProcessor`, `RemotingRequest`, `RemotingResponse`, and `HandlerOutcome`, including deferred responses and explicit no-response outcomes.
- `SessionRegistry` and server push/request capabilities.
- `TransportSecurity`, `RPCHook`, and security contracts supplied by higher-level services.

## File transfers

`FileRegion` retains an immutable storage lease through writer completion. The portable path reads with a reusable 64 KiB buffer on a runtime-owned blocking I/O lane. The optional Linux sendfile path applies to eligible plaintext file regions after capability checks; unsupported preflight conditions fall back before frame bytes are written. TLS uses portable reads so data passes through the TLS record layer.

Shared `Bytes` and vectored writes reduce copies in userspace. They do not imply kernel zero-copy for every request, remote acknowledgement, or NIC offload.

## Features

| Feature | Effect |
| --- | --- |
| `tls`, `socks` | Default package features providing TLS and SOCKS support; connections still require runtime configuration. |
| `simd` | Forwards accelerated protocol decoding support. |
| `observability`, `observability-traces` | Optional metrics and tracing integration. Exporter ownership remains with the application. |
| `linux-sendfile` | Default-off Linux plaintext file transfer backend. |
| `test-support` | Exposes test/benchmark helpers. |

The root workspace dependency disables transport default features, so a workspace consumer must explicitly enable the capabilities it needs. A direct package build and a consumer's feature selection can therefore differ.

## Validation

Run relevant tests from the root workspace; use `--no-default-features` when checking feature absence. Benchmark targets declare their required features in [`Cargo.toml`](Cargo.toml):

```bash
cargo bench -p rocketmq-transport --features test-support --bench frame_write
```

See [`tests`](tests) for lifecycle, admission, TLS, deferred-response, and writer coverage. Licensed under [Apache-2.0](../LICENSE-APACHE).
