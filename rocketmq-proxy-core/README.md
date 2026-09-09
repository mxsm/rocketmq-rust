# rocketmq-proxy-core

Backend-neutral service contracts, protocol bindings, and ingress/session state for RocketMQ Proxy. The application entry point and listeners are assembled in [rocketmq-proxy](../rocketmq-proxy/README.md).

## Public surface

- [`contracts`](src/contracts.rs) defines route, metadata, message, consumer, assignment, and transaction service ports.
- [`processor`](src/processor.rs), [`message`](src/message.rs), and [`context`](src/context.rs) define processing inputs, messages, endpoints, and authenticated/TLS context.
- [`ingress`](src/ingress) includes remoting request classification, dispatch, backend contracts, and status mapping.
- [`session`](src/session.rs), [`receipt_renewal`](src/receipt_renewal.rs), and [`drain`](src/drain.rs) track sessions, receipt ownership/renewal, and bounded draining.
- [`settings`](src/settings.rs), [`config`](src/config.rs), and [`status`](src/status.rs) define configuration and client-visible settings/status values.

Prefer `contracts` and `ingress::remoting` over the deprecated `service` and `remoting` compatibility paths. Although the crate separates backend concerns, it uses Tokio and shared runtime capabilities for ingress/session work; it is not an executor-free model crate.

## Generated protocol and features

`build.rs` generates protobuf message bindings from [`proto/service.proto`](proto/service.proto), including its imported definitions. A working `protoc` is required even with default features. Default features are empty; `grpc-bindings` additionally generates/enables Tonic client and server bindings. It does not start a gRPC server.

The exported default ports are 8081 for gRPC and 8080 for remoting. Applications select actual bindings through configuration.

## Backends and validation

[rocketmq-proxy-cluster](../rocketmq-proxy-cluster/README.md) supplies client-backed remote services. [rocketmq-proxy-local](../rocketmq-proxy-local/README.md) supplies the embedded broker adapter.

Run from the root workspace:

```bash
cargo test -p rocketmq-proxy-core --lib
```

Select `--features grpc-bindings` when validating generated network bindings. Licensed under [Apache-2.0](../LICENSE-APACHE).
