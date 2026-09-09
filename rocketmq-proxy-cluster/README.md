# rocketmq-proxy-cluster

Client-backed cluster adapter for [rocketmq-proxy-core](../rocketmq-proxy-core/README.md). It connects proxy service contracts to NameServer discovery and remote brokers through `rocketmq-client-rust` and `rocketmq-transport`; it does not embed a broker or expose its own server binary.

## Components

`ClusterClient` is the adapter contract and `RocketmqClusterClient` is the concrete client. `ClusterServiceManager` composes route, metadata, assignment, message, consumer, and transaction services. `ClusterRemotingBackend` implements the remoting backend boundary.

[`ClusterConfig`](src/config.rs) controls cluster addresses, client identity, RPC settings, and bounded execution/admission parameters. Execution is managed under supplied service contexts; cancellation, deadlines, queue capacity, retained-byte limits, and shutdown remain part of the request lifecycle. Outbound signing is supplied through the security API rather than inferred from an authenticated inbound connection.

See [`cluster.rs`](src/cluster.rs), [`cluster_execution.rs`](src/cluster_execution.rs), [`cluster_admission.rs`](src/cluster_admission.rs), and [`service.rs`](src/service.rs) for the current integration path. The adapter enables the client's `admin-mutation` feature for required operations; application-level authorization still governs exposed requests.

## Features and validation

Default features are empty. `observability` enables metrics integration, `observability-traces` enables client tracing integration, and `bench-support` exposes benchmark/collector helpers. Exporter setup belongs to the application.

Run from the root workspace:

```bash
cargo test -p rocketmq-proxy-cluster --lib
cargo bench -p rocketmq-proxy-cluster --features bench-support --bench cluster_executor
```

For deployment and listener/security configuration, see [rocketmq-proxy](../rocketmq-proxy/README.md). Licensed under [Apache-2.0](../LICENSE-APACHE).
