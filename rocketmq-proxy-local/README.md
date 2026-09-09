# rocketmq-proxy-local

Embedded broker adapter for [rocketmq-proxy-core](../rocketmq-proxy-core/README.md). It implements proxy services through the broker's in-process facade rather than forwarding to a separately deployed broker.

## Integration

`local_components_from_config_with_service_context` assembles local components from `LocalConfig` and supplied runtime/telemetry capabilities. The public exports also include `LocalBrokerFacadeClient`, `LocalRemotingBackend`, and `LocalServiceManager`.

The adapter creates a bounded command queue with retained-byte and age limits, separates control capacity from ordinary I/O, and tracks long-poll operations. Broker work and the command-processing service have explicit lifecycle owners. Embedded dispatch still traverses the broker request pipeline; it is not a direct unrestricted storage API.

[`LocalConfig`](src/config.rs) includes broker cluster/name/address, store root, assignment strategy, queue capacity/bytes/age, I/O concurrency, control reserve, and long-poll bounds. Defaults include a local broker identity, `127.0.0.1:10911`, and `store/proxy/local-broker`. These are embedded-backend settings, not the proxy's external listener addresses. Use a separate store root per instance.

The component constructor is not the full standalone `BrokerBootstrap` startup path. See [`local.rs`](src/local.rs), [`execution.rs`](src/execution.rs), and [rocketmq-proxy](../rocketmq-proxy/README.md) for composition and application lifecycle.

## Features and validation

Default crate features are empty; the broker dependency supplies its normal default storage feature. `observability` and `observability-traces` forward broker telemetry support; `tieredstore` forwards tiered-storage support. Compiling these features does not activate exporters or storage policies.

Run from the root workspace:

```bash
cargo test -p rocketmq-proxy-local --lib
```

Licensed under [Apache-2.0](../LICENSE-APACHE).
