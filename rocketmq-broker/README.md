# rocketmq-broker

[English](README.md) | [简体中文](README-zh_cn.md)

Broker runtime, remoting request processing, storage integration, and service orchestration for
[RocketMQ-Rust](../README.md). This crate provides the `rocketmq-broker-rust` binary and library entry points for
constructing a broker with an explicitly owned runtime and validated configuration.

## Capabilities

| Area | Current implementation |
|------|------------------------|
| Bootstrap | TOML and Java-properties loading, typed configuration validation, staged startup, readiness, rollback, and coordinated shutdown. |
| Requests | Send, pull, peek, pop, ack, invisible-time changes, notifications, replies, recall, queries, client/consumer management, lite subscriptions, transactions, and administration. |
| Storage | Local file storage by default; optional RocksDB backend and tiered storage integration; timer/scheduled messages and HA services. |
| Metadata | Topic configuration, queue mapping, subscriptions, consumer offsets, ordering, filters, and route information. |
| Security | Optional authentication and authorization through `rocketmq-auth`, ACL import/watch, auth administration, and separately authorized maintenance requests. |
| Operations | Long polling, deferred requests, fast failure, housekeeping, registration, controller-mode integration, and shutdown reports. |
| Observability | Optional metrics, traces, and log exporters; process health probes and configurable logging. |

Availability depends on the selected storage backend, Cargo features, runtime configuration, and broker role.
This list does not imply complete Java broker feature parity.

## Architecture

![Broker configuration, lifecycle, request dispatch, and storage architecture](../resources/broker-runtime-architecture.png)

The binary resolves configuration into `ValidatedBrokerConfig` and passes runtime and telemetry handles to `Builder`.
`BrokerBootstrap` advances through `Configured`, `Initialized`, and `Running` states. Startup failures trigger rollback
of completed startup work. The binary uses `boot_with_lifecycle` to publish readiness and shut down services under the
shared process deadline.

Normal and fast remoting listeners use the same dispatcher as the embedded `ProxyBrokerFacade`. The dispatcher applies
configured security checks and routes requests to processors backed by storage and metadata services. NameServer
registration and optional controller coordination are separate from client request dispatch.

| Public API | Contract |
|------------|----------|
| `Builder::new` | Requires a `ChildServiceContext` and `TelemetryRuntimeGuard`; `with_validated_config` supplies a `ValidatedBrokerConfig`. `build` returns `BrokerBootstrap<Configured>`. |
| `BrokerBootstrap::initialize` / `start` | Consume the preceding state and return the next state or `BrokerStartupError`. |
| `BrokerBootstrap<Running>` | Exposes `readiness()` and an asynchronous, consuming `shutdown()`. |
| `BrokerBootstrap::boot_with_lifecycle` | Runs under `ServiceLifecycle` and returns lifecycle/startup errors to the caller. |
| `ProxyBrokerFacade` | Provides embedded proxy access to broker request processing. |
| `config` | Exposes raw loading, Java-properties conversion, typed sections, and validated configuration APIs. |

The convenience `boot()` method logs startup failures and returns `()`; callers that need to handle errors should use
the staged methods or `boot_with_lifecycle`. `BrokerRuntime` and most implementation modules are crate-private.

## Source Map

| Source | Responsibility |
|--------|----------------|
| [`src/bin/broker_bootstrap_server.rs`](src/bin/broker_bootstrap_server.rs) | CLI/config resolution, security bootstrap, telemetry setup, and process lifecycle. |
| [`src/config`](src/config) | Canonical schema, field ownership, conversion, and semantic validation. |
| [`src/broker_bootstrap.rs`](src/broker_bootstrap.rs), [`src/lifecycle.rs`](src/lifecycle.rs) | Builder, typed startup states, errors, and readiness evidence. |
| [`src/broker_runtime/composition.rs`](src/broker_runtime/composition.rs) | Runtime component construction. |
| [`src/broker_runtime/data_plane.rs`](src/broker_runtime/data_plane.rs), [`metadata.rs`](src/broker_runtime/metadata.rs) | Storage backend selection and metadata managers. |
| [`src/broker_runtime/control_plane.rs`](src/broker_runtime/control_plane.rs), [`control_plane/auth.rs`](src/broker_runtime/control_plane/auth.rs) | Control services, auth runtime, and auth administration. |
| [`src/broker_runtime/request_pipeline.rs`](src/broker_runtime/request_pipeline.rs), [`request_pipeline/startup.rs`](src/broker_runtime/request_pipeline/startup.rs) | Processor composition and normal/fast listener startup. |
| [`src/processor/dispatcher.rs`](src/processor/dispatcher.rs) | Shared dispatch, security checks, fast-failure decisions, and processor selection. |
| [`src/broker_runtime/lifecycle.rs`](src/broker_runtime/lifecycle.rs), [`shutdown_report.rs`](src/broker_runtime/shutdown_report.rs) | Service startup, rollback, shutdown, and completion reporting. |
| [`src/broker_runtime/deferred.rs`](src/broker_runtime/deferred.rs), [`deferred_producer.rs`](src/broker_runtime/deferred_producer.rs) | Deferred request admission and owned workers. |
| [`src/topic`](src/topic), [`subscription`](src/subscription), [`offset`](src/offset), [`pop`](src/pop), [`transaction`](src/transaction) | Topic, subscription, consumption, and transaction services. |

## Build and Local Startup

Run commands from the workspace root with the repository's pinned Rust `1.95.0` toolchain:

```bash
cargo build -p rocketmq-broker --bin rocketmq-broker-rust --release
```

Start a reachable [NameServer](../rocketmq-namesrv/README.md) first. In ordinary standalone startup, failure to register
with any configured NameServer fails broker startup. The binary falls back to `127.0.0.1:9876` when no address is supplied.

Create `conf/broker.toml` with this configuration for a single local master. All listeners shown here bind to loopback:

```toml
[broker]
namesrvAddr = "127.0.0.1:9876"
brokerIp1 = "127.0.0.1"
listenPort = 10911
storePathRootDir = "./store"

[broker.brokerServerConfig]
bindAddress = "127.0.0.1"

[broker.brokerIdentity]
brokerName = "broker-a"
brokerClusterName = "DefaultCluster"
brokerId = 0

[store]
storeType = "LocalFile"
brokerRole = "ASYNC_MASTER"
storePathRootDir = "./store"
haListenAddress = "127.0.0.1"
haListenPort = 10912
```

Set `ROCKETMQ_HOME` to an existing installation/configuration directory for predictable configuration discovery.
For this workspace-local example, use the workspace directory. The development security profile below checks that
configured listeners use loopback; authentication and authorization remain disabled in this example.

Windows PowerShell:

```powershell
$env:ROCKETMQ_HOME = "$PWD"
$env:ROCKETMQ_SECURITY_PROFILE = "development-insecure-loopback"
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c ./conf/broker.toml -n "127.0.0.1:9876"
```

Linux/macOS:

```bash
export ROCKETMQ_HOME="$PWD"
export ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c ./conf/broker.toml -n "127.0.0.1:9876"
```

The normal remoting port is `10911`; the fast port is derived as `listenPort - 2` (`10909`). The HA port is configured
separately (`10912` here). Use distinct ports and storage directories for each broker instance. For remote clients,
configure an appropriate advertised address, listener bindings, and security material before deployment.

## Command Line

Append these flags after Cargo's `--`, or pass them directly to the built binary:

| Flag | Purpose |
|------|---------|
| `-c, --configFile <FILE>` | Select a configuration file. `.toml` selects TOML; `.conf` and `.properties` select Java properties. |
| `--config-format <toml\|properties>` | Explicitly select the configuration parser. Prefer the matching file extension. |
| `--conversion-report <FILE>` | Set the JSON report destination when loading Java properties. |
| `-p, --printConfigItem` | Validate and print broker/store properties, then exit before telemetry, storage, and service listener startup. |
| `-m, --printImportantConfig` | Print a selected subset of properties; mutually exclusive with `-p`. |
| `-n, --namesrvAddr <ADDR>` | Override the NameServer list; quote semicolon-separated addresses for multiple servers. |
| `--log-filter <DIRECTIVE>` | Override the startup log filter. |
| `-h, --help` | Print CLI help. |
| `-V, --version` | Print the package version. The standalone `--version --verbose` form also prints build artifact/feature metadata. |

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- --help
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- --version --verbose
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c ./conf/broker.toml -p
```

`-p` and `-m` still validate configuration and the resolved home directory value. Their output is a diagnostic property
listing, not a reusable canonical TOML file. These modes do not test NameServer connectivity, storage opening, or the full
security/telemetry startup path. Java-properties loading still writes its conversion report in printing mode.

| Exit code | Meaning |
|-----------|---------|
| `0` | Successful completion, including help, version, and configuration printing. |
| `2` | Clap argument parsing errors, such as unknown flags or conflicting print modes. |
| `70` | Errors returned by the service entry point, including argument value validation, config loading/validation, an explicitly empty `ROCKETMQ_HOME`, and startup/shutdown failures. |

## Configuration Contract

Configuration file selection is: explicit `-c`, then `$ROCKETMQ_HOME/conf/broker.toml` if present, then defaults.
Automatic file discovery requires `ROCKETMQ_HOME` to be set. When it is unset, home validation falls back to the current
directory, but this fallback does not trigger automatic config-file discovery. An explicitly empty value is rejected.
NameServer selection is: `-n`, then `NAMESRV_ADDR`, then `broker.namesrvAddr`, then `127.0.0.1:9876`.
An explicitly empty `NAMESRV_ADDR` currently selects the loopback fallback, overriding the file value; unset it to use
the file's addresses.

The canonical TOML uses `[broker]`, `[store]`, `[logging]`, and `[observability]` sections. Unknown fields are rejected;
the former flat broker/store layout is not accepted. Validated configuration normalizes derived fields and checks
ports, addresses, role constraints, resource budgets, and security prerequisites before runtime construction.

| Canonical field | Meaning or constraint |
|-----------------|-----------------------|
| `broker.brokerIp1` / `broker.listenPort` | Advertised remoting address and authoritative normal listener port. |
| `broker.brokerServerConfig.bindAddress` | Local listener binding; separate from the advertised address. |
| `broker.brokerIdentity` | Cluster/name/ID. Outside controller mode, master roles require ID `0`, and slaves require a nonzero ID. |
| `broker.storePathRootDir` | Broker metadata root; bootstrap file logs are placed under its `logs` directory. |
| `store.storePathRootDir` / `store.storePathCommitLog` | Message-store root and optional commitlog override. By default, commitlog is under the store root. The broker and store roots are independent settings. |
| `store.storeType` | `LocalFile` by default; selecting `RocksDB` requires the `rocksdb_store` build feature. |
| `store.brokerRole` | `ASYNC_MASTER`, `SYNC_MASTER`, or `SLAVE`, subject to role/identity validation. |
| `store.haListenAddress` / `store.haListenPort` | HA listener configuration; the HA port must not collide with either remoting port. |
| `broker.enableControllerMode` / `broker.controllerAddr` | Controller mode requires valid controller addresses and compatible heartbeat settings. |

Do not set `broker.brokerServerConfig.listenPort`, `store.enableControllerMode`, or `store.duplicationEnable`:
these are derived from `broker.listenPort`, `broker.enableControllerMode`, and `broker.duplicationEnable` respectively.
Supplying them under the derived owner is rejected even if the values agree.

DLedger mode is explicitly unsupported; enabling it or supplying DLedger identity/path settings is rejected.
Enabling `store.coldDataFlowControlEnable` is also rejected at runtime startup because the legacy cold-data queue
lacks the required owned shutdown contract.

### Java Properties Migration

The converter maps supported Java keys to their canonical broker/store owners and rejects unknown keys, duplicate
assignments, conflicting aliases, invalid values, and DLedger settings. Java `storeType=default` and
`storeType=defaultRocksDB` map to the Rust backend names; those Java values are not the canonical TOML values.

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c ./conf/broker.conf --config-format properties --conversion-report ./conf/broker.conversion.json -p
```

The conversion report records mappings, warnings, and redacted value states. Its default path replaces the source
extension with `.conversion.json`, and report-write failure prevents startup. Conversion loads configuration into
memory; it does not emit a replacement TOML file or copy referenced ACL, certificate, or other files.

### Authentication and Security Bootstrap

Merge these fields into the existing `[broker]` table after provisioning the referenced auth metadata and ACL file:

```toml
[broker]
authConfigPath = "./store/auth"
aclFile = "./conf/plain_acl.yml"
aclFileWatchEnabled = true
authenticationEnabled = true
authorizationEnabled = true
signatureAlgorithm = "HmacSHA1"
```

Both security flags default to `false`. Authorization requires authentication, and enabling either requires a nonempty
`authConfigPath`. The broker builds and shares the auth runtime with its dispatcher and auth admin service; outbound
broker RPC credentials are configured separately. See [rocketmq-auth](../rocketmq-auth/README.md) for ACL format,
credential handling, whitelist behavior, reload, and provider limitations.

The binary also supports `ROCKETMQ_SECURITY_PROFILE=secure-enforced`, which requires both auth flags and the bootstrap
environment settings `ROCKETMQ_SECURITY_TRUST_ANCHOR`, `ROCKETMQ_SECURITY_TLS_CERT`, `ROCKETMQ_SECURITY_TLS_KEY`,
`ROCKETMQ_SECURITY_SECRET_PROVIDER=mounted-files`, `ROCKETMQ_SECURITY_ADMIN_IDENTITY`, and
`ROCKETMQ_SECURITY_REQUEST_POLICY`. This checks startup material before listener binding. Transport TLS still needs
its own configuration under `broker.brokerServerConfig.tlsConfig`; bootstrap environment variables do not enable it.
With no profile or other bootstrap material, this additional bootstrap check is disabled.

Privileged maintenance requests use a dedicated authorization path. Their routes require maintenance configuration,
an initialized auth runtime, and a validated maintenance policy reference; they are not enabled by the auth flags alone.

### Logging and Observability

The startup log filter precedence is `--log-filter`, `RUST_LOG`, `logging.filter`, legacy root `logFilter`, then the
default filter. Runtime filter reload is controlled separately by `logging.reload.enabled`.

For Prometheus, build with `--features prometheus` and add these sections:

```toml
[observability.metrics]
exporter = "prometheus"

[observability.prometheus]
host = "127.0.0.1"
port = 5557
path = "/metrics"
```

Exporters are disabled by default. A build feature makes an exporter available; runtime configuration selects it.
Legacy flat telemetry fields are rejected. Supported environment overrides take precedence over matching
`[observability]` values when present, including `ROCKETMQ_METRICS_EXPORTER`, `ROCKETMQ_METRICS_BIND_ADDR`, and
`ROCKETMQ_METRICS_PATH`. See [rocketmq-observability](../rocketmq-observability/README.md) for the complete signal and
exporter configuration.

## Readiness and Shutdown

Set `ROCKETMQ_HEALTH_BIND_ADDR` to an address such as `127.0.0.1:5558` to enable the shared lifecycle HTTP listener.
`/readyz` reports readiness and `/livez` reports liveness using HTTP `200` or `503`. No probe listener is started when
the variable is absent. This listener also exposes the shutdown-triggering `/drainz` route; restrict access accordingly.

Broker readiness checks the normal and fast listeners, storage readiness, installed processors, resolved security
state, and registration readiness. The storage readiness field is not a guarantee that the current role/controller
lease permits producer writes.

Signal handling and lifecycle shutdown share a deadline configured by `ROCKETMQ_SHUTDOWN_TIMEOUT_SECONDS`
(default `45`, accepted range `1..=300`). Startup rollback and shutdown reports account for owned services;
an unhealthy shutdown result propagates to a nonzero binary exit status.

## Feature Flags

| Feature | Build-time capability |
|---------|-----------------------|
| `local_file_store` | Default feature; enables the local file storage path. |
| `rocksdb_store` / `rocksdb-store` | Optional RocksDB backend and metadata integration; the hyphenated name is an alias. |
| `extended_timeline` | Forwards extended timer timeline support to `rocketmq-store`. |
| `tieredstore` | Enables tiered storage integration and includes `local_file_store`. |
| `observability` | Groups `otel-metrics` and `otel-traces`. |
| `otel-metrics` / `otel-traces` / `otel-logs` | Instrumentation/export support for the selected signal. |
| `otlp-metrics` / `otlp-traces` / `otlp-logs` | OTLP exporter support for the selected signal. |
| `prometheus` / `metrics-prometheus` | Prometheus exporter support; `metrics-prometheus` is an alias. |
| `production-observability` | Groups Prometheus metrics, OTLP traces, and OTLP logs. |
| `production` | Groups local file storage and `production-observability`; runtime exporters and security still require configuration. |
| `test-support` | Test support feature. |

Selecting `RocksDB` without its compiled backend fails instead of falling back to local files. Tiered storage is not
activated merely by compiling `tieredstore`; when configured with an enabled storage level, it requires
`store.storeType = "LocalFile"`. See [rocketmq-store](../rocketmq-store/README.md) for storage configuration.

## Validation and Benchmarks

Select checks for the affected behavior. Configuration and startup contract tests include:

```bash
cargo fmt -p rocketmq-broker -- --check
cargo test -p rocketmq-broker --test config_contract --test java_config_conversion
cargo test -p rocketmq-broker --test broker_readiness --test broker_process_startup
```

Run storage-specific tests with the corresponding features when changing those paths. Broker benchmark targets include:

```bash
cargo bench -p rocketmq-broker --bench consumer_manager_benchmark
cargo bench -p rocketmq-broker --bench consumer_filter_benchmark
cargo bench -p rocketmq-broker --bench subscription_group_manager_benchmark
cargo bench -p rocketmq-broker --bench schedule_message_service_performance
cargo bench -p rocketmq-broker --bench broker_runtime_lifecycle_bench
```

Keep toolchain, features, backend, and workload consistent when comparing measurements. See
[`Cargo.toml`](Cargo.toml) for all declared benchmark targets.

## License

RocketMQ-Rust is licensed under the Apache License 2.0. See [LICENSE-APACHE](../LICENSE-APACHE).
