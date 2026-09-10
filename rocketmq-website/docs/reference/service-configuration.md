---
title: "NameServer, Controller, and Proxy configuration"
---

# NameServer, Controller, and Proxy configuration

These services use different file layouts, CLI spellings, and runtime update paths. A setting with a similar name does not imply identical precedence or persistence. Use [Broker configuration](../configuration/broker-config.md) for Broker and store fields, and [client configuration](../configuration/client-config.md) for application builders.

## Select and inspect a file

| Service binary | File option | Inspect-and-exit option | No explicit file |
| --- | --- | --- | --- |
| `rocketmq-namesrv-rust` | `-c` / `--configFile` | `-p` / `--printConfigItem` | NameServer defaults, then any durable desired configuration at `configStorePath` |
| `rocketmq-controller-rust` | `-c` / `--config-file` | `-p` / `--print-config-item` | Controller defaults |
| `rocketmq-proxy-rust` | `-c` / `--config` | `--printConfig` | Proxy defaults for the compiled modes |

Use TOML examples from the website's [deployment examples](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples). An explicitly selected missing or malformed file fails loading. These services do not inherit Broker's automatic `ROCKETMQ_HOME/conf/broker.toml` selection or Java properties conversion. The print commands inspect configuration; they do not test port binding, certificate handshakes, remote dependencies, or a Controller quorum.

For example, run the following from the repository root after building the named binaries:

```powershell
& .\target\debug\rocketmq-namesrv-rust.exe -c .\rocketmq-website\examples\first-message\namesrv.toml -p
& .\target\debug\rocketmq-controller-rust.exe -c .\rocketmq-website\examples\ha\controller-1.toml -p
& .\target\debug\rocketmq-proxy-rust.exe -c .\rocketmq-website\examples\proxy\cluster.toml --printConfig
```

## NameServer

### Startup order

NameServer settings and its transport overrides use top-level camelCase keys. Startup loads the selected file or defaults, applies `--rocketmqHome` and `--kvConfigPath`, loads file transport/TLS settings, and then reads the durable desired properties file at `configStorePath` if it exists. That durable snapshot can override corresponding values in the startup file. Finally, `--listenPort` and `--bindAddress` override the resolved listener values.

Changing only the TOML file can therefore appear ineffective after a prior administrative update. Inspect both inputs and the effective configuration. The durable properties file is distinct from `kvConfigPath`: one stores configuration intent, the other stores NameServer KV data. Neither is a copy of all live routing tables.

```toml
rocketmqHome = ".rocketmq-reference"
kvConfigPath = ".rocketmq-reference/namesrv/kvConfig.json"
configStorePath = ".rocketmq-reference/namesrv/namesrv.properties"
listenPort = 9876
bindAddress = "127.0.0.1"
scanNotActiveBrokerInterval = 5000
needWaitForService = false
```

| External key | Type / default | Scope and unit |
| --- | --- | --- |
| `rocketmqHome` | string / `rocketmq.home.dir` then `ROCKETMQ_HOME`, otherwise empty | A nonempty home is required for service startup; print mode returns earlier. |
| `kvConfigPath` | string / home + `rocketmq-namesrv/kvConfig.json` | KV metadata file; use an independent path per process. |
| `configStorePath` | string / home + `rocketmq-namesrv/rocketmq-namesrv.properties` | Durable desired configuration loaded at startup. |
| `listenPort` | u32 / `9876` | TCP listener; file transport override range `1..=65535`. |
| `bindAddress` | string / `0.0.0.0` | Listener bind address; choose loopback for the local example. Security bootstrap separately controls whether a listener is allowed. |
| `scanNotActiveBrokerInterval` | u64 / `5000` | Milliseconds; domain range `1..=3600000`, restart required for administrative changes. |
| `needWaitForService` | bool / `false` | Enables the configured startup waiting behavior; restart required. |
| `waitSecondsForService` | i32 / `45` | Seconds, range `0..=3600`; distinct from scan intervals in milliseconds. |
| `namesrvRouteResponseCacheEnable` | bool / `false` | Runtime-selectable route response cache behavior. Cache capacity and shard settings are separate configuration fields. |
| `enableRegistrationDelta` | bool / `false` | Registration-delta setting; do not infer wire compatibility merely from enabling it. |
| `enableControllerInNamesrv` | bool / `false` | Requires the `embedded-controller` feature; without it, startup fails. This is separate from ordinary NameServer route service. |

### Administrative update behavior

The NameServer configuration path classifies properties as live, restart-required, or unsupported. It validates the desired snapshot and submits a durable metadata write before publishing live changes. Responses distinguish desired, durable, and effective generations and report applied versus restart-required keys. A persisted restart-required value has not changed the running listener or service configuration yet.

Live examples include `orderMessageEnable`, `namesrvRouteResponseCacheEnable`, `enableAllTopicList`, `enableTopicList`, and `notifyMinBrokerIdChanged`. Paths such as `kvConfigPath` and `configStorePath` are not online-update targets. Treat the implementation's mutability classification as authoritative; the presence of a field in a printed map does not establish that it is live. See [NameServer design](../architecture/nameserver.md) for route visibility and KV persistence boundaries.

## Controller

### File schema and identity

Controller uses top-level camelCase fields with Serde defaults. Use `nodeId`, `listenAddr`, `raftListenAddr`, `raftPeers`, and `storageBackend` rather than copying Rust snake_case field names. File values override defaults; there is no general CLI override for node identity or storage. `--log-filter` is a separate startup logging override.

| External key | Type / default | Meaning |
| --- | --- | --- |
| `controllerType` | string / `Raft` | The supported Controller type; implemented with OpenRaft. |
| `nodeId` | u64 / `1` | Stable Controller node identity, distinct from Broker ID. Do not reuse another node's data directory. |
| `listenAddr` | SocketAddr / `127.0.0.1:60109` | Controller remoting listener used by Brokers and admin requests. |
| `raftListenAddr` | optional SocketAddr / unset | Explicit Raft bind address; otherwise resolved from configured peer endpoints. Keep remoting and Raft listeners distinct. |
| `raftPeers` | array of `{ id, addr }` / empty | Raft membership endpoint input. `controllerPeers` is the separate remoting peer list; typed endpoint alternatives also exist in the schema. |
| `electionTimeoutMs` | u64 / `1000` ms | Raft election timing input; coordinate it with heartbeat timing and network conditions. |
| `heartbeatIntervalMs` | u64 / `300` ms | Raft heartbeat timing input; not the Broker heartbeat lease timeout. |
| `storageBackend` | enum / `RocksDB` | Serialized variants `RocksDB`, `File`, `Memory`. Memory is for tests and does not provide restart durability. |
| `storagePath` | string / empty | Preferred storage root. If empty, use `controllerStorePath`; if both are empty, use `rocketmqHome/controller/node-<nodeId>`. |
| `snapshotLogsSinceLast` | u64 / `5000` | Positive log-entry threshold for snapshots. |
| `snapshotMaxLogEntriesToKeep` | u64 / `1000` | Positive retained-log setting associated with snapshots. |
| `enableElectUncleanMaster` | bool / `false` | Whether election may choose outside the sync-state set. Keep the data-loss tradeoff explicit. |
| `enableElectUncleanMasterLocal` | bool / `false` | Separate local unclean-election setting; do not assume it is implied by the other flag. |
| `authenticationEnabled` / `authorizationEnabled` | bool / `false` / `false` | Authorization requires authentication. Privileged maintenance additionally requires its dedicated policy and storage configuration. |

Use the complete [three-node Controller example](../deployment/high-availability.md), which assigns separate remoting/Raft ports and persistent roots. `ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER=true` opts into initial cluster bootstrap; the lowest configured node ID performs initialization for a new cluster. It is not a command to replace existing membership. A healthy Raft control plane does not itself replicate Broker message bodies.

### Snapshot updates are not topology operations

Controller's administrative update path checks its blacklist, parses known properties, validates a candidate, and atomically publishes a configuration snapshot. Unknown or invalid properties leave the active snapshot unchanged. This path does not write the managed startup file or recreate existing listeners, storage engines, or the Raft instance. Even a successful update of a structural property must not be treated as an applied node-identity, membership, or backend migration. Persist intended startup settings in deployment configuration and use the supported operational procedure for structural changes.

## Proxy

### Modes, file values, and overrides

The root `mode` is `"cluster"` or `"local"`. A build with `cluster-mode` defaults to Cluster; a Local-only build defaults to Local. The selected mode requires its corresponding compiled feature. Cluster forwards through client infrastructure to external Brokers, while Local owns an embedded Broker path; the local configuration is not a full Broker TOML schema.

After reading the file or defaults, Proxy applies explicit `--mode`, `--grpcListenAddr`, `--remotingListenAddr`, `--enableRemoting`, and `-n` / `--namesrvAddr`. `--enableRemoting` only enables the listener; use the file to disable it. `--namesrvAddr` updates the Cluster configuration and requires `cluster-mode`. There is no universal environment-to-every-field mapping; use explicit file values, with the separately documented telemetry and security environment inputs.

```toml
mode = "cluster"
[grpc]
listenAddr = "127.0.0.1:8081"
[remoting]
enabled = false
[cluster]
namesrvAddr = "127.0.0.1:9876"
brokerClusterName = "DocsCluster"
```

| External key | Type / default | Scope and unit |
| --- | --- | --- |
| `grpc.listenAddr` | string / `0.0.0.0:8081` | gRPC ingress socket address. |
| `grpc.maxDecodingMessageSize` / `grpc.maxEncodingMessageSize` | usize / `8388608` each | Maximum encoded gRPC message sizes in bytes. |
| `grpc.maxMessageBodySize` | usize / `4194304` | Maximum uncompressed body bytes for one message. |
| `grpc.maxSendMessagesPerRequest` | usize / `1024` | Send batch count limit; separate from byte limits. |
| `grpc.maxDecompressedRequestBytes` | usize / `8388608` | Decompressed request byte budget. |
| `grpc.gzipDecodeSlots` | usize / `2` | Resident gzip decode capacity; zero disables gzip. |
| `grpc.concurrencyLimitPerConnection` | usize / `256` | Per-connection request concurrency limit. |
| `grpc.timerMaxDelayMs` / `grpc.timerPrecisionMs` | u64 / `86400000` / `1000` ms | Proxy delay admission horizon and precision; Broker timer support remains independently necessary. |
| `remoting.enabled` | bool / `false` | Enables optional remoting ingress. |
| `remoting.listenAddr` | string / `0.0.0.0:8080` | Remoting ingress address, separate from gRPC. |
| `cluster.namesrvAddr` | optional string / unset | External NameServer selection in Cluster mode. |
| `cluster.brokerClusterName` | string / `DefaultCluster` | Target Broker cluster. |
| `cluster.mqClientApiTimeoutMs` / `cluster.sendMessageTimeoutMs` | u64 / `3000` each | Cluster client API and send timeouts in milliseconds. |
| `cluster.commandQueueCapacity` / `cluster.commandQueueMaxBytes` | usize / `1024` / `67108864` | Independent request count and byte budgets. |
| `cluster.ioMaxInflight` / `cluster.longPollMaxInflight` | usize / `16` / `256` | Ordinary I/O and long-poll concurrency budgets. |
| `local.brokerName` | string / `rocketmq-proxy-local` | Embedded Broker name in Local mode. |
| `local.brokerListenPort` | u16 / `10911` | Embedded Broker port, independent of Proxy ingress. |
| `local.storeRootDir` | string / `store/proxy/local-broker` | Local storage root relative to the working directory unless absolute. |

Treat ordinary mode, listener, backend, and admission-limit changes as startup configuration. Parsing does not establish that all limits form a valid runtime combination; startup and the owning subsystem perform further validation. Use [Proxy deployment](../deployment/proxy.md) for route/send/pull examples and their current execution coverage.

### Reloadable gRPC TLS material

`grpc.tls.enabled` defaults to `false`. When enabled, `certificatePath` and `privateKeyPath` are required. `clientAuth` is `none` by default, or `optional` / `require`; both certificate-validating modes require `clientCaPath`. `reloadIntervalMs` defaults to `5000` and must be positive when TLS is enabled.

The TLS worker detects changed file metadata, validates a complete replacement generation, and installs it for new connections. Invalid replacements retain the last usable generation and produce a diagnostic. Existing connections do not redo their handshake. An unchanged invalid file is not continually retried as a new generation. This reload mechanism does not reload the entire Proxy TOML or rotate upstream RocketMQ credentials. See [security deployment](../deployment/security.md) for the separate ingress, upstream, and ACL boundaries.

## Shared logging and observability

The startup log filter resolves from `--log-filter`, `RUST_LOG`, `logging.filter`, legacy `logFilter`, then fallback. Canonical telemetry settings live under `observability` and combine with supported, present environment overrides. Exporter availability still depends on the compiled feature set. Use [observability configuration](../configuration/observability.md) and [monitoring](../operations/monitoring.md); do not assume that a metric exported by one service exists in every service.

Sources: [NameServer loader](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-namesrv/src/bin/namesrv_bootstrap_server.rs), [NameServer update path](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-namesrv/src/bootstrap/config_apply.rs), [Controller schema](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/src/config/controller_config.rs), [Controller snapshot publication](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/src/config.rs), [Proxy CLI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/src/bin/rocketmq-proxy-rust.rs), and [Proxy ingress schema](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/src/config.rs).

Controller backend selection also depends on build features: `RocksDB` requires `storage-rocksdb`, while the runtime `File` path requires `dev-single`. See [features and platforms](./features-platforms.md).
