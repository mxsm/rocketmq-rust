# Topology Rules and Source Map

Paths below are relative to the repository root. These are targeted inspection entrypoints, not a requirement to read everything. If this checkout changes a field or behavior, adapt the environment and revalidate it.

| Component | Configuration and startup sources |
| --- | --- |
| Build | `Cargo.toml`, `rust-toolchain.toml`, selected package manifests |
| NameServer | `rocketmq-namesrv/src/bin/namesrv_bootstrap_server.rs`, `rocketmq-namesrv/src/config.rs` |
| Broker | `rocketmq-broker/src/command.rs`, `rocketmq-broker/src/config/raw.rs`, `rocketmq-broker/src/config/sections.rs`, `rocketmq-broker/src/config/broker_config.rs`, `rocketmq-store/src/config/message_store_config.rs` |
| Ordinary HA | `distribution/config/broker/1m-1s-async-one-machine/`, `distribution/config/broker/2m-2s-async/`, `scripts/interop/run_default_ha_interop.py` |
| Proxy | `rocketmq-proxy/src/bin/rocketmq-proxy-rust.rs`, `rocketmq-proxy/src/config.rs`, `rocketmq-proxy-local/src/config.rs`, `rocketmq-proxy/README.md` |
| Controller | `rocketmq-controller/src/cli.rs`, `rocketmq-controller/src/config/controller_config.rs`, `rocketmq-controller/src/config/peer_endpoints.rs`, `rocketmq-controller/README.md` |
| Controller HA composition | `distribution/helm/rocketmq-rust-core/templates/_config.tpl`; reuse field/role semantics, not container addresses |
| Security and probes | `rocketmq-security-api/src/secure_deployment.rs`, `rocketmq-runtime/src/service_lifecycle.rs` |

## NameServer and Broker

NameServer uses top-level camelCase fields. `distribution/config/nameserver/namesrv.toml` contains production TLS/ACL settings and `/opt` paths; do not copy it unchanged into a minimal environment. The helper sets loopback binding, per-node `rocketmqHome`, `kvConfigPath`, and `configStorePath`, and disables embedded Controller mode.

Canonical Broker TOML uses `[broker]`, `[broker.brokerIdentity]`, `[broker.brokerServerConfig]`, and `[store]`. Set the business port only through `broker.listenPort`; `broker.brokerServerConfig.listenPort` is derived and explicitly rejected in the input. Set `enableControllerMode` only in `[broker]`, not `[store]`.

The two `storePathRootDir` fields must identify the same instance's storage. Isolate `storePathBrokerIdentity` as well. The helper uses 64 MiB CommitLog segments for small local messages; this is not a production capacity recommendation. Check disk and memory headroom before adding replicas instead of promising one fixed minimum for every topology.

NameServer and Controller address lists use semicolons. Pass each list as one string. Clients must reach the `brokerIp1:listenPort` returned by routing; reaching only the NameServer is insufficient.

## Ordinary replication

- Replicas in a group share `brokerName` and `brokerClusterName`. The master has `brokerId = 0`, the slave `brokerId = 1`. Two master/slave groups have different broker names.
- Give each instance unique remoting, fast-channel, and HA ports. Point each slave's `haMasterAddress` at its own master's **HA port**.
- Synchronous presets use `SYNC_MASTER` / `SLAVE`, `totalReplicas = 2`, `inSyncReplicas = 2`, `minInSyncReplicas = 2`, and `SYNC_FLUSH`. Writes may wait for replicas while the master is alone; preserve this constraint.
- Asynchronous presets use `ASYNC_MASTER`, a minimum in-sync count of 1, and `ASYNC_FLUSH`. Acknowledgment can precede replication; do not promise zero message loss after failure.
- Maintain `1 <= minInSyncReplicas <= inSyncReplicas <= totalReplicas`. NameServer redundancy, readable slaves, and acting-master behavior do not establish automatic promotion to a writable master.

## Proxy

`mode = "cluster"` forwards to the backend through `[cluster].namesrvAddr`, with `brokerClusterName` matching the Brokers. Default ingress is gRPC on `8081`. `--proxy-remoting` additionally enables `8080`. Neither is a NameServer or HTTP management UI.

`mode = "local"` uses `[local]` fields `brokerClusterName`, `brokerName`, `brokerIp`, `brokerListenPort`, and `storeRootDir`. The current `rocketmq-proxy-local/src/local.rs` sets the embedded Broker's `namesrv_addr` to `None`. Do not start a separate NameServer and claim registration. The helper reserves the embedded Broker identity port; check the current embedded implementation for actual network listeners.

Proxy modes require matching Cargo features. Cluster-only builds use `--no-default-features --features cluster-mode`; local-only builds use `--no-default-features --features local-mode`. TLS requires the `tls` feature and certificate configuration. Adding ordinary Broker fields to Proxy's `[local]` table does not create Controller HA.

## Controller HA

- Start three Controller processes with `nodeId = 1/2/3`. Separate Broker-facing `listenAddr` from `raftListenAddr`, and isolate `storagePath`, `configStorePath`, and `controllerStorePath`.
- Every file contains identical three-entry `[[raftPeers]]` and `[[controllerPeers]]` lists pointing to Raft and Remoting endpoints respectively. Broker `controllerAddr` must use Remoting endpoints. DNS-based `raftPeerEndpoints` / `controllerPeerEndpoints` are mutually exclusive with these legacy IP peer lists; the native helper uses only the latter.
- Fresh multi-member clusters do not initialize by default. Set `ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER=true` on the lowest-ID member for bootstrap; explicitly set it to `false` on the others. Launch the whole group before waiting for a leader. Existing committed state is not reinitialized; preserve membership and data on restart.
- The three Brokers form one replica group, start with `brokerId = 1/2/3`, `brokerRole = "SLAVE"`, and `enableControllerMode = true`, and share the full Controller Remoting address list. Controller and persisted identity determine elected roles. Do not configure a fixed ID-zero master or static `haMasterAddress`.
- Use `totalReplicas = 3`, `inSyncReplicas = 2`, `minInSyncReplicas = 2`, and `SYNC_FLUSH`. Keep `enableElectUncleanMaster` and `enableElectUncleanMasterLocal` false. Diagnose an insufficient sync-state set instead of bypassing acknowledgment requirements.
- The preset retains default RocksDB storage and the `storage-rocksdb` feature. The opt-in File backend is documented in `rocketmq-controller/src/storage/file_backend.rs` for single-node development; do not silently substitute it in the HA preset to bypass native build requirements. `dev-single` enables File storage, not multi-node initialization. Memory storage is inappropriate for restart-persistent metadata.
- Separate Controllers are the default. If the user requests embedded NameServer Controllers, additionally inspect the `embedded-controller` feature, configuration bridge, and listener mapping.

## Maintaining the helper

After changing configuration generation, run `python -B -m unittest discover -s .agents/skills/rocketmq-rust-local-cluster/scripts -p test_prepare_cluster.py -v` from the repository root. These tests check topology relationships, output preservation, and port conflicts; they do not replace current binary configuration checks or live-cluster verification. Keep the `.agents/skills` and `.claude/skills` copies aligned.
