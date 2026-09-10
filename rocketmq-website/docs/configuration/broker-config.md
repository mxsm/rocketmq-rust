---
title: "Broker configuration reference"
---

# Broker configuration reference

Broker startup reads a typed configuration, normalizes ownership between sections, validates it, and then constructs the runtime. This page describes canonical TOML names and the supported online update surface. For a runnable local cluster, use [local source setup](../getting-started/local-source.md); for replica settings, use [high availability](../deployment/high-availability.md).

## File selection and precedence

1. `-c` / `--configFile` selects a file. Without it, the command uses `$ROCKETMQ_HOME/conf/broker.toml` if that file exists; otherwise it uses defaults. Set `ROCKETMQ_HOME` explicitly for a reproducible installation.
2. `--config-format toml|properties` selects the input format. Without this option, `.toml` selects canonical TOML, while `.conf` and `.properties` select the Java properties converter. An ambiguous extension needs an explicit format.
3. For the NameServer address, explicit `-n` / `--namesrvAddr` takes precedence over `NAMESRV_ADDR`, the file, and the default `127.0.0.1:9876`. An empty but present `NAMESRV_ADDR` takes the override path and resolves to that default; unset it to retain the file value.
4. Configuration validation precedes service startup. `-p` / `--printConfigItem` and `-m` / `--printImportantConfig` print the resolved configuration and exit without starting listeners.

The printed property map uses flat administrative names. It is an inspection view, not a canonical TOML file to copy back unchanged. Java properties conversion happens in memory and writes a conversion report; review that report for rejected or translated fields. `--conversion-report` selects the report path, otherwise the input extension is replaced by `.conversion.json`. Failure to write the report fails loading.

For example, from the repository root after building the Broker:

```powershell
$env:ROCKETMQ_HOME = (Get-Location).Path
& .\target\debug\rocketmq-broker-rust.exe -c .\rocketmq-website\examples\first-message\broker.toml -p
```

## Canonical section layout

```toml
[broker]
namesrvAddr = "127.0.0.1:9876"
brokerIp1 = "127.0.0.1"
listenPort = 10911
storePathRootDir = ".rocketmq-reference/broker"
autoCreateTopicEnable = false
autoCreateSubscriptionGroup = false

[broker.brokerIdentity]
brokerClusterName = "DocsCluster"
brokerName = "docs-reference-broker"
brokerId = 0

[broker.topicQueueConfig]
defaultTopicQueueNums = 4

[store]
storePathRootDir = ".rocketmq-reference/store"
brokerRole = "ASYNC_MASTER"
flushDiskType = "ASYNC_FLUSH"
mappedFileSizeCommitLog = 1073741824
mappedFileSizeConsumeQueue = 6000000
fileReservedTime = 72
deleteWhen = "04"
```

This is a local configuration example, not an HA topology. Relative paths resolve against the process working directory. `broker.storePathRootDir` owns Broker metadata; `store.storePathRootDir` owns message storage. They are distinct settings even when both default to the user's home directory plus `store`. Preserve both when backing up or restoring.

The root accepts `broker`, `store`, `logging`, legacy `logFilter`, and `observability`. Root, Broker, and store schemas reject unknown fields. A Java-style `brokerName` at the TOML root is therefore invalid. Configure the primary listener through `broker.listenPort`: `broker.brokerServerConfig.listenPort` is derived and cannot be supplied. Likewise, `store.enableControllerMode` and `store.duplicationEnable` are derived from their Broker counterparts and are rejected as independent input.

## Broker identity, routing, and admission

All paths in this table are relative to the TOML root. Defaults describe startup configuration before explicit overrides. Unless the online-update section says otherwise, apply changes through a controlled restart.

| External key | Type / default | Meaning and constraints |
| --- | --- | --- |
| `broker.brokerIdentity.brokerClusterName` | string / `DefaultCluster` | Nonblank cluster name; shared by intended cluster members. |
| `broker.brokerIdentity.brokerName` | string / `HOSTNAME`, then `COMPUTERNAME`, then `DEFAULT_BROKER` | Nonblank Broker group name. Replicas share a name; independent masters use different names. |
| `broker.brokerIdentity.brokerId` | u64 / `0` | Outside Controller mode, a master requires `0` and a slave requires a nonzero ID. Controller authority follows its own assignment and epoch rules. |
| `broker.namesrvAddr` | optional string / normally `127.0.0.1:9876` | Semicolon-separated valid endpoints; subject to CLI and environment precedence above. |
| `broker.brokerIp1` | string / detected local address, with loopback fallback | Advertised address for clients; choose one reachable from the client network. It is separate from the bind address. |
| `broker.listenPort` | u32 / `10911` | Valid TCP port with a valid fast port at `listenPort - 2`; effective range `3..=65535`. |
| `broker.storePathRootDir` | string / home + `store` | Nonblank metadata root. Changing it does not migrate existing metadata. |
| `broker.autoCreateTopicEnable` | bool / `true` | Enables automatic topic creation; explicit topic provisioning is easier to control in shared environments. |
| `broker.autoCreateSubscriptionGroup` | bool / `true` | Enables automatic subscription-group creation. |
| `broker.brokerPermission` | u32 / `6` | Read/write permission bits; online updates require at least one read/write bit. |
| `broker.topicQueueConfig.defaultTopicQueueNums` | u32 / `8` | Default queue count used by topic creation; does not repartition existing topics automatically. |
| `broker.enablePropertyFilter` | bool / `false` | Enables the Broker side of SQL/property filtering; a client selector alone is insufficient. |
| `broker.flushConsumerOffsetInterval` | u64 / `5000` ms | Consumer-offset persistence interval; distinct from the client's offset submission interval. |
| `broker.registerNameServerPeriod` | u64 / `30000` ms | Registration period. Registration visibility can differ between NameServers. |
| `broker.registerBrokerTimeoutMills` | i32 / `24000` ms | Broker registration request timeout. Preserve the external spelling `Mills`. |
| `broker.brokerFastFailurePendingMaxCount` | usize / `4096` | Positive bound on pending request count. |
| `broker.brokerFastFailurePendingMaxBytes` | usize / `67108864` bytes | Positive bound on pending request bytes; a separate limit from individual message size. |
| `broker.enableControllerMode` | bool / `false` | Enables Controller-based authority; requires a valid, nonblank `broker.controllerAddr`. |
| `broker.controllerAddr` | string / empty | Controller endpoints. Use the Controller deployment's remoting addresses, not Raft addresses. |

Compatibility fields are not performance promises. For example, `asyncSendEnable` is retained for Java configuration compatibility, and `sendRequestExecutorDetachedEnable` is ignored with a startup warning. Do not size Rust execution capacity from Java thread-pool settings copied into the file.

## Store, disk, and replication

| External key under `store` | Type / default | Unit and operational meaning |
| --- | --- | --- |
| `storeType` | enum / `LocalFile` | Backend selection; optional backends require the matching build features. Changing this selector is not data migration. |
| `storePathRootDir` | string / home + `store` | Nonblank message-store root. |
| `storePathCommitLog` | optional string / unset | Defaults to the CommitLog location under the store root. Custom paths must be included in backup and recovery. |
| `mappedFileSizeCommitLog` | usize / `1073741824` | Bytes per CommitLog segment; positive. Treat changes on an existing store as a storage-layout operation. |
| `mappedFileSizeConsumeQueue` | usize / `6000000` | Bytes per ConsumeQueue file: `300000 × 20`, not 30 MB. Must be positive. |
| `maxMessageSize` | i32 / `4194304` | Message-size limit in bytes. Client and Broker limits must agree; batching and protocol overhead need their own allowance. |
| `flushDiskType` | enum / `ASYNC_FLUSH` | `ASYNC_FLUSH` or `SYNC_FLUSH`. Local flush policy does not establish remote replication or business completion. |
| `flushIntervalCommitLog` | i32 / `500` | Milliseconds between periodic CommitLog flush attempts. |
| `flushCommitLogLeastPages` | i32 / `4` | Flush threshold in pages. Async mode rejects a zero threshold. |
| `syncFlushTimeout` | u64 / `5000` | Milliseconds to wait for the synchronous flush condition. A timeout is an uncertain send outcome, not proof the message is absent. |
| `fileReservedTime` | usize / `72` | Retention age in hours. Disk pressure and cleanup policy also affect deletion. |
| `deleteWhen` | string / `"04"` | Scheduled cleanup hour expression. Quote it in TOML. |
| `diskMaxUsedSpaceRatio` | usize / `75` | Percentage threshold; must be less than the force-clean threshold. |
| `diskSpaceCleanForciblyRatio` | usize / `85` | Percentage threshold; must be less than the warning threshold. |
| `diskSpaceWarningLevelRatio` | usize / `90` | Disk warning threshold in percent. Runtime normalization also clamps disk ratios to supported bounds. |
| `cleanFileForciblyEnable` | bool / `true` | Allows force-clean behavior; retention hours alone are not a guaranteed minimum retention period. |
| `messageIndexEnable` | bool / `true` | Enables message indexing. A runtime change does not promise a historical index rebuild. |
| `brokerRole` | enum / `ASYNC_MASTER` | `ASYNC_MASTER`, `SYNC_MASTER`, or `SLAVE`; combine with Broker identity and the intended HA mode. |
| `haListenAddress` | IP address / `0.0.0.0` | HA bind address. |
| `haListenPort` | usize / `10912` | Valid nonzero TCP port, distinct from the primary and fast remoting ports. |
| `totalReplicas` | usize / `1` | Expected replica count. |
| `inSyncReplicas` | i32 / `1` | In-sync replica setting. |
| `minInSyncReplicas` | usize / `1` | Minimum in-sync replicas; validation requires `1 <= min <= inSync <= total`. |
| `slaveTimeout` | usize / `3000` | Milliseconds; positive when synchronous replication or a stronger replica requirement uses it. |
| `haMaxTimeSlaveNotCatchup` | usize / `15000` | Milliseconds; positive in Controller mode or automatic ISR management. |

The default replica counts do not create a replicated service. Use a complete [HA configuration](../deployment/high-availability.md), separate store roots, reachable HA addresses, and the documented authority model. DLedger configuration is rejected, including the compatibility spellings `enableDledgerCommitLog` and `enableDlegerCommitLog`; it cannot enable Controller HA indirectly.

## Online updates and persistence

The ordinary Broker configuration transaction accepts exactly these six flat property names:

| Administrative property | Accepted runtime value | Canonical TOML path |
| --- | --- | --- |
| `autoCreateTopicEnable` | `true` or `false` | `broker.autoCreateTopicEnable` |
| `autoCreateSubscriptionGroup` | `true` or `false` | `broker.autoCreateSubscriptionGroup` |
| `brokerPermission` | Canonical integer `2..=7` with read/write permission | `broker.brokerPermission` |
| `defaultTopicQueueNums` | Canonical integer `1..=128` | `broker.topicQueueConfig.defaultTopicQueueNums` |
| `messageIndexEnable` | `true` or `false` | `store.messageIndexEnable` |
| `traceTopicEnable` | `true` or `false` | `broker.traceTopicEnable` |

Boolean spellings are lowercase; noncanonical integers such as `08` are rejected. The transaction validates the candidate configuration and removes unchanged values. Index updates also require a message-store runtime projection; without it, the request fails instead of silently changing a flag.

A successful response reports `applied=true` and **`persisted=false`**. Update the managed startup file separately to retain the setting after restart. Topic reconciliation after a runtime update does not persist the Broker configuration file. Known fields outside this surface require restart; unknown fields are unsupported. The CAS request additionally checks a positive `expectedGeneration` and returns the current generation on conflict. That generation identifies a runtime configuration snapshot, not a storage epoch.

Log-filter updates use a separate authorization-aware path, not this six-field transaction. Startup logging precedence is CLI, environment input, `logging.filter`, legacy `logFilter`, then fallback. Runtime filter changes require the enabled reload path and its authentication/authorization conditions. See [security](../deployment/security.md) and [observability configuration](./observability.md) for security materials and telemetry settings.

## Inspect before changing behavior

Print and review a candidate file before starting it. Compare advertised addresses, metadata/store roots, listener and HA ports, role/ID, and replica requirements as a set. For a running service, distinguish the effective runtime configuration from the file managed by deployment tooling. After a change, check the affected behavior: route registration for topic permissions, index queries for indexing, and actual send/replication results for durability settings.

The defining sources are [raw schema](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/config/raw.rs), [validation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/config/sections.rs), [Broker defaults](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/config/broker_config.rs), [store defaults](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/src/config/message_store_config.rs), and [runtime transactions](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/config/transaction.rs). This reference lists operationally significant fields; consult those schemas for specialized timer, compaction, and backend settings.
