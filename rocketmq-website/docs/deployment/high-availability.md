---
title: "Deploy primary/replica and Controller HA"
---

Choose either a fixed primary/replica group or a Controller-managed group. The two paths below use separate data directories but overlapping ports, so run only one at a time. They are loopback laboratories on one host, not host-failure-tolerant production deployments.

Read [HA authority and acknowledgement design](../architecture/ha-controller.md) before interpreting send results. Two processes, a connected HA socket, and a successful health probe are different observations from replicated durable acknowledgement.

## Common preparation

Use the [local NameServer configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/namesrv.toml) and its startup instructions. Stop previous tutorial Brokers that occupy `10911` or `10931`. Set `ROCKETMQ_HOME` to the repository root and `ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback` in each service terminal. The NameServer remains `127.0.0.1:9876`.

Create the directories belonging to the selected mode. In PowerShell:

```powershell
$docsHaNodes = @('default-master','default-slave')
foreach ($docsHaNode in $docsHaNodes) {
  New-Item -ItemType Directory -Force ".rocketmq-ha-demo/$docsHaNode/metadata", ".rocketmq-ha-demo/$docsHaNode/store"
}
```

For Controller mode, replace the array with `@('controller-broker-1','controller-broker-2','controller-broker-3')` and also create `.rocketmq-ha-demo/controller-1`, `controller-2`, and `controller-3`. On Linux, create the same directories with `mkdir -p`. These names are relative to `.rocketmq-ha-demo`.

All files are provided in the [HA example directory](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples/ha). Keep process working directories consistent so each configuration resolves its intended store.

## Path A: fixed synchronous primary and replica

| Process | Remoting / HA | Identity and role |
| --- | --- | --- |
| Primary | `10911 / 10912` | Broker `docs-ha`, ID 0, `SYNC_MASTER` |
| Replica | `10931 / 10932` | Broker `docs-ha`, ID 1, `SLAVE`; master HA endpoint `127.0.0.1:10912` |

Both use `SYNC_FLUSH` and a two-replica configuration. Fast remoting ports are `10909` and `10929`. Metadata and store roots are separate for each process.

Start these commands in separate terminals:

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/ha/default-master.toml -n 127.0.0.1:9876
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/ha/default-slave.toml -n 127.0.0.1:9876
```

Inspect HA progress before sending. A required replica that is not ready can prevent the requested synchronous result. Do not change the policy to async merely to make a test print success.

This path has no Controller election service. Stopping the primary does not automatically authorize the configured replica to become master. Recovery involves restoring the intended primary or a separately planned role transition with data/identity reconciliation.

## Path B: three Controllers and three Broker replicas

Build Controller with its default RocksDB backend; the native RocksDB toolchain and `protoc` must be available:

```bash
cargo build -p rocketmq-controller --bin rocketmq-controller-rust
```

| Node | Remoting | Raft gRPC | Persistent root |
| --- | --- | --- | --- |
| Controller 1 | `19878` | `19879` | `controller-1` |
| Controller 2 | `19888` | `19889` | `controller-2` |
| Controller 3 | `19898` | `19899` | `controller-3` |

Each file uses the same three Raft peers and a distinct `nodeId`. Remoting and Raft ports must differ. The generic distribution Controller sample is not copied unchanged: use the dedicated files above with distinct endpoints.

For the first bootstrap, set `ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER=true` in the Controller terminals. In PowerShell use `$env:ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER = "true"`; in a Unix shell use `export ROCKETMQ_CONTROLLER_AUTO_INITIALIZE_CLUSTER=true`.

Start each command in a separate terminal:

```bash
cargo run -p rocketmq-controller --bin rocketmq-controller-rust -- -c rocketmq-website/examples/ha/controller-1.toml
cargo run -p rocketmq-controller --bin rocketmq-controller-rust -- -c rocketmq-website/examples/ha/controller-2.toml
cargo run -p rocketmq-controller --bin rocketmq-controller-rust -- -c rocketmq-website/examples/ha/controller-3.toml
```

Only the lowest configured node ID initializes the full membership with this opt-in. Existing committed state is not reinitialized. Observe the elected leader and committed membership before starting Brokers; do not assume ordinal 1 remains leader.

```bash
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/ha/controller-broker-1.toml -n 127.0.0.1:9876
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/ha/controller-broker-2.toml -n 127.0.0.1:9876
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/ha/controller-broker-3.toml -n 127.0.0.1:9876
```

These Brokers share group `docs-ha`, use different IDs and stores, and begin configured as `SLAVE` with Controller mode enabled. Their remoting ports are `10911`, `10931`, `10951`; HA ports are one higher and fast ports two lower. Controller role assignment and a valid write lease are required for the selected master to accept writes. Store identity files persist across restart.

## Observe before and after a fault

Set `NAMESRV_ADDR=127.0.0.1:9876` for Admin commands. Query the actual current Controller leader's remoting endpoint; `19878` below is an example to replace if another node leads:

```bash
cargo run -p rocketmq-admin-cli -- controller getControllerMetaData -a 127.0.0.1:19878
cargo run -p rocketmq-admin-cli -- ha getSyncStateSet -a 127.0.0.1:19878 -c DocsCluster -b docs-ha
cargo run -p rocketmq-admin-cli -- ha haStatus -b 127.0.0.1:10911
```

The first two commands apply to Controller mode; HA status can inspect the selected Broker in either mode. Provision `DocsFirstMessage` and its consumer group through the [quick start](../getting-started/quick-start.md), then record send statuses, received IDs, offsets, current authority, and replica durable progress.

A controlled fault trial stops one dedicated process at a time. Record the interval of unavailable or uncertain writes, observe any new role/epoch and route, and reconcile acknowledged IDs after recovery. Restart the same node with the same identity/store and wait for catch-up before the next fault. Do not delete an old master's data to manufacture a clean rejoin.

Controller quorum loss can stop lease renewal and safe promotion; two surviving Broker processes alone cannot replace that quorum. A replica outside the current in-sync set is not sufficient acknowledgement evidence. Do not enable unclean election as a troubleshooting shortcut.

## Production and rollback boundaries

Place replicas and Controllers on separate failure domains, use reachable advertised endpoints and [security configuration](security.md), and size durable storage for retention, replay, and catch-up. A one-host experiment cannot measure tolerance of host or disk loss.

Changing configured Controller peers does not perform a Raft membership change. Rolling maintenance must retain quorum and the chosen Broker ACK capacity. A two-member group requiring both members has no spare synchronous capacity during a member restart.

These procedures describe the intended source-backed setup and observations. They do not claim that a failover or acknowledged-message recovery trial was run while writing the documentation, and they provide no fixed RPO/RTO. See [backup and recovery](../operations/backup-recovery.md) and [upgrade/rollback](../operations/upgrade-rollback.md) for preserving recoverable state.
