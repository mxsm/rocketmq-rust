---
name: rocketmq-rust-local-cluster
description: Set up, start, verify, inspect, and stop local development clusters from the current rocketmq-rust checkout. Use for single-Broker development, replicated master/slave clusters, Proxy cluster/local mode, or three-node Controller HA with automatic Broker election.
---

# RocketMQ Rust Local Cluster

Turn the requested topology into a running, verifiable local environment with isolated data and an explicit stop procedure. Use this checkout's manifests, configuration parsers, and entrypoints. Java RocketMQ scripts and old flat Broker configuration are not substitutes for the Rust services.

## Select a topology

Preserve the user's topology, paths, ports, and execution method. Otherwise default to `dev-single`, native processes, loopback addresses, and a debug build; state the assumptions and continue. For an unspecified request for HA, select `controller-ha` and explain its larger resource needs. Select `dev-ha` when the user explicitly wants ordinary master/slave replication.

| Profile | Processes | Purpose and limits |
| --- | --- | --- |
| `dev-single` | 1 NameServer + 1 Broker | Minimum message round trip; no replica |
| `dev-ha` | 2 NameServers + 1 master + 1 slave | Synchronous replication by default; no automatic writable-master promotion |
| `dev-ha-2m2s` | 2 NameServers + 2 master/slave groups | Two distinct brokerName values; ordinary replication |
| `proxy-cluster` | `dev-single` + a separate Proxy | gRPC ingress forwarding to the backend cluster |
| `proxy-local` | 1 Proxy with an embedded Broker | Single-process development; no separate NameServer/Broker and no HA |
| `controller-ha` | 2 NameServers + 3 Controllers + 3 Brokers in one group | Raft quorum, automatic Broker election, and replica synchronization |

`--with-proxy` adds a separate Proxy to `dev-single`, `dev-ha`, `dev-ha-2m2s`, or `controller-ha`. It adds one ingress instance; Proxy redundancy needs additional instances and an endpoint strategy when requested.

These are **single-host development topologies**. They do not survive loss of the host or its disks. Explain synchronous replication (`SYNC_MASTER`), disk flush (`SYNC_FLUSH`), and automatic election as separate properties.

## Workflow

For status, stop, or restart requests, open the existing environment manifest and process records first. Skip configuration generation and builds unless a requested restart actually needs updated binaries. Do not create another cluster while inspecting an existing one.

1. Read applicable `AGENTS.md` instructions and `git status --short`. Inspect OS, toolchain, disk capacity, and port occupancy. Keep the selected Rust toolchain. Use [topology rules and source map](references/topologies.md) to check only the requested components.
2. Generate configuration and a run manifest with the helper below. It writes files and optionally probes port availability; it does not compile, start processes, or delete data. Stop at artifacts if the user requested only a plan/configuration; continue through startup and verification when asked to set up an environment.
3. Follow [operations and lifecycle](references/operations.md): build the necessary binaries, check configuration parsing, record process ownership, start dependency groups, and wait for readiness within a deadline. Initial compilation can dominate setup time; report actual progress.
4. Follow [verification and troubleshooting](references/verification.md): check registration and a message round trip. For HA, inspect replication/election state. Perform disruptive failover exercises when requested, then restore the environment.
5. Deliver the actual endpoints, configuration/data/log locations, verification results, and reusable start/status/stop commands. Leave the environment running when that is the requested outcome. Report the last completed stage on failure; generated configuration is not successful startup.

## Configuration helper

Run from the repository root using Python 3.11+. On Linux/macOS, `python3` may replace `python`. When using the Claude copy, replace the script prefix with `.claude/skills`.

```text
python .agents/skills/rocketmq-rust-local-cluster/scripts/prepare_cluster.py --name dev --profile dev-single --check-ports
python .agents/skills/rocketmq-rust-local-cluster/scripts/prepare_cluster.py --name ha --profile dev-ha --port-offset 1000 --check-ports
python .agents/skills/rocketmq-rust-local-cluster/scripts/prepare_cluster.py --name dual --profile dev-ha-2m2s --replication async --port-offset 2000
python .agents/skills/rocketmq-rust-local-cluster/scripts/prepare_cluster.py --name proxy --profile proxy-cluster --port-offset 3000
python .agents/skills/rocketmq-rust-local-cluster/scripts/prepare_cluster.py --name local --profile proxy-local --port-offset 4000
python .agents/skills/rocketmq-rust-local-cluster/scripts/prepare_cluster.py --name controller --profile controller-ha --with-proxy --port-offset 5000 --check-ports
```

- Output defaults to ignored `.rocketmq/clusters/<name>/`. Use `--repo-root` for another checkout or `--output` for a new, dedicated environment directory.
- `--dry-run` prints the manifest without writing files. `--check-ports` checks whether all planned loopback ports can currently bind; it does not reserve them for startup.
- Existing output directories are rejected to preserve configuration and messages. For restart/status/stop, read the existing `manifest.json`. Prefer a new environment for topology changes.
- `--replication async` applies only to ordinary HA profiles. Controller HA retains three replicas and a minimum of two in-sync replicas; do not lower write requirements to conceal startup or replication failures.
- Add `--proxy-remoting` only when Remoting ingress is requested. The default Proxy ingress is gRPC.
- Base ports: NameServer `9876/9886`; Broker remoting `10911 + 20*i`, fast channel `remoting - 2`, HA `remoting + 1`; Controller remoting `9878/9888/9898`, Raft `9879/9889/9899`; Proxy gRPC `8081`, optional remoting `8080`; health `18000 + node index`. `--port-offset` shifts all ports and their references together.

## Execution boundaries

- A request to set up the environment authorizes necessary local configuration, builds, startup, and non-disruptive verification without repeated confirmation. It does not authorize publication, remote-cluster changes, system services, or deleting old data.
- The native generator uses `127.0.0.1` and `development-insecure-loopback`. Inspect conflicting inherited TLS/ACL or listener environment settings; resolve them only for these new processes. Preserve security settings of existing environments.
- On port conflicts, choose another offset or the user's available ports. Do not terminate the current port owner. Isolate every instance's persistence and logs instead of using a shared default home-directory `store`.
- Record PID, process creation time, executable path, arguments, and configuration path. Verify identity before stopping; a process name or stale PID file alone is insufficient.
- Preserve data by default. For an explicitly requested reset, first resolve absolute paths, verify they remain in this environment and do not link elsewhere, and stop its processes. Delete only that environment's intended data.
- Docker/Compose, WSL, and local Kubernetes are optional execution methods. Use the [container branch](references/operations.md#containers-and-local-kubernetes) when requested; native loopback configuration cannot be mounted unchanged into multiple containers. Local setup does not require release, performance, full-workspace Clippy, or integration fault-matrix gates.

## Example requests

```text
Use $rocketmq-rust-local-cluster to start a minimal development cluster and verify one message round trip.
Use $rocketmq-rust-local-cluster to create an asynchronous two-master/two-slave environment with port offset 2000, preserving my existing cluster.
Use $rocketmq-rust-local-cluster to start Controller HA with Proxy and verify message delivery after automatic failover.
Use $rocketmq-rust-local-cluster to inspect the dev environment, then stop it while preserving messages.
```
