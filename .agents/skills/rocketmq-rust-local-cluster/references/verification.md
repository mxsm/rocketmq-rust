# Verification and Troubleshooting

## Readiness and routes

Check live processes, expected listeners, and each `ready_url`. Then verify route registration through **each** configured NameServer; one responsive NameServer does not prove that Brokers registered with both. Match the observed cluster name, broker names, replica IDs, and addresses to the manifest, allowing Controller-assigned persistent identity and elected roles.

The Rust administrative binary is `rocketmq-admin-cli` and has command categories. Use its current `--help`; do not assume Java's flat `mqadmin` invocation or a global `-n` flag. For commands below, set `NAMESRV_ADDR` to the manifest's NameServer list in the admin process environment. Inspect conflicting Java-property-style address overrides before use. In PowerShell use `& $admin`; in Bash use `"$admin"`; examples below use the binary name for readability.

```text
rocketmq-admin-cli cluster clusterList
rocketmq-admin-cli topic topicRoute -t LOCAL_SMOKE_TOPIC
rocketmq-admin-cli ha haStatus -b 127.0.0.1:10911
rocketmq-admin-cli controller getControllerMetaData -a 127.0.0.1:9878
rocketmq-admin-cli ha getSyncStateSet -a 127.0.0.1:9878 -b broker-a
```

Replace sample endpoints with actual manifest values and the current writable Broker/Controller leader. Inspect `haStatus` for ordinary replication; Controller metadata and sync-state commands apply only to Controller HA. The zero-offset examples are not fixed requirements.

## Direct Broker message round trip

Use a new topic name and a unique message key/body token for this environment/run. Do not reuse an unrelated application topic or consumer group. For example, with `NAMESRV_ADDR` scoped to the selected environment:

```text
rocketmq-admin-cli topic updateTopic -c Local-dev -t LOCAL_SMOKE_TOPIC -r 1 -w 1
rocketmq-admin-cli message sendMessage -t LOCAL_SMOKE_TOPIC -p LOCAL_RUN_TOKEN -k LOCAL_RUN_TOKEN -b broker-a -i 0
rocketmq-admin-cli message consumeMessage -t LOCAL_SMOKE_TOPIC -b broker-a -i 0 -o 0 -c 1
```

Substitute the actual cluster name (`Local-<environment name>`), unique topic, broker name, and token. Check exit status and returned send status, message identity, and consumed token. `Consume ok`, an empty poll, or `SEND_OK` alone is insufficient. Offset zero is appropriate only for a fresh topic/queue; for retries, use the actual offset or a fresh topic. Bound the command duration and keep the test to a few messages. In two-master/two-slave mode, repeat on each broker group so success on one group does not hide a failure on the other.

The `rocketmq-example` project is standalone and examples may contain hardcoded addresses, topics, or message loops. Read its local `AGENTS.md` and the selected example before using it. Do not assume a root-workspace `cargo run -p rocketmq-example` works or run an unbounded producer as a smoke test.

## Proxy message round trip

For cluster mode, first verify the backend Broker path, then use a RocketMQ v5 gRPC client against the actual Proxy endpoint. Query routes, send a unique normal message, receive it with a dedicated group, and acknowledge its receipt handle. For local mode, this Proxy path is the message test; no NameServer registration should be expected. Provision topic/group metadata through a supported path for the selected mode, inspecting its current behavior instead of assuming auto-creation.

Useful protocol sources are `rocketmq-proxy-core/proto/service.proto` and `definition.proto`; examples of request construction are in `rocketmq-proxy/tests/grpc_ingress.rs`. Those tests launch their own fixtures and do **not** validate the user's running environment. Prefer an existing compatible SDK or generate a small bounded external smoke client using those schemas when needed. Support required session/metadata exchanges from the current implementation. A raw TCP connection, HTTP curl against the gRPC port, or a successful gRPC transport with non-OK application status does not establish message delivery.

`rocketmq-proxy/examples/proxy_live_fault_driver.rs` is a fault/overload driver requiring ACL and workload environment variables. Do not run it unchanged as a basic smoke test. If a usable v5 client is unavailable, report Proxy process/readiness and backend results separately, state that ingress delivery remains unverified, and provide a concrete client setup step.

If Remoting ingress was requested, additionally test a supported operation through that enabled endpoint. A direct Broker send does not validate Proxy routing.

## HA checks and optional failover

For ordinary HA, inspect master/slave connection state and replication offsets after sending a message. Wait within a deadline for the replica to catch up. Report whether writes use synchronous or asynchronous replication. Stopping a master does not imply automatic promotion in this topology.

For Controller HA, query all Controller endpoints and establish an agreed leader and membership, then inspect each Broker group's elected master and sync-state set. All three configured processes being alive is not quorum evidence. Confirm enough synchronized replicas for the configured write requirement before declaring writable HA ready.

When the user requests a failover exercise, operate only on this environment's verified processes:

1. Send and consume a unique baseline message; record the elected leaders, synchronization state, and successful acknowledgment.
2. Stop one Controller leader, retain two voters, and wait for a new leader with bounded polling. Re-query metadata and verify message delivery. Restore the stopped Controller and wait for recovery before another fault.
3. With the Controller quorum healthy and Broker replicas caught up, stop the writable Broker. Observe a new master and route convergence; send and consume another unique message and check that the pre-failure acknowledged message remains readable.
4. Restore the original Broker with its data intact, wait for synchronization, and verify delivery again. Leave all intended replicas running unless the user requested a degraded environment.

Graceful drain and abrupt process termination exercise different behavior; label which was tested. Do not stop multiple Controller voters or combine faults unless explicitly requested. For ordinary asynchronous replication, successful recovery in one exercise does not establish an RPO guarantee. Single-host testing never proves host-level availability.

## Diagnose the last failed layer

| Symptom | Targeted checks |
| --- | --- |
| Build failure | Selected toolchain; exact package/features; native compiler, bindgen, or Proxy protoc error; no automatic dependency upgrades |
| Config rejected | Broker section ownership; camelCase names; TOML-escaped absolute paths; `ROCKETMQ_HOME`; current `--help` |
| Bind failure | Business, fast, HA, Raft, Proxy, health, metrics, and diagnostic ports; inherited listener overrides; actual owner |
| Broker missing from routes | NameServer list/reachability, cluster/broker identity, registration logs, advertised IP/port; query both NameServers |
| Writes wait/fail in sync mode | Slave connection, HA master address, lag, sync-state set, replica counts; do not reduce acknowledgment thresholds |
| No Controller leader | Both peer lists, separate remoting/Raft ports, lowest-ID bootstrap opt-in, peer reachability, existing membership/storage |
| Proxy ready but delivery fails | Compiled mode feature, backend clusterName, v5 protocol/TLS/client metadata, topic/group provisioning, application status |
| Restart fails | Stale process record vs live process, locked/reused data directory, changed membership, previous forceful shutdown |

Stop and report an actionable blocker after repeated identical failures with no new corrective step. Keep valid independent parts of the setup and preserve logs/data. Do not label an unavailable component as unsupported merely because a local prerequisite is missing.

## Delivery

Report topology and replica/ack semantics, executable/build profile, actual NameServer/Broker/Proxy/Controller endpoints, environment and log paths, reusable lifecycle commands, and what was observed. Distinguish generated, parsed, started, ready, message-verified, replication-verified, and failover-tested states. Include concrete failure reproduction details only when a stage did not pass.
