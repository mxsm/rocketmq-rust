---
title: "Diagnose your first message path"
---

Use this page when [local setup](../getting-started/local-source.md) or [quick start](../getting-started/quick-start.md) does not produce the expected result. Diagnose the next missing boundary: process, registration, route, send result, assignment, processing, then progress. Keep both service terminals visible.

## Begin with read-only evidence

Set the NameServer in this terminal before running the admin commands. In PowerShell:

```powershell
$env:NAMESRV_ADDR = "127.0.0.1:9876"
```

In a Unix shell:

```bash
export NAMESRV_ADDR=127.0.0.1:9876
```

The current `clusterList` and `updateSubGroup` subcommands use the environment and do not accept `-n`. The Topic and progress commands below accept their own `-n` option; it is not a global CLI flag.

Run from the repository root against the tutorial NameServer:

```bash
cargo run -p rocketmq-admin-cli -- cluster clusterList
cargo run -p rocketmq-admin-cli -- topic topicRoute -t DocsFirstMessage -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- consumer consumerProgress -g docs_first_message_consumer -t DocsFirstMessage -n 127.0.0.1:9876
```

These query cluster membership, Topic routing and group consumption progress. A newly created or inactive group may have no useful progress yet; that alone does not prove data loss. An error from the progress query is different from a valid result showing no backlog.

Record the command, error code/message, relevant service startup output, actual addresses and Topic/group names. Preserve enough context to reproduce the failure, while excluding credentials, tokens and message bodies.

## The service will not start

| Observation | Check | Next action |
| --- | --- | --- |
| Configuration parse/unknown-field error | File format, spelling and section placement | Broker settings belong in canonical sections such as `[broker]` and `[store]`; compare the checked-in tutorial config |
| Address already in use | NameServer, Broker, fast and HA listener ports | Identify the owning process; choose a free intended port or stop only the service you own |
| Development profile rejects binding | Every configured listener address | Keep the tutorial on loopback, including nested Broker binding and HA binding |
| Storage cannot open | Working directory, writable paths and directory ownership | Use the tutorial directories; do not share one store between two Brokers |
| Broker reports registration failure | NameServer process and `-n` address | Start NameServer first and confirm the configured endpoint is reachable |

On Windows, inspect listeners without modifying them:

```powershell
Get-NetTCPConnection -State Listen |
    Where-Object { $_.LocalPort -in 9876, 10909, 10911, 10912 } |
    Select-Object LocalAddress, LocalPort, OwningProcess
```

On Linux, `ss -ltnp` can show TCP listeners if available. The important evidence is the actual local address, port and owning process, rather than the presence of a window or a PID alone.

## NameServer responds, but there is no Broker or route

If `clusterList` lacks `docs-broker`, inspect registration before investigating consumer code. Check that the Broker points at this NameServer and is using the expected identity and normal startup path. A `-p` configuration print does not register a Broker.

If the Broker is present but the Topic route is absent, confirm that `updateTopic` succeeded for `DocsCluster` and `DocsFirstMessage`. Automatic Topic creation is disabled in the tutorial. Allow registration updates to propagate, then repeat the route query.

If a route exists but clients cannot connect, inspect its advertised address. `brokerIp1` must be reachable by those clients; changing a bind address alone is insufficient. A loopback address inside a container refers to that container, not to a host service.

## The producer starts, but sending fails

Read the returned status and error before changing timeouts. Check writable queue counts, Topic spelling, Broker reachability, resource permissions and the selected security settings.

Disk-flush or replica-wait timeouts are different from route lookup failures. They may have uncertain write outcomes; repeating the application can duplicate messages. A successful send is also independent of whether this Consumer Group is subscribed or running.

An authentication/authorization rejection is not repaired by creating a differently named Consumer Group. Align credentials, resource permissions and transport requirements for the intended deployment. The loopback tutorial's development profile is specific to its local configuration.

## The consumer polls but receives nothing

Follow this order:

1. Confirm a readable route for the exact Topic and reachable Broker address.
2. Confirm the consumer actually reached `CONSUMER_STARTED`, with the expected group and subscription.
3. Check whether another instance in the same group owns the available queues. Shared groups cooperate; they do not each receive an independent copy.
4. Compare the group's progress with the queue end. Existing committed progress can leave no new data to read.
5. Send new tutorial messages while the consumer's 60-second window is open. Earlier data may already have been consumed.
6. Inspect client diagnostics and assignment if polls remain empty. The poll API's empty vector is not a structured assertion that the Topic contains no messages.

`ConsumeFromFirstOffset` applies to a group without usable stored progress. It does not reset an existing group's position. A filter mismatch or inconsistent group subscription can also exclude messages.

## Messages repeat, or progress appears behind

A send timeout followed by retry can create duplicates. A consumer can also repeat business work after processing succeeds but progress is not persisted. Rebalance and process restart can expose those windows.

In current LitePull, `commit_all` updates client offset-store state separately from persistence and may log individual queue failures. The sample's `OFFSET_COMMIT_REQUESTED` line is not a durable Broker receipt. Read [LitePull commit semantics](../consumer/pull-consumer.md) and use business idempotency as described in [delivery and retry](../guides/delivery-and-retry.md).

Progress metrics describe queue positions; they do not inspect whether your database transaction executed correctly. If progress advanced before processing completed, investigate the application commit/worker order.

## Changes with state effects

| Operation | Effect |
| --- | --- |
| `updateTopic` / `updateSubGroup` | Creates or changes metadata |
| Sending the tutorial again | Adds more messages |
| Starting a consumer | Registers membership and can advance consumption progress |
| Resetting offsets | Changes what the group may replay or skip |
| Deleting a Topic or store directory | Removes configuration or data |

The first three are deliberate parts of the tutorial. Offset resets and deletion are not automatic troubleshooting steps. Keep existing data until the cause and intended recovery action are understood.

If reporting an issue, include the source/release used, operating system, selected binaries and features, sanitized configuration, exact command, and the first useful error. State whether only compilation was checked or a live send/consume path failed. Avoid replacing observed evidence with a claim that the entire cluster is broken.

Sources: [Admin CLI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/README.md), [Broker setup](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/README.md), [LitePull implementation](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/consumer_impl/default_lite_pull_consumer_impl.rs).
