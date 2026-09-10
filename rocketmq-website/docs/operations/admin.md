---
title: "Operate the cluster with Admin CLI"
---

Use `rocketmq-admin-cli` for cluster observations and explicitly selected metadata operations. It is the Rust CLI in this repository, with domain subcommands such as `topic`, `consumer`, and `ha`. A registered command is a client capability; the contacted service must also support and authorize its request.

## Connect to the intended cluster

From the repository root, build the matching CLI:

```bash
cargo build -p rocketmq-admin-cli
```

Use `target/debug/rocketmq-admin-cli` on Unix or `target/debug/rocketmq-admin-cli.exe` on Windows. The examples below use `cargo run -p rocketmq-admin-cli --` so they work without editing PATH. See [source installation](../getting-started/installation.md) for native build prerequisites.

Set the NameServer environment in the Admin terminal:

```powershell
$env:NAMESRV_ADDR = "127.0.0.1:9876"
```

```bash
export NAMESRV_ADDR='127.0.0.1:9876'
```

For multiple NameServers, use a quoted semicolon-separated list. This is `NAMESRV_ADDR`, not `ROCKETMQ_NAMESRV_ADDR`. `-n` is available on selected subcommands, not a global root option. In particular, `cluster clusterList` and `consumer updateSubGroup` use the environment for discovery.

For secured services, provide the paired `ROCKETMQ_ACL_ACCESS_KEY` and `ROCKETMQ_ACL_SECRET_KEY`, plus optional `ROCKETMQ_ACL_SECURITY_TOKEN`, through the operator's protected environment. See [deployment security](../deployment/security.md). Confirm the endpoint and identity before changing state.

## Read the route before diagnosing a client

These commands observe the [tutorial cluster](../getting-started/local-source.md):

```bash
cargo run -p rocketmq-admin-cli -- cluster clusterList -c DocsCluster
cargo run -p rocketmq-admin-cli -- topic topicList -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- topic topicRoute -t DocsFirstMessage -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- topic topicStatus -t DocsFirstMessage -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- broker brokerStatus -b 127.0.0.1:10911
```

`clusterList` shows registration information; `topicRoute` identifies the Broker addresses and queues returned for a Topic. `topicStatus` exposes queue offset bounds; `brokerStatus` queries the selected Broker's runtime data. Route visibility does not establish successful persistence, replication, or application consumption.

With multiple NameServers, query each address separately to investigate divergent observations. Test route-returned addresses from the application's network. An Admin process on the Broker host can succeed even when a remote client cannot reach an advertised loopback address.

## Observe consumers and progress

```bash
cargo run -p rocketmq-admin-cli -- connection consumerConnection -g docs_first_message_consumer -b 127.0.0.1:10911
cargo run -p rocketmq-admin-cli -- consumer consumerProgress -g docs_first_message_consumer -t DocsFirstMessage -n 127.0.0.1:9876
```

The connection query targets a Broker and shows the group's current client/subscription observations. Its domain is `connection`, not `consumer`. Progress relates the group's committed queue position to Broker queue bounds. Query twice over a known interval when investigating movement; preserve Topic, group, Broker name, and queue ID in the comparison.

| Observation | Interpretation and next step |
| --- | --- |
| No registered client | Check lifecycle start, group name, heartbeat/connectivity, auth, and whether that client mode registers the expected observation |
| Connection present, progress stationary | Inspect subscription/filter, assignment, processing results, retry, and offset/receipt completion |
| Progress behind the minimum retained offset | Data may have expired from local retention; inspect the consumer's offset correction and business reconciliation |
| Reported lag reaches zero | The observed committed position caught up at that moment; this is not proof that a separate business database committed |
| Duplicate processing after restart | Check whether business completion preceded offset/ACK persistence; apply [idempotent delivery handling](../guides/delivery-and-retry.md) |

LitePull local `commit_all` and a later Broker-visible persistence are distinct. POP uses receipt completion and invisibility rather than interpreting a classic group-offset query as proof that every popped message was ACKed.

## Inspect replication and Controller authority

For the [HA tutorial](../deployment/high-availability.md), choose the **current** Controller leader's remoting address:

```bash
cargo run -p rocketmq-admin-cli -- controller getControllerMetaData -a 127.0.0.1:19878
cargo run -p rocketmq-admin-cli -- ha getSyncStateSet -a 127.0.0.1:19878 -c DocsCluster -b docs-ha
cargo run -p rocketmq-admin-cli -- ha haStatus -b 127.0.0.1:10911
```

The addresses are examples, not a claim that ordinal one remains leader or port `10911` remains the writable Broker. Compare actual leader/role, authority epochs, synchronized membership, and replication progress before planning a restart. A registered replica is not necessarily eligible to satisfy the current write requirement. The Controller stores authority metadata, not a backup of message bodies.

## Create isolated tutorial resources

The following commands **create or update metadata**. Use them only for the isolated tutorial resource names and intended cluster:

```bash
cargo run -p rocketmq-admin-cli -- topic updateTopic -t DocsFirstMessage -c DocsCluster -r 4 -w 4 -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- consumer updateSubGroup -g docs_first_message_consumer -c DocsCluster
```

Reusing an existing Topic/group can update its configuration. Query the route and group behavior after the operation. A cluster-targeted operation may affect multiple Brokers and is not a transaction across them; inspect failures and resulting state per target.

## Operations that change consumption or remove state

| Command family | Effect to plan before execution |
| --- | --- |
| `offset resetOffsetByTime` | Changes the group's queue positions; older positions replay retained data, newer positions skip processing |
| `offset cloneGroupOffset` | Replaces destination progress based on another group; their subscriptions and business completion may differ |
| `offset skipAccumulatedMessage` | Skips backlog; it does not perform the missing business work |
| `consumer deleteSubGroup` / `topic deleteTopic` | Removes metadata used by applications; data cleanup and route convergence are separate effects |
| Broker configuration / cleanup commands | Can affect admission, retention, or persisted state depending on the specific operation |

Before resetting offsets, stop or coordinate affected consumers, record existing per-queue positions, identify the retained data interval, and decide how the application handles replay or skipped work. Inspect the current help without changing state:

```bash
cargo run -p rocketmq-admin-cli -- offset resetOffsetByTime --help
```

Its timestamp option is `-s` / `--timestamp`, accepting epoch milliseconds, a documented formatted time, or `now`. `now` deliberately skips current backlog. Online notification and legacy offline fallback depend on the server/client path; verify the resulting positions and application behavior. A successful CLI invocation is not a replacement for that observation.

## Errors and offline inspection

Capture the command domain, non-secret arguments, endpoint, time, exit code, and stable error code. `--verbose` adds controlled diagnostics; it is not a request to print full credentials or message bodies. An unsupported server operation, authentication denial, invalid argument, and unreachable Broker require different responses.

`rocketmq-store-inspect` is a separate offline package with binary `rocketmq-cli-rust`. It does not connect to the cluster. Use it on stopped or consistent copied store state as described in [backup/recovery](backup-recovery.md) and [upgrade/rollback](upgrade-rollback.md), not as an online Admin replacement.

## Source map

[CLI command registration](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/commands), [credential/error handling](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/src/rocketmq_cli.rs), [Admin Core](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-core), [offline inspection tool](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-store-inspect/README.md).
