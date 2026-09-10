---
title: "Multiple NameServers and Broker groups"
---

This reproducible development layout adds a second NameServer and a second independent Broker group on one host. It demonstrates discovery and Topic distribution across Brokers. Both Brokers are masters of different groups, so this is not a primary/replica HA deployment.

Stop the [single-Broker tutorial](../getting-started/local-source.md) before reusing its ports. Keep all terminals at the repository root. The checked-in [multi-node configs](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples/multi-node) use new data directories and loopback listeners.

## Fix identities and paths first

| Process | Client endpoint | Other port | Identity | Data root under `.rocketmq-multi-demo` |
| --- | --- | --- | --- | --- |
| NameServer 1 | `127.0.0.1:9876` | — | Independent route service | `ns-1` |
| NameServer 2 | `127.0.0.1:9877` | — | Independent route service | `ns-2` |
| Broker A | `127.0.0.1:10911` | Fast `10909`, HA `10912` | `docs-broker-a`, ID 0 | `broker-a` metadata, `store-a` messages |
| Broker B | `127.0.0.1:10931` | Fast `10929`, HA `10932` | `docs-broker-b`, ID 0 | `broker-b` metadata, `store-b` messages |

Both Broker groups belong to `DocsCluster` and register with both NameServers. Different Broker names make them independent groups; equal ID 0 is valid across those groups. A replica of Broker A would instead share its Broker name and use a different ID and store, as described in [HA deployment](high-availability.md).

## Prepare and start four processes

Create the six data directories. PowerShell:

```powershell
New-Item -ItemType Directory -Force .rocketmq-multi-demo/ns-1, .rocketmq-multi-demo/ns-2, .rocketmq-multi-demo/broker-a, .rocketmq-multi-demo/broker-b, .rocketmq-multi-demo/store-a, .rocketmq-multi-demo/store-b
$env:ROCKETMQ_HOME = "$PWD"
$env:ROCKETMQ_SECURITY_PROFILE = "development-insecure-loopback"
$env:NAMESRV_ADDR = "127.0.0.1:9876;127.0.0.1:9877"
```

Unix shell:

```bash
mkdir -p .rocketmq-multi-demo/{ns-1,ns-2,broker-a,broker-b,store-a,store-b}
export ROCKETMQ_HOME="$PWD"
export ROCKETMQ_SECURITY_PROFILE=development-insecure-loopback
export NAMESRV_ADDR='127.0.0.1:9876;127.0.0.1:9877'
```

Repeat the environment settings in each service terminal. Start the first two commands in separate terminals, observe successful startup, then start the two Brokers separately:

```bash
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- -c rocketmq-website/examples/multi-node/namesrv-1.toml
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- -c rocketmq-website/examples/multi-node/namesrv-2.toml
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/multi-node/broker-a.toml -n '127.0.0.1:9876;127.0.0.1:9877'
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- -c rocketmq-website/examples/multi-node/broker-b.toml -n '127.0.0.1:9876;127.0.0.1:9877'
```

Each command remains running; this is not a sequential script for one terminal. The explicit Broker `-n` prevents an unrelated environment value from replacing the intended endpoint list. Semicolons must be quoted in shell arguments.

## Provision and compare routes

Set `NAMESRV_ADDR` in the Admin terminal as above. These commands create/update metadata in this dedicated cluster:

```bash
cargo run -p rocketmq-admin-cli -- topic updateTopic -t DocsFirstMessage -c DocsCluster -r 4 -w 4 -n '127.0.0.1:9876;127.0.0.1:9877'
cargo run -p rocketmq-admin-cli -- consumer updateSubGroup -g docs_first_message_consumer -c DocsCluster
```

Query both NameServers independently:

```bash
cargo run -p rocketmq-admin-cli -- cluster clusterList
cargo run -p rocketmq-admin-cli -- topic topicRoute -t DocsFirstMessage -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- topic topicRoute -t DocsFirstMessage -n 127.0.0.1:9877
```

After registration converges, both route responses should contain `docs-broker-a` and `docs-broker-b` with their different ports, each with four read/write queues. Check each command's result: a cluster-wide metadata update can encounter partial failures.

The current `clusterList` and `updateSubGroup` commands take the NameServer environment setting rather than `-n`. The Topic commands expose their own `-n` option; it is not a universal root option.

## Exercise messages and discovery

Run the [first-message consumer and producer](../getting-started/quick-start.md). Their fixed NameServer endpoint remains `127.0.0.1:9876`, so the unchanged sample tests message routing through the first discovery node; it does not demonstrate client-side NameServer failover. The route still exposes both Broker groups.

To test discovery failover in your application, configure its NameServer address list with both endpoints, record successful route/message operations, stop only one dedicated NameServer, and observe further operations and eventual route refresh. Warm cached routes can keep working without a new discovery request, so a successful send alone is insufficient to prove endpoint failover.

Do not stop a Broker and call the other Broker its replica. Messages written to group A are not automatically present in group B. For message durability and promotion, use the [HA topology](high-availability.md).

## Move to separate hosts

Replace loopback with addresses reachable from every intended client and peer. Bind addresses describe local interfaces; `brokerIp1` is advertised to clients, and HA addresses serve replication. A reachable NameServer with an unreachable advertised Broker still produces send failures.

Retain a unique data root per process. Configure the same NameServer list in all Brokers and applications. Check firewall rules for actual remoting, fast, HA, and any health endpoints. The development loopback security profile cannot be reused with non-loopback listeners; configure [deployment security](security.md) before exposing a shared network.

## Stop and evidence limits

Stop application clients, then Brokers, then NameServers using their terminal interrupt and inspect shutdown results. Keep data directories for restart; deletion is not required.

The configuration files can be checked with the corresponding binary's `-c <file> -p` mode. Printing configuration establishes parsing, not a four-process message run or failure-recovery measurement. Use actual route, message, and progress observations when recording a deployment trial.
