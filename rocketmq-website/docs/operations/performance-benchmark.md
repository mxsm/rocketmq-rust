---
title: "Performance benchmark: RocketMQ Rust and Java 5.5.0"
---

This page reports how RocketMQ Rust compares with Apache RocketMQ 5.5.0 under the same workloads on the same machine. All figures were measured in November 2026 on one host, with the services and the load generator pinned to separate CPUs. They show relative cost and throughput for the listed workloads, not the capacity of a production deployment; for sizing, see [capacity and performance](capacity-performance.md).

## Summary

| Area | Result |
| --- | --- |
| Broker, synchronous sends (1 KiB) | 1.79× Java's throughput, 64% less Broker CPU per message |
| Broker, asynchronous sends (1 KiB) | 1.19× with 4 connections, 1.23× with 16; 0.96× with a single connection |
| Broker, LitePull consumption | 1.30× Java's throughput, 37% less Broker CPU per message |
| Message size | Rust's lead grows with size: 1.34×–2.14× at 64 KiB, at 14%–37% of Java's Broker CPU per message |
| `SYNC_FLUSH` | 1.04× with 4 connections, 0.96× with one; both are bound by `msync` latency |
| Three replicas (Controller mode) | 1.20×–1.43×, at 70%–79% of Java's Broker CPU per message |
| Proxy, gRPC sends | 1.1×–3.6× the Java Proxy's throughput in the measured loads (not measured back to back, see below) |

The one load where Rust is still below Java is asynchronous sending over a single connection with many requests in flight (0.96×).

## Test setup

| Item | Setting |
| --- | --- |
| Host | Intel Core i7-11700K (8 cores, 16 threads), Docker Desktop on Windows with a WSL 2 Linux VM (kernel 6.18, 16 GB memory) |
| CPU placement | NameServer, Broker and Proxy on logical CPUs 0–7 (4 physical cores with hyper-threading); the load generator on CPUs 8–15 |
| Network and storage | Loopback; the store on the container's file system, not on tmpfs |
| RocketMQ Rust | Built as the release images build it (`docker/Dockerfile.base`): release features, the Broker with profile-guided optimization (PGO) and BOLT, the Proxy with PGO |
| RocketMQ Java | The `apache/rocketmq:5.5.0` image with its default start scripts and JVM settings (OpenJDK 8) |
| Broker defaults | One replica, `ASYNC_FLUSH`, 1 KiB messages, unless a section says otherwise |
| Load generator | The same Rust client for both Brokers: the `client-production-benchmark` example of `rocketmq-client`. For the Proxy, the `grpc_send_benchmark` example of `rocketmq-proxy` |
| Procedure | A fresh Broker or Java container per scenario, a warm-up run, then measured runs; the variants rotate their order across rounds, and medians of 4 to 9 runs per cell are reported. Every reported run had zero failed sends |

In-flight limits: a synchronous send keeps one request outstanding; "1 connection" keeps 256 asynchronous sends outstanding on one connection, "4 connections" 64 on each, and "16 connections" 16 on each. Broker CPU per message is the Broker process's CPU time divided by the messages sent, from `/proc` and `perf stat`.

## Broker: sending and consuming 1 KiB messages

| Load | Java 5.5.0 | Rust | Rust ÷ Java | Broker CPU per message, Java → Rust |
| --- | --- | --- | ---: | --- |
| Synchronous | 2.3k msg/s, p50 0.43 ms | 4.0k msg/s, p50 0.24 ms | 1.79 | 381 → 137 µs |
| Asynchronous, 1 connection × 256 | 116.6k msg/s | 112.4k msg/s | 0.96 | 44.9 → 46.8 µs |
| Asynchronous, 4 connections × 64 | 115.9k msg/s | 137.8k msg/s | 1.19 | 50.5 → 46.4 µs |
| Asynchronous, 16 connections × 16 | 100.9k msg/s | 123.7k msg/s | 1.23 | 58.4 → 51.4 µs |
| LitePull consumption | 107.7k msg/s | 140.0k msg/s | 1.30 | 17.9 → 11.2 µs |

The single-connection throughput is from the round after [#12699](https://github.com/mxsm/rocketmq-rust/pull/12699), which lets a connection's session task read and write without returning to the scheduler queue at every read turn (+13%); its CPU per message is from the round before, and #12699 changed it by 0.4%. The Rust Broker uses fewer instructions per message than the Java Broker in every load; on one busy connection it runs those instructions at a lower rate per cycle, which keeps it just below Java there.

## Message size

Throughput ratio, Rust ÷ Java, single replica (message bodies are not compressed):

| Message size | Synchronous | 1 connection × 256 | 4 connections × 64 | Broker CPU per message, Rust ÷ Java |
| --- | ---: | ---: | ---: | --- |
| 128 B | 1.74 | 1.14 | 1.34 | 0.39 / 0.88 / 0.85 |
| 1 KiB | 1.72 | 0.99 | 1.16 | 0.39 / 1.05 / 0.95 |
| 4 KiB | 1.86 | 1.21 | 1.55 | 0.21 / 0.59 / 0.54 |
| 64 KiB | 2.14 | 1.49 | 1.34 | 0.14 / 0.29 / 0.37 |

At 64 KiB with one connection the Rust Broker stored about 1.05 GB/s, the Java Broker about 0.70 GB/s. The 1 KiB row here comes from a separate, noisier round than the table above.

## Durability settings

**Synchronous flush.** With `flushDiskType = SYNC_FLUSH` and 1 KiB messages, after [#12716](https://github.com/mxsm/rocketmq-rust/pull/12716):

| Load | Java 5.5.0 | Rust | Rust ÷ Java |
| --- | --- | --- | ---: |
| Synchronous | 0.7k msg/s | 0.8k msg/s | 1.15 |
| Asynchronous, 1 connection × 256 | 48.1k msg/s | 46.3k msg/s | 0.96 |
| Asynchronous, 4 connections × 64 | 46.2k msg/s | 47.9k msg/s | 1.04 |

Both Brokers wait for `msync`, about 2 ms per call on this disk, and each flush covers a similar number of messages.

**Three replicas.** One Broker group in Controller mode with three replicas and two required in-sync replicas (`totalReplicas = 3`, `inSyncReplicas = 2`, `minInSyncReplicas = 2`), `ASYNC_FLUSH`. After each run both slaves were stopped and synchronous sends to the master failed, confirming that the master waits for replication.

| Load | Java 5.5.0 | Rust | Rust ÷ Java | CPU per message, all three Brokers |
| --- | --- | --- | ---: | --- |
| Synchronous | 1.3k msg/s | 1.9k msg/s | 1.39 | 1,062 → 748 µs |
| Asynchronous, 1 connection × 256 | 44.2k msg/s | 63.2k msg/s | 1.43 | 127 → 96 µs |
| Asynchronous, 4 connections × 64 | 53.7k msg/s | 64.3k msg/s | 1.20 | 119 → 94 µs |

All three Brokers and the Controllers share the same 8 logical CPUs here, so the asynchronous results show the CPU limit of that layout for both implementations rather than the limit of replication itself.

## With the Java client

To rule out an effect of the Rust client, the same loads were run with a Java 5.5.0 client (a minimal benchmark on the image's `rocketmq-client`, with the same in-flight limits). With the Java client, the Rust Broker used 59% less CPU per message for synchronous sends, 3.6% more with one asynchronous connection, and 11.5% and 23.2% less with 4 and 16 client processes. Synchronous throughput was 1.47× Java's. In the asynchronous loads the Java client itself became the limit, so both Brokers reached about the same throughput.

## Proxy: gRPC sends

Cluster mode, gRPC on the Proxy, one 1 KiB message per RPC; each connection is a separate client. NameServer, Broker and Proxy share CPUs 0–7. The Rust figures are for the current Proxy, including [#12789](https://github.com/mxsm/rocketmq-rust/pull/12789) (concurrent sends per client), [#12798](https://github.com/mxsm/rocketmq-rust/pull/12798) (`TCP_NODELAY` on accepted connections) and [#12804](https://github.com/mxsm/rocketmq-rust/pull/12804) (PGO).

| Load | Java 5.5.0 Proxy | Rust Proxy | Rust Proxy CPU per message | Rust p99 |
| --- | --- | --- | --- | --- |
| Synchronous | 0.8k–0.9k msg/s | 2.89k msg/s | 184 µs | 0.49 ms |
| 1 connection × 256 | 34.9k–35.4k msg/s | 38.6k msg/s | 97 µs | 20 ms |
| 4 connections × 64 | 39.3k–39.7k msg/s | 53.1k msg/s | 82 µs | 10.5 ms |
| 32 connections × 4 | 27.2k–28.4k msg/s | 45.7k msg/s | 91 µs | 6.5 ms |

The Java Proxy runs were one day earlier than the Rust Proxy runs, on the same host and settings, and they are not back-to-back pairs. In an earlier back-to-back run with 32 connections × 4, the Java Proxy's p99 was 9.0 ms. Before the fixes above, the Rust Proxy passed 4.7k msg/s with one connection and rejected most sends; the [architecture page](../architecture/proxy.md) describes the Proxy's execution model.

## Not covered yet

- Separate machines and a real network: every result above comes from one host over loopback. A full benchmark on dedicated machines is planned; [issue #12799](https://github.com/mxsm/rocketmq-rust/issues/12799) lists the machines it needs and how to sponsor them.
- Push and POP consumers, tag and SQL filtering, reads of data no longer in the page cache, many topics and queues, and long-running or failover tests.
- Consumption through the Proxy: only gRPC sends were measured.

## Reproduce a measurement

Build the services as the release images do (`docker/Dockerfile.base`; `ROCKETMQ_BROKER_PGO`, `ROCKETMQ_BROKER_BOLT` and `ROCKETMQ_PROXY_PGO` default to `true`), start a NameServer and a Broker, and pin the load generator to CPUs other than the Broker's. For example, four asynchronous connections are four concurrent client processes like this one:

```bash
taskset -c 8-15 cargo run --release -p rocketmq-client-rust --example client-production-benchmark -- \
  --namesrv 127.0.0.1:9876 --topic BenchTopic --scenario async \
  --message-count 100000 --message-size 1024 --batch-size 32 --inflight 64 --output-json result.json
```

The scenarios are `sync`, `async` and `lite-pull`. For the Proxy, create the topic through the Broker first, then run:

```bash
taskset -c 8-15 cargo run --release -p rocketmq-proxy --example grpc_send_benchmark -- \
  --endpoint http://127.0.0.1:8081 --topic BenchTopic --message-count 200000 --warmup-count 100000 \
  --connections 4 --inflight 64
```

Run the Java side with the same message size, in-flight limits and CPU placement, use a fresh Broker per scenario, and compare medians of several rotated runs rather than single results. Follow the workload checklist in [capacity and performance](capacity-performance.md#run-a-comparable-workload).
