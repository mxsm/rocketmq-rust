---
title: "性能对标：RocketMQ Rust 与 Java 5.5.0"
---

本页给出 RocketMQ Rust 与 Apache RocketMQ 5.5.0 在同一台机器、同样负载下的对比。所有数字都在 2026 年 11 月于同一台主机上测得，服务进程与压测客户端绑定在不同的 CPU 上。它们反映所列负载下的相对开销与吞吐，不是生产部署的容量；容量规划见[容量与性能](capacity-performance.md)。

## 结论

| 方面 | 结果 |
| --- | --- |
| Broker，同步发送（1 KiB） | 吞吐为 Java 的 1.79 倍，Broker 每条消息 CPU 少 64% |
| Broker，异步发送（1 KiB） | 4 连接 1.19 倍，16 连接 1.23 倍；单连接 0.96 倍 |
| Broker，LitePull 消费 | 吞吐为 Java 的 1.30 倍，Broker 每条消息 CPU 少 37% |
| 消息大小 | 消息越大 Rust 优势越大：64 KiB 时为 1.34–2.14 倍，Broker 每条 CPU 为 Java 的 14%–37% |
| `SYNC_FLUSH` | 4 连接 1.04 倍，单连接 0.96 倍；两边都受 `msync` 延迟限制 |
| 三副本（Controller 模式） | 1.20–1.43 倍，Broker 每条 CPU 为 Java 的 70%–79% |
| Proxy，gRPC 发送 | 所测负载下吞吐为 Java Proxy 的 1.1–3.6 倍（不是相邻测得，见下文） |

目前只有一种负载 Rust 仍低于 Java：单连接、大量请求在途的异步发送（0.96 倍）。

## 测试环境

| 项 | 设置 |
| --- | --- |
| 主机 | Intel Core i7-11700K（8 核 16 线程），Windows 上的 Docker Desktop，WSL 2 Linux 虚拟机（内核 6.18，16 GB 内存） |
| CPU 分配 | NameServer、Broker、Proxy 绑定逻辑 CPU 0–7（4 个物理核的超线程）；压测客户端绑定 CPU 8–15 |
| 网络与存储 | 回环地址；存储在容器自身的文件系统上，不是 tmpfs |
| RocketMQ Rust | 按发布镜像的方式构建（`docker/Dockerfile.base`）：发布特性，Broker 使用 PGO 与 BOLT，Proxy 使用 PGO |
| RocketMQ Java | `apache/rocketmq:5.5.0` 镜像，默认启动脚本与 JVM 参数（OpenJDK 8） |
| Broker 默认配置 | 单副本、`ASYNC_FLUSH`、1 KiB 消息，除非章节另有说明 |
| 压测客户端 | 两种 Broker 用同一个 Rust 客户端：`rocketmq-client` 的 `client-production-benchmark` 示例。Proxy 用 `rocketmq-proxy` 的 `grpc_send_benchmark` 示例 |
| 流程 | 每个场景使用全新的 Broker 或 Java 容器，先预热一次再正式测量；各组之间轮换被测对象的顺序，报告每格 4 到 9 次的中位数。所有报告的运行都是 0 失败 |

在途上限：同步发送保持 1 个请求在途；“1 连接”在一条连接上保持 256 个异步发送在途，“4 连接”每条 64 个，“16 连接”每条 16 个。Broker 每条消息 CPU 为 Broker 进程的 CPU 时间除以发送的消息数，取自 `/proc` 与 `perf stat`。

## Broker：收发 1 KiB 消息

| 负载 | Java 5.5.0 | Rust | Rust ÷ Java | Broker 每条 CPU，Java → Rust |
| --- | --- | --- | ---: | --- |
| 同步 | 2.3k 条/s，p50 0.43 ms | 4.0k 条/s，p50 0.24 ms | 1.79 | 381 → 137 µs |
| 异步，1 连接 × 256 | 116.6k 条/s | 112.4k 条/s | 0.96 | 44.9 → 46.8 µs |
| 异步，4 连接 × 64 | 115.9k 条/s | 137.8k 条/s | 1.19 | 50.5 → 46.4 µs |
| 异步，16 连接 × 16 | 100.9k 条/s | 123.7k 条/s | 1.23 | 58.4 → 51.4 µs |
| LitePull 消费 | 107.7k 条/s | 140.0k 条/s | 1.30 | 17.9 → 11.2 µs |

单连接的吞吐来自 [#12699](https://github.com/mxsm/rocketmq-rust/pull/12699) 之后的一轮测量：该改动让连接的会话任务在读轮次结束时不再回到调度队列（+13%）；其每条 CPU 来自之前一轮，#12699 使它变化 0.4%。Rust Broker 在每种负载下每条消息执行的指令都少于 Java Broker；单连接繁忙时它每个周期执行的指令更少，因此在这一种负载下略低于 Java。

## 消息大小

吞吐之比 Rust ÷ Java，单副本（消息体不压缩）：

| 消息大小 | 同步 | 1 连接 × 256 | 4 连接 × 64 | Broker 每条 CPU，Rust ÷ Java |
| --- | ---: | ---: | ---: | --- |
| 128 B | 1.74 | 1.14 | 1.34 | 0.39 / 0.88 / 0.85 |
| 1 KiB | 1.72 | 0.99 | 1.16 | 0.39 / 1.05 / 0.95 |
| 4 KiB | 1.86 | 1.21 | 1.55 | 0.21 / 0.59 / 0.54 |
| 64 KiB | 2.14 | 1.49 | 1.34 | 0.14 / 0.29 / 0.37 |

64 KiB、单连接时，Rust Broker 每秒写入约 1.05 GB，Java Broker 约 0.70 GB。本表 1 KiB 一行来自另一轮噪声较大的测量，与上一节的表不同。

## 持久化配置

**同步刷盘。** `flushDiskType = SYNC_FLUSH`、1 KiB 消息，[#12716](https://github.com/mxsm/rocketmq-rust/pull/12716) 之后：

| 负载 | Java 5.5.0 | Rust | Rust ÷ Java |
| --- | --- | --- | ---: |
| 同步 | 0.7k 条/s | 0.8k 条/s | 1.15 |
| 异步，1 连接 × 256 | 48.1k 条/s | 46.3k 条/s | 0.96 |
| 异步，4 连接 × 64 | 46.2k 条/s | 47.9k 条/s | 1.04 |

两边的 Broker 都在等待 `msync`，在这块磁盘上每次约 2 ms，每次刷盘覆盖的消息数相近。

**三副本。** Controller 模式下的一个 Broker 组，三个副本，要求两个同步副本（`totalReplicas = 3`、`inSyncReplicas = 2`、`minInSyncReplicas = 2`），`ASYNC_FLUSH`。每次测量后停掉两个 slave，再向 master 同步发送，均失败，确认 master 确实等待复制。

| 负载 | Java 5.5.0 | Rust | Rust ÷ Java | 三个 Broker 合计每条 CPU |
| --- | --- | --- | ---: | --- |
| 同步 | 1.3k 条/s | 1.9k 条/s | 1.39 | 1,062 → 748 µs |
| 异步，1 连接 × 256 | 44.2k 条/s | 63.2k 条/s | 1.43 | 127 → 96 µs |
| 异步，4 连接 × 64 | 53.7k 条/s | 64.3k 条/s | 1.20 | 119 → 94 µs |

这里三个 Broker 与 Controller 共用同样的 8 个逻辑 CPU，因此异步结果反映的是两种实现在该布局下的 CPU 上限，而不是复制本身的上限。

## 使用 Java 客户端

为排除 Rust 客户端的影响，同样的负载也用 Java 5.5.0 客户端测了一遍（基于镜像自带 `rocketmq-client` 的最小基准，在途上限相同）。用 Java 客户端时，Rust Broker 每条消息 CPU：同步少 59%，单连接异步多 3.6%，4 个和 16 个客户端进程分别少 11.5% 和 23.2%。同步吞吐为 Java 的 1.47 倍。异步负载下 Java 客户端自身先成为瓶颈，两种 Broker 的吞吐大致相同。

## Proxy：gRPC 发送

集群模式，经 Proxy 的 gRPC 发送，每个 RPC 一条 1 KiB 消息，每条连接是一个独立的客户端。NameServer、Broker 与 Proxy 共用 CPU 0–7。Rust 数据对应当前的 Proxy，包含 [#12789](https://github.com/mxsm/rocketmq-rust/pull/12789)（同一客户端的发送并发执行）、[#12798](https://github.com/mxsm/rocketmq-rust/pull/12798)（接收的连接设置 `TCP_NODELAY`）和 [#12804](https://github.com/mxsm/rocketmq-rust/pull/12804)（PGO）。

| 负载 | Java 5.5.0 Proxy | Rust Proxy | Rust Proxy 每条 CPU | Rust p99 |
| --- | --- | --- | --- | --- |
| 同步 | 0.8k–0.9k 条/s | 2.89k 条/s | 184 µs | 0.49 ms |
| 1 连接 × 256 | 34.9k–35.4k 条/s | 38.6k 条/s | 97 µs | 20 ms |
| 4 连接 × 64 | 39.3k–39.7k 条/s | 53.1k 条/s | 82 µs | 10.5 ms |
| 32 连接 × 4 | 27.2k–28.4k 条/s | 45.7k 条/s | 91 µs | 6.5 ms |

Java Proxy 比 Rust Proxy 早一天测量，主机与设置相同，但不是相邻配对测得。在更早一次相邻的 32 连接 × 4 对比中，Java Proxy 的 p99 为 9.0 ms。上述修复之前，Rust Proxy 单连接只有 4.7k 条/s，且大部分发送被拒绝；Proxy 的执行模型见[架构页](../architecture/proxy.md)。

## 尚未覆盖

- 多台机器与真实网络：以上结果都来自同一台主机的回环地址。在专用机器上的完整基准测试正在筹备，所需机器与赞助方式见 [issue #12801](https://github.com/mxsm/rocketmq-rust/issues/12801)。
- Push 与 POP 消费、TAG 与 SQL 过滤、读取已不在页缓存中的数据、大量 Topic 与队列，以及长时间运行与故障切换测试。
- 经 Proxy 的消费：只测了 gRPC 发送。

## 复现测量

按发布镜像的方式构建服务（`docker/Dockerfile.base`；`ROCKETMQ_BROKER_PGO`、`ROCKETMQ_BROKER_BOLT`、`ROCKETMQ_PROXY_PGO` 默认均为 `true`），启动 NameServer 与 Broker，并把压测客户端绑定到 Broker 之外的 CPU。例如，4 连接异步就是同时运行 4 个如下的客户端进程：

```bash
taskset -c 8-15 cargo run --release -p rocketmq-client-rust --example client-production-benchmark -- \
  --namesrv 127.0.0.1:9876 --topic BenchTopic --scenario async \
  --message-count 100000 --message-size 1024 --batch-size 32 --inflight 64 --output-json result.json
```

场景有 `sync`、`async` 和 `lite-pull`。测 Proxy 时，先通过 Broker 创建 Topic，再运行：

```bash
taskset -c 8-15 cargo run --release -p rocketmq-proxy --example grpc_send_benchmark -- \
  --endpoint http://127.0.0.1:8081 --topic BenchTopic --message-count 200000 --warmup-count 100000 \
  --connections 4 --inflight 64
```

Java 一侧使用相同的消息大小、在途上限与 CPU 分配，每个场景使用全新的 Broker，比较多次轮换运行的中位数，而不是单次结果。按[容量与性能](capacity-performance.md#执行可比较的负载)中的负载检查清单操作。
