---
title: "发送并消费第一批消息"
---

本教程将[本地 Rust 服务](local-source.md)与一组配套的生产者/LitePull 应用连接起来。你将创建主题和消费者组，发送五条消息，处理消息并提交组的偏移量。所有命令均在仓库根目录执行。

## 发送前的准备

NameServer 与 Broker 必须已使用教程配置启动。按照本地搭建说明，通过 `cluster clusterList` 确认 `DocsCluster` 和 `docs-broker`。先构建示例，再启动只有一分钟等待窗口的消费者：

```bash
cargo build --manifest-path rocketmq-website/examples/first-message/Cargo.toml
```

| 配置项 | 值 |
| --- | --- |
| NameServer | `127.0.0.1:9876` |
| 主题 | `DocsFirstMessage` |
| 消费者组 | `docs_first_message_consumer` |
| 生产者组 | `docs_first_message_producer` |
| 读写队列 | 教程 Broker 上各四个 |
| 订阅 | 主题中的全部消息 |

可执行程序在[完整源码](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/src/main.rs)中使用这些常量。两条运行命令使用同一份源码和同一主题。`rocketmq-example` 中的独立演示则使用不同的内置主题。

## 1. 创建主题和消费者组

运行管理命令前，在当前终端中设置 NameServer。PowerShell：

```powershell
$env:NAMESRV_ADDR = "127.0.0.1:9876"
```

Unix shell：

```bash
export NAMESRV_ADDR=127.0.0.1:9876
```

当前 `clusterList` 和 `updateSubGroup` 子命令使用环境变量，不接受 `-n`。下面的主题和进度查询命令各自支持 `-n`，它不是 CLI 全局参数。

以下两条命令会修改集群元数据，请仅对专用本地集群执行：

```bash
cargo run -p rocketmq-admin-cli -- topic updateTopic -t DocsFirstMessage -c DocsCluster -r 4 -w 4 -n 127.0.0.1:9876
cargo run -p rocketmq-admin-cli -- consumer updateSubGroup -g docs_first_message_consumer -c DocsCluster
```

`updateTopic` 创建或更新主题配置，`updateSubGroup` 创建或更新消费者组配置。不要复用现有应用的资源名：重复执行 update 是配置操作，不只是查询。

使用以下只读命令检查路由：

```bash
cargo run -p rocketmq-admin-cli -- topic topicRoute -t DocsFirstMessage -n 127.0.0.1:9876
```

路由应包含 `docs-broker`，公布 `127.0.0.1:10911`，并具有可读、可写队列。如果未立即返回路由，等待 Broker 路由注册传播后再次查询。返回的地址不可达时，即使 NameServer 可达，也仍然属于 Broker 地址公布问题。

## 2. 启动消费者

保持两个服务运行，打开第三个终端：

```bash
cargo run --manifest-path rocketmq-website/examples/first-message/Cargo.toml -- consume
```

等待出现 `CONSUMER_STARTED`。应用先订阅，再启动，使用一秒轮询超时，最多等待 60 秒以接收至少五条消息。没有可消费数据时，轮询返回空批次属于正常情况。

对于没有已存储进度的新组，`ConsumeFromFirstOffset` 允许读取已有数据。对于已有组，已存储偏移量优先；该设置不会强制重放历史消息。

## 3. 从另一个终端发送

在消费者的 60 秒窗口内执行：

```bash
cargo run --manifest-path rocketmq-website/examples/first-message/Cargo.toml -- produce
```

生产者发送五条小消息，每次调用超时为三秒，输出 `SEND 0` 到 `SEND 4`、返回状态、消息 ID 和队列偏移量。缺少结果或非 `SendOk` 状态均按错误处理。发生错误时，命令在清理资源后结束。

消费者输出 `RECEIVED id=...` 和 `OFFSET_COMMIT_REQUESTED received=...`。消息 ID、批次划分和接收顺序可能不同。如果主题仍有旧消息，计数可能超过五。处理非空批次后，应用提交进度，并在总数达到至少五条时退出。

这些输出展示教程中的应用链路，不能证明全局有序、业务精确一次执行、复制或崩溃持久性。示例输出消息 ID 和计数，不记录消息体。

## 4. 理解运行结果

生产者返回值反映发送，消费者输出反映应用处理，`commit_all` 反映消费者组的消费进度。它们不会自动与外部数据库组成一个事务。 当前 LitePull 的 `commit_all` 更新客户端偏移量存储状态，与持久化分离，并可能在内部记录队列级错误。因此，`OFFSET_COMMIT_REQUESTED` 表示调用返回，不代表持久的 Broker 确认。详见[提交语义](../consumer/pull-consumer.md)。

示例将打印每个 ID 视为处理完成，然后提交。用于实际应用时，应先完成业务处理，再提交进度。如果一批消息只成功处理一部分，不要直接提交整个批次；需要明确重试、幂等，以及可以安全推进的连续消费进度。

应用持有一个 `RuntimeOwner`，在其下创建 `Arc<ClientRuntime>`，并传入客户端 facade 的 builder。返回前依次关闭生产者/消费者、共享客户端运行时、运行时所有者和遥测。改造示例时应保留该生命周期。

## 重复运行与诊断

再次运行时，先启动消费者，再发送五条新消息。复用同一消费者组通常会从已提交进度继续。如果存在旧的未消费数据，消费者可能在新生产者启动前就完成接收；本演示没有通过唯一运行 ID 关联消息。

消费者超时时，按照[首次诊断](../operations/first-diagnosis.md)检查返回的 Broker 地址、主题和组是否一致、队列分配以及已存储进度。不要首先删除存储或重置偏移量。

扩展示例前，阅读[生产者概览](../producer/overview.md)、[LitePull 消费](../consumer/pull-consumer.md)和[投递与重试](../guides/delivery-and-retry.md)。
