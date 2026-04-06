---
sidebar_position: 2
title: 性能 FAQ
---

# 性能 FAQ

本页汇总 RocketMQ-Rust 常见性能问题与优化建议。

## 吞吐

### 预期吞吐是多少？

吞吐受消息大小、硬件、网络与处理逻辑等因素影响：

**单生产者**：

- 约 50K-100K msg/s
- 随消息大小变化明显

**多生产者**：

- 约 500K+ msg/s
- 随生产者数量扩展

**单消费者**：

- 约 50K-100K msg/s
- 受业务处理复杂度影响

**多消费者**：

- 约 500K+ msg/s
- 随消费者数量扩展

### 如何提升吞吐？

1. **增大批量参数**：

    ```rust
    let mut producer = DefaultMQProducer::builder()
        .producer_group("perf_group")
        .name_server_addr("localhost:9876")
        .max_message_size(4 * 1024 * 1024)
        .build();

    let mut consumer = DefaultMQPushConsumer::builder()
        .consumer_group("perf_group")
        .name_server_addr("localhost:9876")
        .pull_batch_size(64)
        .build();
    ```

2. **启用压缩**：

    ```rust
    producer.set_compress_msg_body_over_howmuch(4 * 1024);
    ```

3. **优化线程池**：

    ```rust
    consumer.set_consume_thread_min(10);
    consumer.set_consume_thread_max(20);
    ```

4. **使用异步发送**：

    ```rust
    producer.send_with_callback(message, callback).await?;
    ```

5. **调整 Broker 参数**：

    ```toml
    sendMessageThreadPoolNums = 32
    pullMessageThreadPoolNums = 32
    ```

## 延迟

### 典型消息延迟是多少？

- **发送延迟**：1-5ms（同机房）
- **端到端延迟**：5-20ms（生产到消费可见）
- **跨机房延迟**：更高，取决于网络质量

### 如何降低延迟？

1. Broker **使用 `ASYNC_FLUSH`**：

    ```toml
    flushDiskType = ASYNC_FLUSH
    ```

2. **降低刷盘频率**：

    ```toml
    flushCommitLogLeastPages = 4
    ```

3. **优化网络参数**：

    ```toml
    clientSocketRcvBufSize = 262144
    clientSocketSndBufSize = 262144
    ```

4. CommitLog **使用 SSD**：

    ```toml
    storePathCommitLog = /ssd/commitlog
    ```

5. 优化消费端业务处理耗时。

## 消息大小

### 最大消息大小是多少？

默认 4MB。

可在客户端配置：

```rust
producer.set_max_message_size(8 * 1024 * 1024); // 8MB
```

同时需要在 Broker 端配置：

```toml
maxMessageSize = 8388608
```

### 是否建议大消息？

通常**不建议**，建议：

1. **分片**：将大 payload 拆分为多条消息
2. **外部存储**：数据存入 S3/HDFS，消息只传引用
3. **压缩**：发送前压缩消息体

```rust
// 启用压缩
producer.set_compress_msg_body_over_howmuch(4 * 1024);

// 发送前压缩
let compressed = compress(&data)?;
let message = Message::builder()
    .topic("TopicTest")
    .body(compressed)
    .build()?;
```

## 可扩展性

### Topic 可以有多少个？

实践中建议控制在每集群约 50K 以内。

每个 Topic 都会占用：

- 元数据内存
- ConsumeQueue 文件句柄
- Name Server 存储资源

### 每个 Topic 多少队列合适？

建议 4-16 个队列。

考虑因素：

- 队列越多并行度越高
- 队列越多管理开销越高
- 队列数应与消费者并行度匹配

```bash
# 创建 8 队列 topic
sh mqadmin updateTopic -t MyTopic -n localhost:9876 -c DefaultCluster -w 8
```

### 每个消费组可有多少消费者？

没有硬性上限，建议遵循：

- 每个消费者通常处理 1-4 个队列
- 消费者过多会出现空闲实例
- 消费者过少会导致 lag 上升

## 资源使用

### 需要多少内存？

**Broker**：

- 最低建议：4GB
- 推荐：8GB+
- 高吞吐场景：32GB+

**客户端**：

- Producer：约 100MB
- Consumer：约 500MB-2GB（取决于队列与处理规模）

### 需要多少磁盘？

建议按以下方式估算：

1. **当前消息速率** × **保留时长** × **平均消息大小**
2. 结果再乘 2，预留增长与缓冲空间
3. CommitLog 与 ConsumeQueue 尽量**分盘**

示例：

- 10K msg/s × 72 小时 × 1KB ≈ 720GB
- 推荐磁盘容量：2TB（更有余量）

## 监控

### 应重点监控哪些指标？

**Producer**：

- Send TPS
- Send latency（avg/p95/p99）
- Failure rate

**Consumer**：

- Consume TPS
- Consume lag
- Processing time

**Broker**：

- Disk usage
- CPU usage
- Memory usage
- Network I/O

### 如何计算消费 lag？

```bash
# 在 Broker 侧查看消费进度与堆积
sh mqadmin consumerProgress -n localhost:9876 -g <consumer_group>
```

## 基准测试

### 如何执行基准测试？

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;
use std::time::Instant;

async fn benchmark_send(
    producer: &mut DefaultMQProducer,
    count: usize,
) -> rocketmq_error::RocketMQResult<()> {
    let start = Instant::now();

    for i in 0..count {
        let body = format!("Message {}", i).into_bytes();
        let message = Message::builder()
            .topic("BenchmarkTopic")
            .body(body)
            .build()?;
        producer.send(message).await?;
    }

    let elapsed = start.elapsed();
    let tps = count as f64 / elapsed.as_secs_f64();
    println!("TPS: {:.2}", tps);
    Ok(())
}
```

## 最佳实践

1. **先定位瓶颈再优化**：避免无效调参。
2. **持续监控关键指标**：构建性能基线。
3. **使用真实负载压测**：模拟生产流量模式。
4. **CommitLog 优先使用 SSD**：提升存储性能。
5. **优化消费处理逻辑**：降低端到端延迟。
6. **线程池与负载匹配**：防止资源浪费。
7. **大消息场景启用压缩**：降低带宽和磁盘压力。

## 下一步

- [性能调优](../configuration/performance-tuning) - 深入优化手册
- [常见问题](./common-issues) - 查看其他高频问题
- [Broker 配置](../configuration/broker-config) - 查看完整配置选项
