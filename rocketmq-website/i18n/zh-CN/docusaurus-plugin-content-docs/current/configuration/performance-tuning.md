---
sidebar_position: 3
title: 性能调优
---

# 性能调优

本章介绍如何对 RocketMQ-Rust 进行系统化调优，以获得更高吞吐和更低延迟。

## Broker 调优

### 线程池配置

```toml
# 高吞吐场景
sendMessageThreadPoolNums = 32
pullMessageThreadPoolNums = 32

# 低延迟场景
sendMessageThreadPoolNums = 16
pullMessageThreadPoolNums = 16
```

### 刷盘策略

```toml
# 极致性能（异常时可能丢失少量数据）
flushDiskType = ASYNC_FLUSH
flushCommitLogLeastPages = 0

# 平衡型配置
flushDiskType = ASYNC_FLUSH
flushCommitLogLeastPages = 4

# 极致可靠性
flushDiskType = SYNC_FLUSH
flushCommitLogLeastPages = 0
```

### 内存配置

```toml
# 增加 OS Page Cache（Linux）
# sysctl -w vm.dirty_bytes=4194304
# sysctl -w vm.dirty_background_bytes=2097152
```

## Producer 调优

### 批量

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;

// 增大消息上限（按实际需求设置）
let mut producer = DefaultMQProducer::builder()
    .producer_group("perf_group")
    .name_server_addr("localhost:9876")
    .max_message_size(4 * 1024 * 1024) // 4MB
    .build();

// 批量发送
let messages: Vec<Message> = /* ... */;
producer.send_batch(messages).await?;
```

### 压缩

```rust
// 大消息启用压缩
producer.set_compress_msg_body_over_howmuch(4 * 1024);
```

### 连接池

```rust
// 调整请求超时与异步回压参数
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;

let mut producer = DefaultMQProducer::builder()
    .producer_group("perf_group")
    .name_server_addr("localhost:9876")
    .send_msg_max_timeout_per_request(5_000)
    .enable_backpressure_for_async_mode(true)
    .back_pressure_for_async_send_num(10_000)
    .back_pressure_for_async_send_size(64 * 1024 * 1024)
    .build();
```

## Consumer 调优

### 线程池

```rust
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;

// CPU 密集型处理
let mut cpu_consumer = DefaultMQPushConsumer::builder()
    .consumer_group("cpu_group")
    .name_server_addr("localhost:9876")
    .consume_thread_min(num_cpus::get() as u32)
    .consume_thread_max((num_cpus::get() as u32) * 2)
    .build();

// I/O 密集型处理
let mut io_consumer = DefaultMQPushConsumer::builder()
    .consumer_group("io_group")
    .name_server_addr("localhost:9876")
    .consume_thread_min((num_cpus::get() as u32) * 2)
    .consume_thread_max((num_cpus::get() as u32) * 4)
    .build();
```

### 拉取批量

```rust
// 提升吞吐
cpu_consumer.set_pull_batch_size(64);
cpu_consumer.set_pull_interval(0);
```

### 处理队列阈值

```rust
// 控制内存占用
cpu_consumer.set_pull_threshold_for_topic(10000);
cpu_consumer.set_pull_threshold_for_queue(1000);
```

## 网络调优

### TCP 缓冲区

```toml
# broker.conf
clientSocketRcvBufSize = 262144
clientSocketSndBufSize = 262144
```

### 内核参数

```bash
# Linux 网络参数优化
sysctl -w net.core.rmem_max=134217728
sysctl -w net.core.wmem_max=134217728
sysctl -w net.ipv4.tcp_rmem="4096 87380 67108864"
sysctl -w net.ipv4.tcp_wmem="4096 65536 67108864"
```

## JVM 调优（Java Broker 场景）

```bash
# 堆内存
JAVA_OPT="${JAVA_OPT} -Xms8g -Xmx8g"

# GC 参数
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC"
JAVA_OPT="${JAVA_OPT} -XX:MaxGCPauseMillis=200"

# Metaspace 参数
JAVA_OPT="${JAVA_OPT} -XX:MetaspaceSize=128m"
JAVA_OPT="${JAVA_OPT} -XX:MaxMetaspaceSize=256m"
```

## 磁盘 I/O 调优

### 使用 SSD

将 CommitLog 放在 SSD 上通常会获得 5~10 倍性能提升：

```text
Sequential Write (HDD):  ~100 MB/s
Sequential Write (SSD):  ~500 MB/s

Random Read (HDD):       ~1 MB/s
Random Read (SSD):       ~200 MB/s
```

### 存储分离

```toml
# CommitLog 放在高速盘
storePathCommitLog = /fast_disk/commitlog

# ConsumeQueue 放在另一块盘
storePathConsumeQueue = /separate_disk/consumequeue
```

### 文件系统建议

```bash
# 使用 XFS 或 ext4
mkfs.xfs /dev/sdb
mount -t xfs /dev/sdb /data/rocketmq

# 挂载参数
mount -t xfs -o noatime,nodiratime /dev/sdb /data/rocketmq
```

## 监控

### 核心指标

- **发送 TPS**：每秒发送消息数
- **消费 TPS**：每秒消费消息数
- **发送延迟**：消息发送耗时
- **消费 lag**：消息堆积量
- **磁盘使用率**：CommitLog/ConsumeQueue 空间占用
- **CPU 使用率**：Broker 与客户端 CPU 占用
- **内存使用率**：JVM 与 OS 内存占用

### 监控工具

```rust
use std::time::Instant;

let start = Instant::now();
let result = producer.send(message).await?;
let elapsed = start.elapsed();

println!("Send result: {:?}", result);
println!("Latency: {} ms", elapsed.as_millis());
```

## 性能基准

### 预期表现

| 场景 | 预期吞吐 | 预期延迟 |
| -------- | ----------------- | ---------------- |
| 单生产者 | 50K-100K msg/s | < 5ms |
| 多生产者 | 500K+ msg/s | < 10ms |
| 单消费者 | 50K-100K msg/s | < 10ms |
| 多消费者 | 500K+ msg/s | < 20ms |

### benchmark 脚本

```rust
use std::time::Instant;
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;
use rocketmq_client_rust::producer::mq_producer::MQProducer;
use rocketmq_common::common::message::message_single::Message;

async fn benchmark_producer(
    producer: &mut DefaultMQProducer,
    num_messages: usize,
) -> rocketmq_error::RocketMQResult<()> {
    let start = Instant::now();

    for i in 0..num_messages {
        let message = Message::builder()
            .topic("BenchmarkTopic")
            .body(format!("Message {}", i))
            .build()?;
        producer.send(message).await?;
    }

    let elapsed = start.elapsed();
    let tps = num_messages as f64 / elapsed.as_secs_f64();
    let avg_latency = elapsed.as_millis() as f64 / num_messages as f64;

    println!("Sent {} messages in {:.2}s", num_messages, elapsed.as_secs_f64());
    println!("TPS: {:.2}", tps);
    println!("Avg Latency: {:.2} ms", avg_latency);

    Ok(())
}
```

## 最佳实践

1. **优先保证硬件基础**：CommitLog 场景建议使用 SSD。
2. **线程池按业务画像调优**：避免盲目放大。
3. **网络参数与缓冲区联动调优**：减少网络瓶颈。
4. **持续观测关键指标**：用数据驱动参数调整。
5. **大消息场景启用压缩**：降低带宽压力。
6. **尽量批量发送**：减少 RPC 往返次数。
7. **上线前做真实负载压测**：避免线上试错。

## 故障排查

### 吞吐偏低

- 检查磁盘 I/O 性能
- 增大线程池规模
- 调整刷盘频率
- 检查网络带宽与丢包

### 延迟偏高

- 检查 CPU 利用率
- 调整刷盘策略
- 优化业务处理逻辑
- 检查 Java Broker 的 GC 暂停

### 内存问题

- 限制处理队列大小
- 增加 JVM 堆内存（如适用）
- 排查内存泄漏
- 监控 OS Page Cache 使用

## 下一步

- [Broker 配置](./broker-config) - 调整 Broker 参数
- [客户端配置](./client-config) - 调整 Producer 与 Consumer 参数
- [常见问题](../faq/common-issues) - 查看常见问题与排障建议
