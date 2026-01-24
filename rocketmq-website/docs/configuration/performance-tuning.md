---
sidebar_position: 3
title: Performance Tuning
---

# Performance Tuning

Optimize RocketMQ-Rust for maximum throughput and minimal latency.

## Broker Tuning

### Thread Pool Configuration

```toml
# For high-throughput scenarios
sendMessageThreadPoolNums = 32
pullMessageThreadPoolNums = 32

# For low-latency scenarios
sendMessageThreadPoolNums = 16
pullMessageThreadPoolNums = 16
```

### Flush Strategy

```toml
# Maximum performance (may lose data on failure)
flushDiskType = ASYNC_FLUSH
flushCommitLogLeastPages = 0

# Balanced performance
flushDiskType = ASYNC_FLUSH
flushCommitLogLeastPages = 4

# Maximum reliability
flushDiskType = SYNC_FLUSH
flushCommitLogLeastPages = 0
```

### Memory Configuration

```toml
# Increase OS page cache (Linux)
# sysctl -w vm.dirty_bytes=4194304
# sysctl -w vm.dirty_background_bytes=2097152
```

## Producer Tuning

### Batch Size

```rust
// Increase batch size for higher throughput
producer_option.set_max_message_size(4 * 1024 * 1024); // 4MB

// Send multiple messages
let messages: Vec<Message> = /* ... */;
producer.send_batch(messages).await?;
```

### Compression

```rust
// Enable compression for large messages
producer_option.set_compress_msg_body_over_threshold(4 * 1024);
```

### Connection Pool

```rust
// Increase connection pool size
producer_option.set_client_channel_max_idle_time_seconds(300);
```

## Consumer Tuning

### Thread Pool

```rust
// For CPU-intensive processing
consumer_option.set_consume_thread_min(num_cpus::get() as i32);
consumer_option.set_consume_thread_max(num_cpus::get() as i32 * 2);

// For I/O-intensive processing
consumer_option.set_consume_thread_min(num_cpus::get() as i32 * 2);
consumer_option.set_consume_thread_max(num_cpus::get() as i32 * 4);
```

### Pull Batch Size

```rust
// Increase for higher throughput
consumer_option.set_pull_batch_size(64);
consumer_option.set_pull_interval(0);
```

### Process Queue

```rust
// Limit memory usage
consumer_option.set_pull_threshold_for_all(10000);
consumer_option.set_pull_threshold_for_queue(1000);
```

## Network Tuning

### TCP Buffer Sizes

```toml
# broker.conf
clientSocketRcvBufSize = 262144
clientSocketSndBufSize = 262144
```

### Kernel Parameters

```bash
# Linux kernel optimization
sysctl -w net.core.rmem_max=134217728
sysctl -w net.core.wmem_max=134217728
sysctl -w net.ipv4.tcp_rmem="4096 87380 67108864"
sysctl -w net.ipv4.tcp_wmem="4096 65536 67108864"
```

## JVM Tuning (for Java brokers)

```bash
# Heap size
JAVA_OPT="${JAVA_OPT} -Xms8g -Xmx8g"

# GC settings
JAVA_OPT="${JAVA_OPT} -XX:+UseG1GC"
JAVA_OPT="${JAVA_OPT} -XX:MaxGCPauseMillis=200"

# Metaspace size
JAVA_OPT="${JAVA_OPT} -XX:MetaspaceSize=128m"
JAVA_OPT="${JAVA_OPT} -XX:MaxMetaspaceSize=256m"
```

## Disk I/O Tuning

### Use SSDs

Commit logs on SSDs provide 5-10x better performance:

```
Sequential Write (HDD):  ~100 MB/s
Sequential Write (SSD):  ~500 MB/s

Random Read (HDD):       ~1 MB/s
Random Read (SSD):       ~200 MB/s
```

### Separate Storage

```toml
# Commit log on fast disk
storePathCommitLog = /fast_disk/commitlog

# Consume queue on separate disk
storePathConsumeQueue = /separate_disk/consumequeue
```

### Filesystem

```bash
# Use XFS or ext4
mkfs.xfs /dev/sdb
mount -t xfs /dev/sdb /data/rocketmq

# Mount options
mount -t xfs -o noatime,nodiratime /dev/sdb /data/rocketmq
```

## Monitoring

### Key Metrics

- **Send TPS**: Messages sent per second
- **Consume TPS**: Messages consumed per second
- **Send Latency**: Time to send message
- **Consume Lag**: Messages waiting to be consumed
- **Disk Usage**: Commit log and consume queue size
- **CPU Usage**: Broker and client CPU utilization
- **Memory Usage**: JVM and OS memory usage

### Monitoring Tools

```rust
// RocketMQ-Rust metrics
use rocketmq::metrics::Metrics;

let metrics = producer.get_metrics();

println!("Send TPS: {}", metrics.send_tps);
println!("Avg Latency: {} ms", metrics.avg_latency);
println!("Success Rate: {:.2}%", metrics.success_rate * 100.0);
```

## Performance Benchmarks

### Expected Performance

| Scenario | Expected Throughput | Expected Latency |
|----------|-------------------|------------------|
| Single Producer | 50K-100K msg/s | < 5ms |
| Multiple Producers | 500K+ msg/s | < 10ms |
| Single Consumer | 50K-100K msg/s | < 10ms |
| Multiple Consumers | 500K+ msg/s | < 20ms |

### Benchmark Script

```rust
use std::time::Instant;

async fn benchmark_producer(producer: &Producer, num_messages: usize) -> Result<(), Error> {
    let start = Instant::now();

    for i in 0..num_messages {
        let body = format!("Message {}", i).into_bytes();
        let message = Message::new("BenchmarkTopic".to_string(), body);
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

## Best Practices

1. **Use appropriate hardware**: SSDs for commit logs
2. **Tune thread pools**: Match to your workload
3. **Optimize network settings**: Increase buffer sizes
4. **Monitor metrics**: Track performance continuously
5. **Use compression**: For larger messages
6. **Batch when possible**: Reduce round trips
7. **Test before production**: Benchmark your workload

## Troubleshooting

### Low Throughput

- Check disk I/O performance
- Increase thread pool sizes
- Reduce flush frequency
- Check network bandwidth

### High Latency

- Check CPU usage
- Reduce flush frequency
- Optimize message processing
- Check GC pauses (for Java brokers)

### Memory Issues

- Limit process queue size
- Increase JVM heap (if applicable)
- Check for memory leaks
- Monitor OS page cache

## Next Steps

- [Broker Configuration](./broker-config) - Broker settings
- [Client Configuration](./client-config) - Client settings
- [FAQ](../category/faq) - Common issues
