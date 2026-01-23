---
sidebar_position: 2
title: Performance FAQ
---

# Performance FAQ

Common questions about RocketMQ-Rust performance.

## Throughput

### What throughput can I expect?

Throughput depends on many factors:

**Single Producer**:
- ~50K-100K messages/second
- Varies by message size

**Multiple Producers**:
- ~500K+ messages/second
- Scales with number of producers

**Single Consumer**:
- ~50K-100K messages/second
- Depends on processing complexity

**Multiple Consumers**:
- ~500K+ messages/second
- Scales with number of consumers

### How to increase throughput?

1. **Increase batch size**:
```rust
producer_option.set_max_message_size(4 * 1024 * 1024);
consumer_option.set_pull_batch_size(64);
```

2. **Use compression**:
```rust
producer_option.set_compress_msg_body_over_threshold(4 * 1024);
```

3. **Optimize thread pools**:
```rust
consumer_option.set_consume_thread_min(10);
consumer_option.set_consume_thread_max(20);
```

4. **Use async sending**:
```rust
producer.send_async(message, callback).await?;
```

5. **Tune broker**:
```toml
sendMessageThreadPoolNums = 32
pullMessageThreadPoolNums = 32
```

## Latency

### What is typical message latency?

- **Send latency**: 1-5ms (same datacenter)
- **End-to-end latency**: 5-20ms (consumer receives after producer sends)
- **Cross-datacenter**: Higher, depends on network

### How to reduce latency?

1. **Use ASYNC_FLUSH** on broker:
```toml
flushDiskType = ASYNC_FLUSH
```

2. **Reduce flush frequency**:
```toml
flushCommitLogLeastPages = 4
```

3. **Optimize network**:
```toml
clientSocketRcvBufSize = 262144
clientSocketSndBufSize = 262144
```

4. **Use SSDs** for commit log:
```toml
storePathCommitLog = /ssd/commitlog
```

5. **Reduce message processing time** in consumers

## Message Size

### What is the maximum message size?

Default: 4MB

Can be configured:
```rust
producer_option.set_max_message_size(8 * 1024 * 1024); // 8MB
```

Also configure broker:
```toml
maxMessageSize = 8388608
```

### Should I use large messages?

**Not recommended**. Consider:

1. **Chunking**: Split large payloads into multiple messages
2. **External storage**: Store large data in S3/HDFS, send reference in message
3. **Compression**: Compress before sending

```rust
// Enable compression
producer_option.set_compress_msg_body_over_threshold(4 * 1024);

// Compress before sending
let compressed = compress(&data)?;
let message = Message::new("TopicTest".to_string(), compressed);
```

## Scalability

### How many topics can I have?

Practical limit: ~50K topics per cluster

Each topic uses:
- Memory for metadata
- File handles for consume queues
- Name server storage

### How many queues per topic?

Recommended: 4-16 queues

Factors:
- More queues = higher parallelism
- More queues = more overhead
- Match to number of consumers

```rust
// Create topic with 8 queues
sh mqadmin updateTopic -t MyTopic -n localhost:9876 -c DefaultCluster -w 8
```

### How many consumers per group?

No hard limit

Guidelines:
- Each consumer typically handles 1-4 queues
- Too many consumers = idle consumers
- Too few consumers = lag

## Resource Usage

### How much memory do I need?

**Broker**:
- Minimum: 4GB
- Recommended: 8GB+
- High throughput: 32GB+

**Client**:
- Producer: ~100MB
- Consumer: ~500MB-2GB (depends on queue size)

### How much disk space?

Plan for:

1. **Current rate** × **Retention period** × **Message size**
2. **Double** for buffer and growing capacity
3. **Separate disks** for commit log and consume queue

Example:
- 10K msg/s × 72 hours × 1KB = ~720GB
- Recommend: 2TB for comfortable headroom

## Monitoring

### What metrics should I monitor?

**Producer**:
- Send TPS
- Send latency (avg, p95, p99)
- Failure rate

**Consumer**:
- Consume TPS
- Consume lag
- Processing time

**Broker**:
- Disk usage
- CPU usage
- Memory usage
- Network I/O

### How to calculate consumer lag?

```rust
let max_offset = consumer.get_max_offset(queue)?;
let current_offset = consumer.get_current_offset(queue)?;
let lag = max_offset - current_offset;

println!("Consumer lag: {} messages", lag);
```

## Benchmarks

### How to run benchmarks?

```rust
use std::time::Instant;

async fn benchmark_send(producer: &Producer, count: usize) {
    let start = Instant::now();

    for i in 0..count {
        let body = format!("Message {}", i).into_bytes();
        let message = Message::new("BenchmarkTopic".to_string(), body);
        producer.send(message).await?;
    }

    let elapsed = start.elapsed();
    let tps = count as f64 / elapsed.as_secs_f64();
    println!("TPS: {:.2}", tps);
}
```

## Best Practices

1. **Profile before optimizing**: Identify bottlenecks
2. **Monitor continuously**: Track performance metrics
3. **Test with realistic workload**: Simulate production
4. **Use SSDs**: For commit logs
5. **Optimize message processing**: Reduce consumer latency
6. **Tune thread pools**: Match to workload
7. **Use compression**: For large messages

## Next Steps

- [Performance Tuning](../configuration/performance-tuning) - Detailed optimization
- [Common Issues](./common-issues) - Other problems
- [Configuration](../category/configuration) - All configuration options
