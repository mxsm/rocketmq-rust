---
sidebar_position: 1
title: Common Issues
---

# Common Issues

Solutions to frequently encountered problems when using RocketMQ-Rust.

## Connection Issues

### Connection Refused

**Problem**: `Connection refused` error when connecting to broker.

**Solutions**:
1. Verify broker is running:
```bash
# Check broker status
ps aux | grep rocketmq

# Check if port is listening
netstat -an | grep 10911
```

2. Verify name server address:
```rust
producer_option.set_name_server_addr("localhost:9876");
```

3. Check firewall settings:
```bash
# Allow ports 9876 and 10911
sudo ufw allow 9876
sudo ufw allow 10911
```

### Cannot Find Brokers

**Problem**: Producer or consumer cannot discover brokers.

**Solutions**:
1. Verify name server connectivity:
```bash
# Test name server connection
telnet localhost 9876
```

2. Check broker registration:
```bash
# Use RocketMQ admin tools
sh mqadmin clusterList -n localhost:9876
```

3. Verify topic exists:
```bash
# List topics
sh mqadmin topicList -n localhost:9876
```

## Message Issues

### Message Not Received

**Problem**: Consumer not receiving messages.

**Solutions**:
1. Check subscription:
```rust
// Verify topic and subscription expression match
consumer.subscribe("TopicTest", "*").await?;
```

2. Check consumer group:
```rust
// Ensure consumer group is correct
consumer_option.set_group_name("correct_consumer_group");
```

3. Check message model:
```rust
// Verify clustering vs broadcasting
consumer_option.set_message_model(MessageModel::Clustering);
```

4. Check consumer position:
```rust
// May be consuming from last offset
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromFirstOffset);
```

### Duplicate Messages

**Problem**: Receiving duplicate messages.

**Explanation**: This is expected behavior. RocketMQ guarantees at-least-once delivery.

**Solution**: Implement idempotency:
```rust
use std::collections::HashSet;

struct IdempotentProcessor {
    processed: HashSet<String>,
}

impl IdempotentProcessor {
    fn process(&mut self, msg_id: &str) -> bool {
        if self.processed.contains(msg_id) {
            return false; // Already processed
        }
        self.processed.insert(msg_id.to_string());
        true // Process new message
    }
}
```

## Performance Issues

### Low Throughput

**Problem**: Send or consume rate is low.

**Solutions**:
1. Increase batch size:
```rust
producer_option.set_max_message_size(4 * 1024 * 1024);
consumer_option.set_pull_batch_size(64);
```

2. Use compression:
```rust
producer_option.set_compress_msg_body_over_threshold(4 * 1024);
```

3. Optimize thread pools:
```rust
consumer_option.set_consume_thread_min(10);
consumer_option.set_consume_thread_max(20);
```

4. Use async sending:
```rust
producer.send_async(message, |result| {
    // Handle result
}).await?;
```

### High Latency

**Problem**: Messages take too long to be delivered.

**Solutions**:
1. Check broker flush settings:
```toml
# broker.conf
flushDiskType = ASYNC_FLUSH
flushCommitLogLeastPages = 4
```

2. Reduce message size:
```rust
// Compress large messages
producer_option.set_compress_msg_body_over_threshold(4 * 1024);
```

3. Check network latency:
```bash
# Test network latency
ping broker_hostname
```

4. Monitor system resources:
```bash
# Check CPU usage
top

# Check disk I/O
iostat -x 1
```

## Memory Issues

### Out of Memory

**Problem**: Consumer crashes with out of memory error.

**Solutions**:
1. Limit process queue size:
```rust
consumer_option.set_pull_threshold_for_all(10000);
consumer_option.set_pull_threshold_for_queue(1000);
```

2. Reduce pull batch size:
```rust
consumer_option.set_pull_batch_size(32);
```

3. Process messages faster:
```rust
// Optimize message processing logic
// Use async processing
```

### Memory Leak

**Problem**: Memory usage keeps increasing.

**Solutions**:
1. Check for unbounded collections:
```rust
// Use bounded channels
let (tx, rx) = mpsc::channel(1000);

// Periodically clean up processed data
if processed.len() > 10000 {
    processed.clear();
}
```

2. Release message references:
```rust
// Don't hold onto messages after processing
impl MessageListener for MyListener {
    fn consume_message(&self, messages: Vec<MessageExt>) -> ConsumeResult {
        for msg in messages {
            process_message(&msg);
            // Don't store msg in long-lived structures
        }
        ConsumeResult::Success
    }
}
```

## Build Issues

### Compilation Errors

**Problem**: Cargo build fails.

**Solutions**:
1. Check Rust version:
```bash
# Requires Rust 1.70+
rustc --version
```

2. Update dependencies:
```bash
cargo update
```

3. Clean build:
```bash
cargo clean
cargo build
```

### Link Errors

**Problem**: Link errors on Windows.

**Solutions**:
1. Install C++ build tools:
```bash
# Install Visual Studio Build Tools
# Or use: rustup component add llvm-tools-preview
```

2. Use MSVC toolchain:
```bash
rustup default stable-x86_64-pc-windows-msvc
```

## Troubleshooting Checklist

Before asking for help, check:

- [ ] Broker is running and accessible
- [ ] Name server is running and accessible
- [ ] Client configuration is correct
- [ ] Topic exists on broker
- [ ] Consumer group is correct
- [ ] Network connectivity is working
- [ ] Firewall rules are configured
- [ ] Sufficient disk space
- [ ] Sufficient memory
- [ ] Latest version of RocketMQ-Rust

## Getting Help

If you're still stuck:

1. Check [GitHub Issues](https://github.com/mxsm/rocketmq-rust/issues)
2. Read [Architecture Documentation](../category/architecture)
3. Ask on [Stack Overflow](https://stackoverflow.com/questions/tagged/rocketmq)
4. Join the [mailing list](https://rocketmq.apache.org/about/contact)

## Next Steps

- [Performance FAQ](./performance) - Performance-related issues
- [Troubleshooting](./troubleshooting) - Advanced debugging
- [Configuration](../category/configuration) - Configuration options
