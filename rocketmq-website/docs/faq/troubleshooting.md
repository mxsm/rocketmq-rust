---
sidebar_position: 3
title: Troubleshooting
---

# Troubleshooting

Advanced debugging techniques for RocketMQ-Rust.

## Enable Debug Logging

```rust
// Set RUST_LOG environment variable
// export RUST_LOG=rocketmq=debug,rocketmq_client=debug

// Or in code
use log::debug;

fn main() {
    env_logger::init();
    // Your code
}
```

## Common Debugging Scenarios

### Message Not Appearing in Queue

**Check**:
1. Was message sent successfully?
```rust
let result = producer.send(message).await?;
println!("Send result: {:?}", result);
```

2. Does topic exist?
```bash
sh mqadmin topicList -n localhost:9876
```

3. Check broker logs:
```bash
tail -f ~/logs/rocketmqlogs/broker.log
```

### Consumer Not Processing Messages

**Check**:
1. Is consumer started?
```rust
consumer.start().await?;
println!("Consumer started");
```

2. Check subscription:
```rust
// Verify correct topic and subscription expression
consumer.subscribe("TopicTest", "*").await?;
```

3. Check consumer status:
```rust
// Use RocketMQ admin tools
sh mqadmin consumerProgress -n localhost:9876 -g my_consumer_group
```

## Using RocketMQ Admin Tools

### Check Cluster Status

```bash
# List all clusters
sh mqadmin clusterList -n localhost:9876

# List all brokers
sh mqadmin brokerList -n localhost:9876

# List all topics
sh mqadmin topicList -n localhost:9876
```

### Check Topic Status

```bash
# Get topic info
sh mqadmin topicStatus -n localhost:9876 -t TopicTest

# Get topic route
sh mqadmin topicRoute -n localhost:9876 -t TopicTest
```

### Check Consumer Status

```bash
# Consumer progress
sh mqadmin consumerProgress -n localhost:9876 -g my_consumer_group

# Consumer connection
sh mqadmin consumerConnection -n localhost:9876 -g my_consumer_group

# Consumer offset
sh mqadmin getConsumerOffset -n localhost:9876 -g my_consumer_group -t TopicTest
```

### Reset Consumer Offset

```bash
# Reset to specific timestamp
sh mqadmin resetOffsetByTime -n localhost:9876 -g my_consumer_group -t TopicTest -timestamp 1699200000000

# Reset to earliest
sh mqadmin resetOffsetByTime -n localhost:9876 -g my_consumer_group -t TopicTest -timestamp 0
```

## Monitoring with JMX (for Java Brokers)

```bash
# Enable JMX
JAVA_OPT="${JAVA_OPT} -Dcom.sun.management.jmxremote"
JAVA_OPT="${JAVA_OPT} -Dcom.sun.management.jmxremote.port=9999"
JAVA_OPT="${JAVA_OPT} -Dcom.sun.management.jmxremote.authenticate=false"
JAVA_OPT="${JAVA_OPT} -Dcom.sun.management.jmxremote.ssl=false"
```

Connect with JConsole or VisualVM to monitor:
- Memory usage
- Thread pools
- Message queues
- Consumer lag

## Network Debugging

### Test Connectivity

```bash
# Test name server connection
telnet localhost 9876

# Test broker connection
telnet localhost 10911

# Check DNS resolution
nslookup broker_hostname

# Check network latency
ping broker_hostname
```

### Capture Network Traffic

```bash
# Capture traffic on port 9876
tcpdump -i any port 9876 -w name_server.pcap

# Capture traffic on port 10911
tcpdump -i any port 10911 -w broker.pcap
```

## Disk Issues

### Check Disk Usage

```bash
# Check disk space
df -h

# Check disk I/O
iostat -x 1

# Check commit log size
du -sh /path/to/rocketmq/store/commitlog
```

### Monitor File Descriptors

```bash
# Check open files
lsof -p $(pidof java)

# Increase file descriptor limit
ulimit -n 65536
```

## Memory Issues

### Check Memory Usage

```bash
# Check RSS (Resident Set Size)
ps aux | grep java

# Check JVM heap (for Java broker)
jmap -heap <pid>

# Check for memory leaks
jmap -histo:live <pid> | head -20
```

## Thread Dumps

### Get Thread Dump

```bash
# For Java broker
jstack <pid> > thread_dump.txt

# Analyze for deadlocks
jstack <pid> | grep -A 10 "Found one Java-level deadlock"
```

## Common Error Messages

### `No route info of this topic`

**Cause**: Topic doesn't exist or broker not registered

**Solution**:
```bash
# Create topic
sh mqadmin updateTopic -n localhost:9876 -t TopicTest -c DefaultCluster
```

### `The consumer group not online`

**Cause**: Consumer not started or disconnected

**Solution**:
```rust
// Ensure consumer is started
consumer.start().await?;
```

### `The broker is not available`

**Cause**: Broker down or network issue

**Solution**:
```bash
# Check broker status
sh mqadmin brokerList -n localhost:9876

# Start broker if needed
sh mqbroker -n localhost:9876
```

## Debugging Tools

### Log Analyzer

```bash
# Analyze broker logs for errors
grep "ERROR" ~/logs/rocketmqlogs/broker.log | tail -100

# Analyze consumer logs
grep "ERROR" ~/logs/rocketmqfiles/consumer.log | tail -100
```

### Message Tracing

Enable message trace:
```rust
producer_option.set_enable_msg_trace(true);
consumer_option.set_enable_msg_trace(true);
```

Query messages by key:
```bash
sh mqadmin queryMsgById -n localhost:9876 -i <msg_id>

sh mqadmin queryMsgByKey -n localhost:9876 -t TopicTest -k <key>
```

## Performance Profiling

### Rust Code Profiling

```bash
# Install profiler
cargo install flamegraph

# Generate flame graph
cargo flamegraph --bin my_app

# Analyze output
# flamegraph.svg will be generated
```

### Message Processing Time

```rust
use std::time::Instant;

impl MessageListener for MyListener {
    fn consume_message(&self, messages: Vec<MessageExt>) -> ConsumeResult {
        let start = Instant::now();

        for msg in messages {
            process_message(&msg);
        }

        let elapsed = start.elapsed();
        log::debug!("Processed {} messages in {:?}", messages.len(), elapsed);

        ConsumeResult::Success
    }
}
```

## Getting Help

If you're still stuck:

1. **Check logs**: Review broker and client logs
2. **Admin tools**: Use RocketMQ admin commands
3. **GitHub Issues**: Search existing issues
4. **Mailing List**: Ask on RocketMQ mailing list
5. **Stack Overflow**: Search with `rocketmq` tag

## Next Steps

- [Common Issues](./common-issues) - Frequent problems
- [Performance FAQ](./performance) - Performance issues
- [Configuration](../category/configuration) - Configuration reference
