---
sidebar_position: 2
title: Client Configuration
---

# Client Configuration

Configure RocketMQ-Rust clients (producers and consumers) for optimal performance.

## Producer Configuration

### Basic Configuration

```rust
let mut producer_option = ProducerOption::default();

// Required settings
producer_option.set_name_server_addr("localhost:9876");
producer_option.set_group_name("my_producer_group");

// Optional settings
producer_option.set_send_msg_timeout(3000); // milliseconds
producer_option.set_retry_times_when_send_failed(2);
producer_option.set_max_message_size(4 * 1024 * 1024); // 4MB
```

### Advanced Configuration

```rust
// Compression
producer_option.set_compress_msg_body_over_threshold(4 * 1024); // 4KB

// Retry settings
producer_option.set_retry_times_when_send_failed(3);
producer_option.set_retry_next_server(true);

// Timeout settings
producer_option.set_tcp_transport_try_lock_timeout(1000);
producer_option.set_tcp_transport_connect_timeout(3000);

// Connection pool
producer_option.set_client_channel_max_idle_time_seconds(120);
```

### Complete Example

```rust
use rocketmq::producer::Producer;
use rocketmq::conf::ProducerOption;

fn create_producer() -> Producer {
    let mut producer_option = ProducerOption::default();

    // Basic
    producer_option.set_name_server_addr("localhost:9876");
    producer_option.set_group_name("order_producer");

    // Performance
    producer_option.set_compress_msg_body_over_threshold(4 * 1024);
    producer_option.set_max_message_size(4 * 1024 * 1024);

    // Reliability
    producer_option.set_send_msg_timeout(3000);
    producer_option.set_retry_times_when_send_failed(3);
    producer_option.set_retry_next_server(true);

    Producer::new(producer_option)
}
```

## Consumer Configuration

### Basic Configuration

```rust
let mut consumer_option = ConsumerOption::default();

// Required settings
consumer_option.set_name_server_addr("localhost:9876");
consumer_option.set_group_name("my_consumer_group");

// Thread pool
consumer_option.set_consume_thread_min(2);
consumer_option.set_consume_thread_max(10);
```

### Advanced Configuration

```rust
// Offset management
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

// Retry settings
consumer_option.set_max_reconsume_times(3);

// Message model
consumer_option.set_message_model(MessageModel::Clustering);

// Pull settings
consumer_option.set_pull_batch_size(32);
consumer_option.set_pull_interval(0);
```

### Complete Example

```rust
use rocketmq::consumer::PushConsumer;
use rocketmq::conf::ConsumerOption;

fn create_consumer() -> PushConsumer {
    let mut consumer_option = ConsumerOption::default();

    // Basic
    consumer_option.set_name_server_addr("localhost:9876");
    consumer_option.set_group_name("order_consumer");

    // Threading
    consumer_option.set_consume_thread_min(2);
    consumer_option.set_consume_thread_max(10);

    // Offset
    consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

    // Pull settings
    consumer_option.set_pull_batch_size(32);
    consumer_option.set_pull_interval(0);

    // Retry
    consumer_option.set_max_reconsume_times(3);

    PushConsumer::new(consumer_option)
}
```

## Configuration Options

### Producer Options

| Option | Default | Description |
|--------|---------|-------------|
| `name_server_addr` | Required | Name server address |
| `group_name` | Required | Producer group name |
| `send_msg_timeout` | 3000 | Send timeout (ms) |
| `retry_times_when_send_failed` | 2 | Retry count |
| `max_message_size` | 4MB | Maximum message size |
| `compress_msg_body_over_threshold` | 4KB | Compression threshold |
| `retry_next_server` | false | Retry on next broker |

### Consumer Options

| Option | Default | Description |
|--------|---------|-------------|
| `name_server_addr` | Required | Name server address |
| `group_name` | Required | Consumer group name |
| `consume_thread_min` | 1 | Min consume threads |
| `consume_thread_max` | 10 | Max consume threads |
| `pull_batch_size` | 32 | Messages per pull |
| `pull_interval` | 0 | Pull interval (ms) |
| `max_reconsume_times` | 16 | Max retry count |
| `message_model` | Clustering | Clustering or Broadcasting |

## Environment Variables

```rust
use std::env;

// Read from environment
let name_server = env::var("ROCKETMQ_NAME_SERVER")
    .unwrap_or_else(|_| "localhost:9876".to_string());

let group_name = env::var("ROCKETMQ_GROUP")
    .unwrap_or_else(|_| "default_group".to_string());

producer_option.set_name_server_addr(&name_server);
producer_option.set_group_name(&group_name);
```

## Configuration File

```toml
# rocketmq-client.toml

[producer]
name_server_addr = "localhost:9876"
group_name = "my_producer"
send_msg_timeout = 3000
retry_times_when_send_failed = 3
max_message_size = 4194304
compress_msg_body_over_threshold = 4096

[consumer]
name_server_addr = "localhost:9876"
group_name = "my_consumer"
consume_thread_min = 2
consume_thread_max = 10
pull_batch_size = 32
max_reconsume_times = 3
```

```rust
use serde::Deserialize;
use std::fs;

#[derive(Deserialize)]
struct Config {
    producer: ProducerConfig,
    consumer: ConsumerConfig,
}

#[derive(Deserialize)]
struct ProducerConfig {
    name_server_addr: String,
    group_name: String,
    send_msg_timeout: u64,
}

fn load_config(path: &str) -> Config {
    let contents = fs::read_to_string(path).unwrap();
    toml::from_str(&contents).unwrap()
}
```

## Best Practices

1. **Use sensible timeouts**: Balance between reliability and performance
2. **Configure retries**: Set appropriate retry counts
3. **Tune thread pools**: Match to your workload
4. **Set message size limits**: Prevent oversized messages
5. **Use compression**: For larger messages
6. **Monitor performance**: Track success rates and latency

## Next Steps

- [Broker Configuration](./broker-config) - Configure brokers
- [Performance Tuning](./performance-tuning) - Optimize performance
