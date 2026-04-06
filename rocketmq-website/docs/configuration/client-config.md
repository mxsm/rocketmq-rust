---
sidebar_position: 2
title: Client Configuration
---

# Client Configuration

This page describes how to configure RocketMQ-Rust producers and consumers with the current builder-based APIs.

## Producer Configuration

### Basic Producer Configuration

```rust
use rocketmq_client_rust::producer::default_mq_producer::DefaultMQProducer;

let mut producer = DefaultMQProducer::builder()
    .producer_group("my_producer_group")
    .name_server_addr("localhost:9876")
    .send_msg_timeout(3_000)
    .retry_times_when_send_failed(2)
    .max_message_size(4 * 1024 * 1024)
    .build();
```

### Advanced Producer Configuration

```rust
let mut producer = DefaultMQProducer::builder()
    .producer_group("my_producer_group")
    .name_server_addr("localhost:9876")
    .compress_msg_body_over_howmuch(4 * 1024)
    .retry_times_when_send_failed(3)
    .retry_times_when_send_async_failed(3)
    .retry_another_broker_when_not_store_ok(true)
    .send_msg_max_timeout_per_request(5_000)
    .batch_max_delay_ms(10)
    .batch_max_bytes(512 * 1024)
    .total_batch_max_bytes(4 * 1024 * 1024)
    .enable_backpressure_for_async_mode(true)
    .back_pressure_for_async_send_num(10_000)
    .back_pressure_for_async_send_size(64 * 1024 * 1024)
    .build();
```

## Push Consumer Configuration

### Basic Push Consumer Configuration

```rust
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;

let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("my_consumer_group")
    .name_server_addr("localhost:9876")
    .consume_thread_min(2)
    .consume_thread_max(10)
    .build();
```

### Advanced Push Consumer Configuration

```rust
use rocketmq_common::common::consumer::consume_from_where::ConsumeFromWhere;
use rocketmq_remoting::protocol::heartbeat::message_model::MessageModel;

let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("my_consumer_group")
    .name_server_addr("localhost:9876")
    .consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset)
    .message_model(MessageModel::Clustering)
    .max_reconsume_times(3)
    .pull_batch_size(32)
    .pull_interval(0)
    .pull_threshold_for_queue(1_000)
    .pull_threshold_for_topic(10_000)
    .build();
```

## Lite Pull Consumer Configuration

```rust
use rocketmq_client_rust::consumer::default_lite_pull_consumer::DefaultLitePullConsumer;

let consumer = DefaultLitePullConsumer::builder()
    .consumer_group("my_pull_group")
    .name_server_addr("localhost:9876")
    .pull_batch_size(32)
    .pull_threshold_for_queue(1_000)
    .pull_threshold_for_all(10_000)
    .auto_commit(false)
    .auto_commit_interval_millis(5_000)
    .build();
```

## Configuration Options Reference

### Producer Builder Options

| Option | Description |
| -------- | ------------- |
| `producer_group` | Producer group name |
| `name_server_addr` | Name server address |
| `send_msg_timeout` | Send timeout in milliseconds |
| `retry_times_when_send_failed` | Retry count for sync send |
| `retry_times_when_send_async_failed` | Retry count for async send |
| `retry_another_broker_when_not_store_ok` | Retry another broker on store failure |
| `max_message_size` | Max message bytes |
| `compress_msg_body_over_howmuch` | Compression threshold |
| `batch_max_delay_ms` | Max batch hold time |
| `batch_max_bytes` | Max single batch bytes |
| `total_batch_max_bytes` | Max buffered batch bytes |

### Push Consumer Builder Options

| Option | Description |
| -------- | ------------- |
| `consumer_group` | Consumer group name |
| `name_server_addr` | Name server address |
| `consume_thread_min` | Min consume threads |
| `consume_thread_max` | Max consume threads |
| `consume_from_where` | Start point when no committed offset |
| `message_model` | Clustering or Broadcasting |
| `max_reconsume_times` | Max retry count |
| `pull_batch_size` | Pull batch size |
| `pull_threshold_for_queue` | Cache threshold per queue |
| `pull_threshold_for_topic` | Cache threshold per topic |

### Lite Pull Consumer Builder Options

| Option | Description |
| -------- | ------------- |
| `consumer_group` | Consumer group name |
| `name_server_addr` | Name server address |
| `pull_batch_size` | Messages per pull request |
| `pull_threshold_for_queue` | Cache threshold per queue |
| `pull_threshold_for_all` | Cache threshold for all queues |
| `auto_commit` | Enable/disable auto commit |
| `auto_commit_interval_millis` | Auto commit interval |

## Environment Variables

```rust
use std::env;

let name_server = env::var("ROCKETMQ_NAME_SERVER")
    .unwrap_or_else(|_| "localhost:9876".to_string());

let producer_group = env::var("ROCKETMQ_PRODUCER_GROUP")
    .unwrap_or_else(|_| "default_producer_group".to_string());

let mut producer = DefaultMQProducer::builder()
    .producer_group(producer_group)
    .name_server_addr(name_server)
    .build();
```

## Config File Example

```toml
[producer]
name_server_addr = "localhost:9876"
group_name = "my_producer"
send_msg_timeout = 3000
retry_times_when_send_failed = 3
max_message_size = 4194304
compress_msg_body_over_howmuch = 4096

[push_consumer]
name_server_addr = "localhost:9876"
group_name = "my_consumer"
consume_thread_min = 2
consume_thread_max = 10
pull_batch_size = 32
max_reconsume_times = 3
```

## Best Practices

1. Keep producer and consumer groups stable; do not randomize group names in production.
2. Tune retry and timeout settings according to your SLA.
3. Use push consumer for online processing, lite pull for controlled replay.
4. Set conservative cache thresholds before scaling up.
5. Track send/consume latency and retry rates continuously.

## Next Steps

- [Broker Configuration](./broker-config) - Configure brokers
- [Performance Tuning](./performance-tuning) - Optimize performance
