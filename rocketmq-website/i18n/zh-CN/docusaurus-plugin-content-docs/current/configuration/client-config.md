---
sidebar_position: 2
title: 客户端配置
---

# 客户端配置

本页介绍当前 RocketMQ-Rust 中 Producer 与 Consumer 的 builder 配置方式。

## 生产者配置

### 生产者基础配置

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

### 生产者高级配置

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

## 推模式消费者配置

### 基础配置

```rust
use rocketmq_client_rust::consumer::default_mq_push_consumer::DefaultMQPushConsumer;

let mut consumer = DefaultMQPushConsumer::builder()
    .consumer_group("my_consumer_group")
    .name_server_addr("localhost:9876")
    .consume_thread_min(2)
    .consume_thread_max(10)
    .build();
```

### 高级配置

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

## 拉模式消费者配置

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

## 配置项速查

### Producer Builder

| 选项 | 说明 |
| -------- | ------------- |
| `producer_group` | 生产者组名 |
| `name_server_addr` | NameServer 地址 |
| `send_msg_timeout` | 发送超时（毫秒） |
| `retry_times_when_send_failed` | 同步发送重试次数 |
| `retry_times_when_send_async_failed` | 异步发送重试次数 |
| `retry_another_broker_when_not_store_ok` | 存储失败时切换 Broker 重试 |
| `max_message_size` | 最大消息大小 |
| `compress_msg_body_over_howmuch` | 压缩阈值 |
| `batch_max_delay_ms` | 批量聚合最大等待时长 |
| `batch_max_bytes` | 单批最大字节数 |
| `total_batch_max_bytes` | 总缓冲批量字节上限 |

### Push Consumer Builder

| 选项 | 说明 |
| -------- | ------------- |
| `consumer_group` | 消费者组名 |
| `name_server_addr` | NameServer 地址 |
| `consume_thread_min` | 最小消费线程数 |
| `consume_thread_max` | 最大消费线程数 |
| `consume_from_where` | 无位点时起始消费位置 |
| `message_model` | 集群/广播模式 |
| `max_reconsume_times` | 最大重试次数 |
| `pull_batch_size` | 单次拉取批量 |
| `pull_threshold_for_queue` | 单队列缓存阈值 |
| `pull_threshold_for_topic` | 单主题缓存阈值 |

### Lite Pull Consumer Builder

| 选项 | 说明 |
| -------- | ------------- |
| `consumer_group` | 消费者组名 |
| `name_server_addr` | NameServer 地址 |
| `pull_batch_size` | 每次拉取条数 |
| `pull_threshold_for_queue` | 单队列缓存阈值 |
| `pull_threshold_for_all` | 总缓存阈值 |
| `auto_commit` | 是否自动提交位点 |
| `auto_commit_interval_millis` | 自动提交间隔 |

## 环境变量示例

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

## 配置文件示例

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

## 最佳实践

1. 生产环境保持稳定的 producer/consumer group 命名。
2. 重试和超时参数要与业务 SLA 对齐。
3. 在线处理优先推模式，回放与批处理优先拉模式。
4. 缓存阈值从保守值开始，压测后逐步放大。
5. 持续监控发送/消费延迟与重试率。

## 下一步

- [Broker 配置](./broker-config) - 配置服务端参数
- [性能调优](./performance-tuning) - 学习性能优化策略
