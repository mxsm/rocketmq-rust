---
sidebar_position: 2
title: 客户端配置
---

# 客户端配置

本章介绍如何配置 RocketMQ-Rust 客户端（Producer 与 Consumer），以获得更好的稳定性和性能。

## 生产者配置

### 生产者基础配置

```rust
let mut producer_option = ProducerOption::default();

// 必填参数
producer_option.set_name_server_addr("localhost:9876");
producer_option.set_group_name("my_producer_group");

// 可选参数
producer_option.set_send_msg_timeout(3000); // 毫秒
producer_option.set_retry_times_when_send_failed(2);
producer_option.set_max_message_size(4 * 1024 * 1024); // 4MB
```

### 生产者高级配置

```rust
// 压缩
producer_option.set_compress_msg_body_over_threshold(4 * 1024); // 4KB

// 重试
producer_option.set_retry_times_when_send_failed(3);
producer_option.set_retry_next_server(true);

// 超时
producer_option.set_tcp_transport_try_lock_timeout(1000);
producer_option.set_tcp_transport_connect_timeout(3000);

// 连接池
producer_option.set_client_channel_max_idle_time_seconds(120);
```

### 生产者完整示例

```rust
use rocketmq::producer::Producer;
use rocketmq::conf::ProducerOption;

fn create_producer() -> Producer {
    let mut producer_option = ProducerOption::default();

    // 基础参数
    producer_option.set_name_server_addr("localhost:9876");
    producer_option.set_group_name("order_producer");

    // 性能参数
    producer_option.set_compress_msg_body_over_threshold(4 * 1024);
    producer_option.set_max_message_size(4 * 1024 * 1024);

    // 可靠性参数
    producer_option.set_send_msg_timeout(3000);
    producer_option.set_retry_times_when_send_failed(3);
    producer_option.set_retry_next_server(true);

    Producer::new(producer_option)
}
```

## 消费者配置

### 消费者基础配置

```rust
let mut consumer_option = ConsumerOption::default();

// 必填参数
consumer_option.set_name_server_addr("localhost:9876");
consumer_option.set_group_name("my_consumer_group");

// 线程池
consumer_option.set_consume_thread_min(2);
consumer_option.set_consume_thread_max(10);
```

### 消费者高级配置

```rust
// 消费起点
consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

// 重试参数
consumer_option.set_max_reconsume_times(3);

// 消息模型
consumer_option.set_message_model(MessageModel::Clustering);

// 拉取参数
consumer_option.set_pull_batch_size(32);
consumer_option.set_pull_interval(0);
```

### 消费者完整示例

```rust
use rocketmq::consumer::PushConsumer;
use rocketmq::conf::ConsumerOption;

fn create_consumer() -> PushConsumer {
    let mut consumer_option = ConsumerOption::default();

    // 基础参数
    consumer_option.set_name_server_addr("localhost:9876");
    consumer_option.set_group_name("order_consumer");

    // 线程参数
    consumer_option.set_consume_thread_min(2);
    consumer_option.set_consume_thread_max(10);

    // 位点参数
    consumer_option.set_consume_from_where(ConsumeFromWhere::ConsumeFromLastOffset);

    // 拉取参数
    consumer_option.set_pull_batch_size(32);
    consumer_option.set_pull_interval(0);

    // 重试参数
    consumer_option.set_max_reconsume_times(3);

    PushConsumer::new(consumer_option)
}
```

## 常用配置项

### Producer 配置项

| 选项 | 默认值 | 说明 |
| -------- | --------- | ------------- |
| `name_server_addr` | 必填 | Name Server 地址 |
| `group_name` | 必填 | 生产者组名称 |
| `send_msg_timeout` | 3000 | 发送超时（毫秒） |
| `retry_times_when_send_failed` | 2 | 发送失败重试次数 |
| `max_message_size` | 4MB | 最大消息大小 |
| `compress_msg_body_over_threshold` | 4KB | 压缩阈值 |
| `retry_next_server` | false | 是否切换 Broker 重试 |

### Consumer 配置项

| 选项 | 默认值 | 说明 |
| -------- | --------- | ------------- |
| `name_server_addr` | 必填 | Name Server 地址 |
| `group_name` | 必填 | 消费者组名称 |
| `consume_thread_min` | 1 | 最小消费线程数 |
| `consume_thread_max` | 10 | 最大消费线程数 |
| `pull_batch_size` | 32 | 单次拉取消息数 |
| `pull_interval` | 0 | 拉取间隔（毫秒） |
| `max_reconsume_times` | 16 | 最大重试次数 |
| `message_model` | Clustering | 集群或广播模式 |

## 使用环境变量

```rust
use std::env;

// 从环境变量读取配置
let name_server = env::var("ROCKETMQ_NAME_SERVER")
    .unwrap_or_else(|_| "localhost:9876".to_string());

let group_name = env::var("ROCKETMQ_GROUP")
    .unwrap_or_else(|_| "default_group".to_string());

producer_option.set_name_server_addr(&name_server);
producer_option.set_group_name(&group_name);
```

## 使用配置文件

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

## 最佳实践

1. **设置合理超时**：在可靠性与响应速度之间平衡。
2. **配置重试策略**：避免瞬时故障导致消息丢失。
3. **按负载调优线程池**：提升吞吐并避免资源浪费。
4. **限制消息大小**：降低大消息对系统冲击。
5. **启用压缩**：在大消息场景减少带宽占用。
6. **持续观测指标**：关注成功率、延迟与重试率。

## 下一步

- [Broker 配置](./broker-config) - 配置 Broker 核心参数
- [性能调优](./performance-tuning) - 优化性能
