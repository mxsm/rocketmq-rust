# rocketmq-common

[English](README.md) | [简体中文](README-zh_cn.md)

[RocketMQ-Rust](../README-zh_cn.md) 的共享消息模型、协议常量、配置类型和基础工具 crate。

`rocketmq-common` 是 RocketMQ-Rust workspace 中的基础 crate。它提供 Java 兼容的 RocketMQ 数据结构、消息辅助
能力、Broker 和 NameServer 配置模型、压缩与序列化工具、一致性哈希、统计模型，以及 broker、client、NameServer、
store、proxy 和 tools 等 crate 共同依赖的运行时基础能力。

这个 crate 本身不是独立的 RocketMQ 服务。它面向上层 crate 提供通用构件，用于实现 RocketMQ 兼容行为。

## 能力概览

| 领域 | 提供能力 |
|------|----------|
| 消息模型 | `Message`、`MessageExt`、Broker 内部消息、批量消息、队列元数据、消息访问器、消息属性、延迟消息元数据、撤回句柄和消息 ID 工具。 |
| 配置模型 | Broker、Controller、NameServer、Server、TLS、Plain Access 和通用配置模型，并在需要时保留 Java 风格字段命名。 |
| 协议常量 | 权限、系统标记、操作/资源模型、消费者和生产者常量、Topic 属性、队列类型、MQ 版本数据和 RocketMQ 协议键。 |
| 路由与拓扑 | `MessageQueue`、Topic 元数据、Key 构建器、命名空间辅助工具、Lite Topic 工具，以及带虚拟节点的一致性哈希路由。 |
| 序列化与压缩 | Serde JSON 工具、可选 SIMD JSON 工具、LZ4/Zlib/Zstd 压缩支持、CRC、Base64、MD5 和字节转换工具。 |
| 运行时工具 | 文件、环境变量、时间、网络、HTTP、字符串和配置文件解析工具，以及线程池和执行器服务抽象。 |
| 可观测性与统计 | Metrics exporter 枚举、空实现 metric wrapper、统计项、快照、定时打印器和运行状态模型。 |

## 目录结构

| 路径 | 说明 |
|------|------|
| [`src/lib.rs`](src/lib.rs) | crate 对外导出入口，供 workspace 内其它 crate 使用。 |
| [`src/common.rs`](src/common.rs) | 汇总 message、broker、controller、topic、stats、metrics、compression 等 RocketMQ 领域模块。 |
| [`src/common/message`](src/common/message) | 消息模型、builder、属性、decoder、批量格式、队列模型和 Broker 侧消息变体。 |
| [`src/common/broker`](src/common/broker) | Broker 运行时代码共享的 Broker 配置和角色模型。 |
| [`src/common/controller`](src/common/controller) | Controller 配置基础类型。 |
| [`src/common/namesrv`](src/common/namesrv) | NameServer 地址管理、配置和更新回调辅助工具。 |
| [`src/common/compression`](src/common/compression) | 压缩 trait、类型分发，以及 LZ4/Zlib/Zstd 实现。 |
| [`src/common/consistenthash`](src/common/consistenthash) | 一致性哈希 router、节点 trait、哈希函数抽象和虚拟节点支持。 |
| [`src/common/lite`](src/common/lite) | Lite Topic/Subscription DTO 和 offset 辅助工具。 |
| [`src/common/metrics`](src/common/metrics) | Metrics exporter 类型和空实现 metric instrument。 |
| [`src/common/statistics`](src/common/statistics) | 统计管理器、统计项状态、格式化、拦截器和定时输出基础能力。 |
| [`src/common/stats`](src/common/stats) | Java 兼容的 stats item set、snapshot 和 call snapshot。 |
| [`src/utils.rs`](src/utils.rs) | 文件、JSON、CRC、时间、网络、配置解析和通用工具模块导出。 |
| [`src/thread_pool.rs`](src/thread_pool.rs) | 线程池和执行器服务抽象。 |
| [`src/log.rs`](src/log.rs) | examples 和服务 crate 使用的日志初始化辅助工具。 |

## 环境要求

- workspace 最低 Rust 版本为 `1.85.0`。
- 请使用仓库根目录的 [`../rust-toolchain.toml`](../rust-toolchain.toml) 指定的 toolchain 构建。该 crate 当前启用了
  nightly Rust feature。
- 运行本 crate 的本地 examples、tests 或工具能力时，不需要 RocketMQ Broker 或 NameServer。

## 安装

在本 workspace 内使用：

```toml
[dependencies]
rocketmq-common = { path = "../rocketmq-common" }
```

外部项目使用：

```toml
[dependencies]
rocketmq-common = "1.0.0"
```

可选 features：

```toml
[dependencies]
rocketmq-common = { version = "1.0.0", features = ["async_fs"] }
```

```toml
[dependencies]
rocketmq-common = { version = "1.0.0", features = ["simd"] }
```

## 快速开始

使用 builder API 创建 RocketMQ 消息：

```rust
use rocketmq_common::common::message::message_single::Message;
use rocketmq_common::RocketMQResult;

fn main() -> RocketMQResult<()> {
    let message = Message::builder()
        .topic("TopicTest")
        .tags("TagA")
        .key("order-1001")
        .body_slice(b"Hello RocketMQ")
        .build()?;

    println!("topic: {}", message.topic());
    println!("tags: {:?}", message.tags());
    println!("keys: {:?}", message.keys());

    Ok(())
}
```

使用一致性哈希 router 分配 key：

```rust
use rocketmq_common::common::consistenthash::{ConsistentHashRouter, Node};

#[derive(Clone)]
struct BrokerNode {
    address: String,
}

impl Node for BrokerNode {
    fn get_key(&self) -> &str {
        &self.address
    }
}

fn main() {
    let nodes = vec![
        BrokerNode {
            address: "broker-a:10911".to_string(),
        },
        BrokerNode {
            address: "broker-b:10911".to_string(),
        },
    ];

    let router = ConsistentHashRouter::new(nodes, 150);
    let node = router.route_node("order-1001").expect("router has nodes");
    println!("selected broker: {}", node.get_key());
}
```

构建并解析延迟消息撤回句柄：

```rust
use rocketmq_common::{RecallMessageHandle, RecallMessageHandleV1};

fn main() -> rocketmq_common::RocketMQResult<()> {
    let handle = RecallMessageHandleV1::build_handle(
        "TopicTest",
        "broker-a",
        "1707111111111",
        "message-id-1001",
    );

    let decoded = RecallMessageHandle::decode_handle(&handle)?;
    println!("recall topic: {}", decoded.topic());

    Ok(())
}
```

## Feature Flags

| Feature | 默认开启 | 用途 |
|---------|----------|------|
| `async_fs` | 否 | 启用 `utils::file_utils` 中基于 Tokio 的异步文件辅助能力。 |
| `simd` | 否 | 通过 `simd-json` 启用 `utils::simd_json_utils`，用于高吞吐 JSON 解析和序列化路径。 |

## Examples

从 workspace 根目录运行 examples：

```bash
cargo run -p rocketmq-common --example message_builder_examples
cargo run -p rocketmq-common --example consistent_hash_router_example
cargo run -p rocketmq-common --example recall_message_handle_example
cargo run -p rocketmq-common --example json_comparison
cargo run -p rocketmq-common --features simd --example json_comparison
```

## 验证

本 crate 的聚焦检查：

```bash
cargo test -p rocketmq-common --lib
cargo test -p rocketmq-common --features async_fs --lib
cargo test -p rocketmq-common --examples --no-run
cargo test -p rocketmq-common --features simd --examples --no-run
```

当修改 Rust 代码时，需要从仓库根目录执行 workspace 级验证：

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Benchmarks

Criterion benchmarks 覆盖部分工具热点路径：

```bash
cargo bench -p rocketmq-common --bench delete_property
cargo bench -p rocketmq-common --bench perm_name
cargo bench -p rocketmq-common --bench json_utils_comparison
```

## License

基于 [Apache License, Version 2.0](../LICENSE-APACHE) 开源。
