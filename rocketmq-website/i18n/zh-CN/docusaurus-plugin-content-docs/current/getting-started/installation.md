---
sidebar_position: 1
title: 安装
---

# 安装 RocketMQ-Rust

让 RocketMQ-Rust 在你的环境中快速运行起来。

## 环境要求

在安装 RocketMQ-Rust 前，请确认具备以下条件：

- **Rust**：1.70.0 或更高版本（[安装 Rust](https://www.rust-lang.org/tools/install)）
- **Cargo**：随 Rust 一起安装
- **操作系统**：Linux、macOS 或 Windows

### 验证安装环境

```bash
rustc --version
cargo --version
```

## 从 Crates.io 安装

使用 RocketMQ-Rust 最简单的方式，是在你的 `Cargo.toml` 中添加依赖：

```toml
[dependencies]
rocketmq = "0.3"
```

然后执行：

```bash
cargo build
```

## 从源码构建

如果你希望使用最新特性与修复，可以从源码构建：

```bash
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust
cargo build --release
```

## 运行示例

RocketMQ-Rust 提供了多种示例，便于快速上手：

```bash
# 克隆仓库
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust

# 运行简单生产者示例
cargo run --example simple_producer

# 运行简单消费者示例
cargo run --example simple_consumer
```

## Docker 环境

在开发与测试场景中，你可以使用 Docker：

```bash
# 拉取 RocketMQ nameserver 镜像
docker pull apache/rocketmq:nameserver

# 拉取 RocketMQ broker 镜像
docker pull apache/rocketmq:broker

# 启动 nameserver
docker run -d -p 9876:9876 --name rmqnamesrv apache/rocketmq:nameserver

# 启动 broker
docker run -d -p 10911:10911 -p 10909:10909 --name rmqbroker \
  -e "NAMESRV_ADDR=rmqnamesrv:9876" \
  --link rmqnamesrv:rmqnamesrv \
  apache/rocketmq:broker
```

## 下一步

- [快速开始](./quick-start) - 创建你的第一个生产者和消费者
- [基本概念](./basic-concepts) - 理解 RocketMQ 的核心概念
- [配置](../category/configuration) - 配置你的 RocketMQ 实例

## 常见问题排查

### Rust 版本过低

如果遇到 Rust 版本相关报错，请升级：

```bash
rustup update stable
```

### 构建失败

如果构建失败，可尝试：

```bash
cargo clean
cargo build
```

### 端口已被占用

如果默认端口被占用，可以在 Broker 配置中调整端口。详情请参考[配置](../category/configuration)。
