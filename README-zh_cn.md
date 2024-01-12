# rocketmq-rust

[Apache Rocketmq](https://github.com/apache/rocketmq)非官方的Rust实现。RocketMQ-Rust 是基于 Rust 语言重新实现的 Apache RocketMQ 消息中间件。该项目旨在为 Rust 开发者提供高性能、可靠的消息队列服务，并充分利用 Rust 语言的特性。

### RocketMQ-Rust 的特点

- **Rust 语言优势：** 利用 Rust 语言的内存安全性、零成本抽象、并发性能等特性，提供高效、可靠的消息中间件。
- **异步和非阻塞设计：** RocketMQ-Rust 充分利用 Rust 异步编程的能力，采用非阻塞设计，支持高并发消息处理。
- **生态整合：** 作为 Rust 生态系统的一部分，RocketMQ-Rust 与 Rust 生态中其他库和框架的兼容性良好，为开发者提供灵活的集成选项。
- **跨平台支持：** RocketMQ-Rust 支持多种平台，包括 Linux、Windows、macOS 等，方便在不同环境下使用。

## 快速开始

### 配置要求

1.  Rust的msrv版本1.75.0(stable,nightly)

### 运行NameServer

**运行下面命令查看使用**：

- **windows 平台**

  ```cmd
  cargo run --bin rocketmq-namesrv-rust -- --help
  
  RocketMQ Name server(Rust)
  
  Usage: rocketmq-namesrv-rust.exe [OPTIONS]
  
  Options:
    -p, --port <PORT>  rocketmq name server port [default: 9876]
    -i, --ip <IP>      rocketmq name server ip [default: 127.0.0.1]
    -h, --help         Print help
    -V, --version      Print version
  ```

- **Linux 平台**

  ```shell
  $ cargo run --bin rocketmq-namesrv-rust -- --help
  
  RocketMQ Name server(Rust)
  
  Usage: rocketmq-namesrv-rust [OPTIONS]
  
  Options:
    -p, --port <PORT>  rocketmq name server port [default: 9876]
    -i, --ip <IP>      rocketmq name server ip [default: 127.0.0.1]
    -h, --help         Print help
    -V, --version      Print version
  ```

**根据下面的命令运行NameServer**

```shell
cargo run --bin rocketmq-namesrv-rust
```

## 模块

现有的Rocketmq主要有如下几个功能模块：

- **Name Server**
- **Broker**
- **Store(存储-本地)**
- **Controller(高可用)**
- **Client(SDK)**
- **Proxy(代理)**
- **Tiered store(分级存储模块)**

每个模块的具体作用可以参照[Rocketmq的官方说明文档](https://github.com/apache/rocketmq/tree/develop/docs) 。Rust的实现会根据下面的顺序逐一来进行实现。

## Name Server

### Broker管理

- [x] **Broker注册(请求码：103)-暂时只支持基本的Broker注册，对于Controller模式待支持**
- [x] **获取集群信息(请求码：106)**
- [ ] **HeartBeat消息处理**

TODO

后续会进行其他的模块实现，首先会对NameServer进行Rust实现，目标是能够达到和Java版本一样的功能。

## 贡献

欢迎贡献代码、报告问题或提出建议。RocketMQ-Rust 的发展离不开开发者的支持，让我们共同推动 Rust 在消息中间件领域的发展。

![Alt](https://repobeats.axiom.co/api/embed/6ca125de92b36e1f78c6681d0a1296b8958adea1.svg "Repobeats analytics image")

<a href="https://github.com/mxsm/rocketmq-rust/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=mxsm/rocketmq-rust&anon=1" />
</a>

|                   **Stargazers Over Time**                   |                  **Contributors Over Time**                  |
| :----------------------------------------------------------: | :----------------------------------------------------------: |
| [![Stargazers over time](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date)](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date) | [![GitHub Contributor Over Time](https://contributor-overtime-api.git-contributor.com/contributors-svg?chart=contributorOverTime&repo=mxsm/rocketmq-rust)](https://git-contributor.com?chart=contributorOverTime&repo=mxsm/rocketmq-rust) |

## 许可证

RocketMQ-Rust 使用 [Apache License 2.0](https://github.com/mxsm/rocketmq-rust/blob/main/LICENSE-APACHE) 和 [MIT license](https://github.com/mxsm/rocketmq-rust/blob/main/LICENSE-MIT)