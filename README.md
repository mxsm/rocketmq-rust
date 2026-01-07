<p align="center">
    <img src="resources/logo.png" width="30%" height="auto"/>
</p>

<div align="center">
    
[![GitHub last commit](https://img.shields.io/github/last-commit/mxsm/rocketmq-rust)](https://github.com/mxsm/rocketmq-rust/commits/main)
[![Crates.io](https://img.shields.io/crates/v/rocketmq-rust.svg)](https://crates.io/crates/rocketmq-rust)
[![Docs.rs](https://docs.rs/rocketmq-rust/badge.svg)](https://docs.rs/rocketmq-rust)
[![CI](https://github.com/mxsm/rocketmq-rust/workflows/CI/badge.svg)](https://github.com/mxsm/rocketmq-rust/actions)
[![CodeCov][codecov-image]][codecov-url] [![GitHub contributors](https://img.shields.io/github/contributors/mxsm/rocketmq-rust)](https://github.com/mxsm/rocketmq-rust/graphs/contributors) [![Crates.io License](https://img.shields.io/crates/l/rocketmq-rust)](#license) 
<br/>
![GitHub repo size](https://img.shields.io/github/repo-size/mxsm/rocketmq-rust)
![Static Badge](https://img.shields.io/badge/MSRV-1.85.0%2B-25b373)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/mxsm/rocketmq-rust)

</div>

<div align="center">
  <a href="https://trendshift.io/repositories/12176" target="_blank"><img src="https://trendshift.io/api/badge/repositories/12176" alt="mxsm%2Frocketmq-rust | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
  <a href="https://trendshift.io/developers/3818" target="_blank"><img src="https://trendshift.io/api/badge/developers/3818" alt="mxsm | Trendshift" style="width: 250px; height: 55px;" width="250" height="55"/></a>
</div>

# RocketMQ-Rust

Welcome to [Apache Rocketmq](https://github.com/apache/rocketmq) Rust implementation. **RocketMQ-Rust(Rust library)** is
a reimplementation of the Apache RocketMQ message middleware in the Rust language. This project aims to provide Rust
developers with a high-performance and reliable message queue service, making full use of the features of the Rust
language.

### RocketMQ-Rust Features

- **Rust Language Advantages:** Leveraging the benefits of Rust, such as memory safety, zero-cost abstractions, and high
  concurrency performance, RocketMQ-Rust offers an efficient and reliable message middleware.
- **Asynchronous and Non-blocking Design:** RocketMQ-Rust takes full advantage of Rust's asynchronous programming
  capabilities, adopting a non-blocking design that supports high-concurrency message processing.
- **Ecosystem Integration:** As part of the Rust ecosystem, RocketMQ-Rust integrates well with other libraries and
  frameworks within the Rust ecosystem, providing flexible integration options for developers.
- **Cross-platform Support:** RocketMQ-Rust supports multiple platforms, including Linux, Windows, macOS, making it
  convenient for use in different environments.

## Architecture

![](resources/architecture.png)

### Document

- **`Manual official documentation`**：[Rocketmq-rust website](https://rocketmqrust.com)
- **`AI documentation`**: [Rocketmq-rust Deepwiki](https://deepwiki.com/mxsm/rocketmq-rust)

## Getting Started

### Requirements

1. rust toolchain MSRV is 1.85.0(stable,nightly)

### Run name server

**Run the following command to see usage：**

- **windows platform**

  ```cmd
  cargo run --bin rocketmq-namesrv-rust -- --help
  
  RocketMQ Name remoting_server(Rust)
  
  Usage: rocketmq-namesrv-rust.exe [OPTIONS]
  
  Options:
    -p, --port <PORT>                rocketmq name remoting_server port [default: 9876]
    -i, --ip <IP>                    rocketmq name remoting_server ip [default: 0.0.0.0]
    -c, --config-file <CONFIG FILE>  Name server config properties file
    -h, --help                       Print help
    -V, --version                    Print version
  ```

- **Linux platform**

  ```shell
  $ cargo run --bin rocketmq-namesrv-rust -- --help
  
  RocketMQ Name server(Rust)
  
  Usage: rocketmq-namesrv-rust [OPTIONS]
  
  Options:
    -p, --port <PORT>                rocketmq name remoting_server port [default: 9876]
    -i, --ip <IP>                    rocketmq name remoting_server ip [default: 0.0.0.0]
    -c, --config-file <CONFIG FILE>  Name server config properties file
    -h, --help                       Print help
    -V, --version                    Print version
  ```

Run the following command to start the name server

```shell
cargo run --bin rocketmq-namesrv-rust
```

### Run Broker

**Run the following command to see usage：**

- **windows platform**

  ```shell
  cargo run --bin rocketmq-broker-rust -- --help
  
  RocketMQ Broker Server(Rust)
  
  Usage: rocketmq-broker-rust.exe [OPTIONS]
  
  Options:
    -c, --config-file <FILE>      Broker config properties file
    -m, --print-important-config  Print important config item
    -n, --namesrv-addr <IP>       Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876' [default: 127.0.0.1:9876]
    -p, --print-config-item       Print all config item
    -h, --help                    Print help
    -V, --version                 Print version
  ```


- **Linux platform**

  ```shell
  $ cargo run --bin rocketmq-broker-rust -- --help
  
  RocketMQ Broker Server(Rust)
  
  Usage: rocketmq-broker-rust [OPTIONS]
  
  Options:
    -c, --config-file <FILE>      Broker config properties file
    -m, --print-important-config  Print important config item
    -n, --namesrv-addr <IP>       Name server address list, eg: '192.168.0.1:9876;192.168.0.2:9876' [default: 127.0.0.1:9876]
    -p, --print-config-item       Print all config item
    -h, --help                    Print help
    -V, --version                 Print version
  ```

Run the following command to start the broker

```shell
cargo run --bin rocketmq-broker-rust
```

## Client how to send message

First, start the RocketMQ NameServer and Broker services.

- [**Send a single message**](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md#Send-a-single-message)

- [**Send batch messages**](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md#Send-batch-messages)

- [**Send RPC messages**](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/README.md#Send-RPC-messages)

[**For more examples, you can check here**](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-client/examples)

## Modules

The existing RocketMQ has the following functional modules:

- [**Name Server**](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-namesrv)
- [**Broker**](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-broker)
- [**Store (Local Storage)**](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-store)
- **Controller (High Availability)**
- [**Client (SDK)**](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-client)
- **Proxy**
- **Tiered Store (Tiered Storage Module)**

The specific functions of each module can be referred to in
the [official RocketMQ documentation](https://github.com/apache/rocketmq/tree/develop/docs). The Rust implementation
will be carried out sequentially in the following order.

## Contributing

Contributions to code, issue reporting, and suggestions are welcome. The development of RocketMQ-Rust relies on the
support of developers. Let's collaborate to advance Rust in the message middleware
domain. [**`Contribute Guide`**](https://rocketmqrust.com/docs/contribute-guide/)

![](https://repobeats.axiom.co/api/embed/6ca125de92b36e1f78c6681d0a1296b8958adea1.svg "Repobeats analytics image")

<a href="https://github.com/mxsm/rocketmq-rust/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=mxsm/rocketmq-rust&anon=1" />
</a>

[![Stargazers over time](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date)](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date)

## License

RocketMQ-Rust is licensed under the [Apache License 2.0](https://github.com/mxsm/rocketmq-rust/blob/main/LICENSE-APACHE)
and [MIT license](https://github.com/mxsm/rocketmq-rust/blob/main/LICENSE-MIT)


[codecov-image]: https://codecov.io/gh/mxsm/rocketmq-rust/branch/main/graph/badge.svg

[codecov-url]: https://codecov.io/gh/mxsm/rocketmq-rust

