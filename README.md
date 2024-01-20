# rocketmq-rust

 Welcome to [Apache Rocketmq](https://github.com/apache/rocketmq) Rust implementation (Unofficial ). **RocketMQ-Rust** is a reimplementation of the Apache RocketMQ message middleware in the Rust language. This project aims to provide Rust developers with a high-performance and reliable message queue service, making full use of the features of the Rust language.

### RocketMQ-Rust Features

- **Rust Language Advantages:** Leveraging the benefits of Rust, such as memory safety, zero-cost abstractions, and high concurrency performance, RocketMQ-Rust offers an efficient and reliable message middleware.
- **Asynchronous and Non-blocking Design:** RocketMQ-Rust takes full advantage of Rust's asynchronous programming capabilities, adopting a non-blocking design that supports high-concurrency message processing.
- **Ecosystem Integration:** As part of the Rust ecosystem, RocketMQ-Rust integrates well with other libraries and frameworks within the Rust ecosystem, providing flexible integration options for developers.
- **Cross-platform Support:** RocketMQ-Rust supports multiple platforms, including Linux, Windows, macOS, making it convenient for use in different environments.

## Getting Started

### Requirements

1. rust toolchain MSRV is 1.75.(stable,nightly)

### Run name server

**Run the following command to see usage：**

- **windows platform**

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

- **Linux platform**

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

Run the following command to start the name server

```shell
cargo run --bin rocketmq-namesrv-rust
```

## Modules

The existing RocketMQ has the following functional modules:

- **Name Server**
- **Broker**
- **Store (Local Storage)**
- **Controller (High Availability)**
- **Client (SDK)**
- **Proxy**
- **Tiered Store (Tiered Storage Module)**

The specific functions of each module can be referred to in the [official RocketMQ documentation](https://github.com/apache/rocketmq/tree/develop/docs). The Rust implementation will be carried out sequentially in the following order.

## Name Server

Feature list:

> Not support: :broken_heart: :x:
>
> Base support: :heart: :white_check_mark:
>
> Perfect support: :sparkling_heart: :white_check_mark:

| Feature                                | request code | Support        | remark |
| -------------------------------------- | ------------ | -------------- | ------ |
| Put KV Config                          | 100          | :sparkling_heart: :white_check_mark: |        |
| Get KV Config                          | 101          | :sparkling_heart: :white_check_mark: |        |
| Delete KV Config                       | 102          | :sparkling_heart: :white_check_mark: |        |
| Get kv list by namespace               | 219          | :broken_heart: :x: |        |
| Query Data Version                     | 322          | :broken_heart: :x: |        |
| Register Broker                        | 103          | :heart:        |        |
| Unregister Broker                      | 104          | :broken_heart: :x: |        |
| Broker Heartbeat                       | 904          | :broken_heart: :x: |        |
| Get broker member_group                | 901          | :broken_heart: :x: |        |
| Get broker cluster info                | 106          | :broken_heart: :x: |        |
| Wipe write perm of boker               | 205          | :broken_heart: :x: |        |
| Add write perm of brober               | 327          | :broken_heart: :x: |        |
| Get all topic list from name server    | 206          | :broken_heart: :x: |        |
| Delete topic in name server            | 216          | :broken_heart: :x: |        |
| Register topic in name server          | 217          | :broken_heart: :x: |        |
| Get topics by cluster                  | 224          | :broken_heart: :x: |        |
| Get system topic list from name server | 304          | :broken_heart: :x: |        |
| Get unit topic list                    | 311          | :broken_heart: :x: |        |
| Get has unit sub topic list            | 312          | :broken_heart: :x: |        |
| Get has unit sub ununit topic list     | 313          | :broken_heart: :x: |        |
| Update name server config              | 318          | :broken_heart: :x: |        |
| Get name server config                 | 318          | :broken_heart: :x: |        |

Other module implementations will be done subsequently, starting with the Rust implementation of the Name Server. The goal is to achieve functionality similar to the Java version.

## Contributing

Contributions to code, issue reporting, and suggestions are welcome. The development of RocketMQ-Rust relies on the support of developers. Let's collaborate to advance Rust in the message middleware domain.

![Alt](https://repobeats.axiom.co/api/embed/6ca125de92b36e1f78c6681d0a1296b8958adea1.svg "Repobeats analytics image")

<a href="https://github.com/mxsm/rocketmq-rust/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=mxsm/rocketmq-rust&anon=1" />
</a>


|                   **Stargazers Over Time**                   |                  **Contributors Over Time**                  |
| :----------------------------------------------------------: | :----------------------------------------------------------: |
| [![Stargazers over time](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date)](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date) | [![GitHub Contributor Over Time](https://contributor-overtime-api.git-contributor.com/contributors-svg?chart=contributorOverTime&repo=mxsm/rocketmq-rust)](https://git-contributor.com?chart=contributorOverTime&repo=mxsm/rocketmq-rust) |

## License

RocketMQ-Rust is licensed under the [Apache License 2.0](https://github.com/mxsm/rocketmq-rust/blob/main/LICENSE-APACHE) and [MIT license](https://github.com/mxsm/rocketmq-rust/blob/main/LICENSE-MIT)