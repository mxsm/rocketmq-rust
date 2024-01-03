# rocketmq-rust

Unofficial Rust implementation of [Apache Rocketmq](https://github.com/apache/rocketmq).

|                   **Stargazers Over Time**                   |                  **Contributors Over Time**                  |
| :----------------------------------------------------------: | :----------------------------------------------------------: |
| [![Stargazers over time](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date)](https://api.star-history.com/svg?repos=mxsm/rocketmq-rust&type=Date) | [![GitHub Contributor Over Time](https://contributor-overtime-api.git-contributor.com/contributors-svg?chart=contributorOverTime&repo=mxsm/rocketmq-rust)](https://git-contributor.com?chart=contributorOverTime&repo=mxsm/rocketmq-rust) |

## Getting Started

### Requirements

1. rust toolchain MSRV is 1.75.(stable,nightly)

### Run name server

Run the following command to start the name server

```shell
cargo run --bin namesrv
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

### Broker Management

- [x] **Broker registration(request code: 103)-Currently, only basic Broker registration is supported. Support for the Controller mode is pending.**
- [x] **Retrieve cluster information(request code: 106)**
- [ ] **Heartbeat message processing**

TODO

Other module implementations will be done subsequently, starting with the Rust implementation of the Name Server. The goal is to achieve functionality similar to the Java version.

## Contributing

![Alt](https://repobeats.axiom.co/api/embed/6ca125de92b36e1f78c6681d0a1296b8958adea1.svg "Repobeats analytics image")

<a href="https://github.com/mxsm/rocketmq-rust/graphs/contributors">
  <img src="https://contrib.rocks/image?repo=mxsm/rocketmq-rust&anon=1" />
</a>