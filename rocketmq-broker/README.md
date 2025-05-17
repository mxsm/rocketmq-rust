# The Rust Implementation of Apache RocketMQ Broker

## Overview

This module is mainly the implementation of the [Apache RocketMQ](https://github.com/apache/rocketmq) Broker, containing all the functionalities of the Java version Broker.

## Getting Started

### Requirements

1. rust toolchain MSRV is 1.75.(stable,nightly)

### Run Borker

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

Run the following command to start the name server

```
cargo run --bin rocketmq-broker-rust
```

## Feature

**Feature list**:

- **Not support**: 💔 ❌
- **Base support**: ❤️ ✅
- **Perfect support**: 💖 ✅

| Feature                      | request code       | Support | remark                                  |
|------------------------------|--------------------|---------|-----------------------------------------|
| topic config load            | :heavy_minus_sign: | 💔 ❌    | TopicConfigManager class function       |
| topic queue mapping load     | :heavy_minus_sign: | 💔 ❌    | TopicQueueMappingManager class function |
| consume offset load          | :heavy_minus_sign: | 💔 ❌    | ConsumerOffsetManager class function    |
| subscription group load      | :heavy_minus_sign: | 💔 ❌    | SubscriptionGroupManager class function |
| consumer filter load         | :heavy_minus_sign: | 💔 ❌    | ConsumerFilterManager class function    |
| consumer order info load     | :heavy_minus_sign: | 💔 ❌    | ConsumerOrderInfoManager class function |
| message store load           | :heavy_minus_sign: | 💔 ❌    |                                         |
| timer message store load     | :heavy_minus_sign: | 💔 ❌    |                                         |
| schedule message store load  | :heavy_minus_sign: | 💔 ❌    |                                         |
| send message hook            | :heavy_minus_sign: | 💔 ❌    |                                         |
| consume message hook         | :heavy_minus_sign: | 💔 ❌    |                                         |
| send message                 | 10                 | ❤️ ✅    |                                         |
| send message v2              | 310                | ❤️ ✅    |                                         |
| send batch message           | 320                | ❤️ ✅    |                                         |
| consume send message back    | 36                 | ❤️ ✅    |                                         |
| pull message                 | 11                 | ❤️ ✅    |                                         |
| lite pull message            | 361                | ❤️ ✅    |                                         |
| peek message                 | 200052             | 💔 ❌    |                                         |
| pop message                  | 200050             | ❤️ ✅    |                                         |
| ack message                  | 200051             | ❤️ ✅    |                                         |
| batch ack message            | 200151             | ❤️ ✅    |                                         |
| change message invisibletime | 200053             | ❤️ ✅    |                                         |
| notification                 | 200054             | 💔 ❌    |                                         |
| polling info                 | 200055             | 💔 ❌    |                                         |
| send reply message           | 324                | ❤️ ✅    |                                         |
| send reply message v2        | 325                | ❤️ ✅    |                                         |
| query message                | 12                 | ❤️ ✅    |                                         |
| view message by id           | 33                 | ❤️ ✅    |                                         |
| heart beat                   | 34                 | ❤️ ✅    |                                         |
| unregister client            | 35                 | ❤️ ✅    |                                         |
| check client config          | 46                 | 💔 ❌    |                                         |
| get consumer list by group   | 38                 | ❤️ ✅    |                                         |
| update consumer offset       | 15                 | ❤️ ✅    |                                         |
| query consumer offset        | 14                 | ❤️ ✅    |                                         |
| query assignment             | 400                | ❤️ ✅    |                                         |
| set message request mode     | 401                | ❤️ ✅    |                                         |
| end transacation             | 37                 | ❤️ ✅    |                                         |
| default processor            | :heavy_minus_sign: | 💔 ❌    | AdminBrokerProcessor class function     |







