# The Rust Implementation of Apache RocketMQ Name server

## Overview

Here is the rust implementation of the **name server** for [Apache RocketMQ](https://rocketmq.apache.org/). 

## Feature

Feature list:

- **Not support**: :broken_heart:

- **Base support**: :heart:

- **Perfect support**: :sparkling_heart:

| Feature                                | request code | Support        | remark |
| -------------------------------------- | ------------ | -------------- | ------ |
| Put KV Config                          | 100          | :broken_heart: |        |
| Get KV Config                          | 101          | :broken_heart: |        |
| Delete KV Config                       | 102          | :broken_heart: |        |
| Get kv list by namespace               | 219          | :broken_heart: |        |
| Query Data Version                     | 322          | :broken_heart: |        |
| Register Broker                        | 103          | :heart:        |        |
| Unregister Broker                      | 104          | :broken_heart: |        |
| Broker Heartbeat                       | 904          | :broken_heart: |        |
| Get broker member_group                | 901          | :broken_heart: |        |
| Get broker cluster info                | 106          | :broken_heart: |        |
| Wipe write perm of boker               | 205          | :broken_heart: |        |
| Add write perm of brober               | 327          | :broken_heart: |        |
| Get all topic list from name server    | 206          | :broken_heart: |        |
| Delete topic in name server            | 216          | :broken_heart: |        |
| Register topic in name server          | 217          | :broken_heart: |        |
| Get topics by cluster                  | 224          | :broken_heart: |        |
| Get system topic list from name server | 304          | :broken_heart: |        |
| Get unit topic list                    | 311          | :broken_heart: |        |
| Get has unit sub topic list            | 312          | :broken_heart: |        |
| Get has unit sub ununit topic list     | 313          | :broken_heart: |        |
| Update name server config              | 318          | :broken_heart: |        |
| Get name server config                 | 318          | :broken_heart: |        |

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

