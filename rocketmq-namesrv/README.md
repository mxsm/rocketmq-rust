# The Rust Implementation of Apache RocketMQ Name server

## Overview

Here is the rust implementation of the **name server** for [Apache RocketMQ](https://rocketmq.apache.org/). 

## Feature

Feature list:

- **Not support**: :broken_heart: :x: 

- **Base support**: :heart: :white_check_mark:

- **Perfect support**: :sparkling_heart: :white_check_mark:

| Feature                                | request code | Support                              | remark |
| -------------------------------------- | ------------ |--------------------------------------|--------|
| Put KV Config                          | 100          | :sparkling_heart: :white_check_mark: |        |
| Get KV Config                          | 101          | :sparkling_heart: :white_check_mark: |        |
| Delete KV Config                       | 102          | :sparkling_heart: :white_check_mark: |        |
| Get kv list by namespace               | 219          | :sparkling_heart: :white_check_mark: |        |
| Query Data Version                     | 322          | :sparkling_heart: :white_check_mark: |        |
| Register Broker                        | 103          | :sparkling_heart: :white_check_mark: |        |
| Unregister Broker                      | 104          | :sparkling_heart: :white_check_mark: |        |
| Broker Heartbeat                       | 904          | :sparkling_heart: :white_check_mark: |        |
| Get broker member_group                | 901          | :sparkling_heart: :white_check_mark: |        |
| Get broker cluster info                | 106          | :sparkling_heart: :white_check_mark: |        |
| Wipe write perm of boker               | 205          | :sparkling_heart: :white_check_mark: |        |
| Add write perm of brober               | 327          | :sparkling_heart: :white_check_mark: |        |
| Get all topic list from name server    | 206          | :sparkling_heart: :white_check_mark: |        |
| Delete topic in name server            | 216          | :sparkling_heart: :white_check_mark: |        |
| Register topic in name server          | 217          | :sparkling_heart: :white_check_mark: |        |
| Get topics by cluster                  | 224          | :sparkling_heart: :white_check_mark: |        |
| Get system topic list from name server | 304          | :sparkling_heart: :white_check_mark: |        |
| Get unit topic list                    | 311          | :sparkling_heart: :white_check_mark: |        |
| Get has unit sub topic list            | 312          | :sparkling_heart: :white_check_mark: |        |
| Get has unit sub ununit topic list     | 313          | :sparkling_heart: :white_check_mark: |        |
| Update name server config              | 318          | :broken_heart: :x:                   |        |
| Get name server config                 | 319          | :broken_heart: :x:                   |        |

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

