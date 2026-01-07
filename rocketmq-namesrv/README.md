# The Rust Implementation of Apache RocketMQ Name server

## Overview

Here is the rust implementation of the **name server** for [Apache RocketMQ](https://rocketmq.apache.org/). 

## Getting Started

### Requirements

1. rust toolchain MSRV is 1.75.(stable,nightly)

### Run name server

**Run the following command to see usage：**

```shell
cargo run --bin rocketmq-namesrv-rust -- --help

Apache RocketMQ Name Server - Rust implementation providing lightweight service discovery and routing

Usage: rocketmq-namesrv-rust [OPTIONS]

Options:
  -c, --configFile <FILE>
          Name server config properties file

  -p, --printConfigItem
          Print all config items and exit

      --listenPort <PORT>
          Name server listen port (default: 9876)

      --bindAddress <ADDRESS>
          Name server bind address (default: 0.0.0.0)

      --rocketmqHome <PATH>
          RocketMQ home directory

      --kvConfigPath <PATH>
          KV config file path

  -h, --help
          Print help (see a summary with '-h')

  -V, --version
          Print version
```

### Usage Examples

**Start with default configuration:**

```shell
cargo run --bin rocketmq-namesrv-rust
```

**Start with configuration file:**

```shell
cargo run --bin rocketmq-namesrv-rust -- -c /path/to/namesrv.toml
```

**Print all configuration items:**

```shell
cargo run --bin rocketmq-namesrv-rust -- -p
```

**Override specific parameters:**

```shell
cargo run --bin rocketmq-namesrv-rust -- --listenPort 19876 --bindAddress 127.0.0.1
```

**Combined usage (config file + overrides):**

```shell
cargo run --bin rocketmq-namesrv-rust -- -c config.toml --listenPort 19876 --rocketmqHome /opt/rocketmq
```

### Configuration Priority

Configuration values are applied in the following order (later values override earlier ones):

1. Default values
2. Configuration file (specified with `-c`)
3. Command line arguments

### Testing

Run the feature test script to validate the parameter parsing:

```shell
# Windows
.\scripts\test_namesrv_features.ps1

# Linux/Mac
chmod +x scripts/test_namesrv_features.sh
./scripts/test_namesrv_features.sh
```

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
