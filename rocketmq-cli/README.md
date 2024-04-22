# Rocketmq-rust cli

## Overview

Provide some command-line tools to read data from RocketMQ files.

## Getting Started

### Requirements

1. rust toolchain MSRV is 1.75.(stable,nightly)

## Run rocketmq-rust cli

**Run the following command to see usage：**

- **windows platform**

  ```cmd
  cargo run --bin rocketmq-cli-rust -- --help
  
  RocketMQ CLI(Rust)
  
  Usage: rocketmq-cli-rust.exe <COMMAND>
  
  Commands:
    read-message-log  read message log file
    help              Print this message or the help of the given subcommand(s)
  
  Options:
    -h, --help     Print help
    -V, --version  Print version
    
  
  cargo run --bin rocketmq-cli-rust help read-message-log
  read message log file
  
  Usage: rocketmq-cli-rust.exe read-message-log [OPTIONS]
  
  Options:
    -c, --config <FILE>  message log file path
    -f, --from <FROM>    The number of data started to be read, default to read from the beginning. start from 0
    -t, --to <TO>        The position of the data for ending the reading, defaults to reading until the end of the file.
    -h, --help           Print help
    -V, --version        Print version
  ```

- **Linux platform**

  ```shell
  $ cargo run --bin rocketmq-cli-rust -- --help
  
  RocketMQ CLI(Rust)
  
  Usage: rocketmq-cli-rust <COMMAND>
  
  Commands:
    read-message-log  read message log file
    help              Print this message or the help of the given subcommand(s)
  
  Options:
    -h, --help     Print help
    -V, --version  Print version
    
  
  $ cargo run --bin rocketmq-cli-rust help read-message-log
  read message log file
  
  Usage: rocketmq-cli-rust read-message-log [OPTIONS]
  
  Options:
    -c, --config <FILE>  message log file path
    -f, --from <FROM>    The number of data started to be read, default to read from the beginning. start from 0
    -t, --to <TO>        The position of the data for ending the reading, defaults to reading until the end of the file.
    -h, --help           Print help
    -V, --version        Print version$ cargo run --bin rocketmq-namesrv-rust -- --help
  
  ```

### read-message-log Command

example for **`read-message-log`** (Linux platform)

```bash
$ ./rocketmq-cli-rust read-message-log -c /mnt/c/Users/ljbmx/store/commitlog/00000000000000000000 -f 0 -t 2
file size: 1073741824B
+----------------------------------+
| message_id                       |
+----------------------------------+
| AC16B00100002A9F0000000000000000 |
+----------------------------------+
| AC16B00100002A9F000000000000032A |
+----------------------------------+
```

