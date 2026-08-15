# Rocketmq-rust cli

## Overview

Provide some command-line tools to read data from RocketMQ files.

The CLI also contains offline safety commands for Rust Broker upgrades. Stop the
Broker before running either command; both commands require the Store lock.

## Getting Started

### Requirements

1. Stable Rust `1.95.0`, using the pinned repository toolchain.

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

### downgrade-preflight Command

Inspect Rust-owned Store formats before starting an older Rust Broker binary:

```shell
rocketmq-cli-rust downgrade-preflight \
  --target-version 0.9.0 \
  --config /etc/rocketmq-rust/broker.toml \
  --output downgrade-report.json
```

The command exits with code `2` when the downgrade is unsafe. A denied result is
a startup fence, not a warning. Keep the 1.0 tool available until the rollback
window has closed.

### consolidate-multipath Command

Consolidate a stopped Broker's multipath CommitLog into a new single root:

```shell
rocketmq-cli-rust consolidate-multipath \
  --source-root /data-a/commitlog \
  --source-root /data-b/commitlog \
  --target /data-consolidated/commitlog \
  --mapped-file-size 1073741824 \
  --store-root /var/lib/rocketmq-rust/store
```

The destination must not exist. The tool validates segment ownership,
continuity, frame structure, byte equality, and available space before it
atomically publishes the destination. Source files are never modified.
