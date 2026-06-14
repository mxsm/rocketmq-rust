# RocketMQ Rust Examples

Standalone examples for RocketMQ Rust client APIs.

This project is not part of the root Cargo workspace. Run all commands from
`rocketmq-example/`.

## Prerequisites

- Rust 1.85.0 or later.
- A running RocketMQ name server and broker.
- Default name server address used by examples: `127.0.0.1:9876`.
- Create topics and subscription groups before running examples. The examples do
  not create broker resources automatically.

Example topic setup with RocketMQ `mqadmin`:

```powershell
bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t BasicSendTestTopic -r 4 -w 4
bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t DelaySendTestTopic -r 4 -w 4 -a +message.type=DELAY
bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t OrderSendTestTopic -r 4 -w 4 -o true
bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t TransactionSendTestTopic -r 4 -w 4 -a +message.type=TRANSACTION
bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t RequestSendTestTopic -r 4 -w 4
bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t BroadcastConsumerTestTopic -r 4 -w 4
bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t TagFilterConsumerTestTopic -r 4 -w 4
bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t SqlFilterConsumerTestTopic -r 4 -w 4
bin\mqadmin.cmd updateTopic -n 127.0.0.1:9876 -c DefaultCluster -t LitePullConsumerTestTopic -r 4 -w 4
```

Create subscription groups as needed:

```powershell
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_broadcast_group -d true
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_orderly_group -o true
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_tag_filter_group
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_sql_filter_group
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_lite_pull_group
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_request_reply_group
```

## Producer Examples

| Mode | Example | Command | Topic | Notes |
| --- | --- | --- | --- | --- |
| Simple send | `producer-simple` | `cargo run --example producer-simple` | `TopicTest` | Basic producer startup and send. |
| Timeout send | `producer-with-timeout` | `cargo run --example producer-with-timeout` | `TopicTest` | Existing timeout and delayed-message sample. |
| Basic send APIs | `producer-basic-send` | `cargo run --example producer-basic-send` | `BasicSendTestTopic` | Sync, timeout, callback, callback timeout, one-way. |
| Batch send | `producer-batch-send` | `cargo run --example producer-batch-send` | `BatchSendTestTopic` | Batch send variants. |
| Send to queue | `producer-send-to-queue` | `cargo run --example producer-send-to-queue` | `QueueSendTestTopic` | Targets a selected message queue. |
| Queue selector | `producer-send-with-selector` | `cargo run --example producer-send-with-selector` | `SelectorSendTestTopic` | Uses custom queue selector. |
| Advanced selector | `producer-send-with-selector-advanced` | `cargo run --example producer-send-with-selector-advanced` | `AdvancedSelectorTestTopic` | Hash, round-robin, random, weighted examples. |
| Delay send | `producer-delay-send` | `cargo run --example producer-delay-send` | `DelaySendTestTopic` | `delay_level`, `delay_secs`, `delay_millis`, `deliver_time_ms`. |
| Ordered send | `producer-order-send` | `cargo run --example producer-order-send` | `OrderSendTestTopic` | Same order id is routed to the same queue. |
| Transaction send | `producer-transaction-send` | `cargo run --example producer-transaction-send` | `TransactionSendTestTopic` | Commit, rollback, and unknown local states. |
| Request/reply | `producer-request-send` | `cargo run --example producer-request-send` | `RequestSendTestTopic` | Start `consumer-request-reply` first. |
| Request/reply callback | `producer-request-callback-send` | `cargo run --example producer-request-callback-send` | `RequestSendTestTopic` | Start `consumer-request-reply` first. |

## Consumer Examples

| Mode | Example | Command | Topic | Notes |
| --- | --- | --- | --- | --- |
| Cluster push | `consumer-cluster` | `cargo run --example consumer-cluster` | `TopicTest` | Load-balanced push consumption. |
| Pop consumer | `pop-consumer` | `cargo run --example pop-consumer` | `TopicTest` | Pop request mode example. |
| Broadcast push | `consumer-broadcast` | `cargo run --example consumer-broadcast` | `BroadcastConsumerTestTopic` | Every consumer instance receives every message; group must enable broadcast consumption. |
| Orderly push | `consumer-orderly` | `cargo run --example consumer-orderly` | `OrderSendTestTopic` | Uses `MessageListenerOrderly`. |
| Tag filter push | `consumer-tag-filter` | `cargo run --example consumer-tag-filter` | `TagFilterConsumerTestTopic` | Uses `MessageSelector::by_tag("TagA || TagB")`. |
| SQL92 filter push | `consumer-sql-filter` | `cargo run --example consumer-sql-filter` | `SqlFilterConsumerTestTopic` | Uses `MessageSelector::by_sql`; broker must support SQL filtering. |
| Lite pull | `consumer-lite-pull` | `cargo run --example consumer-lite-pull` | `LitePullConsumerTestTopic` | Polls messages and commits offsets manually. |
| Request/reply responder | `consumer-request-reply` | `cargo run --example consumer-request-reply` | `RequestSendTestTopic` | Sends reply messages for request producer examples. |

## Validation

Build an individual example:

```powershell
cargo build --example producer-delay-send
```

Required validation after changing this project:

```powershell
cargo fmt --all
cargo clippy --all-targets -- -D warnings
```

## Project Structure

```text
rocketmq-example/
|-- examples/
|   |-- consumer/
|   |-- interop/
|   `-- producer/
|-- Cargo.toml
|-- README.md
`-- README-zh_cn.md
```

## Notes

- Examples use local path dependencies to the parent RocketMQ Rust crates.
- Constants such as topic, group, and name server address are defined in each
  example source file.
- Producer examples exit after sending messages. Consumer examples run until
  interrupted with Ctrl+C.
