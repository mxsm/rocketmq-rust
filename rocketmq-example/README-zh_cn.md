# RocketMQ Rust 示例

这是 RocketMQ Rust client API 的独立示例项目。

该目录不是根 Cargo workspace 的一部分。所有命令都应在 `rocketmq-example/`
目录下执行。

## 前置条件

- Stable Rust 1.95.0，使用仓库固定的工具链。
- 已启动 RocketMQ Namesrv 和 Broker。
- 常规示例默认 Namesrv 地址为 `127.0.0.1:9876`；互操作示例允许环境变量覆盖。
- 运行示例前先创建 topic 和 consumer group。示例本身不自动创建 broker 资源。

下面命令在 RocketMQ 发行包目录执行，或将 `bin\mqadmin.cmd` 替换为其绝对路径。它们是部分 Topic 的配置示例；运行其他示例前，也需创建表中对应的 Topic 和消费组。

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

按需创建消费组：

```powershell
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_broadcast_group -d true
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_orderly_group -o true
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_tag_filter_group
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_sql_filter_group
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_lite_pull_group
bin\mqadmin.cmd updateSubGroup -n 127.0.0.1:9876 -c DefaultCluster -g consumer_request_reply_group
```

## Producer 示例

| 模式 | 示例 | 命令 | Topic | 说明 |
| --- | --- | --- | --- | --- |
| 简单发送 | `producer-simple` | `cargo run --example producer-simple` | `TopicTest` | 基础 producer 启动和发送。 |
| 超时发送 | `producer-with-timeout` | `cargo run --example producer-with-timeout` | `TopicTest` | 已有超时和延迟消息示例。 |
| 基础发送 API | `producer-basic-send` | `cargo run --example producer-basic-send` | `BasicSendTestTopic` | 同步、超时、callback、callback 超时、one-way。 |
| 批量发送 | `producer-batch-send` | `cargo run --example producer-batch-send` | `BatchSendTestTopic` | 批量发送各种模式。 |
| 指定队列发送 | `producer-send-to-queue` | `cargo run --example producer-send-to-queue` | `QueueSendTestTopic` | 发送到指定 message queue。 |
| 队列选择器 | `producer-send-with-selector` | `cargo run --example producer-send-with-selector` | `SelectorSendTestTopic` | 自定义 queue selector。 |
| 高级选择器 | `producer-send-with-selector-advanced` | `cargo run --example producer-send-with-selector-advanced` | `AdvancedSelectorTestTopic` | hash、round-robin、random、weighted 示例。 |
| 延迟发送 | `producer-delay-send` | `cargo run --example producer-delay-send` | `DelaySendTestTopic` | `delay_level`、`delay_secs`、`delay_millis`、`deliver_time_ms`。 |
| 顺序发送 | `producer-order-send` | `cargo run --example producer-order-send` | `OrderSendTestTopic` | 同一个 order id 路由到同一个队列。 |
| 事务发送 | `producer-transaction-send` | `cargo run --example producer-transaction-send` | `TransactionSendTestTopic` | commit、rollback、unknown 本地事务状态。 |
| Request/reply | `producer-request-send` | `cargo run --example producer-request-send` | `RequestSendTestTopic` | 先启动 `consumer-request-reply`。 |
| Request/reply callback | `producer-request-callback-send` | `cargo run --example producer-request-callback-send` | `RequestSendTestTopic` | 先启动 `consumer-request-reply`。 |

## Consumer 示例

| 模式 | 示例 | 命令 | Topic | 说明 |
| --- | --- | --- | --- | --- |
| 集群 Push | `consumer-cluster` | `cargo run --example consumer-cluster` | `TopicTest` | 负载均衡消费。 |
| Pop 消费 | `pop-consumer` | `cargo run --example pop-consumer` | `TopicTest` | Pop request mode 示例。 |
| 广播 Push | `consumer-broadcast` | `cargo run --example consumer-broadcast` | `BroadcastConsumerTestTopic` | 每个 consumer 实例都会收到消息；消费组必须开启广播消费。 |
| 顺序 Push | `consumer-orderly` | `cargo run --example consumer-orderly` | `OrderSendTestTopic` | 使用 `MessageListenerOrderly`。 |
| Tag 过滤 Push | `consumer-tag-filter` | `cargo run --example consumer-tag-filter` | `TagFilterConsumerTestTopic` | 使用 `MessageSelector::by_tag("TagA || TagB")`。 |
| SQL92 过滤 Push | `consumer-sql-filter` | `cargo run --example consumer-sql-filter` | `SqlFilterConsumerTestTopic` | 使用 `MessageSelector::by_sql`，Broker 需要支持 SQL 过滤。 |
| Lite Pull | `consumer-lite-pull` | `cargo run --example consumer-lite-pull` | `LitePullConsumerTestTopic` | 主动 poll 消息并手动提交 offset。 |
| Request/reply responder | `consumer-request-reply` | `cargo run --example consumer-request-reply` | `RequestSendTestTopic` | 为 request producer 示例发送 reply 消息。 |

## 互操作与运行时行为

`cargo run --example request-code-smoke` 发送并消费有限数量的消息，校验收到的消息体，
成功时输出 `INTEROP_OK`。默认 Topic 为 `RustExampleRequestCodeSmokeTopic`，
默认发送 1 条消息；`ROCKETMQ_NAMESRV_ADDR`、`ROCKETMQ_TOPIC`、
`ROCKETMQ_CONSUMER_GROUP` 等变量可覆盖配置，详见
[源码](examples/interop/request_code_smoke.rs)。

`pop-consumer` 在消费前通过 Admin API 调用 `set_consumer_request_mode`，
把对应 Topic/Group 切换为 POP；需要相应管理权限，退出时不会自动恢复请求模式。
这与仅收发消息的示例不同。

公共 [support 模块](examples/support/mod.rs) 创建 RuntimeOwner、遥测 guard 和共享
ClientRuntime，在示例返回后关闭客户端运行时、RuntimeOwner 和遥测。

## 验证

构建单个示例：

```powershell
cargo build --example producer-delay-send
```

按修改范围选择格式和静态检查：

```powershell
cargo fmt --all -- --check
cargo clippy --example producer-delay-send -- -D warnings
```

## 可观测性配置

[broker_observability.yaml](examples/broker_observability.yaml) 是 Broker 的
结构化配置片段。测试 OTLP、Prometheus、跟踪或日志时，将相关字段合并到完整 Broker
配置，并为 Broker 构建启用对应的遥测 exporter features。

## 项目结构

```text
rocketmq-example/
|-- examples/
|   |-- consumer/
|   |-- interop/
|   |-- producer/
|   `-- broker_observability.yaml
|-- Cargo.toml
|-- README.md
`-- README-zh_cn.md
```

## 注意事项

- 示例通过本地 path dependency 引用父目录中的 RocketMQ Rust crates。
- topic、group、Namesrv 地址等常量在每个示例源码中定义。
- Producer 示例发送完成后退出；Consumer 示例持续运行，按 Ctrl+C 退出。
