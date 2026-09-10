---
title: "客户端配置参考"
---

# 客户端配置参考

客户端配置由 Rust 应用管理。库不会自动加载应用的 TOML 文件，也不会读取任意 `ROCKETMQ_*` 环境变量。应用应显式读取设置，构建客户端配置，并向各客户端外观对象注入应用持有的 `Arc<ClientRuntime>`。[首条消息应用](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/src/main.rs)包含运行时所有权、启动、消息收发与清理的完整流程。

## 配置层次与生效时机

`ClientConfig` 保存公共的发现、连接、心跳与偏移量持久化设置。`ClientOptions` 将它与强类型 NameServer 发现配置组合。Producer、Push Consumer 和 LitePull builder 再补充各自的操作参数。这些设置应在 `start()` 前完成；builder 只创建对象，不会启动连接，也不会在 Broker 上建立订阅。

Producer 和 Push builder 接受 `client_config`、`client_options` 和 `nameserver_discovery`。调用 `client_config` 会替换公共配置，并清除此前附加的强类型发现配置，因此 builder 调用顺序会影响结果。其 `name_server_addr` 方法修改当前持有的公共配置。LitePull 接受 `client_options`，并在构建时应用显式设置的外观对象参数；即使传入公共选项，它仍会应用自身的 `use_tls` 值。因此，通过公共选项启用 TLS 时，也应在 LitePull 上设置 `use_tls(true)`。

`ClientConfig::builder().build()?` 会校验公共设置。Producer 和 Push 的 `build()` 直接返回外观对象，启动时再执行进一步检查。LitePull 的 `build()?` 返回结果，且要求提供消费者组；构造成功仍不能证明路由、凭据、订阅或 Broker 兼容性正确。多个外观对象可能已共享运行时基础设施，不能认为修改旧配置值就会动态更新这些基础设施。变更启动设置时，应受控关闭并重新创建受影响的客户端。

## 公共配置

| `ClientConfig` builder 方法 | 类型 / 默认值 | 单位与含义 |
| --- | --- | --- |
| `namesrv_addr` | string 输入 / 见下文地址发现默认规则 | 显式指定 NameServer 端点；多个静态地址用分号分隔。 |
| `poll_name_server_interval` | u32 / `30000` | 路由轮询间隔，单位为毫秒；校验范围 `10000..=600000`。 |
| `heartbeat_broker_interval` | u32 / `30000` | Broker 心跳间隔，单位为毫秒；校验范围 `10000..=600000`。 |
| `persist_consumer_offset_interval` | u32 / `5000` | 偏移量持久化工作间隔，单位为毫秒；校验范围 `1000..=60000`。它不是 Broker 的磁盘持久化间隔。 |
| `mq_client_api_timeout` | u64 / `3000` | 公共 API 超时，单位为毫秒；校验范围 `100..=60000`。具体 API 可以使用独立超时。 |
| `enable_tls` | bool / `false` | 请求建立 TLS 连接；还需要传输层构建包含 TLS 支持，并与服务端配置一致。 |

构造公共配置时若未显式指定地址，会依次检查进程环境键 `rocketmq.namesrv.addr`、旧式 `rocketmq.rocketmq-namesrv.addr`、`NAMESRV_ADDR`。使用旧拼写会产生弃用警告。选择依据是变量是否存在；高优先级变量为空不会继续回退。三者均不存在时，地址字段保持未设置，不会隐式采用 `localhost:9876`。动态发现应使用强类型 `NameServerDiscoveryConfig` 路径，不应在静态地址列表中自行约定 URL 语法。

以下片段位于已持有 `client: Arc<ClientRuntime>`、且返回兼容错误结果的函数中。它仅构造生产者；生命周期处理应参考完整示例补充。

```rust
use rocketmq_client_rust::{ClientConfig, DefaultMQProducer};

let common = ClientConfig::builder()
    .namesrv_addr("127.0.0.1:9876")
    .poll_name_server_interval(30_000)
    .heartbeat_broker_interval(30_000)
    .persist_consumer_offset_interval(5_000)
    .build()?;

let mut producer = DefaultMQProducer::builder(client.clone())
    .client_config(common)
    .producer_group("docs_reference_producer")
    .send_msg_timeout(3_000)
    .retry_times_when_send_failed(2)
    .build();
```

## Producer 配置

以下默认值来自 `ProducerConfig`。builder 接受某个值，并不代表目标 Broker 接受相同大小的消息或支持所请求的行为。

| Producer builder 方法 | 类型 / 默认值 | 含义 |
| --- | --- | --- |
| `producer_group` | string / 初始为空 | 启动前设置有实际意义的生产者组，与消费者组无关。 |
| `send_msg_timeout` | u32 / `3000` ms | 默认发送截止时间；显式的单次调用超时可以为该操作选择其他截止时间。 |
| `send_msg_max_timeout_per_request` | u32 输入 / 未设置 | 可选的单次请求超时上限，单位为毫秒；未设置表示没有附加上限，并非整个发送操作无限等待。 |
| `retry_times_when_send_failed` | u32 / `2` | 同步发送的额外重试次数；每次尝试仍消耗整个操作的时间预算。 |
| `retry_times_when_send_async_failed` | u32 / `2` | 异步发送的额外重试次数；仍需处理回调与最终发送状态。 |
| `retry_another_broker_when_not_store_ok` | bool / `false` | 存储状态非 OK 时是否改向其他 Broker 重试。重试可能重复已经存储的消息。 |
| `max_message_size` | u32 / `4194304` bytes | 客户端大小限制，应与 Broker 限制及所选 API 编码协调。 |
| `compress_msg_body_over_howmuch` | u32 / `4096` bytes | 消息体压缩阈值；接收方必须理解所用压缩格式。 |
| `default_topic_queue_nums` | u32 / `4` | 生产者自动创建主题请求中的队列数设置，与 Broker 默认队列数及已有主题不同。 |
| `auto_batch` | bool / `false` | 选择自动批量发送；批量设置不是普通单条消息的保证。 |
| `batch_max_delay_ms`、`batch_max_bytes`、`total_batch_max_bytes` | u32 / u64 / u64 输入；未设置 | 可选的累积器等待时间和字节数限制，应结合批量路径配置；未设置并不等于零上限。 |
| `enable_backpressure_for_async_mode` | bool / `false` | 启用异步发送接纳限制。 |
| `back_pressure_for_async_send_num` | u32 / `1024` | 待完成异步发送的数量预算。 |
| `back_pressure_for_async_send_size` | u32 / `104857600` bytes | 待完成异步发送的字节数预算，与数量预算独立。 |

应用应选择一个包含发现、连接、发送和重试的总截止时间。增加重试次数不能延长已耗尽的时间预算。除外层 Rust 结果外，还应检查 `SendResult.send_status`；超时不能证明 Broker 未存储消息。具体见[发送方式](../producer/sending-messages.md)及[投递与重试](../guides/delivery-and-retry.md)。

## Push Consumer 配置

| Push builder 方法 | 类型 / 默认值 | 含义与启动约束 |
| --- | --- | --- |
| `consumer_group` | string 输入 | 显式选择消费者组，并保持组内实例的订阅一致。 |
| `message_model` | enum / `Clustering` | 集群或广播语义；变更会影响队列分配与偏移量所有权。 |
| `consume_from_where` | enum / `ConsumeFromLastOffset` | 无可用已提交偏移量时的初始位置，不会重置已有消费者组的进度。 |
| `consume_thread_min` / `consume_thread_max` | u32 / `20` / `64` | 消费并发配置；各自范围为 `1..=1000`，且最小值不能超过最大值。这不是 JVM 线程池配置规则。 |
| `pull_batch_size` | u32 / `32` | 每次拉取请求的消息数，范围 `1..=1024`。 |
| `consume_message_batch_max_size` | u32 / `1` | 监听器批次大小，范围 `1..=1024`，与网络拉取批次不同。 |
| `pull_interval` | u64 / `0` ms | 拉取间隔，范围 `0..=65535`。零值不会绕过流量控制。 |
| `pull_threshold_for_queue` | u32 / `1000` 条消息 | 每队列缓存消息数阈值，范围 `1..=65535`。 |
| `pull_threshold_for_topic` | i32 / `-1` | `-1` 表示不启用主题级数量覆盖，否则范围为 `1..=6553500` 条消息。 |
| `max_reconsume_times` | i32 / `-1` | 遵循所选消费模式的重试约定，不能将 `-1` 理解为统一的固定重试次数。 |
| `consume_timeout` | u64 / `15` 分钟 | 消费超时设置，单位不是毫秒，也不是拉取 RPC 截止时间。 |

应注册适当的监听器、设置订阅，再启动消费者。监听器成功返回必须晚于业务副作用完成；如果仅将处理工作加入调度队列就返回成功，可能过早推进进度。顺序消费、并发消费和 POP 消费具有不同的重试与确认行为，分别见 [Push Consumer](../consumer/push-consumer.md)、[顺序消息](../guides/ordered-messages.md)和 [POP](../consumer/pop.md)。

## LitePull 配置

| LitePull builder 方法 | 类型 / 默认值 | 含义 |
| --- | --- | --- |
| `consumer_group` | string / 构建时必填 | 消费者组身份。 |
| `pull_batch_size` | i32 / `10` | 每次拉取请求的消息数。 |
| `pull_thread_nums` | usize / `20` | 拉取执行配置，与业务处理并发度不同。 |
| `pull_threshold_for_queue` | i64 / `1000` 条消息 | 每队列缓存消息数阈值。 |
| `pull_threshold_for_all` | i64 / `10000` 条消息 | 总缓存消息数阈值。 |
| `poll_timeout_millis` | u64 / `5000` ms | poll 等待本地可用消息的时长。 |
| `broker_suspend_max_time_millis` | u64 / `20000` ms | 请求 Broker 挂起的时间预算。 |
| `consumer_timeout_millis_when_suspend` | u64 / `30000` ms | 挂起拉取的客户端超时，应在 Broker 挂起时间之外留出余量。 |
| `consumer_pull_timeout_millis` | u64 / `10000` ms | 拉取 RPC 超时设置，与本地 poll 等待不同。 |
| `auto_commit` | bool / `true` | 自动提交偏移量行为。需要明确关联业务完成时，应有意识地选择手动提交。 |
| `auto_commit_interval_millis` | u64 / `5000` ms | setter 仅接受至少 `1000` 的值；更小的值会被忽略，并保留此前设置。 |
| `topic_metadata_check_interval_millis` | u64 / `30000` ms | 主题元数据检查间隔。 |

例如，复用同一个由应用持有的 `client`：

```rust
use rocketmq_client_rust::DefaultLitePullConsumer;

let consumer = DefaultLitePullConsumer::builder(client.clone())
    .consumer_group("docs_reference_consumer")
    .name_server_addr("127.0.0.1:9876")
    .pull_batch_size(16)
    .poll_timeout_millis(2_000)
    .auto_commit(false)
    .build()?;
```

构造之后仍需执行订阅或显式分配、启动、poll、业务处理与偏移量管理。`commit_all()` 更新本地偏移量状态，远端提交与 Broker 持久化是后续独立步骤。即使外层操作返回成功，单队列持久化错误仍可能只记录到日志。完整的完成语义见 [LitePull 消费](../consumer/pull-consumer.md)，不要在尚未完成的异步业务处理周围启用自动提交。

## TLS 与应用自定义设置

通过公共 builder 配置对端验证与可选客户端身份：

```rust
let secure_common = ClientConfig::builder()
    .namesrv_addr("namesrv.internal:9876")
    .enable_tls(true)
    .tls_test_mode_enable(false)
    .tls_client_auth_server(true)
    .tls_client_trust_cert_path("/etc/rocketmq/ca.pem")
    .tls_client_cert_path("/etc/rocketmq/client.pem")
    .tls_client_key_path("/etc/rocketmq/client.key")
    .build()?;
```

客户端证书与私钥用于 mTLS；仅验证服务端身份时应同时省略两项。需要将结果配置应用到实际外观对象，单独构造一个未使用的 `secure_common` 不会改变任何连接。TLS 支持属于传输层依赖，而不是客户端中一个并不存在的 `tls` feature。测试模式或禁用对端验证会改变信任行为，不应将本地实验设置直接复制到安全部署中。

应用配置文件及 `ROCKETMQ_PRODUCER_GROUP` 等变量，只有在应用自行读取时才有效。集成测试使用的 `ROCKETMQ_ENABLE_TLS_SMOKE` 等变量属于测试工具输入，并非通用客户端配置加载器。示例配置输出与诊断信息中不应包含凭据。

来源：[公共默认值](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/base/client_config.rs)、[公共校验](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/base/client_config_validation.rs)、[Producer 默认值](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/producer/default_mq_producer.rs)、[Push 默认值](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_push_consumer.rs)和 [LitePull builder](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_lite_pull_consumer_builder.rs)。
