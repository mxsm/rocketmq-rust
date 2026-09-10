---
title: "消息过滤"
---

# 消息过滤

过滤为订阅选择消息，不会从主日志删除消息、替代授权，也不能保证所有业务条件均已检查。Tag 适合粗粒度事件分类；受支持的 SQL 表达式适合对消息属性执行条件判断。

## 选择过滤层

| 选择方式 | 输入 | 边界 |
| --- | --- | --- |
| Tag 表达式 | 消息 Tag 与 `TagA || TagB` 或 `*` | 基于简单分类模型的订阅过滤 |
| SQL92 风格选择器 | 命名字符串属性及受支持表达式 | 需要 Broker 属性过滤支持和有效订阅元数据 |
| 应用条件判断 | 已投递消息及业务状态 | 消耗网络和客户端容量，由应用决定是否处理成功 |

同组成员的主题订阅和选择器应一致。修改过滤条件会改变该组认为相关的消息集合，不等于要求重放此前跳过的记录。

## 设置生产者元数据

以下片段在生产者已启动、主题已创建后执行：

```rust
use rocketmq_model::common::message::message_single::Message;

let message = Message::builder()
    .topic("SqlFilterConsumerTestTopic")
    .tags("order_created")
    .raw_property("region", "cn")?
    .raw_property("priority", "3")?
    .body("order event")
    .build()?;
let result = producer.send_with_timeout(message, 3_000).await?;
```

按照[发送消息](../producer/sending-messages.md)检查发送状态。消息中的属性值是字符串，SQL 求值时应用支持的类型转换规则。消息体内的 JSON 字段不会自动变成可过滤属性。

Tag 应作为精确类别使用。`TagA || TagB` 是订阅表达式，不应把整个表达式当作一条消息的 Tag。

## 使用 Tag 或 SQL 选择器订阅

对已注册监听器、尚待启动的 `DefaultMQPushConsumer`：

```rust
consumer.subscribe("OrderEvents", "order_created || order_paid").await?;
```

SQL 使用公开的 `MessageSelector`：

```rust
use rocketmq_client_rust::MessageSelector;

consumer.subscribe_with_selector(
    "SqlFilterConsumerTestTopic",
    Some(MessageSelector::by_sql("region = 'cn' AND priority >= 3")),
).await?;
```

按照 [Push 消费者](./push-consumer.md)补齐启动和关闭。LitePull 具有自己的 `subscribe` 和选择器方法，不要将 Push 签名直接复制到 LitePull 循环。

Broker 的 `enablePropertyFilter` 默认为 false。使用标准 TOML 配置时，将该字段合入已有 `[broker]` 表，并按照正常搭建流程重启对应 Broker：

```toml
[broker]
enablePropertyFilter = true
```

这是配置片段，不是完整 Broker 文件。禁用支持时，非 Tag 拉取路径会拒绝请求；客户端订阅检查还会编译请求的表达式。所有可能服务该订阅的 Broker 都需要兼容支持，启用一个 Broker 不会自动配置其他节点。

## 使用已实现的表达式语言

当前 SQL 运行时支持逻辑 `AND`/`OR`/`NOT`、比较、`IS NULL`/`IS NOT NULL`、`IN`/`NOT IN`、`BETWEEN`/`NOT BETWEEN`，以及 `CONTAINS`、`STARTSWITH`、`ENDSWITH` 等受支持字符串谓词。它是属性条件语言，不是支持连接查询和任意函数的数据库 `SELECT` 语句。

字符串使用单引号，内部单引号用两个连续单引号转义。缺失属性求值为 `NULL`；三值逻辑意味着缺失值并不在所有中间表达式里都等同于 false。Broker 最终匹配要求真布尔结果。应验证缺失、格式错误和边界值，而不只测试一个匹配样本。

求值器在需要时可转换数字形式的字符串。保持生产者属性模式稳定，避免表示方式变化悄悄改变匹配结果。不要假定所有 Java 客户端或其他 Broker 版本都支持相同表达式扩展。

## 理解预过滤与最终求值

ConsumeQueue Tag 编码或可选 Bloom 元数据，可在加载完整属性之前排除候选。Bloom 命中只表示候选，不证明 SQL 最终匹配。相关预过滤元数据缺失或不适用时，当前过滤器会回退到后续求值。

SQL 路径中，Broker 对读取路径提供的消息属性求值已编译表达式。订阅版本与已编译过滤元数据必须一致；过时订阅或缺失过滤元数据可能在消息到达监听器前就导致失败。

需要当前业务状态的决策仍可使用应用过滤。应用有意忽略已投递消息并返回成功，就意味着选择推进该消费者进度。如需以后处理，应定义重放或独立订阅，不能依赖应用代码中的拒绝判断。

## 排查匹配偏差

1. 确认目标集群、主题、组、起始偏移量和生产者发送结果。
2. 使用有界、已授权的工具查看实际 Tag 和属性，不能从消息体推断。
3. SQL 场景检查 `enablePropertyFilter` 和表达式编译错误。
4. 比较同组所有成员的订阅和版本。
5. 在隔离组测试匹配、不匹配、属性缺失三类事件。
6. 分别观察进度与投递数量：扫描被过滤记录可能推进下一位置，而不返回消息。

`rocketmq-example/` 中的 `consumer-tag-filter` 和 `consumer-sql-filter` 分别演示对应选择器。SQL 示例使用 `SqlFilterConsumerTestTopic` 和 `region = 'cn' AND priority >= 3`。应创建对应主题和组，并发送符合条件的属性；运行无关的简单生产者不足以完成验证。

源码依据：[选择器 API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/message_selector.rs)、[SQL 语言](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-filter/README.md)、[Broker 表达式过滤](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/filter/expression_message_filter.rs)、[拉取校验](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/processor/pull_message_processor.rs)、[Broker 配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/config/broker_config.rs)。
