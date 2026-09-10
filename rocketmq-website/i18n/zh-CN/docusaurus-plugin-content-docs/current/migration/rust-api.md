---
title: "迁移 Rust 应用 API"
---

# 迁移 Rust 应用 API

同时迁移导入、运行时构造和消息完成语义。修复导入可以使代码编译，却仍留下未绑定运行时的消费者或错误的运行时所有权。本文针对当前源码 API，应为所用版本选择匹配源码和依赖，不要混用不同版本的片段。

## 定位归属 crate

| 职责 | 当前归属 | 集成方式 |
| --- | --- | --- |
| 客户端门面、builder、客户端配置和类型化结果 | Cargo 包 `rocketmq-client-rust`，Rust 导入 `rocketmq_client_rust` | 优先使用 crate 根公共导出，如 `ClientConfig`、`ClientRuntime`、`DefaultMQProducer`、`DefaultLitePullConsumer`。 |
| 消息、队列身份及不依赖运行时的领域类型 | `rocketmq-model` / `rocketmq_model` | 使用规范模型类型，不复制结构体，也不保留过时的 common/remoting 导入。 |
| Remoting 请求头、请求/响应码及编解码 | `rocketmq-protocol` / `rocketmq_protocol` | 仅在集成确实实现协议边界时依赖它。 |
| 连接、传输准入与 TLS 执行 | `rocketmq-transport` / `rocketmq_transport` | 业务客户端通常使用门面；直接使用传输层会增加生命周期和协议责任。 |
| 运行时所有者、服务上下文及任务所有权 | `rocketmq-runtime` / `rocketmq_runtime` | 在应用边界建立所有权并传入子上下文。 |
| 遥测所有者和句柄 | `rocketmq-observability` / `rocketmq_observability` | 保持所有者存活；克隆句柄不会成为独立的关闭所有者。 |
| 规范错误与稳定描述符 | `rocketmq-error` / `rocketmq_error` | 匹配类型化描述符和重试提示，不解析面向人的错误字符串。 |

这是职责映射，不是对过去位于 `rocketmq-common` 或 `rocketmq-remoting` 的所有符号进行机械重命名。库开发参见[模块归属](../architecture/module-map.md)。私有 `base`、`producer`、`consumer` 实现路径不能作为公共再导出的稳定替代。

解决导入问题时，可从当前检出生成所选包的公共 API：

```bash
cargo doc -p rocketmq-client-rust --no-deps
```

源码集成可参考[第一条消息 manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/Cargo.toml)，其中使用一致的本地依赖，并为应用客户端设置 `default-features = false`。包默认值与工作区选定 feature 可能不同。只有应用使用对应 API 时才启用 `admin-read` 或 `admin-mutation`，参见 [features](../reference/features-platforms.md)。

## 将隐式构造改为显式所有权

下列内容是前后概念对照，不是待编译代码：

```text
Before: each library constructs a client and assumes background execution exists
After:  application owns RuntimeOwner
        -> creates a service context and shared ClientRuntime
        -> passes Arc<ClientRuntime> into facade builders
        -> stops facades, shuts down the shared client and its runtime owner
```

`ClientRuntime::try_new` 接收服务上下文、`ClientRuntimeConfig` 和遥测句柄。`Arc` 共享一个客户端运行时，不为每个门面创建回退运行时。应在已经运行的异步任务之外创建应用运行时。库应接受所需运行时或上下文，不额外引入顶层运行时或嵌套 `block_on`。

完整的[第一条消息 main](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/examples/first-message/src/main.rs)展示启动、Ctrl+C/有界消费、门面清理和最终关闭报告。保留错误路径清理，清理前提前执行 `?` 可能跳过预期关闭流程。对于有自身关闭工作的遥测导出器，应遵循[运行时所有权](../architecture/runtime.md)和[可观测性所有权](../architecture/errors-observability.md)；该示例使用空操作遥测。

## 当前生产者构造与结果处理

下面的独立函数需要调用方持有活动客户端运行时、已配置 NameServer/Broker，以及 `DocsMigration` 主题。调用时会发送一条真实消息。代码已检查编译，但没有在集群执行此迁移探测。

```rust
use std::sync::Arc;
use rocketmq_client_rust::{ClientRuntime, DefaultMQProducer};
use rocketmq_model::common::message::message_single::Message;
use rocketmq_model::result::SendStatus;

async fn send_once(client: Arc<ClientRuntime>) -> Result<(), Box<dyn std::error::Error>> {
    let mut producer = DefaultMQProducer::builder(client)
        .producer_group("docs_migration_producer")
        .name_server_addr("127.0.0.1:9876")
        .build();
    let outcome = async {
        producer.start().await?;
        let message = Message::builder()
            .topic("DocsMigration")
            .body("migration probe")
            .build()?;
        let result = producer.send_with_timeout(message, 3_000).await?;
        let result = result.ok_or_else(|| std::io::Error::other("send returned no result"))?;
        if result.send_status != SendStatus::SendOk {
            return Err(std::io::Error::other("send did not return SEND_OK").into());
        }
        Ok(())
    }.await;
    producer.shutdown().await;
    outcome
}
```

生产者 builder 直接返回门面，其他 builder 可能返回 `Result`。应遵循具体签名，不要统一增加或删除 `?`。`send_with_timeout` 使用毫秒，返回可选发送结果，需同时检查缺失结果和 `send_status`。本函数发生操作错误时仍执行 `shutdown`。持续工作负载应启动并复用一个生产者，而不是逐条创建。

`SEND_OK` 不能证明恰好一次业务执行。其他发送状态可能表示数据已接受，但未满足要求的刷盘或复制条件。替换旧布尔值或 unwrap 处理时，保留幂等性及预期持久性解释，参见[发送消息](../producer/sending-messages.md)。

## 仅通过可运行 builder 保留 Classic Pull

| 旧构造模式 | 当前迁移方式 |
| --- | --- |
| `DefaultMQPullConsumer::new()` / `default()` / `with_consumer_group(...)` 后执行操作 | 这些方法创建未绑定运行时的兼容值。改为 `builder(client_runtime)` 才能运行 Classic 行为。 |
| 显式队列、选择器、偏移量和批量大小 | 使用 `PullOptions` 保留输入，队列所有权仍由应用负责。 |
| 按返回消息数量隐式累加游标 | 解释 `PullStatus` 和返回的下一偏移量；过滤和间隙会使简单累加失效。 |
| 复用已停止消费者 | 关闭或启动失败后新建门面；关闭不会把对象重置为新建状态。 |

下面的运行时兼容函数保留显式队列和下一偏移量。调用方处理结果并决定何时推进业务进度，函数不会自动提交该进度。

```rust
use std::sync::Arc;
use rocketmq_client_rust::{
    ClientResult, ClientRuntime, DefaultMQPullConsumer,
    MessageSelector, PullOptions, PullResult,
};
use rocketmq_model::common::message::message_queue::MessageQueue;

#[allow(deprecated)]
async fn pull_once(
    runtime: Arc<ClientRuntime>,
    queue: MessageQueue,
    next_offset: i64,
) -> ClientResult<PullResult> {
    let consumer = DefaultMQPullConsumer::builder(runtime)
        .consumer_group("docs_classic_group")
        .name_server_addr("127.0.0.1:9876")
        .build()?;
    let result = async {
        consumer.start().await?;
        let options = PullOptions::new(
            queue, MessageSelector::by_tag("*"), next_offset, 16,
        )?;
        consumer.pull_with_options(options).await
    }.await;
    let shutdown = consumer.shutdown().await;
    let result = result?;
    shutdown?;
    Ok(result)
}
```

弃用警告豁免仅限于该兼容示例，不应以大范围屏蔽警告作为迁移策略。长轮询时保持客户端超时大于 Broker 挂起时间，并保留所选请求预算。[Classic Pull 兼容性](../consumer/classic-pull-compatibility.md)解释分配、状态和生命周期行为。

## 将改用 LitePull 视为行为变更

新轮询应用应在构造 LitePull 时明确提交策略：

```rust
use std::sync::Arc;
use rocketmq_client_rust::{ClientResult, ClientRuntime, DefaultLitePullConsumer};

fn build_polling_consumer(client: Arc<ClientRuntime>) -> ClientResult<DefaultLitePullConsumer> {
    DefaultLitePullConsumer::builder(client)
        .consumer_group("docs_migration_consumer")
        .name_server_addr("127.0.0.1:9876")
        .auto_commit(false)
        .poll_timeout_millis(1_000)
        .build()
}
```

本函数只构造消费者。调用方订阅或分配队列、启动、执行有界轮询、处理业务、更新进度，并始终关闭消费者。完整的 [LitePull 指南](../consumer/pull-consumer.md)及第一条消息应用提供周边生命周期。

| Classic 流程 | LitePull 替代决策 |
| --- | --- |
| 从指定队列和显式偏移量拉取 | 选择带消费者组分配的 `subscribe` 或显式 `assign`；只在有意改变位置时使用 `seek`。 |
| 逐请求选择器 | 消费前配置等价订阅和过滤语义。 |
| 每个结果后持有下一读取游标 | 先处理，再按所选提交策略更新进度。 |
| 应用分配回调 | 停止已撤销队列上的工作，遵循所选分配模式。 |
| 拉取成功即工作完成 | 区分获取消息、业务完成、本地提交、远程提交和 Broker 持久化。 |

当前 `commit_all` 更新本地偏移量存储状态，内部可能只记录各队列错误，不是立即持久化的全队列事务。周期/关闭路径与 Broker 持久化仍相互独立，迁移时不要仅凭方法名称强化完成契约。

交接时记录最后完成的业务范围，停止旧队列所有者，以明确的消费者组和位置设置启动新模式，并检查重放和缺口。相同消费者组不能替代所有权协调。“从首偏移量开始”不一定覆盖消费者组已有存储进度。

## 验证应用变更

1. 将 manifest 和导入改为实际公共归属，编译应用所选 feature 图，包括相关独立 manifest。
2. 检查成功、超时、启动失败和取消时的启动/关闭行为。取消不会撤销远程已接受的请求。
3. 执行应用使用的消息模式及结果处理，随后检查重启/再平衡期间的分配和进度。
4. 在集成中保留业务幂等性和错误描述符，避免通过诊断日志暴露原始原因链或包含密钥的配置。

本文函数是经过编译检查的片段，不是新的端到端迁移测试。已有第一条消息应用提供完整可运行生命周期。服务实现也变化时阅读 [Java 迁移](./java-to-rust.md)，持久状态变化时阅读[升级与回退](../operations/upgrade-rollback.md)。

来源：[客户端公共导出](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/public_api.rs)、[crate 导出](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/lib.rs)、[Classic builder](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/consumer/default_mq_pull_consumer_builder.rs)、[模型](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-model/src)。
