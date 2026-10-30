---
title: "延迟投递与召回"
---

# 延迟投递与召回

延迟消息在配置的延迟或时间戳之后具备投递资格。具备资格与消费者实际处理分开，分派积压、消费者可用性和业务工作都会增加延迟。定时器不提供实时截止保证。

## 选择一种调度形式

| Message builder 选项 | 含义 | 服务端条件 |
| --- | --- | --- |
| `delay_level(n)` | 配置延迟级别表中的索引 | Broker、Store 的级别调度器及当前 `messageDelayLevel` 映射 |
| `delay_secs(n)` | 以秒为单位的相对延迟 | 定时请求规范化与所选定时器引擎 |
| `delay_millis(n)` | 以毫秒为单位的相对延迟 | 定时请求规范化与配置精度 |
| `deliver_time_ms(timestamp)` | Unix 纪元绝对毫秒时间 | 时钟解释、未来时间戳与接纳范围 |

每条消息使用一种调度形式。当前定时属性优先级为秒、毫秒、绝对投递时间；依赖此优先级会使混合属性消息更难理解。不要混用延迟级别与定时属性。

默认级别表前三项为 1 秒、5 秒、10 秒，因此该表下级别 3 为 10 秒；映射可配置。当前默认定时精度为 1,000 ms，标准最大延迟为三天；兼容精度集合为 100、200、500、1,000 ms。这些是源码配置默认值，不是所有部署的统一保证。

## 发送并观察定时消息

采用[发送消息](../producer/sending-messages.md)中的完整生产者生命周期。创建 `DelaySendTestTopic` 和订阅该主题的消费者。以下片段在生产者启动后执行：

```rust
let message = Message::builder()
    .topic("DelaySendTestTopic")
    .key("reminder-1001")
    .delay_secs(30)
    .body("reminder")
    .build()?;
let send_result = producer.send_with_timeout(message, 3_000).await?;
```

将发送视为已确认前，应检查可选结果及 `send_status`。记录业务事件 ID，以及结果存在时返回的 `recall_handle`。消息键本身不是召回句柄。

绝对时间戳应使用经过溢出检查的运算计算 Unix 纪元毫秒，并同步参与主机的时钟。规范化拒绝格式错误、溢出、过去时间戳和超出配置范围的请求。协议使用毫秒单位，不意味着毫秒级投递精度。

标准 TOML 中相关键放在已有 `[store]` 表：

```toml
[store]
timerWheelEnable = true
timerPrecisionMs = 1000
timerMaxDelaySec = 259200
```

这是片段，不是完整 Broker 配置。配置定时器后，还需要所选后端及服务生命周期处于活动状态。ExtendedTimeline 模式具有独立的 RocksDB、feature 和接纳时间范围条件，不能根据标准定时示例推断它已经可用。

## 使用返回句柄召回

召回面向到期前符合条件的定时消息。当前句柄生成路径不会为普通延迟级别消息提供相同召回句柄。应要求真实的 `SendResult.recall_handle`，不要根据消息 ID 虚构句柄。

以下片段假设 `result` 是已经成功检查的 `SendResult`，且生产者仍处于启动状态：

```rust
if let Some(handle) = result.recall_handle.as_deref() {
    let recalled_message_id = producer
        .recall_message("DelaySendTestTopic", handle)
        .await?;
}
```

客户端校验生命周期、主题和非空可解码句柄，然后解析对应 Broker。该 facade 不支持重试或死信主题。Broker 检查召回开关、角色和可用性、写权限策略、主题存在性、句柄中的主题和 Broker 匹配，以及剩余时间窗口。

Broker 的 `recallMessageEnable` 当前默认为 true，但仍适用实际部署的授权。剩余时间必须为正，且严格小于所选最大召回范围。看起来有效的句柄不是绕过主题权限的凭据。

## 根据引擎解释召回结果

标准路径通过追加定时删除标记召回。响应说明标记写入路径的结果，不是同步扫描后证明没有任何消费者见过原消息。投递与删除处理可能竞争。

ExtendedTimeline 模式中，当前处理器使用类型化取消操作。`Cancelled` 和 `AlreadyCancelled` 映射成功；`TooLate`、`NotFound` 映射非法操作；`Retry` 映射服务不可用；`Quarantined`、`Unsupported` 映射系统错误。这种显式状态结果与标准标记路径不同。

`timerStoreMode = "native"` 时，Broker 先在 native 引擎中登记召回，再写入删除标记，因此两者之间开始的投递也能看到这次召回。召回结果同样是类型化的：只有定时消息尚未到期时召回才会成功；对已到期定时消息的召回会被拒绝，按非法操作返回，且不写入标记。如果标记写入失败，登记会被撤回，召回返回系统错误。

任何模式的召回都不会撤销消费者已经完成的外部副作用。业务取消状态应保持权威，使迟到提醒可以按应用规则识别、忽略或补偿。

## 失败与恢复决策

| 观察结果 | 解释 |
| --- | --- |
| 发送超时 | 定时接纳结果可能不确定，创建第二份业务提醒前先协调状态 |
| 发送结果没有召回句柄 | 不能仅从延迟设置推断支持召回 |
| 召回报告主题或 Broker 不匹配 | 检查原始句柄与目标，不要修改句柄字段 |
| 召回过晚 | 消息可能已具备投递资格，使用应用取消策略 |
| 召回超时 | 远端取消或标记可能成功，协调时保留原始标识 |
| 实际投递晚于请求时间 | 分别检查定时分派、时钟、存储及消费者积压 |

批次校验和事务生产者在各自路径拒绝延迟或定时组合。延迟发送也不会与后来发往同一业务主题的普通消息建立顺序关系。

## 定时文件与无法投递的定时消息

标准定时引擎默认把索引保存在 Apache RocketMQ 5.x 的 `timerlog`、`timerwheel` 和 `config/timercheck` 文件中（`timerFileFormat = "java"`）。因此 Rust Broker 与 Java 5.x Broker 可以接管同一个存储目录，而不丢失待投递的定时消息。设置 `timerFileFormat = "rust_v2"` 则改用分段的 Rust 文件。修改该设置后，仍保存在原格式中的定时消息会继续从原格式投递。

`[store]` 中的两个键决定无法投递的定时消息如何处理。它们沿用 Apache RocketMQ 中的同名开关，但只要消息体还在 CommitLog 中，Rust 的默认值就不会丢弃定时消息：

| 键 | Rust 默认值 | Java 默认值 | 作用 |
| --- | --- | --- | --- |
| `timerSkipUnknownError` | `false` | `false` | 设为 `true` 时，隔离并跳过这类定时消息：记录无法读取或路由，或写入被 Store 拒绝。为 `false` 时，该消息被隔离，排在它之后的定时消息等待运维处理。 |
| `timerEnableRetryUntilSuccess` | `true` | `false` | 设为 `false` 时，因临时原因写入失败三次的投递会被丢弃并记录日志，与 Java 的默认行为一致。为 `true` 时，写入会一直重试直到成功。 |

如果 CommitLog 清理（例如磁盘空间不足时）已删除某条定时消息或召回的消息体，无论这两个键如何设置，都会跳过它并记录告警日志，与 Java 一致：消息体已无法恢复，继续等待只会拖住之后所有的定时消息。被跳过的召回不再取消它的目标定时消息。

即使 `timerSkipUnknownError = false`，Java 也会丢弃无法读取的消息体和被拒绝的消息，而 Rust 会保留它们。被隔离的定时消息记录在存储根目录下的 `timer-v2/quarantine.jsonl` 中。

## 使用 native 引擎投递长延迟消息

`timerStoreMode = "native"` 时，延迟不超过 `timerNative.maxDelayDays`（默认 400 天）的定时消息由 native 引擎接纳。该引擎是存储根目录下 `timer-native/` 中的分层时间桶日志。在中期范围内到期的定时消息直接引用 CommitLog；更晚到期的会被复制一次，因此在到期之前，CommitLog 清理不会删除它。到期时间与标准引擎一样按 `timerPrecisionMs` 取整，并且该引擎要求 `timerWheelEnable = true`。

```toml
[store]
timerWheelEnable = true
timerStoreMode = "native"

[store.timerNative]
maxDelayDays = 400
```

| `[store.timerNative]` 下的键 | 默认值 | 含义 |
| --- | --- | --- |
| `maxDelayDays` | `400` | 可接纳的最长延迟 |
| `laneCount` | `16` | 投递通道数；`timer-native/` 创建后不可更改 |
| `nearWindowSec` | `600` | 在此秒数内到期的定时消息保存在内存中 |
| `nearWheelMemoryBytes` | 256 MiB | 这些定时消息可使用的内存 |
| `midHorizonHours` | `48` | 晚于此范围到期的定时消息会被复制；引擎实际最多使用 `fileReservedTime` 减去六小时 |
| `payloadSegmentBytes` | 256 MiB | 每个定时消息副本段的大小 |
| `checkpointIntervalMs` | `5000` | 已索引的定时消息与投递进度落盘的间隔 |
| `deliveryBatchMessages` | `512` | 每个通道每批投递的到期定时消息数 |
| `deliveryInflightWrites` | `32` | 每批同时进行中的写入数 |

每条定时消息都由接纳它的引擎投递，因此 `timerStoreMode` 可以双向切换：

- 切换到 `native` 后，标准引擎继续投递切换前接纳的定时消息。
- 从 `native` 切换到其他模式后，native 引擎继续投递它持有的定时消息。正常关闭时如果发现它已不再持有任何定时消息，存储就不再运行它；之后再切换回 `native` 会重新启动它。
- 无论当前是哪种模式，召回都会送达接纳该定时消息的引擎。只有 native 模式会返回类型化结果。
- 仍持有 native 定时消息的存储不能以 ExtendedTimeline 模式加载。

Broker 运行时信息用三项描述 native 引擎：`timerNativeRole`（`active` 或 `draining`）、`timerNativeMetrics` 和 `timerNativePipelineMetrics`。`timerNativeMetrics` 包含引擎当前持有的内容，例如 `wheelTimers`、`hourBuckets`、`minuteRuns`、`payloadDays`、`recalls` 和 `sourceBehind`，以及 Broker 启动以来的处理量，例如 `indexed`、`written`、`recalled`、`dropped` 和 `skipped`；从节点上还有 `followed`。OpenTelemetry 的定时消息入队与出队总数同样统计 native 定时消息。

在主从部署中，从节点会索引 native 定时消息，并通过主节点写入的消息跟随其投递进度。因此从节点被提升为主节点后，只会写入原主节点尚未写入的定时消息；与标准引擎一样，故障切换前后仍可能重复投递一条定时消息。

checkpoint 使投递进度落盘后，已投递定时消息的文件及其副本会被删除，它们引用的 CommitLog 文件也可以被正常清理。召回在其目标定时消息投递后会从内存中移除，但磁盘上的召回日志暂不会压缩。

## 示例与验证范围

在 `rocketmq-example/` 中执行：

```bash
cargo check --example producer-delay-send
cargo run --example producer-delay-send
```

示例向 `DelaySendTestTopic` 发送四条消息，分别使用级别、相对秒、相对毫秒和绝对时间形式。它使用回环 NameServer，之后关闭生产者。应配合订阅相同主题的消费者，比较请求的可投递时间与实际接收时间。

该示例不调用召回，不测试临近截止时间的竞争，也不演示崩溃后的定时器恢复。这些场景需要针对所选引擎和持久化配置单独观察。

源码依据：[延迟示例](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-example/examples/producer/delay_send.rs)、[定时请求规范化](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/common/message/timer_request.rs)、[存储配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/src/config/message_store_config.rs)、[句柄生成](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/processor/send_message_processor/message_builder.rs)、[召回处理器](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/src/processor/recall_message_processor.rs)。
