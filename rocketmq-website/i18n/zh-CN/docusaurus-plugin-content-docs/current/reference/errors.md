---
title: "错误、状态与恢复决策"
---

# 错误、状态与恢复决策

首先确定报告失败的边界。Rust 错误码、remoting 响应码、gRPC 消息体状态、gRPC 传输状态和进程退出码描述不同契约。应保留错误码与操作上下文，不要通过匹配人类可读的错误句子实现集成逻辑。

## 调用方应检查哪个值？

| 接口层 | 应检查的值 | 含义 |
| --- | --- | --- |
| 规范 Rust 错误 | `Error::descriptor()` 及其稳定的点分错误码 | 标识声明的失败类型与恢复提示，同时保留供内部诊断使用的强类型原因。 |
| 客户端外观错误 | `ClientError::descriptor()` 或 `shared_error()` | 保留规范描述符，而不是将原因压平成字符串。 |
| remoting 响应 | 数值响应码及允许公开的响应字段 | 多个规范错误可能映射到同一数值码；数值码不是唯一的内部原因。 |
| Proxy gRPC | 方法响应状态、存在时的逐项状态及传输结果 | RPC 成功送达仍可能包含失败的 RocketMQ 操作；传输失败时可能没有任何消息体结果。 |
| 生产者结果 | 外层结果、结果是否存在、再检查 `SendResult.send_status` | 返回结果对象仍可能报告刷盘或复制超时。 |
| CLI | 进程退出状态、安全 stderr 及命令专用输出 | 退出状态是自动化信号；部分操作结果也可能需要进一步检查。 |

[错误目录](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-error/src/catalog.rs)及其组件模块定义描述符身份和映射。外层服务边界可以使用服务级描述符包装原因，因此启动失败不一定暴露最内层配置错误码。

## 常见规范错误

恢复提示属于目录元数据。操作列说明如何结合具体操作使用提示，并不承诺自动重试。

| 稳定错误码 | 目录提示 | 常见原因与下一步 |
| --- | --- | --- |
| `core.configuration.parse_failed` | `Never` | 输入语法或反序列化失败。修正所选文件与格式后再启动。 |
| `core.configuration.missing` / `core.configuration.invalid` | `Never` | 缺少必填设置，或值/组合无效。检查所属配置模型与加载优先级。 |
| `protocol.header.invalid` / `protocol.body.invalid` | `Never` | 字段或消息体编码无效。应修正请求，重复发送相同字节无法修复问题。 |
| `protocol.request.unsupported` / `protocol.version.unsupported` | `Never` | 所选端点或版本未实现请求。检查端点、feature 与兼容范围。 |
| `route.topic.not_found` | `RefreshRoute` | 主题不存在、尚未注册，或在所选 NameServer 上不可见。确认主题创建与 Broker 注册，再在有限重试预算内刷新路由。 |
| `auth.credentials.invalid` | `RefreshCredentials` | 身份/签名无效或凭据过期。修正或轮换凭据，诊断中不能包含凭据值。 |
| `auth.permission.denied` | `Never` | 当前身份没有目标资源/操作权限。应检查策略与资源范围，而不是无限重试相同凭据。 |
| `transport.admission.queue_saturated` | `Backoff` | 本地请求接纳已满。降低并发，检查待处理数量与字节数，并仅在操作预算内重试。 |
| `transport.connection.timeout` / `transport.response.timeout` | `Backoff` | 连接或响应截止时间已到。决定是否可以重复变更前，应判断请求字节是否可能已经到达对端。 |
| `controller.leadership.not_leader` | `RefreshLeader` | 请求到达非当前 Leader 的 Controller。刷新 Leader 元数据，并保留原操作身份。 |
| `storage.capacity.exhausted` | `OperatorAction` | 存储无法接纳操作。检查剩余空间、保留压力和配置限制；重试本身不会创造容量。 |
| `storage.read.failed` / `storage.write.failed` | `OperatorAction` | 存储操作失败。保留有界诊断上下文，并调查 I/O 与后端状态。 |
| `storage.state.corrupted` | `OperatorAction` | 存储状态违反预期格式或不变量。隔离受影响的恢复路径，使用备份恢复流程，不要删除格式元数据来压制错误。 |

此表用于常见问题查询，不是完整目录。Broker、Proxy、Controller、Client、认证、可观测性和工具在[组件错误目录](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-error/src/catalog)中定义了更具体的描述符。已有更具体描述符时，应保留它。

## 描述符映射的准确示例

以下是目录声明的映射，不表示所有 HTTP 或 gRPC 端点使用相同外层封装，也不表示它们始终返回表中的传输状态。

| 规范描述符 | remoting | gRPC 消息体 / 传输映射 | HTTP / CLI 映射 |
| --- | --- | --- | --- |
| `protocol.header.invalid` | `InvalidParameter (29)` | `BadRequest / InvalidArgument` | `400 / 64` |
| `route.topic.not_found` | `TopicNotExist (17)` | `TopicNotFound / NotFound` | `404 / 66` |
| `auth.credentials.invalid` | `NoPermission (16)` | `Unauthorized / Unauthenticated` | `401 / 77` |
| `auth.permission.denied` | `NoPermission (16)` | `Forbidden / PermissionDenied` | `403 / 77` |
| `transport.admission.queue_saturated` | `SystemBusy (2)` | `TooManyRequests / ResourceExhausted` | `429 / 75` |
| `controller.leadership.not_leader` | `ControllerNotLeader (2007)` | `InternalError / FailedPrecondition` | `409 / 65` |
| `storage.capacity.exhausted` | `SystemError (1)` | `InternalError / ResourceExhausted` | `507 / 65` |

例如，凭据无效与权限被拒绝都映射到 remoting 响应码 `16`。仅看到 `16` 的重试实现，不能推断刷新凭据就能解决策略拒绝。同样，通用 `SystemError` 可以表示多种存储或服务失败，不能据此安全地重复写入。

数值与轻量枚举定义于[边界类型](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-error/src/boundary.rs)。适配器代码将它们转换为具体协议响应类型，错误内核本身不负责网络通信。

## 发送与拉取结果并非都是错误

| 结果 | 含义 | 调用方行为 |
| --- | --- | --- |
| `SendStatus::SendOk` / `SEND_OK` | 所选发送路径满足其配置的响应条件 | 按业务契约继续处理；这不能证明消费者已处理消息。 |
| `FlushDiskTimeout` / `FLUSH_DISK_TIMEOUT` | 等待所要求的本地刷盘条件超时 | 将持久性结果视为不确定，诊断磁盘进度并使用幂等重试逻辑。 |
| `FlushSlaveTimeout` / `FLUSH_SLAVE_TIMEOUT` | 等待所要求的副本条件超时 | 检查复制进度与所选确认策略。 |
| `SlaveNotAvailable` / `SLAVE_NOT_AVAILABLE` | 所要求的副本不可用 | 恢复预期拓扑，或明确作出可用性与持久性取舍。 |
| 拉取 `Found` | 已返回消息 | 完成处理后再推进与业务完成对应的偏移量。 |
| 拉取 `NoNewMsg` / `NoMatchedMsg` | 没有新消息，或没有匹配选择条件的消息 | 按返回进度与等待策略继续轮询，不应将其视为传输故障。 |
| 拉取 `OffsetIllegal` | 请求偏移量不在队列可接受范围内 | 移动进度前，检查返回的偏移量建议、保留情况及消费者恢复策略。 |

规范结果定义位于 [`rocketmq-model::result`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/result.rs)。单向发送没有可检查的 Broker 响应。POP 回执或不可见时间错误需要使用 POP 确认模型，不能套用经典拉取的偏移量恢复方式。具体见[投递与重试](../guides/delivery-and-retry.md)、[LitePull](../consumer/pull-consumer.md)和 [POP](../consumer/pop.md)。

## 安全的 CLI 诊断

规范 CLI 视图按以下格式输出单行错误：

```text
ERROR route.topic.not_found: Topic route was not found
```

默认输出包含稳定错误码与固定公开消息。详细模式只能附加描述符允许的有界诊断字段，包含机密的值显示为 `<redacted>`。两种模式都不会通过该视图输出源错误、源码位置或回溯。所选工具是否暴露详细模式，应查阅该工具的帮助。

| 规范 CLI 退出码 | 含义 |
| --- | --- |
| `64` | 用法或参数失败 |
| `65` | 需要处理的数据或状态问题 |
| `66` | 未找到请求的资源 |
| `69` | 服务或资源不可用 |
| `70` | 软件或内部失败 |
| `75` | 临时失败 |
| `77` | 权限或认证失败 |
| `78` | 配置失败 |

这些值描述共享错误视图。CLI 参数解析器、包装程序和专用命令可能定义其他退出行为；例如，某个工具的预检拒绝不会自动属于上述目录类别。应保留实际退出码与结构化结果，而不是仅保留最后一行输出。

## 作出恢复决策

1. 确定操作、端点、响应层、稳定错误/状态及剩余截止时间。
2. 判断操作是只读、幂等，还是完成结果不确定的变更。传输取消不会撤销远端工作。
3. 应用相关提示：退避、刷新路由/Leader/凭据，或修复配置与容量。重试应同时受次数和耗时约束。
4. 恢复后确认原始业务结果或资源状态。重复发送必须保留业务事件身份，并能处理重复投递。

收集服务角色、操作名称、允许记录的资源标识、耗时及相关队列/存储/副本进度。共享日志与 Issue 报告中不应包含凭据、ACL/TLS 材料、消息体、任意请求对象或原始配置值。通用公开错误有意比内部原因暴露更少信息，应使用受支持的诊断字段及[故障排查](../operations/troubleshooting.md)，而不是削弱脱敏。
