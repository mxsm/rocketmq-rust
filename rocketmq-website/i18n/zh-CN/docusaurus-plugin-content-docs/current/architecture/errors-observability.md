---
title: "错误映射与可观测性所有权"
---

错误与遥测用途不同。类型化错误保留失败身份及允许对外返回的响应；遥测记录操作发生的频率和位置。日志字符串不应变成协议契约，也不应成为自动重试策略的输入。

## 统一错误与显式映射

`rocketmq-error` 拥有不透明 `Error` 封装和不可变 `ErrorDescriptor` 目录。描述符包含稳定点分错误码、故障/分类元数据、固定公开消息、严重性、恢复建议、暴露规则、上下文字段定义，以及明确的 Remoting/gRPC/HTTP/CLI 映射。

`Error` 不可克隆。多个所有者需要同一个失败时，`SharedError = Arc<Error>` 共享相同类型化来源、描述符和上下文。组件门面保留该封装，不通过解析 Display 文本重建错误。

| 边界 | 描述符拥有的映射 | 适配器职责 |
| --- | --- | --- |
| Remoting | `RemotingSpec` | 设置预期数字响应码及安全备注/上下文 |
| gRPC | `GrpcSpec` | 区分消息体状态与传输状态 |
| HTTP | `HttpSpec` | 应用 HTTP 状态及允许的公开响应体 |
| CLI | `CliSpec` | 应用进程退出状态和安全命令行输出 |

错误内核不依赖传输实现或生成的 protobuf 绑定。适配器将轻量值转换成具体协议。规范的点分错误码与 RocketMQ 数字响应码不是同一个概念。

例如，`route.topic.not_found` 具有固定公开消息及刷新路由建议。边界应从描述符取得这些值；在任意错误字符串中匹配“not found”，会丢失身份和预期映射。

## 公开与诊断上下文

`PublicErrorView` 只公开允许字段。采用通用暴露规则的描述符输出固定消息，不输出动态公开字段；公开暴露规则也要求各字段在该描述符定义中被声明为公开。

类型化原因和诊断上下文仍可用于内部分析。安全公开视图不会将来源错误、源码位置或回溯转成字符串。`ErrorContext` 保存有界字段及不含具体值的秘密存在标记，不保存凭据内容。因此即便内部 I/O 原因含有私有路径，存储错误响应仍可以保持稳定。

不要为了弥补公开错误脱敏而记录完整请求或配置对象。应增加明确、有界的诊断字段，回答运行问题，同时避免记录凭据、消息体或任意用户数据。

## 恢复建议不是重试引擎

目录建议包括 `Never`、`Backoff`、`RefreshRoute`、`RefreshLeader`、`SwitchBroker`、`RefreshCredentials`、`OperatorAction`。操作所有者结合幂等性、写入进度、截止时间和重试预算使用建议。

变更请求后未获得响应，结果可能不确定。描述符无法判断应用业务效果是否可以安全重复。客户端重试、运维修复和消费重新投递是不同机制，详见[投递与重试](../guides/delivery-and-retry.md)。

## 遥测所有权

应用拥有不可克隆的 `TelemetryRuntimeGuard`。组件取得可克隆的 `TelemetryHandle` 能力，通过 `from_handle` 创建各角色记录器。句柄不能关闭提供方；guard 关闭后，存活句柄停止记录。

SDK 提供方属于 guard。初始化不会安装全局 OpenTelemetry meter/tracer 提供方，但 tracing subscriber 和可选文本映射传播器属于进程级状态。嵌入式应用需要考虑宿主已有 subscriber：必需安装可能失败，尽力安装则记录其状态。

预期生命周期是：用应用服务上下文初始化遥测，将句柄注入服务，停止服务，明确刷写/关闭遥测，再关闭运行时。Prometheus 需要带作用域初始化，并等待监听器/任务关闭。释放 guard 不能替代检查 `TelemetryShutdownReport`。

## 信号、features 与基数

| 能力 | 设计边界 |
| --- | --- |
| 控制台/文件日志 | 独立于可选 OpenTelemetry；过滤器解析使用明确输入 |
| 指标 | 各角色仪表及有界主题/消费者组标签策略 |
| 追踪 | 可选 span，并按句柄策略通过消息属性传播上下文 |
| OTLP | 信号 feature、传输/导出配置及可达采集器 |
| Prometheus | 指标 feature、具有作用域的 HTTP 导出器所有权及抓取配置 |

可观测性 crate 没有默认 features。运行时选择未编译的导出器会返回类型化 feature-disabled 错误。`observability` 是指标/追踪支持的别名，不证明所有导出器或日志信号均已启用。

客户端 `observability` feature 启用追踪，`observability-metrics` 启用指标。客户端通过 OTLP 导出指标还需要依赖图中的 `rocketmq-observability/otlp-metrics`。不要在不同 crate 之间直接照搬同名 feature，而不检查其转发内容。

指标标签应使用低基数结果、服务角色及稳定操作名。消息 ID、Key、receipt、任意地址和无界主题/消费者组名称可能使时间序列数量成倍增长。追踪上下文属性使用 `TRACEPARENT`、`TRACESTATE`；传播不会替代消息业务身份或授权。

## 运行解释

应把请求结果与路由可用性、准入压力、存储/副本进度及生命周期状态关联。遥测导出器健康不代表 Broker 健康；导出器故障本身也不证明消息丢失。编译检查证明 API 可用，要证明信号送达还需采集器/抓取测试。

## 源码地图

- [错误内核](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-error/README.md)。
- [可观测性设计](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/README.md)、[句柄](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/handle.rs)、[日志/启动装配](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/logging.rs)。
- [运行时](runtime.md)、[安全](security.md)、[可观测性配置](../configuration/observability.md)。
