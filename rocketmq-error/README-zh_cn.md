# rocketmq-error

[English](README.md) | [简体中文](README-zh_cn.md)

[![Crates.io](https://img.shields.io/crates/v/rocketmq-error.svg)](https://crates.io/crates/rocketmq-error)
[![Documentation](https://docs.rs/rocketmq-error/badge.svg)](https://docs.rs/rocketmq-error)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../LICENSE-APACHE)

`rocketmq-error` 是 RocketMQ Rust workspace 的共享错误内核。它提供类型化原因、
稳定 descriptor 标识、显式协议投影、有界上下文，以及可安全脱敏的边界视图。

## 本 crate 职责

- 不透明的规范 `Error`、`Result<T>` 和 `SharedError` 类型。
- workspace 中使用的 `RocketMQError`、`RocketMQResult<T>` 和保留的领域错误枚举。
- `ErrorDescriptor` 和唯一的 `ALL_DESCRIPTORS` catalog。
- 稳定 descriptor 元数据：code、class、condition、fault attribution、component、
  固定公开消息、severity、recovery hint、backtrace policy、exposure、四个显式边界投影，
  以及有序字段 schema。
- `ErrorContext`、`PublicErrorView`、`DiagnosticView`、
  `BoundaryErrorView` 和 `CliErrorView`。

该 crate 有意不依赖 transport 实现或生成的 protobuf 绑定。remoting、gRPC、HTTP
和 CLI 投影类型是由边界适配器消费的轻量值。

## 快速开始

```rust
use rocketmq_error::RocketMQError;
use rocketmq_error::RocketMQResult;

fn validate_broker_addr(addr: &str) -> RocketMQResult<()> {
    if addr.is_empty() {
        return Err(RocketMQError::Network(
            rocketmq_error::NetworkError::InvalidAddress {
                addr: addr.to_owned(),
            },
        ));
    }

    Ok(())
}
```

`NetworkError`、`SerializationError`、`ProtocolError`、
`RpcClientError`、`AuthError`、`ControllerError`、`ToolsError`、
`FilterError`、`ObservabilityError` 和 `UnifiedServiceError` 等嵌套领域错误
都可通过 `From` 转换为 `RocketMQError`。

## 规范 Descriptor

每个保留的错误 leaf 都且仅关联一个不可变 descriptor。需要稳定行为的代码读取
descriptor，不从枚举、显示字符串或调用方 override 推导 policy。

```rust
use rocketmq_error::RocketMQError;

let error = RocketMQError::route_not_found("TopicA");
let descriptor = error.descriptor();

assert_eq!(descriptor.code().as_str(), "route.topic.not_found");
assert_eq!(descriptor.public_message(), "Topic route was not found");
assert_eq!(
    descriptor.recovery_hint(),
    rocketmq_error::RecoveryHint::RefreshRoute
);
```

`ALL_DESCRIPTORS` 是唯一 catalog。`descriptor_by_code` 对规范的小写点分 code
进行精确查询。`ErrorKind` 仅作为本地穷举匹配使用的结构化类型 discriminator，
不再拥有第二份元数据或 policy 表。

每个 descriptor 显式拥有四类投影：

- `RemotingSpec`：RocketMQ response code。
- `GrpcSpec`：gRPC payload code 和 transport status。
- `HttpSpec`：HTTP status。
- `CliSpec`：进程 exit status。

## 边界视图与脱敏

向 remoting、gRPC、HTTP、CLI、dashboard 或其他公开边界适配错误时，使用
`boundary_view()`。边界视图从 descriptor 读取标识和投影，并强制执行 exposure policy。

```rust
use rocketmq_error::RocketMQError;

let error = RocketMQError::storage_read_failed(
    "/var/lib/rocketmq/commitlog/00000000000000000000",
    "permission denied",
);
let view = error.boundary_view();

assert_eq!(view.code().as_str(), "storage.read.failed");
assert_eq!(view.message(), "Storage read failed");
assert!(view.context().is_empty());
```

`Exposure::Generic` 只暴露固定消息，不暴露任何动态公开字段。
`Exposure::Public` 只暴露 descriptor schema 中声明为
`ContextVisibility::Public` 的字段。

原始诊断上下文仍可从类型化错误和 `DiagnosticView` 读取。敏感值不会存入
`ErrorContext`，只记录有界且无值的存在标记。安全公开视图不会渲染 typed source
文本、源码位置或 backtrace。

```rust
use rocketmq_error::fields;
use rocketmq_error::ErrorContext;

let context = ErrorContext::new()
    .with_text(fields::TOPIC, "TopicA")
    .with_secret_presence(fields::CREDENTIALS_PRESENT);

assert_eq!(context.to_string(), "topic=TopicA, credentials_present=<redacted>");
```

## 恢复建议与严重级别

`RecoveryHint` 是 catalog 拥有的建议，不是完整重试决策。操作 owner 会结合
idempotency、执行进度、deadline 和 retry budget 作出决定。

当前 recovery hint 为 `Never`、`Backoff`、`RefreshRoute`、
`RefreshLeader`、`SwitchBroker`、`RefreshCredentials` 和
`OperatorAction`。当前 severity 为 `Debug`、`Info`、`Warn`、
`Error` 和 `Critical`。

```rust
use rocketmq_error::ErrorSeverity;
use rocketmq_error::RocketMQError;

let error = RocketMQError::ControllerNotLeader { leader_id: None };
assert_eq!(
    error.descriptor().recovery_hint(),
    rocketmq_error::RecoveryHint::RefreshLeader
);
assert_eq!(error.descriptor().severity(), ErrorSeverity::Warn);
```

## 类型化 Source

底层操作失败时使用保留 source 的构造函数。`std::error::Error::source()`
保留原始类型化原因；边界视图不会将它字符串化。

```rust
use std::error::Error as _;
use rocketmq_error::RocketMQError;

let error = RocketMQError::request_header_source(
    "decode header",
    std::io::Error::other("private detail"),
);

assert!(error
    .source()
    .and_then(|source| source.downcast_ref::<std::io::Error>())
    .is_some());
assert!(error.boundary_view().context().is_empty());
```

## 公共 API 说明

- 稳定集成使用 descriptor code 和投影，不解析 `Display`。
- Descriptor 和 projection 的构造保持私有；catalog 常量是只读公开值。
- 已删除的 legacy `ErrorSpec`、recovery/observability policy 表及
  category/scope 元数据不会保留兼容别名。
- 六个废弃 `ProtocolError` leaf、四个冲突 `ControllerError` leaf，
  以及未使用的 required-property leaf 已删除；保留的调用方使用规范
  `RocketMQError` variant 和 descriptor。

## 测试

从 workspace 根目录运行：

```bash
cargo test -p rocketmq-error
cargo fmt -p rocketmq-error -- --check
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

聚焦 catalog 与 association 测试：

```bash
cargo test -p rocketmq-error --test error_descriptor_catalog
cargo test -p rocketmq-error --test legacy_descriptor_associations
cargo test -p rocketmq-error --test error_context_redaction
```

## 许可证

基于 [Apache License, Version 2.0](../LICENSE-APACHE) 授权。

## 贡献

欢迎贡献。提交变更前，请阅读 workspace 的
[Contributing Guide](../CONTRIBUTING.md)。
