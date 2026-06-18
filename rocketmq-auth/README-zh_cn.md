# rocketmq-auth

[English](README.md) | [简体中文](README-zh_cn.md)

[RocketMQ-Rust](../README-zh_cn.md) 的认证、授权、ACL 迁移和 auth runtime 支持 crate。

`rocketmq-auth` 提供 RocketMQ-Rust 服务端和客户端使用的 auth 层。它聚焦 RocketMQ 兼容的 access-key 认证、
ACL 授权、Java 风格 ACL 文件兼容、本地元数据 provider、有状态缓存以及 remoting 请求集成。

## 能力边界

| 领域 | 提供能力 |
|------|----------|
| 认证 | 基于 access-key 的认证、remoting 签名解析、请求时间戳校验和可插拔认证策略。 |
| 授权 | 基于 subject/resource/action 的 ACL 授权、allow/deny policy 判断、super-user 绕过和默认拒绝行为。 |
| ACL 兼容 | Java 风格 `plain_acl.yml` 加载、递归 ACL 文件发现、全局/账号级 white remote address 和旧 ACL 迁移。 |
| 运行时集成 | 面向 broker/proxy 的 `AuthRuntime`，封装 remoting 命令检查、元数据初始化、ACL reload 和缓存失效。 |
| 元数据 | 本地认证和授权 metadata provider，并在 `authConfigPath` 下持久化快照。 |
| 可观测性 | ACL reload、缓存命中/未命中、签名、白名单、认证和授权结果的轻量计数器。 |

## Crate 结构

| 模块 | 职责 |
|------|------|
| [`authentication`](src/authentication.rs) | 认证上下文、provider、strategy、ACL 签名和客户端 RPC hook 支持。 |
| [`authorization`](src/authorization.rs) | 授权上下文、provider、handler、strategy、ACL model、resource 和 policy。 |
| [`acl`](src/acl.rs) | Java ACL 文件加载、校验、存储辅助能力和 white remote address 匹配。 |
| [`runtime`](src/runtime.rs) | 面向服务集成的 auth runtime，串联 provider、ACL 文件、reload、缓存和 remoting 检查。 |
| [`config`](src/config.rs) | `AuthConfig`，支持 serde camelCase 字段，并在 debug 输出中隐藏密钥。 |
| [`migration`](src/migration.rs) | 旧 ACL 模型兼容，用于从 RocketMQ v1 ACL 文件迁移。 |
| [`observability`](src/observability.rs) | Auth metrics 快照和稳定的 metric sample 名称。 |
| [`permission`](src/permission.rs) | ACL 迁移和 policy 生成使用的权限转换辅助能力。 |

## 安装

在当前 workspace 内使用本地路径依赖：

```toml
[dependencies]
rocketmq-auth = { path = "../rocketmq-auth" }
```

外部项目可使用已发布的 workspace 版本：

```toml
[dependencies]
rocketmq-auth = "1.0.0"
```

如需启用可选的 gRPC metadata 解析支持，可开启 `grpc` feature：

```toml
[dependencies]
rocketmq-auth = { version = "1.0.0", features = ["grpc"] }
```

## 快速开始

通过 `AuthConfig` 创建 auth runtime，并指向 RocketMQ 风格的 ACL 文件：

```rust
use cheetah_string::CheetahString;
use rocketmq_auth::config::AuthConfig;
use rocketmq_auth::AuthRuntimeBuilder;

#[tokio::main]
async fn main() -> rocketmq_error::RocketMQResult<()> {
    let config = AuthConfig {
        auth_config_path: CheetahString::from_static_str("store/auth"),
        acl_file: CheetahString::from_static_str("conf/plain_acl.yml"),
        authentication_enabled: true,
        authorization_enabled: true,
        acl_file_watch_enabled: true,
        ..AuthConfig::default()
    };

    let runtime = AuthRuntimeBuilder::new(config).build().await?;

    // Broker/proxy 集成时，会在处理受保护请求前用 channel context
    // 和 RemotingCommand 调用 check_remoting(...)。
    let metrics = runtime.metrics_snapshot();
    println!("auth reload attempts: {}", metrics.acl_reload_attempts);

    runtime.shutdown().await?;
    Ok(())
}
```

最小 `plain_acl.yml` 示例：

```yaml
globalWhiteRemoteAddresses:
  - 10.10.*.*
accounts:
  - accessKey: alice
    secretKey: alice-secret
    admin: false
    defaultTopicPerm: DENY
    defaultGroupPerm: DENY
    topicPerms:
      - TopicA=PUB
    groupPerms:
      - GroupA=SUB
```

Runtime 会把该文件加载到本地认证和授权元数据中。启用 `aclFileWatchEnabled` 后，文件变化会在后台自动 reload；
reload 失败时会保留上一份可用快照。

## 配置

`AuthConfig` 支持 camelCase 字段名的 serde 反序列化。重要字段包括：

| 字段 | 作用 |
|------|------|
| `authConfigPath` | 本地 metadata provider 持久化 users 和 ACL snapshots 的目录/文件根路径。 |
| `aclFile` | 要加载的 Java 风格 ACL YAML 文件或目录。目录会递归搜索 `.yml` 和 `.yaml` 文件。 |
| `aclFileWatchEnabled` | 启用后台 ACL 文件 reload。 |
| `aclFileWatchIntervalMillis` | ACL 文件 reload 轮询间隔，默认 `5000`。 |
| `authenticationEnabled` | 启用 `AuthenticationService` 的认证检查。 |
| `authorizationEnabled` | 启用 `AuthorizationService` 的授权检查。 |
| `authenticationWhitelist` | 逗号分隔的 remoting request code 列表，用于跳过认证。 |
| `authorizationWhitelist` | 逗号分隔的 remoting request code 列表，用于跳过授权。 |
| `signatureAlgorithm` | Java 兼容签名算法：`HmacSHA1`、`HmacSHA256` 或 `HmacMD5`。 |
| `requestTimestampExpiredMillis` | 可选重放保护窗口。`0` 保持 Java 兼容的不校验过期行为。 |
| `migrateAuthFromV1Enabled` | 通过 v1 plain permission manager 启用旧 ACL 迁移。 |

## 示例

在 workspace 根目录运行示例：

```bash
cargo run -p rocketmq-auth --example authentication_strategy_usage
cargo run -p rocketmq-auth --example authentication_manager_usage
cargo run -p rocketmq-auth --example authorization_evaluator_usage
cargo run -p rocketmq-auth --example acl_authorization_handler_usage
cargo run -p rocketmq-auth --example metadata_provider_example
```

## 校验

针对本 crate：

```bash
cargo test -p rocketmq-auth
cargo test -p rocketmq-auth --test java_alignment
cargo test -p rocketmq-auth --examples --no-run
```

`java_alignment` 集成测试覆盖 Java 可见行为，包括 remoting 签名内容、旧 `plain_acl.yml` 语义、deny 优先级、
super-user 绕过、安全 ACL reload、factory 缓存和 request context model 字段。

工作区级校验在仓库根目录运行：

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Benchmark

运行认证和授权热路径 benchmark：

```bash
cargo bench -p rocketmq-auth --bench auth_hot_path_bench
```

该 benchmark 覆盖 remoting 签名计算、white remote address 匹配、ACL policy 匹配，以及有状态认证/授权缓存命中。

在 CI 或本地回归门禁中，可以先确认 benchmark target 能编译：

```bash
cargo test -p rocketmq-auth --benches --no-run
```

采集基线时，应在尽量空闲的机器上运行 benchmark，并保留 `target/criterion/auth_*` 下的 Criterion 报告。后续变更应在相同
toolchain 和硬件上对比；涉及签名、白名单匹配、ACL policy 匹配或有状态缓存 key 的变更，如果出现 auth 热路径回归，应先排查再合入。

## 许可证

RocketMQ-Rust 使用 Apache License 2.0。详见 [../LICENSE-APACHE](../LICENSE-APACHE)。
