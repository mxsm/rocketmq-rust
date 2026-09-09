# rocketmq-admin-core

> Runtime 所有权：示例中的 `client_runtime` 是应用持有的 `Arc<ClientRuntime>`，它从 `RuntimeOwner` 的 child scope 创建，并在进程边界显式关闭。

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-admin-core` 是 RocketMQ Rust 展示无关的管理边界。它拥有管理请求/结果类型、校验、
类型化错误、安全输入、能力 trait 和会话配置。CLI、TUI、MCP、Dashboard 与 Example 都通过
这条边界访问管理能力，不再从 Client SDK 引入 Admin façade。

[English](README.md)

## 架构

```text
CLI / TUI / MCP / Dashboard / Example
                |
                v
        Admin-owned core contract
                |
                v
      可选 Client adapter + session
                |
                v
          RocketMQ Client SDK
```

默认构建提供管理契约及共享的模型、协议、错误和安全类型，不引入可选的 Client/Transport 适配器依赖。
SDK 集成由特性控制；完整 `client-adapter` 提供 CLI/TUI 使用的会话和服务，
较窄的读取与变更适配器提供各自的能力边界。

## Features

| Feature | 默认开启 | 作用 |
|---|---:|---|
| `read-client-adapter` | 否 | 只读 SDK 适配能力。 |
| `mutation-client-adapter` | 否 | 变更 SDK 适配能力。 |
| `client-adapter` | 否 | 启用基于 RocketMQ Client 的 `AdminSession` 和 adapter 实现。 |
| `rocksdb-export` | 否 | 为确实需要的管理工具启用本地 RocksDB 元数据导出。 |

只使用 contract 的 consumer 保持默认配置：

```toml
[dependencies]
rocketmq-admin-core = { path = "rocketmq-tools/rocketmq-admin/rocketmq-admin-core" }
```

需要真实 RPC 能力的 consumer 显式启用 adapter：

```toml
[dependencies.rocketmq-admin-core]
path = "rocketmq-tools/rocketmq-admin/rocketmq-admin-core"
features = ["client-adapter"]
```

## 显式会话生命周期

`AdminSession` 拥有 Client SDK handle。调用方必须显式关闭会话；Drop 不会启动脱离所有权的清理任务。以下示例需要 `client-adapter`。
传入由应用持有的 `Arc<ClientRuntime>`；所有会话关闭后，再关闭共享客户端运行时及其
`RuntimeOwner`。参见[客户端生命周期示例](../../../rocketmq-client/README-zh_cn.md)。

```rust,no_run
use rocketmq_admin_core::core::AdminResult;
use rocketmq_admin_core::client_adapter::AdminBuilder;
use rocketmq_admin_core::core::topic::ListTopicsRequest;
use rocketmq_admin_core::core::topic::TopicAdmin;

async fn list_topics(
    client_runtime: std::sync::Arc<rocketmq_admin_core::client_adapter::ClientRuntime>,
) -> AdminResult<()> {
    let mut session = AdminBuilder::new(client_runtime.clone())
        .namesrv_addr("127.0.0.1:9876")
        .instance_name("admin-core-example")
        .build_and_start()
        .await?;

    let result = session.list_topics(&ListTopicsRequest::default()).await;
    session.shutdown().await;

    for topic in result?.topics {
        println!("{}", topic.topic);
    }
    Ok(())
}
```

认证操作使用 `AdminCredentials`。它的 Debug 输出会脱敏，只有 adapter 内部可以把凭证转换成 Client
RPC hook。

## 源码布局

```text
rocketmq-admin-core/
├── src/
│   ├── lib.rs
│   ├── core/                 # Admin-owned contract；始终可用
│   └── client_adapter/       # Client SDK 集成；feature-gated
│       ├── lifecycle.rs      # AdminSession 所有权
│       └── services/         # CLI/TUI command adapter
└── tests/                    # Contract、model 与边界测试
```

旧 `admin/`、`client_adapter/legacy/`、self alias 和 `legacy-common-compat` feature 已删除。项目尚未
正式发布，因此不保留 compatibility façade。

## 边界规则

- request/result model 和校验属于 `core`。
- SDK 和传输集成遵循适配器特性边界。
- 核心请求/结果契约不暴露 SDK 门面或原始钩子；完整适配器显式导出 `ClientRuntime` 类型供应用注入生命周期。
- 命令解析和渲染属于 CLI/TUI crate。
- 每个已启动的 `AdminSession` 在成功与失败路径都必须显式关闭。

`tests/boundary_source_guard.rs` 保护源码和 feature 边界。

## 本地验证

```bash
cargo fmt -p rocketmq-admin-core -- --check
cargo test -p rocketmq-admin-core --features client-adapter
cargo check -p rocketmq-admin-core --no-default-features
cargo check -p rocketmq-admin-core --no-default-features --features read-client-adapter
```

校验只在本地执行。临时日志、报告和一次性校验脚本应存入 `target/` 等已忽略目录，不提交到仓库。

## 相关 Crates

- [`rocketmq-admin-cli`](../rocketmq-admin-cli)
- [`rocketmq-admin-tui`](../rocketmq-admin-tui)
- [`rocketmq-mcp`](../../../rocketmq-ai/rocketmq-mcp)
- [`rocketmq-client`](../../../rocketmq-client)

## License

基于 [Apache License, Version 2.0](../../../LICENSE-APACHE) 发布。
