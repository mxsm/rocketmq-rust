---
title: "Rust API 入口"
---

本页按任务定位公共 API，并说明如何从实际构建的源码生成文档。网站 1.0.0 开发版 文档、注册表中的包和源码工作副本可能对应不同修订。阅读 Rustdoc 时同时核对包清单与所选 Cargo feature；其他构建中可见的条目不一定在当前构建中可用。

## 按职责选择 API

| 任务 | Cargo 包 / Rust crate | 公共入口与契约 |
| --- | --- | --- |
| 发送、消费或请求应答 | `rocketmq-client-rust` / `rocketmq_client_rust` | crate 根导出的生产者/消费者 builder、请求类型和 `ClientRuntime`；[客户端配置](../configuration/client-config.md) 解释构造与生命周期 |
| 表示消息、队列或结果 | `rocketmq-model` / `rocketmq_model` | 规范领域类型；[消息模型](../architecture/message-model.md) 解释标识与偏移量单位 |
| 拥有进程和服务任务 | `rocketmq-runtime` / `rocketmq_runtime` | `RuntimeOwner`、`RuntimeOwnerPlan`、`RootServiceContext`、`ChildServiceContext`、`TaskGroup`、`BlockingExecutor` 和 `ShutdownReport` |
| 通过能力接口扩展存储 | `rocketmq-store-api` / `rocketmq_store_api` | `MessageAppender`、`MessageReader`、`OffsetIndex`、`ReplicationControl`、`StoreHealth` 和 `StoreLifecycle`；使用存储组装提供的一致端口集合 |
| 解码协议请求 | `rocketmq-protocol` / `rocketmq_protocol` | 请求/响应码、头部和序列化；结合[协议与传输](../architecture/protocol-transport.md) 阅读 |
| 交换帧和管理会话 | `rocketmq-transport` / `rocketmq_transport` | 传输接口与配置；本地写入成功不证明远端已处理 |
| 分类失败 | `rocketmq-error` / `rocketmq_error` | 错误目录标识和类型化上下文；[错误参考](./errors.md) 解释跨边界保留标识 |
| 配置遥测 | `rocketmq-observability` / `rocketmq_observability` | 配置、遥测所有权与句柄；[可观测性设计](../architecture/errors-observability.md) 解释关闭和导出 |

这是一份入口索引，不表示这些 crate 中每个公共条目都具有同等稳定的扩展承诺。Client 和 Runtime 在 `src/public_api.rs` 中维护明确选择的导出。优先使用这些有文档的根导出，避免深入实现模块。现有兼容性重导出可能仍然公开，但已被弃用；按 [Rust API 迁移](../migration/rust-api.md) 中的替代说明处理。

## 生成并打开本地 Rustdoc

在仓库根目录使用选定的 Rust 工具链运行：

```bash
cargo doc -p rocketmq-client-rust --no-deps --open
cargo doc -p rocketmq-runtime -p rocketmq-store-api --no-deps
```

使用默认 Cargo 目标目录时，第一条命令打开 `target/doc/rocketmq_client_rust/index.html`；另外两个 crate 的索引为 `target/doc/rocketmq_runtime/index.html` 和 `target/doc/rocketmq_store_api/index.html`。`CARGO_TARGET_DIR` 或目标目录配置会改变位置。使用 Rustdoc 的条目搜索和源码链接浏览类型、trait 实现和 feature 标注。

`--no-deps` 限制生成的文档范围；Cargo 仍需处理依赖，因此原生依赖条件可能影响生成。缺少 Clang、C++ 工具链或 `protoc` 属于所选依赖图的问题，而非正文渲染问题。启用存储或 Proxy feature 前，参阅 [feature 与平台](./features-platforms.md)。

对于客户端只读管理集成，生成对应的精确接口范围：

```bash
cargo doc -p rocketmq-client-rust --no-default-features --features admin-read --no-deps --open
cargo tree -p rocketmq-client-rust --no-default-features --features admin-read -e features
```

客户端默认启用 `admin-full`，它同时启用 `admin-read` 和 `admin-mutation`。`MQAdminReadExt` 受 `admin-read` 控制，`MQAdminMutationExt` 受 `admin-mutation` 控制。Cargo feature 选择编译能力，不授予调用者访问运行中 Broker 的权限。凭据、策略及各服务限制仍然生效。

独立应用使用自己的 manifest 生成文档。例如，只读 MCP 工程位于主 Cargo 工作区之外：

```bash
cargo doc --manifest-path rocketmq-ai/rocketmq-mcp/Cargo.toml --no-deps
```

仅包含二进制目标的包也可以为自身可执行文件生成内部条目文档。这些输出不会使应用自动成为受支持的库依赖。外部集成应使用其协议与配置文档。

## 将签名作为运行契约阅读

实现集成前，回答四个问题：

1. **由谁拥有？** 可克隆句柄可以共享访问，但不拥有进程关闭职责。显式保留 `RuntimeOwner` 和服务关闭顺序；单独使用 `Arc` 不会等待后台任务退出。
2. **完成意味着什么？** 工作入队、帧写入、记录追加、越过持久水位及业务完成是不同的观察结果。阅读返回类型和[消息生命周期](../architecture/message-lifecycle.md)。
3. **失败或取消之后发生什么？** 保留错误标识和操作上下文。超时可能使远端结果处于未知状态。存在 `Errors`、`Panics` 和 `Safety` 章节时，应阅读相应约束；取消不等于回滚。
4. **什么条件使条目可用？** 核对 feature、平台及运行时条件。类型可见不代表端点、存储后端或凭据已配置。

实现具体生产者或消费者时，从[显式拥有运行时的首条消息示例](../getting-started/quick-start.md)以及[生产者](../producer/overview.md)或[消费者](../consumer/overview.md)指南开始。通过 Rustdoc 获取精确签名，避免复制实现模块中不完整的初始化片段。

## 保持应用文档一致

在正常项目 manifest 中记录应用使用的依赖版本或源码引用及 feature 选择。升级时重新生成相关本地 Rustdoc，分别审查弃用、默认值、协议和持久化变化。不要用 `latest` API 页面替代应用实际使用版本的文档。编写文档不要求指纹校验或固定工作副本。

## 源码参考

- [Client 清单](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/Cargo.toml)、[crate 导出](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/lib.rs)和[明确选择的公共 API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/src/public_api.rs)。
- [Runtime 公共 API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-runtime/src/public_api.rs)和 [Store API 导出](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store-api/src/lib.rs)。
- [工作区清单](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml)和 [MCP 清单](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-mcp/Cargo.toml)。
