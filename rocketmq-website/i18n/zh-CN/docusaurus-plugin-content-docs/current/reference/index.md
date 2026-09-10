---
title: "参考手册入口"
---

已明确操作、需要查询准确选项、类型或兼容规则时，使用参考手册。操作流程从[快速开始](../getting-started/quick-start.md)入手，职责解释见[架构总览](../architecture/overview.md)。

## 找到问题的所有者

| 问题 | 权威入口 |
| --- | --- |
| 当前源码包含哪些包，最低 Rust 版本是多少？ | [根 manifest](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml) 与[工具链](https://github.com/mxsm/rocketmq-rust/blob/main/rust-toolchain.toml) |
| Broker 使用哪些文件和设置启动？ | [Broker 配置](../configuration/broker-config.md)与[部署总览](../deployment/overview.md) |
| NameServer 支持哪些参数与合并规则？ | [服务配置](service-configuration.md#nameserver) |
| Controller/Proxy 各模式如何启动？ | [服务配置](service-configuration.md)与对应[部署指南](../deployment/overview.md) |
| 客户端有哪些 builder 与 Cargo feature？ | [客户端配置](../configuration/client-config.md)与 [manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/Cargo.toml) |
| 发送结果表示什么？ | [生产者结果表](../producer/overview.md)与[规范结果类型](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-model/src/result.rs) |
| LitePull 提交表示什么？ | [轮询与提交语义](../consumer/pull-consumer.md) |
| 哪个管理命令接受该选项？ | [Admin CLI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/README.md) 与对应子命令的 `--help` |
| 集成应保留哪种错误身份？ | [错误指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-error/README.md)与[目录](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-error/src/catalog.rs) |
| 使用哪些请求/响应码或头部类型？ | [Protocol 指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-protocol/README.md)与[源码](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-protocol/src) |
| 哪些限制与 TLS 能力属于网络层？ | [Transport 指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-transport/README.md) |
| 存储使用哪些持久性与进度类型？ | [存储设计](../architecture/storage.md)与 [Store API](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store-api/README.md) |
| 如何配置指标、日志与追踪？ | [可观测性指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/README.md)与[配置](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/src/config.rs) |
| 应使用哪个产品专用配置？ | [生态总览](../ecosystem/overview.md)与所选独立产品 |

这些源码参考描述当前源码系列。已发布发行版可能具有不同选项和 API，应配套使用对应源码/tag 与资产。

## 完整理解配置字段

每个字段都需要确认外部 key、所属分节、类型、默认值、单位、约束、优先级和重载行为，不能仅根据 Rust 字段名推断。

例如，Broker 的 `listenPort` 属于 `[broker]`，嵌套 server 配置提供监听细节；源码分别使用 Broker 元数据根和消息存储根。复制扁平旧配置，或将字段移动到名称相近的分节，可能改变解析结果或被拒绝。

选项可能同时要求编译 feature 和运行设置。Rust Client 没有独立 `tls` Cargo feature，该能力由 Transport 实现和真实端点配置控制。同样，指标 feature 不会自动选择 exporter 端点。

## 查看实际将要运行的命令

在仓库根目录执行：

```bash
cargo run -p rocketmq-admin-cli -- --help
cargo run -p rocketmq-admin-cli -- topic updateTopic --help
cargo run -p rocketmq-admin-cli -- consumer updateSubGroup --help
```

CLI 选项属于其声明层级。当前 `clusterList` 和 `updateSubGroup` 使用 `NAMESRV_ADDR`，不接受 `-n`；主题命令各自接受 `-n`。从另一种 RocketMQ 工具复制命令名，不能证明其全部参数都可互换。

执行前，应区分读取、元数据修改、偏移量修改和数据操作。[首次诊断](../operations/first-diagnosis.md)提供小范围只读顺序，并说明状态影响。

## 为当前源码生成 Rust API 文档

在本地查看当前客户端 API：

```bash
cargo doc -p rocketmq-client-rust --no-deps --open
```

该命令为所选包与 feature 图生成文档。增加应用实际使用的 feature，独立产品从自己的 manifest 生成。已发布 API 文档对应的发行版可能不同于 1.0.0 开发版。

从 crate 根或文档指定的 `api`/`prelude` 导入公开类型。实现模块中的文件不会自动成为公开集成契约。

## 兼容性包含多个维度

Rust 源码 API、序列化字段、请求/响应码、持久布局、Controller 内部协议和操作行为，是不同兼容面。某一项匹配不能证明其余各项。

[能力矩阵](../overview/capability-matrix.md)记录相关模式/feature 条件。[模块地图](../architecture/module-map.md)帮助定位共享契约变更的所有者。兼容声明应附带准确版本和实际场景，不使用笼统的“完全兼容”标签。

## 参考与迁移页面

- [Features 与平台](./features-platforms.md)：包默认值、原生前置条件和运行时条件。
- [错误与状态](./errors.md)：稳定标识、边界映射和重试决策。
- [协议兼容性](./protocol-compatibility.md)：端点、范围、证据及存储边界。
- [Admin CLI](./admin-cli.md)：命令目录、参数、影响和退出行为。
- [Java 迁移](../migration/java-to-rust.md)：客户端、集群及数据迁移流程。
- [Rust API 迁移](../migration/rust-api.md)：公共导入、运行时所有权和 Classic/LitePull 调整。

## API 与术语

- [Rust API 入口](./rust-api.md)
- [中英文术语表](./glossary.md)
