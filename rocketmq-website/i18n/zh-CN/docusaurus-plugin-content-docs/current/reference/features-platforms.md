---
title: "构建 features 与平台边界"
---

# 构建 features 与平台边界

应同时选择产品、Cargo 依赖图与运行时配置。Cargo feature 使实现可用，但不会启动监听器、选择存储、授予权限，也不能证明该部署已在所有操作系统上运行。[能力矩阵](../overview/capability-matrix.md)说明了这些附加条件。

## 工具链与工作区边界

根[工具链文件](https://github.com/mxsm/rocketmq-rust/blob/main/rust-toolchain.toml)选择 Rust `1.95.0`。根工作区包声明相同的最低版本，通常使用 Rust 2021。单个 manifest 可以选择其他 edition：Admin CLI 及独立的 SRE/GPUI 项目使用 Rust 2024。edition 是源码语言设置，不是服务端线协议版本。

根包命令应以根 `Cargo.toml` 的成员列表为准。GPUI、Tauri 后端、Web Dashboard 后端、示例工作区与 AI SRE 均有独立构建边界。根工作区构建成功不表示这些产品全部构建完成。前端与 `rocketmq-website` 是 Node 项目；网站的 `.nvmrc` 选择 Node `24.13.0`，正常构建会生成英文和中文页面。

feature 在依赖图中累加。某条依赖边禁用默认 feature，不能移除其他依赖边启用的同一 feature。尤其是，工作区中的 Client 和 Transport 依赖禁用对应 crate 的默认项，而 Store 依赖保留默认项。因此，仅查看某个包公开的默认列表，并不能描述所有嵌入该包的应用。

## 核心服务与客户端 feature 表

| 包 | 直接默认 features | 常用可选项 | 运行时条件 |
| --- | --- | --- | --- |
| [`rocketmq-broker`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-broker/Cargo.toml) | `local_file_store` | `rocksdb_store`，别名 `rocksdb-store`；`extended_timeline`；`tieredstore` | 选择兼容的存储/定时配置；构建选择不会迁移已有数据。 |
| [`rocketmq-namesrv`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-namesrv/Cargo.toml) | `tls` | `embedded-controller` | TLS 仍需端点配置。嵌入式 Controller 还需 `enableControllerInNamesrv` 及 Controller 配置。 |
| [`rocketmq-controller`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/Cargo.toml) | `storage-rocksdb` | `storage-file`；`dev-single` 启用 `storage-file` | `storageBackend` 应匹配已编译实现。`dev-single` 本身不会改变默认的 `RocksDB` 配置值。 |
| [`rocketmq-proxy`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy/Cargo.toml) | `cluster-mode` 和 `local-mode` | `tls`；`tieredstore` 同时启用 Local 模式 | 选择 `mode = "cluster"` 或 `"local"`。内置 gRPC TLS 同时要求 TLS feature 和 TLS 材料。 |
| [`rocketmq-client-rust`](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/Cargo.toml) | `admin-full`，展开为 `admin-read` 和 `admin-mutation` | `nameserver-dns-discovery`；下文列出的独立遥测 features | 注入应用持有的客户端运行时并配置发现。管理 API 已编译不代表能够绕过认证或授权。 |

Broker 的 `production` 组合本地存储与 `production-observability`。后者通过 feature 别名启用 Prometheus 指标、OTLP 追踪和 OTLP 日志。这是构建便利项，不能证明机器、拓扑或工作负载已具备生产可用性。`test-support` feature 暴露测试设施，普通消息应用无需启用。

客户端没有独立的 `tls` feature。应用通过依赖图启用 `rocketmq-transport/tls`，再配置客户端连接。普通生产者和消费者代码若不需要管理接口，可以禁用 Client 默认 features。

## 存储与传输层选择

| 包 / feature | 编译效果 | 平台或运行时边界 |
| --- | --- | --- |
| [Store](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-store/Cargo.toml) 默认项 | `local_file_store` 和 `fast-load` | 即使顶层 feature 列表较少，Broker 的 Store 依赖仍可能启用这些项。 |
| Store `safe-load` | 启用保守的本地加载支持 | 若通过 feature 选择 safe-load，需要从实际依赖图中移除 `fast-load`；两者同时存在时 fast-load 优先。`ROCKETMQ_SAFE_LOAD=true` 是独立的运行时覆盖项。 |
| Store `rocksdb_store` / `extended_timeline` | 引入 RocksDB 基础依赖 | 适用原生 RocksDB 工具链要求。存储后端选择与定时存储所有权仍相互独立。 |
| Store `tieredstore` | 引入辅助分层存储集成 | 行为由当前适配器及其恢复契约决定；不意味着提供 S3 消息存储后端，也不会增强主存储确认。 |
| Store `io_uring` | 转发到 `rocketmq-store-local/io_uring` | 依赖和优化路径专用于 Linux，仍取决于内核和运行时是否满足条件。 |
| [Transport](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-transport/Cargo.toml) 默认项 | 对直接启用默认项的使用方启用 `tls` 和 `socks` | 根工作区依赖禁用这些默认项，应检查最终依赖图。 |
| Transport `linux-sendfile` | 引入可选的 Linux 文件区域写入路径 | 要求 Linux 及满足条件的明文连接。TLS 和不满足条件的场景使用可移植路径；本地写入完成不等于对端确认。 |
| Transport `simd` | 转发协议层 SIMD 支持 | 是否适用由 CPU、目标平台与依赖支持决定，不改变 RocketMQ 线协议契约。 |

修改后端、加载策略或文件传输策略前，应阅读[存储后端](../architecture/storage-backends.md)与[传输层设计](../architecture/protocol-transport.md)。同时编译多个后端，不表示可以让不兼容配置共用一个数据目录。

## 可观测性 feature 名称由各包定义

| 使用方 | 指标 | 追踪 | 导出器选择 |
| --- | --- | --- | --- |
| Broker | `otel-metrics` | `otel-traces` | `otlp-metrics`、`otlp-traces`、`otlp-logs`、`prometheus` / `metrics-prometheus` |
| NameServer | `otel-metrics` | `otel-traces` | 对应 `otlp-*` features；不能将 Broker 的 Prometheus 别名复制到此 manifest。 |
| Controller | `metrics` | `otel-traces` | `metrics-otlp`、`metrics-prometheus`、`otlp-traces`、`otlp-logs` |
| Proxy | `observability` | `otel-traces` | `otlp-metrics`、`otlp-traces`、`otlp-logs` |
| Client | `observability-metrics` | `observability` | `otlp-traces` 转发追踪导出；OTLP 指标要求依赖图包含 observability 依赖的 `otlp-metrics`。 |
| [Observability 库](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-observability/Cargo.toml) | `otel-metrics` | `otel-traces` | 默认项为空；`otlp-metrics` / `otlp-traces` / `otlp-logs` 包含对应信号与 gRPC 导出器；`prometheus` 包含指标。 |

日志信号支持使用 `otel-logs`，与普通控制台日志不同。运行时请求未编译的导出器会产生强类型错误。实际端点、资源身份、标签策略与生命周期所有权见[可观测性配置](../configuration/observability.md)。

## Admin、Dashboard、MCP、SRE 与网站

| 产品 | 构建选择 | 外部或平台要求 |
| --- | --- | --- |
| [Admin CLI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/Cargo.toml) / [Admin TUI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/Cargo.toml) | 根工作区包；这些 manifest 没有产品级 `[features]` 开关表 | TUI 需要交互终端。依赖适配器 features 与运行时凭据决定实际可执行操作。 |
| [GPUI Dashboard](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-gpui/README.md) | 独立 Cargo 项目，无产品级 feature 开关表 | 图形桌面与原生 GUI 构建依赖。Windows 使用 MSVC/Windows SDK，macOS 使用 Xcode 工具，Linux 需要对应 GUI 开发库。 |
| [Tauri Dashboard](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-tauri/README.md) | Node 前端加独立 `src-tauri` Cargo 项目 | Tauri 平台前置依赖与原生打包环境；仅编译后端不会生成完整桌面应用。 |
| [Web Dashboard](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-web/README.md) | 前后端分别构建；后端无产品级 feature 开关表 | 浏览器、已配置的后端及所选数据库部署。数据库模式属于运行时配置，不是 Cargo feature。 |
| [MCP](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-mcp/Cargo.toml) | 默认 `read-only`、`diagnose`、`stdio`；可选 `streamable-http`、`observability`、`otlp`、`change-planning` | HTTP 传输启用对应认证支持，并需要匹配的服务端配置。规划能力不会向只读产品添加变更执行功能。 |
| [MCP Control](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-mcp-control/Cargo.toml) | 默认项为空；`write-tools` 显式引入管理依赖 | 独立配置的变更服务，具有自身授权与审计边界。 |
| [AI SRE](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-ai/rocketmq-sre/Cargo.toml) | 独立多 crate 工作区，另有 UI 和 SDK 项目；选择实际成员包 | 连接器、控制平面、模型网关和执行代理的依赖不同。只读连接器使用读取适配器，执行代理依赖包含变更支持。工作区依赖不代表启用所有执行器。 |
| [网站](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/package.json) | Docusaurus Node 项目；`npm run build` | 已安装的 Node 依赖及指定 Node 环境，没有 Cargo features。 |

没有产品 feature 表，不代表产品不存在可选能力或原生依赖。应遵循各自 manifest、package scripts 和部署指南，不能运行一次根 Cargo 命令就声称覆盖了所有独立产品。

## 原生工具与平台限制

使用 RocksDB 的构建会编译原生依赖，可能需要 C/C++ 编译器及绑定生成工具。应根据实际选中的依赖构建输出诊断；纯 Rust 客户端构建不会覆盖这一路径。Controller [构建脚本](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-controller/build.rs)显式选择 `protoc-bin-vendored`。Proxy Core [构建脚本](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-proxy-core/build.rs)通过外部编译器路径执行 protobuf 生成，因此构建环境需要提供 `protoc`。两者的前置条件不同。

部分安全提供方行为与平台有关。可移植的 Windows 机密文件权限投影目前会保持拒绝，直到适配器提供仅所有者可访问的 ACL 信息；Windows 编译成功不能证明所有安全机密文件部署配置都能在该平台运行。所选提供方的约束见[安全边界](../architecture/security.md)。

Linux 专用优化、桌面打包、存储恢复及多节点部署，都需要对应环境中的验证。本页记录 manifest 和源码条件，不声称文档编写期间执行了所有 feature/平台组合。

## 检查实际构建的依赖图

在根工作区执行：

```bash
cargo tree -p rocketmq-broker -e features
cargo tree -p rocketmq-client-rust -e features
cargo build -p rocketmq-proxy --no-default-features --features cluster-mode
cargo build -p rocketmq-controller --no-default-features --features dev-single
```

最后一条命令选择文件存储支持，应配套使用 `storageBackend = "File"` 及预期的单节点开发配置。这些是不同构建选择的示例，并非要求构建全部组合。对于嵌入式应用，应检查该应用自己的 manifest 和 feature 图，因为它可能与上述包级命令不同。

Controller 的 `File` 后端仅在启用 `dev-single` 时可用，单独选择 `storage-file` 不会启用运行时 File 路径。该开发配置还需显式设置 `storageBackend = "File"`。
