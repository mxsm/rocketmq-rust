---
title: "安装与源码构建"
---

本页为本地教程准备当前 Rust 源码，构建 Rust NameServer 和 Broker，不使用 Java 服务端镜像替代。需要旧发行版时，应完整采用该发行版对应的安装说明和 API。

## 选择源码与工具链

仓库在 `rust-toolchain.toml` 固定 Rust **1.95.0**，根 MSRV 也为该版本。准备 Git、rustup/Cargo 及操作系统对应的编译/链接工具。Windows MSVC 构建需要 Visual C++ Build Tools 与 Windows SDK，Unix 构建需要可用的原生编译器和链接器。可选原生依赖由实际 Cargo 依赖图决定，应按缺失工具的错误定位，不必先安装所有产品的可选依赖。

```bash
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust
rustup show active-toolchain
cargo --version
```

后续命令默认从仓库根目录运行，另有说明除外。rustup 会使用仓库选择的工具链。根源码包版本为 1.0.0；假定存在同标签 crate、压缩包或镜像之前，先查看 [GitHub releases](https://github.com/mxsm/rocketmq-rust/releases)。

## 只构建本次需要的组件

单机教程使用：

```bash
cargo build -p rocketmq-namesrv --bin rocketmq-namesrv-rust
cargo build -p rocketmq-broker --bin rocketmq-broker-rust
cargo build -p rocketmq-admin-cli --bin rocketmq-admin-cli
```

默认输出目录为 `target/debug/`，Windows 文件带 `.exe`。配置 `CARGO_TARGET_DIR` 后输出位置会改变。需要优化构建时加 `--release`，之后统一使用 `target/release/`。

| 目标 | 用途 |
| --- | --- |
| `rocketmq-namesrv-rust` | 主题 路由发现与 Broker 注册 |
| `rocketmq-broker-rust` | 本地文件存储和消息处理 |
| `rocketmq-admin-cli` | 显式创建教程 主题/消费者组，检查路由 |

当前 Admin CLI 显式启用 Admin Core 的 `rocksdb-export` 依赖，即使运行中的 Broker 使用 `LocalFile`，构建该 CLI 仍需要 RocksDB 原生构建条件，包括 C++ 编译器及用于生成绑定的 Clang/libclang。对 CLI 使用 `--no-default-features` 不会移除这项显式依赖。准备构建机器时应核对 [CLI manifest](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/Cargo.toml)。

首个消息客户端是 `rocketmq-website/examples/first-message/` 下的小型独立包，依赖本 checkout 的源码路径：

```bash
cargo build --manifest-path rocketmq-website/examples/first-message/Cargo.toml
```

其代码沿用已有 生产者/LitePull 示例模式，并统一 主题 和 Group。它不属于根 workspace，也不发布为 crate；请保留在源码 checkout 内，以便相对依赖路径有效。

## 其他示例与产品

完整示例集合也是独立工程：

```bash
cd rocketmq-example
cargo build --example producer-simple
cargo build --example consumer-lite-pull
```

这两个示例内置的 主题 不同，原样运行不会形成配套收发。使用[第一条消息应用](quick-start.md)，或有意统一各示例常量并创建对应资源。

Proxy 构建涉及 protobuf 生成，需要 `protoc`。RocksDB 和桌面 UI 可能引入其他原生工具。网站需要 Node/npm，但最小 Rust 服务/客户端路径不需要它们。独立 Dashboard 或 AI 产品按自己的 manifest 构建，不将其全部纳入首次安装。

## 构建成功说明了什么

使用构建的程序或 Cargo 目标查看真实选项：

```bash
cargo run -p rocketmq-namesrv --bin rocketmq-namesrv-rust -- --help
cargo run -p rocketmq-broker --bin rocketmq-broker-rust -- --help
cargo run -p rocketmq-admin-cli -- --help
```

编译与帮助输出说明可执行程序可用，不能证明 Broker 注册、消息投递、TLS 配置或恢复已经成功。[本地源码搭建](local-source.md)解释进程和数据目录，[快速开始](quick-start.md)随后走通消息链路。

磁盘不足时，可在对应 workspace 运行 `cargo clean`，或为示例指定 `--manifest-path`，清理 Cargo 构建产物。这不会删除消息数据，不应把清理编译输出与删除 Broker 存储目录混淆。

来源：[根工具链](https://github.com/mxsm/rocketmq-rust/blob/main/rust-toolchain.toml)、[workspace 清单](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml)、[示例清单](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-example/Cargo.toml)。
