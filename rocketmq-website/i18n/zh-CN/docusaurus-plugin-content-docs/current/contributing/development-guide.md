---
title: "开发指南"
---

# 开发指南

有效的开发循环从负责目标行为的包开始。仓库包含主 Cargo workspace、独立 Cargo 应用和 Node 项目；在根目录构建一次无法覆盖所有项目。选择命令之前，先通过[模块地图](../architecture/module-map.md)定位责任归属。

## 准备工具链与工作目录

安装 Git 和 rustup，克隆仓库，然后在仓库根目录运行：

```bash
git clone https://github.com/mxsm/rocketmq-rust.git
cd rocketmq-rust
rustup toolchain install 1.95.0 --profile minimal --component rustfmt,clippy
rustc --version
cargo --version
git status --short
```

仓库中的 `rust-toolchain.toml` 选择 Rust 1.95.0。保留所修改包清单中的 edition；主 workspace 默认为 Rust 2021，部分独立项目和 Admin CLI 使用 Rust 2024。编辑器中的 rust-analyzer 应打开对应的 Cargo 根目录，独立项目尤其如此。

原生构建依赖取决于所选目标。启用 RocksDB 的构建需要 C++ 工具链和 Clang/libclang；即使教程 Broker 使用 LocalFile，Admin CLI 仍启用了 RocksDB 导出依赖。Proxy 的协议代码生成需要 `protoc`。环境配置参见[安装指南](../getting-started/installation.md)，不要把缺少原生依赖当作 Rust API 故障。

## 选择项目边界

| 修改内容 | 命令选择位置 | 覆盖范围 |
| --- | --- | --- |
| 根 `Cargo.toml` 列出的成员 | 仓库根目录，使用 `-p` 和清单中的包名 | 所选包及其必需依赖 |
| 生产者、消费者示例 | `rocketmq-example/`，遵循该目录指南 | 独立示例 workspace |
| 首条消息教程应用 | 仓库根目录，显式指定 `--manifest-path` | 网站自带的完整示例 |
| Dashboard 通用模型 | 主 workspace | 共享模型，不包含所有 Dashboard 应用 |
| Web、GPUI、Tauri Dashboard | 对应应用目录；Web 的后端和前端根目录各自独立 | 对应应用构建或前端产物 |
| MCP、MCP Control、SRE | 对应独立项目根目录 | 对应服务；SRE UI 和 TypeScript SDK 是独立 Node 项目 |
| 网站 | `rocketmq-website/` | Docusaurus 页面、路由、翻译和站点资源 |

编辑前阅读最近的 `AGENTS.md`、包清单、README 和已有测试。局部指南决定适用的验证范围；其中的命令不是叠加在全 workspace 检查单上的额外要求。

例如，在仓库根目录执行以下只读命令，可以查看当前主 workspace 成员，以及客户端包的 feature 依赖树：

```bash
cargo metadata --no-deps --format-version 1
cargo tree -p rocketmq-client-rust -e features
```

`rocketmq-client` 是目录名；`rocketmq-client-rust` 才是 `-p` 使用的 Cargo 包名。包内的可执行程序也可能使用不同名称，或存在多个可执行程序。应检查 `[[bin]]` 或 CLI 的 `--help`，不要假定裸 `cargo run` 会选择目标应用。

## 建立聚焦的反馈循环

假设修改发生在 `rocketmq-model` 包，在仓库根目录执行：

```bash
cargo fmt -p rocketmq-model -- --check
cargo test -p rocketmq-model --lib
```

测试命令会编译该目标并运行库测试。行为变更更小时，可以选择已有测试名或模块；检查结果中的测试数量，避免把拼错过滤条件造成的零测试当作成功。只需要编译检查时，使用 `cargo check -p rocketmq-model`。针对改变的行为增加聚焦回归覆盖，避免编写只是重复实现的测试。

修改其他包时，应替换为清单中的真实包名，并选择相关 feature 和目标。保持可选 feature 的可选性：全 feature 测试并不能证明默认或禁用 feature 的组合正常。变更有需要时，可以执行包级 Clippy：

```bash
cargo clippy -p rocketmq-model --no-deps -- -D warnings
```

独立 Cargo 项目应在自己的根目录执行命令。存在无关修改时，不要运行会改写整个 workspace 的格式化命令。完整集成、互操作、故障恢复、性能或平台矩阵应服务于需要这些证据的工作，不是每次局部修改的前置条件。

## 运行并调试真实消息链路

使用[本地源码搭建](../getting-started/local-source.md)在隔离的教程数据目录启动 NameServer 和 Broker，然后按照[快速开始](../getting-started/quick-start.md)创建主题和消费者组，运行配套应用。这样可以避免消费者订阅主题与生产者内置示例主题不同造成的排查偏差。

示例清单位于 `rocketmq-website/examples/first-message/Cargo.toml`。它显式持有并关闭生产者或消费者、客户端运行时、进程运行时和遥测设施。提取最小复现时应保留这些生命周期边界；遗留应用持有的任务会改变正在调查的故障。

配置调试器时，选择真实的包和可执行程序，并提供与成功命令行一致的工作目录、配置和环境变量。首先观察发生问题的边界：

| 现象 | 优先收集的证据 |
| --- | --- |
| 客户端找不到路由 | NameServer 地址、Broker 注册、Broker 对外公布地址、主题路由 |
| 发送超时 | 客户端截止时间、Broker 响应、存储及副本确认状态；单凭超时不能证明没有追加 |
| 消费者收不到消息 | 订阅、消费者组、消费模式、队列分配、已存偏移量、过滤条件 |
| 关闭过程卡住 | 任务所有权、排空截止时间、正在运行的阻塞工作、关闭报告 |

对应流程参见[首次诊断](../operations/first-diagnosis.md)和[运行时设计](../architecture/runtime.md)。限制日志量，脱敏凭据、消息体以及完整请求和配置对象。

## 准备便于审阅的变更

让代码、相关测试和说明聚焦于本次改变的行为。公开协议字段、持久化格式、配置名称和公共 API 都属于兼容性边界；修改时应说明迁移方式。为后台工作说明取消与关闭行为，并保留产品的认证和授权边界。

仓库的 Issue 和 PR 模板提供需要填写的公开字段。说明具体触发条件、变更后的行为，以及实际执行的检查，包括相关 feature 和项目根目录。区分实际观察结果与未验证场景，保留无关的本地修改。

纯文档工作遵循[文档贡献指南](./documentation.md)。它需要常规内容检查，并在渲染内容变更时运行现有网站构建；不要求源码指纹、干净 checkout、审批阶段或新增 CI 门禁。

## 源码依据

- [主 workspace 清单](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml)与[工具链选择](https://github.com/mxsm/rocketmq-rust/blob/main/rust-toolchain.toml)。
- [仓库工程指南](https://github.com/mxsm/rocketmq-rust/blob/main/AGENTS.md)与[示例工程指南](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-example/AGENTS.md)。
- [可运行的首条消息应用](https://github.com/mxsm/rocketmq-rust/tree/main/rocketmq-website/examples/first-message)。
