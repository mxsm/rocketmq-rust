# rocketmq-admin-tui

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-admin-tui` 是 RocketMQ Rust 的交互式终端管理面板。它使用 Ratatui 和 crossterm 构建终端体验，所有 RocketMQ 管理能力都通过
`TuiAdminFacade` 委托给 `rocketmq-admin-core`。

该 crate 面向希望通过可搜索、键盘驱动界面管理 RocketMQ 的运维和开发用户，同时避免重复实现 CLI 解析或 RocketMQ RPC 逻辑。当前命令目录包含 18 个
RocketMQ 管理域，共 102 个 facade-backed 管理命令。

[English](README.md)

## 架构

![rocketmq-admin-tui architecture](../../../resources/admin-tui-architecture.svg)

稳定的运行时流程如下：

```text
terminal event -> app action -> state/form validation -> TuiAdminFacade -> admin-core DTO/service -> result view model -> Ratatui renderer
```

TUI 负责交互、状态、布局和渲染。核心管理请求、校验、RPC 编排和结构化结果由 `rocketmq-admin-core` 负责。

## 预览

![rocketmq-admin-tui preview](../../../resources/rocketmq-cli-ui.png)

## 核心能力

- 可搜索的命令树，并按 RocketMQ 管理域分组。
- 五个焦点区域：NameServer、Search、Commands、Parameters、Result。
- 键盘优先工作流，带上下文快捷键提示和内置帮助浮层。
- 类型化参数模型，支持 string、optional string、number、boolean、enum、key/value map 和毫秒时间戳。
- 命令执行前执行表单级校验。
- 基于风险等级的执行模型：
  - safe 命令直接执行；
  - mutating 命令需要输入 `confirm`；
  - dangerous 命令在可用时需要输入目标值。
- 后台命令通过独立 Tokio runtime 执行，避免阻塞终端渲染和输入处理。
- 长时间工作流支持进度更新，例如 monitoring 和 message pull。
- 支持以 table、key/value、JSON、text、operation summary 渲染结构化结果，并支持纵向和横向滚动。
- 边界测试确保 `rocketmq-admin-tui -> rocketmq-admin-core`，并拒绝依赖 CLI adapter。

## 快速开始

在仓库根目录的交互式终端中运行：

```bash
cargo run -p rocketmq-admin-tui
```

TUI 启动时不强制要求 NameServer 地址。执行需要访问集群的命令前，可在 NameServer 焦点区域设置地址。

常用按键：

| 按键 | 行为 |
|---|---|
| `Tab` / `Shift+Tab` | 在 NameServer、Search、Commands、Parameters、Result 之间移动焦点。 |
| `n` | 在非参数编辑状态下聚焦 NameServer 输入。 |
| `/` 或 `s` | 在非参数编辑状态下聚焦命令搜索。 |
| `j` / `k` 或方向键 | 移动命令、参数或结果行。 |
| `Left` / `Right` | 折叠命令组、切换 enum 参数或横向滚动结果列。 |
| `Space` | 切换 boolean 参数。 |
| `Enter` | 提交输入、选择命令、执行或确认。 |
| `Ctrl+R` | 重新执行当前命令。 |
| `Ctrl+L` | 清空当前结果。 |
| `?` | 打开或关闭帮助。 |
| `Esc` | 关闭帮助、取消本地等待中的任务或退出。 |
| `q` | 退出或关闭帮助。 |

## 命令覆盖

命令目录定义在 `src/commands/catalog.rs`，并由测试保护。当前覆盖如下：

| 领域 | 命令数 | 示例 |
|---|---:|---|
| Auth | 12 | user 和 ACL 的 get/list/create/update/delete/copy。 |
| Broker | 15 | config、runtime stats、consume stats、epoch、cleanup、cold data flow control、commitlog read-ahead、timer engine。 |
| Cluster | 3 | cluster list、broker names、send-message RT 诊断。 |
| Connection | 2 | consumer 和 producer connection 检查。 |
| Consumer | 8 | config、running info、progress、monitoring、subscription group、consume mode。 |
| Container | 2 | broker container 中的 add/remove broker。 |
| Controller | 5 | config、metadata、elect master、clean metadata。 |
| Export | 6 | configs、metrics、metadata、RocksDB metadata、RocksDB RPC export、POP records。 |
| HA | 2 | HA status 和 sync-state-set query。 |
| Lite | 6 | broker、parent topic、lite topic、group、client、dispatch。 |
| Message | 12 | decode、query、trace、direct consume、dump compaction log、print、consume。 |
| NameServer | 6 | config、KV config、write permission。 |
| Offset | 5 | clone、consumer status、skip accumulated、reset by time。 |
| Producer | 4 | producer info、send message、send status、send RT。 |
| Queue | 2 | consume queue 和 RocksDB CQ write progress。 |
| Static Topic | 2 | update 和 remap static topic。 |
| Stats | 1 | stats-all query。 |
| Topic | 9 | list、cluster、route、status、update、permission、delete、order config、allocate MQ。 |

## 运行模型

`RocketmqTuiApp` 持有事件循环。它以 30 FPS tick，读取 crossterm 事件，应用内部 action，并渲染当前 `AppState`。

命令执行与 UI 处理解耦：

1. 选中的 `CommandSpec` 定义参数、结果视图类型和风险等级。
2. `CommandFormState` 校验用户输入的表单值。
3. `execute_command_with_progress` 根据 command ID 分发。
4. `TuiAdminFacade` 将表单值转换为 `rocketmq-admin-core` request DTO。
5. core service 执行管理操作。
6. `CommandResultViewModel` 将结构化结果转换成适合 TUI 渲染的 table、JSON、text、key/value 或 summary。
7. 已取消本地任务的迟到结果会通过 execution ID 被忽略。

## 边界约定

`rocketmq-admin-tui` 必须保持为终端 UI adapter：

- 依赖 `rocketmq-admin-core`，不依赖 `rocketmq-admin-cli`。
- 不使用 `clap`、`clap_complete`、`tabled`、`colored`、`dialoguer` 或 `indicatif`。
- 不调用 CLI command module，也不解析 CLI command struct。
- 共享 admin request/result/service 行为属于 `rocketmq-admin-core`。
- TUI 专属能力放在本 crate：layout、focus、command catalog、form、result view model、keyboard action、progress display 和 terminal rendering。

这些规则由 `tests/no_cli_dependency.rs` 保护。

## Crate 布局

```text
rocketmq-admin-tui/
├── src/
│   ├── main.rs                 # Terminal 初始化和 app 启动
│   ├── rocketmq_tui_app.rs     # 事件循环、action 处理、后台任务
│   ├── state.rs                # App state、form state、校验、focus model
│   ├── ui.rs                   # Ratatui layout 和 rendering
│   ├── action.rs               # 内部 action message
│   ├── event.rs                # 键盘辅助函数
│   ├── admin_facade.rs         # TUI 到 admin-core 的 facade
│   ├── admin_facade/           # Core request builders 和 async operations
│   ├── commands.rs             # Command metadata surface
│   ├── commands/               # Catalog 和 executor dispatch
│   └── view_model/             # 终端渲染用结果转换
└── tests/
    └── no_cli_dependency.rs    # Adapter 边界保护
```

## 新增 TUI 命令

1. 在 `rocketmq-admin-core` 中添加或复用 admin request/result/service。
2. 在 `TuiAdminFacade` 中添加 request-builder 和 async operation 方法。
3. 在合适的 catalog domain 中添加 `CommandSpec`。
4. 在 `execute_command_with_progress` 中接入 command ID。
5. 将结果转换为 `CommandResultViewModel`。
6. 为 catalog 覆盖、参数校验、facade 映射和结果渲染添加聚焦测试。

## 验证

如果只修改文档，通常执行本地 Markdown/SVG 检查即可。如果修改本 crate 的 Rust 代码，运行：

```bash
cargo test -p rocketmq-admin-tui
```

如果修改了 root workspace 内的 Rust 代码，还需要在 workspace 根目录运行仓库要求的检查：

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## 相关 Crates

- [`rocketmq-admin-core`](../rocketmq-admin-core) - 可复用 admin request、service 和 result 层。
- [`rocketmq-admin-cli`](../rocketmq-admin-cli) - 复用同一个 core 层的命令行适配器。
- [`rocketmq-remoting`](../../../rocketmq-remoting) - RocketMQ remoting 协议和 RPC 类型。
- [`rocketmq-client`](../../../rocketmq-client) - admin service 使用的 RocketMQ client API。

## License

基于 [Apache License, Version 2.0](../../../LICENSE-APACHE) 发布。
