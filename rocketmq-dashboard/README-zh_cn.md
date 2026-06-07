# RocketMQ-Rust Dashboard

RocketMQ-Rust 的多实现 Dashboard 目录，兼容 Apache RocketMQ。

## 概览

这个目录下面包含多个 Dashboard 实现，它们目标相近，但当前是分别独立维护和构建的项目：

- [rocketmq-dashboard-common](./rocketmq-dashboard-common)：共享模型与通用逻辑
- [rocketmq-dashboard-gpui](./rocketmq-dashboard-gpui)：基于 GPUI 的原生桌面实现
- [rocketmq-dashboard-tauri](./rocketmq-dashboard-tauri)：基于 Tauri 的跨平台桌面实现，前端使用 React 和 TypeScript

注意：`rocketmq-dashboard` 目录本身不是 Cargo workspace 根目录，所以不能在这里直接执行：

```bash
cargo build --workspace
cargo build -p rocketmq-dashboard-tauri
```

这两种写法对当前目录结构都是错误的。

## 环境要求

- Rust 1.85.0 或更高版本
- `rocketmq-dashboard-tauri` 需要 Node.js 和 npm
- 需要满足 Tauri 在当前操作系统上的依赖要求

## `rocketmq-dashboard-tauri` 的正确开发与打包方式

### 开发模式

使用 npm：

```bash
cd rocketmq-dashboard/rocketmq-dashboard-tauri
npm install
npm run tauri dev
```

使用 Cargo：

```bash
cargo install tauri-cli

cd rocketmq-dashboard/rocketmq-dashboard-tauri
cargo tauri dev
```

### 前端构建校验

这一步只会构建前端资源，用于检查 TypeScript 和 Vite 是否通过：

```bash
cd rocketmq-dashboard/rocketmq-dashboard-tauri
npm install
npm run build
```

### 桌面安装包打包

使用 npm：

```bash
cd rocketmq-dashboard/rocketmq-dashboard-tauri
npm install
npm run tauri build
```

使用 Cargo：

```bash
cargo install tauri-cli

cd rocketmq-dashboard/rocketmq-dashboard-tauri
cargo tauri build
```

打包产物默认位于：

```text
rocketmq-dashboard/rocketmq-dashboard-tauri/src-tauri/target/release/bundle/
```

说明：

- `npm run build` 只构建前端，不会生成桌面安装包。
- `cargo build` 即使在 `src-tauri` 目录下执行，也只会编译 Rust 侧代码，不会生成 Tauri 安装包。
- 真正生成桌面安装包的方式是 `npm run tauri build` 或 `cargo tauri build`。
- 如果使用 `cargo tauri build`，需要先安装 `tauri-cli`。

## `rocketmq-dashboard-gpui` 的正确构建方式

开发运行：

```bash
cd rocketmq-dashboard/rocketmq-dashboard-gpui
cargo run
```

Release 构建：

```bash
cd rocketmq-dashboard/rocketmq-dashboard-gpui
cargo build --release
```

## 验证方式

### `rocketmq-dashboard-tauri`

```bash
cd rocketmq-dashboard/rocketmq-dashboard-tauri
npm run build

cd src-tauri
cargo check
cargo test
```

### `rocketmq-dashboard-gpui`

```bash
cd rocketmq-dashboard/rocketmq-dashboard-gpui
cargo check
cargo test
```

## 文档

- [rocketmq-dashboard-common](./rocketmq-dashboard-common/README.md)
- [rocketmq-dashboard-gpui](./rocketmq-dashboard-gpui/README.md)
- [rocketmq-dashboard-tauri](./rocketmq-dashboard-tauri/README.md)

## License

继承父项目 RocketMQ-Rust 的双许可证：

- Apache License 2.0
- MIT License

## Web Dashboard

当前补充：`rocketmq-dashboard-web` 已包含独立 Rust Axum 后端和 React/Vite 前端。后端不加入根 Cargo workspace，避免影响现有 GPUI 与 Tauri 构建。当前已完成健康检查、配置管理、可选 auth/session API 与受保护 API 中间件、文件/SQLite 持久化、Dashboard 首页 `DOWN` 降级、Dashboard history 内存采集、Topic/Broker/Consumer/Producer/Message 只读 RocketMQ Admin 查询、Message trace 查询、Topic create/update/delete 写操作、Broker config update 写操作、Consumer reset offset 写操作、Message direct consume resend、ACL user create/update/delete、ACL policy create/update/delete、DLQ key/messageId 查询、DLQ 分页扫描查询与批量重发/导出 payload、Monitor 本地 JSON 存储，以及面向核心 Web admin service 的 common `DashboardAdminFacade` adapter。剩余硬化工作包括将 Tauri Admin Manager 内部实现进一步上移为 common 可复用模块和补充浏览器 E2E 测试。

`rocketmq-dashboard-web` 是新增的浏览器 Web 版本 Dashboard，目录结构为：

```text
rocketmq-dashboard-web/
  backend/   # Rust 2024 + Axum HTTP API
  frontend/  # React + TypeScript + Vite 前端
```

模块定位：

- `rocketmq-dashboard-common`：共享模型与可复用配置逻辑。
- `rocketmq-dashboard-gpui`：GPUI 原生桌面版本。
- `rocketmq-dashboard-tauri`：Tauri 桌面版本。
- `rocketmq-dashboard-web`：Web 浏览器版本。

Web 后端保持独立 Cargo 项目，不加入根 workspace，避免影响现有 GPUI / Tauri 构建。

后端启动：

```bash
cd rocketmq-dashboard/rocketmq-dashboard-web/backend
cargo run
```

前端启动：

```bash
cd rocketmq-dashboard/rocketmq-dashboard-web/frontend
npm install
npm run dev
```

NameServer 与持久化配置：

```bash
NAMESRV_ADDR=127.0.0.1:9876
DASHBOARD_WEB_STORAGE_BACKEND=file
DASHBOARD_WEB_STORAGE_PATH=data/dashboard-config.json
```

如需 SQLite：

```bash
DASHBOARD_WEB_STORAGE_BACKEND=sqlite
DASHBOARD_WEB_STORAGE_PATH=data/dashboard.db
```

当前已完成：健康检查、配置管理、统一响应结构、REST API 路由表、React/Vite 运维界面骨架、表格搜索分页、loading/error/empty 状态、危险操作确认和消息详情 Drawer。

当前 TODO：将 Tauri 中成熟的 RocketMQ Admin Manager 内部实现进一步上移为可复用模块，并补充浏览器 E2E 测试。
