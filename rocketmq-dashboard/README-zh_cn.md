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
