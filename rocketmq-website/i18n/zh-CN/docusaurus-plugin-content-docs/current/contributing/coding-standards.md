---
title: "编码规范"
---

# 编码规范

遵循归属层现有实现及最近的工程指南。这些约定用于维护公共契约、生命周期所有权和可诊断故障，概括当前仓库规则，不另设贡献流程。

## Rust 结构与公共契约

使用选定工具链、包 edition、`rustfmt.toml` 和 `.clippy.toml`。根目录默认 Rust 2021，Admin CLI 及部分独立应用使用 Rust 2024，应保留各自 manifest 选择。模块/函数使用 snake_case，类型使用 PascalCase，常量使用 SCREAMING_SNAKE_CASE；新 Rust 文件保留 Apache 2.0 头部。

保持实现模块私有，显式选择公共导出。优先使用枚举、配置/请求结构体、builder 和 newtype，避免位置布尔标志及过长参数列表。穷尽匹配项目自有、兼容性敏感的枚举。可选 feature 保持增量性质，变更影响默认或关闭 feature 行为时检查对应组合。

请求码、响应码、请求头、Serde 字段/默认值和持久化布局都属于兼容性表面。保留内部 Rust 调用的重命名仍可能破坏线协议或存储契约。说明有意改变的语义和迁移路径，参见[协议兼容性](../reference/protocol-compatibility.md)。

需要时按内聚行为拆分模块。文件长度是审阅信号，不应为满足数字而任意拆散无关逻辑。Web 后端和 GPUI 本地指南采用不含 `mod.rs` 的扁平模块布局，不应未经核对就把局部规则强加给其他模块。

## 错误与文档

对可恢复的配置、输入、I/O、传输、存储和生命周期故障使用类型化错误。在公共边界映射到已有组件错误模型和稳定描述符，不以通用字符串替代有意义错误，也不匹配面向人的消息来决定重试。

避免生产环境 `todo!`、`unimplemented!` 及可恢复路径中的 `unwrap`/`expect`/panic。测试断言是适当的；有意设计的不可失败门面需要记录不变量或提供可失败的配套 API。限定 lint 豁免范围，并解释原因。

Rustdoc 应说明不明显的不变量，以及适用的 `# Errors`、`# Panics`、`# Safety` 契约。注释解释为什么需要某种选择，不逐行复述代码。unsafe 块应尽量小，前面紧接 `// SAFETY:` 说明，并明确安全包装器或调用方契约。

## 异步执行与关闭

| 关注点 | 设计要求 |
| --- | --- |
| 后台任务 | 通过 `ServiceContext`、`TaskGroup` 或已有生命周期所有者持有工作，关闭时取消并等待完成。 |
| 阻塞操作 | 使用 `BlockingExecutor` 或已有顶层边界，不引入原始 `spawn_blocking`、嵌套 `block_on` 或临时运行时。 |
| 同步 | 不跨 `.await` 持有同步锁守卫，缩小锁范围。 |
| 准入 | 保留有界任务、字节和阻塞预算；超时不一定停止底层阻塞工作。 |
| 异步 trait | 优先原生异步 trait 方法，不引入 `#[async_trait]`。 |
| 完成语义 | 区分已接受、已写入、已持久化、已复制和业务完成。 |

[运行时设计](../architecture/runtime.md)说明所有权和预算边界。只丢弃句柄而不等待所属工作，不等价于完成关闭。运行时变更需要针对改变行为的取消/资源测试，无需指纹或基线仪式。

## 可观测性与敏感数据

优先使用 `#[tracing::instrument(skip_all, ...)]` 并显式列出低基数字段。不得记录凭据、ACL/TLS 材料、令牌、消息体或整个请求/配置对象。避免未采样的逐消息 span 和无界主题/消费者组标签。

使用已有错误脱敏及遥测所有权路径。某个包装器具有安全 `Debug` 实现，不代表任意字符串都可安全记录。[错误与可观测性](../architecture/errors-observability.md)解释信号所有权和边界映射。

## 前端与桌面代码

| 工程 | 本地约定 |
| --- | --- |
| 网站 | Docusaurus/MDX、已有组件和页面 ID；英文/中文配对正文及导航翻译。 |
| Web 前端 | React/TypeScript/Vite、共享设计 token/组件和统一 `src/api/` 客户端；运维表格包含加载、空、错误、搜索、分页及刷新状态。 |
| Web 后端 | 精简 Axum handler、服务编排、分离 API DTO 与内部模型、显式本地错误映射；可复用逻辑放入 Dashboard common。 |
| GPUI | 确定性渲染路径，通过 Context/Window 修改状态，稳定元素 ID、持有所属订阅和不阻塞 UI 的工作。 |
| Tauri | 从应用根目录运行前端命令，从 `src-tauri` 运行 Rust 命令；前端资源编译与桌面打包分开。 |

保留无障碍、焦点、键盘操作、明暗主题一致性和可用的窗口缩放。破坏性操作的产品确认对话框仍属于应用行为，与文档编写流程无关。不要把内部迁移或 API 对齐说明暴露为面向用户的产品控件。

## 选择相关检查

使用能够说明变更效果的最小目标，例如模型变更可以运行：

```bash
cargo fmt -p rocketmq-model -- --check
cargo test -p rocketmq-model --lib
```

这些是对应包的示例，不是每次编辑都要执行的命令。复用有意义测试，并查看实际测试数量。完成编译的测试可以替代重复的 `cargo check`；只有存在具体问题时才增加包级 Clippy 或消费者验证。不要在有无关 Rust 修改时执行会改写整个工作区的格式化。

网站渲染内容变化时，在 `rocketmq-website` 运行 `npm run build`。独立工程采用各自本地检查配置。完整 feature/平台矩阵、互操作、长时间故障测试和发行验证属于需要对应证据的任务，不宣称未运行场景已通过。

来源：[根规则](https://github.com/mxsm/rocketmq-rust/blob/main/AGENTS.md)、[Web 前端](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-web/frontend/AGENTS.md)、[Web 后端](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-web/backend/AGENTS.md)、[GPUI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-gpui/AGENTS.md)、[Tauri](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-tauri/AGENTS.md)、[网站](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/AGENTS.md)。
