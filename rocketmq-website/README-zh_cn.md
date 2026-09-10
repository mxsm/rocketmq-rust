# RocketMQ-Rust 网站

[English](README.md) · [文档写作指南](DOCUMENTATION-zh-CN.md)

本目录是 RocketMQ-Rust 文档网站，属于独立 Node 工程，使用 Docusaurus 3.9、React 18、TypeScript 和 Mermaid。英文为默认语言，简体中文在对应翻译目录中使用相同文档 ID。

## 本地开发

使用 `.nvmrc` 中的 Node.js 24.13.0 和 npm。网站工作流采用 Node 24 系列；`package.json` 的最低 engine 声明范围更宽，并不代表本项目工作流验证了所有更早的 Node 版本。

从 `rocketmq-website/` 目录运行：

```bash
node --version
npm --version
npm ci
npm run start
```

`npm ci` 按 `package-lock.json` 安装依赖。日常编辑复用已安装依赖，缺少依赖或 lockfile 变化时再安装。开发服务器默认使用 `http://localhost:3000/`，终端会输出实际地址，按 Ctrl+C 停止。

编写中文页面时运行：

```bash
npm run start:zh
```

中文使用 `/zh-CN/` 前缀。开发服务器服务选定语言；需要同时查看两种语言时，使用完整构建。

## 构建与预览

```bash
npm run build
npm run serve
```

构建包含配置的两种语言，静态输出写入 `build/`。预览命令会打印服务地址，也可以单独构建某种语言：

```bash
npm run build -- --locale en
npm run build -- --locale zh-CN
```

Docusaurus 生成数据过时时使用 `npm run clear`。不要提交 `build/`、`.docusaurus/` 或 `node_modules/`。网站构建不会构建或测试 Rust 消息服务。

## 内容位置

| 位置 | 用途 |
| --- | --- |
| `docs/` | 英文文档 |
| `i18n/zh-CN/docusaurus-plugin-content-docs/current/` | 同 ID 的完整中文文档 |
| `i18n/zh-CN/code.json` | 可翻译组件文本 |
| `i18n/zh-CN/docusaurus-theme-classic/` | 导航与页脚翻译 |
| `sidebars.ts` | 显式文档导航 |
| `releases/` | 发布说明博客源文件 |
| `src/` | 页面、组件、主题定制和样式 |
| `static/` | 图片与其他静态资源 |
| `docusaurus.config.ts` | 网站地址、插件、语言、搜索及版本显示 |

新增教程、技术设计、操作指南或参考页前，阅读[写作指南](DOCUMENTATION-zh-CN.md)，了解双语文件、技术来源、完整示例、图示和保留 URL 的方法。已有实质内容的页面再加入导航，不发布空白占位页。

## 文档版本与发布范围

当前文档标记为 **1.0.0 开发版**。根 Cargo 源码版本与网站 package 版本含义不同，任一数值都不能单独表示存在对应的已发布下载版本。已发布发行物和特定版本信息见项目的 [GitHub releases](https://github.com/mxsm/rocketmq-rust/releases)。

不要把当前源码 API 与旧教程中的依赖版本混用。源码、示例和发行版安装说明需要明确它们描述的版本。

## 部署配置

现有网站配置使用：

- 网站地址：`https://rocketmqrust.com`，base URL 为 `/`。
- GitHub 所有者/项目：`mxsm/rocketmq-rust`。
- 部署分支：`gh-pages`。
- 英文与简体中文两种语言。

仓库的[部署工作流](../.github/workflows/deploy.yml)描述自动发布过程。`npm run deploy` 会执行发布，不是本地预览命令。日常文档编辑使用本地开发或静态预览即可。

## 贡献与维护

保持中英文一致、保留原有路由，并说明未运行示例的适用限制。文档编写不需要新增元数据校验器、文件指纹或审批门禁。修改正文或配置时使用已有构建，并如实报告实际完成的检查。

参与贡献或提问可查看[项目贡献指南](../CONTRIBUTING.md)、[Issues](https://github.com/mxsm/rocketmq-rust/issues)和[Discussions](https://github.com/mxsm/rocketmq-rust/discussions)。
