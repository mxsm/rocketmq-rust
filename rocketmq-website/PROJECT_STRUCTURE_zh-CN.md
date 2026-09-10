# RocketMQ-Rust 网站项目结构

[English](./PROJECT_STRUCTURE.md) · [搭建说明](./README-zh_cn.md) · [写作指南](./DOCUMENTATION-zh-CN.md)

网站是独立的 Docusaurus 3.9 / React 18 / TypeScript 工程，当前文档描述 **RocketMQ-Rust 1.0.0 开发版**。`.nvmrc` 选择 Node **24.13.0**，`package-lock.json` 记录实际解析的 Node 依赖。Cargo 构建不能验证网站。

## 目录职责

| 路径 | 职责 |
| --- | --- |
| `docs/` | 当前英文技术正文 |
| `i18n/zh-CN/docusaurus-plugin-content-docs/current/` | 同 ID 的完整中文正文 |
| `i18n/zh-CN/docusaurus-plugin-content-docs/current.json` | 中文版本及侧栏分类标签 |
| `releases/` | 使用博客插件的英文历史发行文章，目前由两种语言构建共用 |
| `src/pages/` | 独立于文档侧栏的自定义页面 |
| `src/components/`、`src/theme/`、`src/css/` | 组件、主题覆盖及样式 |
| `static/` | 复制到网站的静态图片与文件 |
| `examples/` | 首条消息独立 Rust 应用及部署配置示例 |
| `docusaurus.config.ts` | 网站 URL、语言、docs/blog 插件、版本标签与主题配置 |
| `sidebars.ts` | 显式技术文档导航 |
| `i18n/zh-CN/code.json`、`i18n/zh-CN/docusaurus-theme-classic/` | 界面、导航栏及页脚翻译 |
| `.docusaurus/`、`build/`、`node_modules/` | 生成元数据、静态产物及安装依赖，不是作者维护的正文 |

## 技术内容组织

主体文档方案包含 79 篇逻辑页面，每篇都有完整英文与中文文件。旧入口及发布索引是额外补充页面。

| 分类 | 内容 |
| --- | --- |
| Introduction 与 overview | 项目身份、能力及发行范围 |
| `getting-started/` | 概念、安装、本地服务及首条消息 |
| `architecture/` | 模块所有权、消息流程、运行时、存储、路由、HA、Proxy、安全及设计取舍 |
| `producer/`、`consumer/`、`guides/` | API、生命周期、重试、过滤、顺序、事务、延迟召回及请求应答 |
| `deployment/`、`operations/` | 拓扑、安全、资源准备、监控、诊断、维护及恢复 |
| `configuration/`、`reference/`、`migration/` | 字段与默认值、构建特性、错误、CLI、协议/API 契约及迁移 |
| `ecosystem/` | Web/原生 Dashboard、只读 MCP、MCP Control 及 AI SRE |
| `contributing/` | 开发、编码、文档、测试及发行工程 |
| `faq/`、`author.md`、`release-notes/` | 保留的补充 URL，以及指向持续维护技术正文和发行内容的链接 |

实际顺序以 `sidebars.ts` 为准。`_category_.json` 不能代替显式侧栏条目。新页面需要两种语言正文，以及可到达的导航或交叉引用入口。

## 路由、版本与翻译

英文技术页面使用 `/docs/<route>`，中文使用 `/zh-CN/docs/<route>`。默认路由由文件路径形成，但必须保留已有显式 ID/slug。发行历史使用 `/releases`，中文构建使用带语言前缀的路由。当前没有中文博客正文目录，因此中文路由不代表历史文章已经翻译；双语技术发布索引在 `docs/release-notes/` 下单独维护。`baseUrl`、尾斜杠行为及生产 URL 统一配置。

当前文档标签为 `1.0.0 (development)`，中文为 `1.0.0（开发版）`，在技术页面中可见。更改标签不会创建已发布的 1.0.0 快照。保留现有 `current/` 翻译路径；这套开发文档无需另建 `versioned_docs/` 或 `versions.json`。

界面消息提取与正文翻译是不同工作。`npm run write-translations -- --locale zh-CN` 提取消息；作者仍需维护完整中文 Markdown 和图示标签。历史发行正文保留原先描述的版本。

搜索通过主题的外部 Algolia 集成配置，本地正文构建不会更新远端爬虫或索引。源码编辑链接使用 `editLocalizedFiles: true`，使中文页面打开对应中文源文件。

## 构建与维护

依赖安装和本地预览见[快速开始](./QUICKSTART_zh-CN.md)。`npm run build` 渲染两种语言，`npm run serve` 预览结果。检查受影响路由、代码块、图示和链接，修复内容变更引入的问题。生成产物不纳入源码提交。

首条消息示例拥有独立 Cargo manifest 和路径依赖。部署示例是服务输入，不是 Node 应用配置。可执行行为变化时，在所属运行环境中验证对应示例；Docusaurus 构建只验证网站渲染。

发布遵循现有[部署工作流](../.github/workflows/deploy.yml)。静态托管、远端搜索索引与 Rust 产品发行均独立于网站编辑。
