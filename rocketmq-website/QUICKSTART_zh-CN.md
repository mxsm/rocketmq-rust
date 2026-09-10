# 开发 RocketMQ-Rust 文档网站

[English](./QUICKSTART.md) · [完整搭建说明](./README-zh_cn.md) · [写作指南](./DOCUMENTATION-zh-CN.md)

本指南启动文档网站。若要运行 RocketMQ-Rust **1.0.0 开发版**服务并收发消息，请阅读[源码安装](./i18n/zh-CN/docusaurus-plugin-content-docs/current/getting-started/installation.md)和[首条消息教程](./i18n/zh-CN/docusaurus-plugin-content-docs/current/getting-started/quick-start.md)。

## 安装与预览

从仓库根目录开始，使用 `.nvmrc` 指定的 Node **24.13.0** 和 npm：

```bash
cd rocketmq-website
node --version
npm ci
npm run start
```

`npm ci` 安装 lockfile 中的依赖集合。日常编辑复用现有依赖，缺少依赖或 lockfile 变化时再安装。终端显示实际地址，通常为 `http://localhost:3000/`。按 Ctrl+C 停止。

用以下命令替代英文开发服务器，启动中文站点：

```bash
npm run start:zh
```

开发模式一次服务一种语言。构建并预览两种语言：

```bash
npm run build
npm run serve
```

静态产物位于 `build/`，中文内容在 `zh-CN/` 下。通过静态预览确认受影响的页面、链接和图示。网站构建不会编译或启动消息服务。

## 同步更新两种语言

1. 同时编辑 `docs/<id>.md` 和 `i18n/zh-CN/docusaurus-plugin-content-docs/current/<id>.md`，保留已有 ID 与 slug。
2. 新页面的 ID 加入 `sidebars.ts`；需要时，在中文 docs 翻译 JSON 中翻译分类名称。
3. 两种语言的命令、配置键、单位与技术限制保持一致；Mermaid 标签及解释文字完整配对。
4. 运行现有构建并检查受影响路由。来源、MDX、资源和示例规则见[写作指南](./DOCUMENTATION-zh-CN.md)。

`npm run write-translations -- --locale zh-CN` 提取界面翻译消息，**不会**翻译 Markdown 正文或自动生成完整中文页面。

## 配置与发布

| 修改内容 | 文件或目录 |
| --- | --- |
| 网站 URL、版本标签、语言、导航及页脚结构 | `docusaurus.config.ts` |
| 文档导航 | `sidebars.ts` |
| 共享样式 | `src/css/custom.css` |
| 界面翻译 | `i18n/zh-CN/code.json` 及插件/主题翻译 JSON |
| 历史发行公告 | `releases/`，目前为两种语言构建共用的英文文章 |

当前文档集面向 **1.0.0 开发版**，不表示 1.0.0 发行产物已经发布。历史发行文章保留原版本。网站私有 package 的 `0.0.0` 版本不是 RocketMQ-Rust 产品版本。

发布使用现有[部署工作流](../.github/workflows/deploy.yml)或有意配置的 `npm run deploy`，与本地预览分开。搜索依赖已配置的外部索引，本地构建不能证明索引已更新。目录职责和路由见[项目结构](./PROJECT_STRUCTURE_zh-CN.md)。
