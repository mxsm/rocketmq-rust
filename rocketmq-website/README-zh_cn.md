# RocketMQ-Rust 网站

RocketMQ-Rust 的官方文档网站，使用 Docusaurus 3.9.2 构建。

## 🚀 技术栈

- **框架**: [Docusaurus 3.9.2](https://docusaurus.io/)
- **语言**: TypeScript
- **Node 版本**: v24.13.0 (参见 `.nvmrc`)
- **样式**: CSS Modules + 自定义 CSS

## 📋 前置要求

- Node.js v24.13.0 或更高版本
- npm 或 yarn 包管理器

## 🛠️ 安装

```bash
# 安装依赖
npm install
```

## 💻 开发

### 启动开发服务器

```bash
# 启动英文版本（默认）
npm run start

# 启动中文版本
npm run start:zh
```

网站将在以下地址可用：

- **英文**: http://localhost:3000/
- **中文**: http://localhost:3000/zh-CN/

### 生产构建

```bash
# 构建所有语言版本
npm run build

# 构建特定语言版本
npm run build -- --locale en
npm run build -- --locale zh-CN
```

### 清理缓存

```bash
npm run clear
```

## 🌍 国际化 (i18n)

本网站支持两种语言：

- **English** (默认): `/`
- **简体中文**: `/zh-CN/`

### 翻译文件结构

```
i18n/
├── en/
│   ├── code.json                     # UI 文本翻译
│   ├── docusaurus-theme-classic/    # 主题翻译
│   └── docusaurus-plugin-content-docs/
└── zh-CN/
    ├── code.json
    ├── docusaurus-theme-classic/
    │   ├── navbar.json               # 导航栏翻译
    │   └── footer.json               # 页脚翻译
    └── docusaurus-plugin-content-docs/
        └── current/                  # 翻译后的文档
```

### 添加翻译

1. **UI 文本**: 编辑 `i18n/{locale}/code.json`
2. **导航栏/页脚**: 编辑 `i18n/{locale}/docusaurus-theme-classic/` 中的文件
3. **文档**: 在 `i18n/{locale}/docusaurus-plugin-content-docs/current/` 中添加/编辑文件

## 📁 项目结构

```
rocketmq-website/
├── docs/                    # 文档源文件（英文）
│   ├── author.md
│   ├── introduction.md
│   ├── getting-started/
│   ├── architecture/
│   ├── producer/
│   ├── consumer/
│   ├── configuration/
│   ├── contributing/
│   └── faq/
├── releases/                # 版本发布说明
│   └── 2024-01-28-v0.1.0.md → 2025-12-07-v0.7.0.md
├── i18n/                    # 国际化文件
│   ├── en/                  # 英文翻译
│   └── zh-CN/               # 中文翻译
├── src/                     # 自定义 React 组件
│   ├── components/          # UI 组件
│   ├── css/                 # 自定义样式
│   ├── pages/               # 自定义页面
│   └── theme/               # 主题定制
├── static/                  # 静态资源（图片、CNAME 等）
├── .docusaurus/             # 构建输出（自动生成）
├── docusaurus.config.ts     # Docusaurus 配置
├── sidebars.ts              # 侧边栏配置
├── package.json             # 依赖和脚本
└── tsconfig.json            # TypeScript 配置
```

详细结构请参见 [PROJECT_STRUCTURE_zh-CN.md](./PROJECT_STRUCTURE_zh-CN.md)。

## 🎨 自定义

### 主题颜色

编辑 `src/css/custom.css` 来自定义主题颜色和样式。

### 组件

自定义 React 组件位于 `src/components/`：

- `HomepageFeatures.tsx` - 首页特性卡片
- `DeveloperStyleHero.tsx` - 开发者风格的 Hero 区块
- `AnnouncementBanner.tsx` - 全站公告横幅
- `DevWarningBanner.tsx` - 开发环境警告横幅
- `OrbBackground.tsx` - 动画球体背景效果
- `SimpleOrb.tsx` - 简单球体组件

## 📝 编写文档

1. 在 `docs/` 目录中创建/编辑 Markdown 文件
2. 使用 `_category_.json` 文件添加分类元数据
3. 对于中文翻译，在 `i18n/zh-CN/docusaurus-plugin-content-docs/current/` 中创建对应文件

### 文档 Frontmatter 示例

```markdown
---
sidebar_position: 1
title: 你的标题
description: 你的描述
---

# 你的内容
```

## 🚢 部署

网站配置为部署到 GitHub Pages：

```bash
npm run deploy
```

配置信息：

- 组织: `apache`
- 项目: `rocketmq-rust`
- 分支: `gh-pages`

## 📚 其他资源

- [Docusaurus 文档](https://docusaurus.io/docs)
- [RocketMQ-Rust 仓库](https://github.com/mxsm/rocketmq-rust)
- [项目结构](./PROJECT_STRUCTURE_zh-CN.md)
- [快速开始指南](QUICKSTART_zh-CN.md)

## 🤝 贡献

欢迎贡献！在提交 Pull Request 之前，请阅读我们的[贡献指南](../CONTRIBUTING.md)。

### 帮助翻译

如果您想帮助翻译文档，请：

1. 检查 `i18n/zh-CN/` 中现有的翻译文件
2. 提交包含您翻译的 Issue 或 Pull Request
3. 查看我们的 [GitHub Issues](https://github.com/mxsm/rocketmq-rust/issues/new/choose) 了解翻译需求

## 📄 许可证

本项目与 RocketMQ-Rust 使用相同的许可证。详情请参见根目录中的 [LICENSE](../LICENSE-APACHE) 文件。

## 📧 联系方式

- GitHub: https://github.com/mxsm/rocketmq-rust
- Issues: https://github.com/mxsm/rocketmq-rust/issues

---

使用 [Docusaurus](https://docusaurus.io/) 用 ❤️ 构建

基于 Apache License 2.0 许可。详情请参见 [LICENSE](LICENSE)。

## 链接

- [RocketMQ-Rust GitHub](https://github.com/mxsm/rocketmq-rust)
- [Apache RocketMQ](https://rocketmq.apache.org/)
- [Docusaurus 文档](https://docusaurus.io/docs)

## 支持

- GitHub Issues: https://github.com/mxsm/rocketmq-rust/issues
- 邮件列表: general@mxsm.apache.org
