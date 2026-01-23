# RocketMQ-Rust Website 项目结构说明

## 项目概述

这是一个基于 Docusaurus v3.9.2 构建的 RocketMQ-Rust 官方文档网站，支持国际化（英文和中文），采用 React 18 + TypeScript 技术栈。

## 目录结构

```
rocketmq-website/
├── docusaurus.config.ts           # Docusaurus 主配置文件
├── sidebars.ts                    # 文档侧边栏配置
├── package.json                   # 项目依赖管理
├── tsconfig.json                  # TypeScript 配置
├── README.md                      # 项目说明
├── .gitignore                     # Git 忽略文件
├── .nvmrc                         # Node.js 版本管理
│
├── docs/                          # 文档内容目录
│   ├── introduction.md            # 项目介绍
│   │
│   ├── getting-started/           # 快速入门
│   │   ├── installation.md        # 安装指南
│   │   ├── quick-start.md         # 快速开始
│   │   └── basic-concepts.md      # 基本概念
│   │
│   ├── architecture/              # 架构文档
│   │   ├── overview.md            # 架构概述
│   │   ├── message-model.md       # 消息模型
│   │   └── storage.md             # 存储机制
│   │
│   ├── producer/                  # 生产者文档
│   │   ├── overview.md            # 生产者概述
│   │   ├── sending-messages.md    # 发送消息
│   │   └── transaction-messages.md # 事务消息
│   │
│   ├── consumer/                  # 消费者文档
│   │   ├── overview.md            # 消费者概述
│   │   ├── push-consumer.md       # 推送消费者
│   │   ├── pull-consumer.md       # 拉取消费者
│   │   └── message-filtering.md   # 消息过滤
│   │
│   ├── configuration/             # 配置文档
│   │   ├── broker-config.md       # Broker 配置
│   │   ├── client-config.md       # 客户端配置
│   │   └── performance-tuning.md  # 性能调优
│   │
│   ├── faq/                       # 常见问题
│   │   ├── common-issues.md       # 常见问题
│   │   ├── performance.md         # 性能问题
│   │   └── troubleshooting.md     # 故障排除
│   │
│   └── contributing/              # 贡献指南
│       ├── overview.md            # 贡献概述
│       ├── development-guide.md   # 开发指南
│       └── coding-standards.md    # 代码规范
│
├── src/                           # 源代码目录
│   ├── components/                # React 组件
│   │   ├── HomepageFeatures.tsx   # 首页特性组件
│   │   └── HomepageFeatures.module.css
│   │
│   ├── pages/                     # 页面组件
│   │   ├── index.tsx              # 首页
│   │   └── index.module.css
│   │
│   └── css/                       # 样式文件
│       └── custom.css             # 自定义样式
│
├── static/                        # 静态资源
│   └── img/                       # 图片资源
│       └── rust-logo.svg          # Rust Logo
│
└── i18n/                          # 国际化文件
    ├── en/                        # 英文翻译
    │   ├── code.json
    │   └── docusaurus-theme-classic.json
    │
    └── zh-CN/                     # 中文翻译
        ├── code.json
        └── docusaurus-theme-classic.json
```

## 核心配置文件

### 1. docusaurus.config.ts

Docusaurus 的主配置文件，包含：
- 网站元数据（标题、描述、URL）
- 主题配置（导航栏、页脚）
- 文档配置
- 国际化配置
- 插件配置

### 2. sidebars.ts

定义文档的侧边栏结构，将文档组织成分类：
- Getting Started
- Architecture
- Producer
- Consumer
- Configuration
- FAQ
- Contributing

### 3. package.json

定义项目依赖和脚本：
- Docusaurus 核心依赖
- React 和 TypeScript
- 构建脚本（start、build、serve）

## 技术栈

### 框架与库
- **Docusaurus**: v3.9.2（静态网站生成器）
- **React**: v18.3.1（UI 框架）
- **TypeScript**: v5.3.3（类型系统）
- **Prism**: v2.3.0（代码高亮）

### 开发工具
- **Node.js**: 18.0+
- **npm**: 9.0+

## 文档结构

### 文档分类

1. **Getting Started** (3篇)
   - Installation: 安装指南
   - Quick Start: 快速开始教程
   - Basic Concepts: 核心概念介绍

2. **Architecture** (3篇)
   - Overview: 系统架构概述
   - Message Model: 消息模型详解
   - Storage: 存储机制

3. **Producer** (3篇)
   - Overview: 生产者概述
   - Sending Messages: 消息发送
   - Transaction Messages: 事务消息

4. **Consumer** (4篇)
   - Overview: 消费者概述
   - Push Consumer: 推送消费者
   - Pull Consumer: 拉取消费者
   - Message Filtering: 消息过滤

5. **Configuration** (3篇)
   - Broker Config: Broker 配置
   - Client Config: 客户端配置
   - Performance Tuning: 性能调优

6. **FAQ** (3篇)
   - Common Issues: 常见问题
   - Performance: 性能问题
   - Troubleshooting: 故障排除

7. **Contributing** (3篇)
   - Overview: 贡献概述
   - Development Guide: 开发指南
   - Coding Standards: 代码规范

## 国际化 (i18n)

### 支持的语言
- **en** (English): 默认语言
- **zh-CN** (简体中文): 预留扩展

### 翻译文件位置
- `i18n/en/`: 英文翻译
- `i18n/zh-CN/`: 中文翻译

### 翻译内容
- 主题 UI 文本（导航栏、页脚等）
- 文档内容
- 首页内容

## 自定义功能

### 1. 首页 (Landing Page)
- 渐变背景设计
- 特性展示（6个特性卡片）
- 快速开始按钮
- GitHub 链接

### 2. 自定义样式
- 主题颜色定制
- 暗色模式优化
- 响应式设计
- 自定义滚动条

### 3. Mermaid 图表支持
- 架构图
- 流程图
- 序列图

## 构建命令

```bash
# 安装依赖
npm install

# 启动开发服务器
npm start

# 构建生产版本
npm run build

# 预览生产构建
npm run serve

# 生成翻译文件
npm run write-translations

# 类型检查
npm run typecheck
```

## 部署

### GitHub Pages
```bash
npm run deploy
```

### 其他平台
构建后部署 `build/` 目录到任何静态网站托管服务（Nginx、CDN 等）。

## 扩展建议

### 1. 多语言扩展
- 添加更多语言版本（日语、韩语等）
- 使用 `npm run write-translations -- --locale <locale>` 生成翻译模板

### 2. 多版本文档
- 使用 Docusaurus 的版本功能维护多个版本文档
- 配置 `versions.json` 管理版本

### 3. 博客功能
- 在 `docusaurus.config.ts` 中启用博客功能
- 创建 `blog/` 目录存放博客文章

### 4. 搜索功能
- 集成 Algolia DocSearch
- 或使用本地搜索插件

## 维护建议

1. **定期更新依赖**: `npm update`
2. **检查构建警告**: 确保 `npm run build` 无警告
3. **测试多语言**: 切换语言验证翻译
4. **性能优化**: 定期检查 Lighthouse 分数
5. **备份数据**: 定期备份文档内容

## 资源链接

- [Docusaurus 官方文档](https://docusaurus.io/docs)
- [React 官方文档](https://react.dev)
- [TypeScript 官方文档](https://www.typescriptlang.org/docs)
- [RocketMQ-Rust GitHub](https://github.com/apache/rocketmq-rust)
