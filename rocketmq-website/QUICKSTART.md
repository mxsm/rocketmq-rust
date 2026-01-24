# RocketMQ-Rust Website 快速启动指南

## 🚀 快速开始

### 1. 安装依赖

```bash
cd rocketmq-website
npm install
```

### 2. 启动开发服务器

```bash
npm start
```

网站将在 [http://localhost:3000](http://localhost:3000) 启动。

### 3. 构建生产版本

```bash
npm run build
```

构建产物将输出到 `build/` 目录。

### 4. 预览生产构建

```bash
npm run serve
```

## 📁 项目结构速览

```
rocketmq-website/
├── docs/                    # 文档内容（Markdown）
├── src/                     # React 组件和页面
├── static/                  # 静态资源（图片、文件）
├── i18n/                    # 国际化翻译文件
├── docusaurus.config.ts     # 网站配置
└── sidebars.ts              # 文档侧边栏结构
```

## ✨ 主要功能

- ✅ 完整的文档系统（22+ 篇文档）
- ✅ 国际化支持（英文 + 中文）
- ✅ 响应式设计（移动端友好）
- ✅ 暗色模式支持
- ✅ 代码高亮
- ✅ Mermaid 图表支持
- ✅ 搜索功能
- ✅ TypeScript 类型检查

## 📝 添加新内容

### 添加文档

1. 在 `docs/` 目录创建新的 Markdown 文件
2. 在 `sidebars.ts` 中添加文档引用

### 添加翻译

```bash
npm run write-translations -- --locale zh-CN
```

### 自定义样式

编辑 `src/css/custom.css`

## 🌐 部署

### GitHub Pages

```bash
npm run deploy
```

### 其他平台

上传 `build/` 目录到你的服务器或 CDN。

## 🔧 常用命令

```bash
npm start              # 启动开发服务器
npm run build          # 构建生产版本
npm run serve          # 预览生产构建
npm run typecheck      # TypeScript 类型检查
npm run write-translations  # 生成翻译文件
```

## 📚 文档分类

- Getting Started (3篇)
- Architecture (3篇)
- Producer (3篇)
- Consumer (4篇)
- Configuration (3篇)
- FAQ (3篇)
- Contributing (3篇)

## 🎨 自定义

- 主题颜色: 编辑 `docusaurus.config.ts`
- 网站样式: 编辑 `src/css/custom.css`
- 导航栏: 编辑 `docusaurus.config.ts` 中的 `themeConfig.navbar`
- 页脚: 编辑 `docusaurus.config.ts` 中的 `themeConfig.footer`

## 📖 更多信息

- 完整文档: [PROJECT_STRUCTURE.md](./PROJECT_STRUCTURE.md)
- Docusaurus 文档: https://docusaurus.io/docs
- RocketMQ-Rust GitHub: https://github.com/mxsm/rocketmq-rust
