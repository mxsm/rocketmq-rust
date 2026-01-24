# RocketMQ-Rust Website Quick Start Guide

## 🚀 Quick Start

### 1. Install Dependencies

```bash
cd rocketmq-website
npm install
```

### 2. Start Development Server

```bash
npm run start
```

The website will be available at [http://localhost:3000](http://localhost:3000).

### 3. Build for Production

```bash
npm run build
```

Build output will be placed in the `build/` directory.

### 4. Preview Production Build

```bash
npm run serve
```

## 📁 Project Structure Overview

```
rocketmq-website/
├── docs/                    # Documentation content (Markdown)
├── src/                     # React components and pages
├── static/                  # Static assets (images, files)
├── i18n/                    # Internationalization translation files
├── docusaurus.config.ts     # Website configuration
└── sidebars.ts              # Documentation sidebar structure
```

## ✨ Key Features

- ✅ Complete documentation system (22+ documents)
- ✅ Internationalization support (English + Chinese)
- ✅ Responsive design (mobile-friendly)
- ✅ Dark mode support
- ✅ Code syntax highlighting
- ✅ Mermaid diagram support
- ✅ Search functionality
- ✅ TypeScript type checking

## 📝 Adding New Content

### Adding Documentation

1. Create a new Markdown file in the `docs/` directory
2. Add the document reference in `sidebars.ts`

### Adding Translations

```bash
npm run write-translations -- --locale zh-CN
```

### Customizing Styles

Edit `src/css/custom.css`

## 🌐 Deployment

### GitHub Pages

```bash
npm run deploy
```

### Other Platforms

Upload the `build/` directory to your server or CDN.

## 🔧 Common Commands

```bash
npm start              # Start development server
npm run build          # Build for production
npm run serve          # Preview production build
npm run typecheck      # TypeScript type checking
npm run write-translations  # Generate translation files
```

## 📚 Documentation Categories

- Getting Started (3 docs)
- Architecture (3 docs)
- Producer (3 docs)
- Consumer (4 docs)
- Configuration (3 docs)
- FAQ (3 docs)
- Contributing (3 docs)

## 🎨 Customization

- Theme colors: Edit `docusaurus.config.ts`
- Website styles: Edit `src/css/custom.css`
- Navigation bar: Edit `themeConfig.navbar` in `docusaurus.config.ts`
- Footer: Edit `themeConfig.footer` in `docusaurus.config.ts`

## 📖 More Information

- Full documentation: [PROJECT_STRUCTURE.md](./PROJECT_STRUCTURE.md)
- Docusaurus documentation: https://docusaurus.io/docs
- RocketMQ-Rust GitHub: https://github.com/mxsm/rocketmq-rust
