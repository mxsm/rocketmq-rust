# RocketMQ-Rust Website

Official documentation website for RocketMQ-Rust, built with Docusaurus 3.9.2.

## 🚀 Tech Stack

- **Framework**: [Docusaurus 3.9.2](https://docusaurus.io/)
- **Language**: TypeScript
- **Node Version**: v24.13.0 (see `.nvmrc`)
- **Styling**: CSS Modules + Custom CSS

## 📋 Prerequisites

- Node.js v24.13.0 or higher
- npm or yarn package manager

## 🛠️ Installation

```bash
# Install dependencies
npm install
```

## 💻 Development

### Start Development Server

```bash
# Start English version (default)
npm start
# or
node ./node_modules/@docusaurus/core/bin/docusaurus.mjs start

# Start Chinese version
node ./node_modules/@docusaurus/core/bin/docusaurus.mjs start --locale zh-CN
```

The website will be available at:
- **English**: http://localhost:3000/
- **Chinese**: http://localhost:3000/zh-CN/

### Build for Production

```bash
# Build all locales
npm run build

# Build specific locale
npm run build -- --locale en
npm run build -- --locale zh-CN
```

### Clear Cache

```bash
npm run clear
# or
node ./node_modules/@docusaurus/core/bin/docusaurus.mjs clear
```

## 🌍 Internationalization (i18n)

This website supports two languages:

- **English** (default): `/`
- **简体中文**: `/zh-CN/`

### Translation Files Structure

```
i18n/
├── en/
│   ├── code.json                     # UI translations
│   ├── docusaurus-theme-classic/    # Theme translations
│   └── docusaurus-plugin-content-docs/
└── zh-CN/
    ├── code.json
    ├── docusaurus-theme-classic/
    │   ├── navbar.json               # Navbar translations
    │   └── footer.json               # Footer translations
    └── docusaurus-plugin-content-docs/
        └── current/                  # Translated docs
```

### Adding Translations

1. **UI Text**: Edit `i18n/{locale}/code.json`
2. **Navbar/Footer**: Edit files in `i18n/{locale}/docusaurus-theme-classic/`
3. **Documentation**: Add/edit files in `i18n/{locale}/docusaurus-plugin-content-docs/current/`

## 📁 Project Structure

```
rocketmq-website/
├── docs/                    # Documentation source files (English)
│   ├── getting-started/
│   ├── architecture/
│   ├── producer/
│   ├── consumer/
│   ├── configuration/
│   ├── contributing/
│   ├── faq/
│   └── release-notes/
├── i18n/                    # Internationalization files
├── src/                     # Custom React components
│   ├── components/
│   ├── css/
│   └── pages/
├── static/                  # Static assets (images, etc.)
├── docusaurus.config.ts     # Docusaurus configuration
├── sidebars.ts             # Sidebar configuration
└── tsconfig.json           # TypeScript configuration
```

## 🎨 Customization

### Theme Colors

Edit `src/css/custom.css` to customize theme colors and styles.

### Components

Custom React components are located in `src/components/`:
- `DeveloperStyleHero.tsx` - Homepage hero section
- `HomepageFeatures.tsx` - Feature cards

## 📝 Writing Documentation

1. Create/edit markdown files in `docs/` directory
2. Add category metadata with `_category_.json` files
3. For Chinese translation, create corresponding files in `i18n/zh-CN/docusaurus-plugin-content-docs/current/`

### Document Frontmatter Example

```markdown
---
sidebar_position: 1
title: Your Title
description: Your description
---

# Your Content
```

## 🚢 Deployment

The website is configured for GitHub Pages deployment:

```bash
npm run deploy
```

Configuration:
- Organization: `apache`
- Project: `rocketmq-rust`
- Branch: `gh-pages`

## 📚 Additional Resources

- [Docusaurus Documentation](https://docusaurus.io/docs)
- [RocketMQ-Rust Repository](https://github.com/mxsm/rocketmq-rust)
- [Developer Style Guide](./DEVELOPER_STYLE_README.md)
- [Project Structure](./PROJECT_STRUCTURE.md)
- [Quick Start Guide](./QUICKSTART.md)

## 🤝 Contributing

Contributions are welcome! Please read our [Contributing Guide](../CONTRIBUTING.md) before submitting a pull request.

### Help with Translations

If you'd like to help translate the documentation, please:
1. Check existing translation files in `i18n/zh-CN/`
2. Submit an issue or pull request with your translations
3. See our [GitHub Issues](https://github.com/mxsm/rocketmq-rust/issues/new/choose) for translation requests

## 📄 License

This project is licensed under the same license as RocketMQ-Rust. See the [LICENSE](../LICENSE-APACHE) files in the root directory.

## 📧 Contact

- GitHub: https://github.com/mxsm/rocketmq-rust
- Issues: https://github.com/mxsm/rocketmq-rust/issues

---

Built with ❤️ using [Docusaurus](https://docusaurus.io/)


Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## Links

- [RocketMQ-Rust GitHub](https://github.com/mxsm/rocketmq-rust)
- [Apache RocketMQ](https://rocketmq.apache.org/)
- [Docusaurus Documentation](https://docusaurus.io/docs)

## Support

- GitHub Issues: https://github.com/mxsm/rocketmq-rust/issues
- Mailing List: general@rocketmq.apache.org
