# RocketMQ-Rust Official Website

> Production-ready official documentation website for RocketMQ-Rust project.

Built with [Docusaurus](https://docusaurus.io/) v3.9.2, a modern static website generator.

## Features

- 📖 **Comprehensive Documentation**: Complete guides for RocketMQ-Rust
- 🌍 **Internationalization**: English and Chinese language support
- 🎨 **Modern Design**: Clean, responsive UI with dark mode support
- 🔍 **Search**: Built-in search functionality
- 📱 **Responsive**: Mobile-friendly design
- ⚡ **Fast**: Static site generation for optimal performance

## Quick Start

### Prerequisites

- Node.js 18.0 or higher
- npm 9.0 or higher

### Installation

```bash
# Install dependencies
npm install

# Start development server
npm start
```

Open [http://localhost:3000](http://localhost:3000) to view the website.

### Build for Production

```bash
# Build static files
npm run build

# Serve production build locally
npm run serve
```

The built files will be in the `build/` directory.

## Project Structure

```
rocketmq-website/
├── docusaurus.config.ts     # Docusaurus configuration
├── sidebars.ts              # Documentation sidebar structure
├── package.json             # Project dependencies
├── tsconfig.json            # TypeScript configuration
├── docs/                    # Documentation content
│   ├── introduction.md
│   ├── getting-started/
│   ├── architecture/
│   ├── producer/
│   ├── consumer/
│   ├── configuration/
│   ├── faq/
│   └── contributing/
├── src/                     # React components and pages
│   ├── components/          # Custom React components
│   ├── pages/               # Additional pages
│   └── css/                 # Custom CSS
├── static/                  # Static assets
│   └── img/                 # Images and logos
└── i18n/                    # Internationalization files
    ├── en/                  # English translations
    └── zh-CN/               # Chinese translations
```

## Documentation

- [Getting Started](https://rocketmq.apache.org/rust/docs/category/getting-started)
- [Architecture](https://rocketmq.apache.org/rust/docs/category/architecture)
- [Producer Guide](https://rocketmq.apache.org/rust/docs/category/producer)
- [Consumer Guide](https://rocketmq.apache.org/rust/docs/category/consumer)
- [Configuration](https://rocketmq.apache.org/rust/docs/category/configuration)
- [FAQ](https://rocketmq.apache.org/rust/docs/category/faq)
- [Contributing](https://rocketmq.apache.org/rust/docs/category/contributing)

## Development

### Adding New Content

1. **Add documentation**: Create `.md` files in the `docs/` directory
2. **Update sidebar**: Modify `sidebars.ts` to include new content
3. **Update i18n**: Add translations in `i18n/` directories

### Customizing Styles

Edit `src/css/custom.css` to customize the website appearance.

### Adding Components

Add React components in `src/components/` and import them in pages or docs.

## Internationalization

The website supports multiple languages:

- English (en) - Default
- Simplified Chinese (zh-CN)

### Adding a New Language

```bash
# Configure locale in docusaurus.config.ts
# Create translation files in i18n/{locale}/
# Write translated documentation

npm run write-translations -- --locale zh-CN
```

## Deployment

### GitHub Pages

```bash
# Deploy to GitHub Pages
npm run deploy
```

### Manual Deployment

```bash
# Build the site
npm run build

# Deploy the contents of build/ directory to your web server
```

## Contributing

We welcome contributions! Please see [Contributing Guide](https://rocketmq.apache.org/rust/docs/category/contributing) for details.

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE) for details.

## Links

- [RocketMQ-Rust GitHub](https://github.com/apache/rocketmq-rust)
- [Apache RocketMQ](https://rocketmq.apache.org/)
- [Docusaurus Documentation](https://docusaurus.io/docs)

## Support

- GitHub Issues: https://github.com/apache/rocketmq-rust/issues
- Mailing List: general@rocketmq.apache.org
