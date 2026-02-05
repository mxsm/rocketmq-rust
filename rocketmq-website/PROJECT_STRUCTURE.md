# RocketMQ-Rust Website Project Structure

## Project Overview

This is the official RocketMQ-Rust documentation website built with Docusaurus v3.9.2, supporting internationalization (English and Chinese), using React 18 + TypeScript technology stack.

## Directory Structure

```
rocketmq-website/
├── docusaurus.config.ts           # Docusaurus main configuration file
├── sidebars.ts                    # Documentation sidebar configuration
├── package.json                   # Project dependency management
├── tsconfig.json                  # TypeScript configuration
├── README.md                      # Project description
├── QUICKSTART.md                  # Quick start guide (English)
├── QUICKSTART_zh-CN.md            # Quick start guide (Chinese)
├── PROJECT_STRUCTURE.md           # Project structure (English)
├── PROJECT_STRUCTURE_zh-CN.md     # Project structure (Chinese)
├── .gitignore                     # Git ignore file
├── .nvmrc                         # Node.js version management
│
├── docs/                          # Documentation content directory
│   ├── author.md                  # Author information
│   ├── introduction.md            # Project introduction
│   │
│   ├── getting-started/           # Getting Started
│   │   ├── _category_.json        # Category configuration
│   │   ├── installation.md        # Installation guide
│   │   ├── quick-start.md         # Quick start
│   │   └── basic-concepts.md      # Basic concepts
│   │
│   ├── architecture/              # Architecture documentation
│   │   ├── _category_.json        # Category configuration
│   │   ├── overview.md            # Architecture overview
│   │   ├── message-model.md       # Message model
│   │   └── storage.md             # Storage mechanism
│   │
│   ├── producer/                  # Producer documentation
│   │   ├── _category_.json        # Category configuration
│   │   ├── overview.md            # Producer overview
│   │   ├── sending-messages.md    # Sending messages
│   │   └── transaction-messages.md # Transaction messages
│   │
│   ├── consumer/                  # Consumer documentation
│   │   ├── _category_.json        # Category configuration
│   │   ├── overview.md            # Consumer overview
│   │   ├── push-consumer.md       # Push consumer
│   │   ├── pull-consumer.md       # Pull consumer
│   │   └── message-filtering.md   # Message filtering
│   │
│   ├── configuration/             # Configuration documentation
│   │   ├── _category_.json        # Category configuration
│   │   ├── broker-config.md       # Broker configuration
│   │   ├── client-config.md       # Client configuration
│   │   └── performance-tuning.md  # Performance tuning
│   │
│   ├── faq/                       # FAQ
│   │   ├── _category_.json        # Category configuration
│   │   ├── common-issues.md       # Common issues
│   │   ├── performance.md         # Performance issues
│   │   └── troubleshooting.md     # Troubleshooting
│   │
│   └── contributing/              # Contributing guide
│       ├── _category_.json        # Category configuration
│       ├── overview.md            # Contributing overview
│       ├── development-guide.md   # Development guide
│       └── coding-standards.md    # Coding standards
│
├── releases/                      # Release notes directory
│   ├── authors.yml                # Release authors
│   ├── 2024-01-28-v0.1.0.md       # v0.1.0 release notes
│   ├── 2024-06-05-v0.2.0.md       # v0.2.0 release notes
│   ├── 2024-11-17-v0.3.0.md       # v0.3.0 release notes
│   ├── 2025-02-16-v0.4.0.md       # v0.4.0 release notes
│   ├── 2025-05-19-v0.5.0.md       # v0.5.0 release notes
│   ├── 2025-08-19-v0.6.0.md       # v0.6.0 release notes
│   └── 2025-12-07-v0.7.0.md       # v0.7.0 release notes
│
├── src/                           # Source code directory
│   ├── components/                # React components
│   │   ├── HomepageFeatures.tsx   # Homepage features component
│   │   ├── HomepageFeatures.module.css
│   │   ├── DeveloperStyleHero.tsx # Developer style hero section
│   │   ├── AnnouncementBanner.tsx # Announcement banner component
│   │   ├── DevWarningBanner.tsx   # Development warning banner
│   │   ├── DevWarningBannerWrapper.tsx # Banner wrapper
│   │   ├── OrbBackground.tsx      # Orb background effect
│   │   ├── OrbBackground.centered.tsx
│   │   ├── OrbBackground.global.tsx
│   │   ├── OrbBackground.module.css
│   │   ├── OrbBackground.test.tsx # Orb background tests
│   │   ├── OrbBackground.test.module.css
│   │   └── SimpleOrb.tsx          # Simple orb component
│   │
│   ├── pages/                     # Page components
│   │   ├── index.tsx              # Homepage
│   │   └── index.module.css
│   │
│   ├── css/                       # Style files
│   │   └── custom.css             # Custom styles
│   │
│   └── theme/                     # Theme customization
│
├── static/                        # Static assets
│   ├── CNAME                      # Custom domain configuration
│   ├── google0e604df170413726.html # Google verification
│   ├── img/                       # Image assets
│   └── js/                        # JavaScript files
│
├── i18n/                          # Internationalization files
│   ├── en/                        # English translation
│   │   ├── code.json              # UI text translations
│   │   ├── docusaurus-theme-classic.json
│   │   ├── docusaurus-plugin-content-pages/
│   │   └── docusaurus-theme-classic/
│   │
│   └── zh-CN/                     # Chinese translation
│       ├── code.json              # UI text translations
│       ├── docusaurus-theme-classic.json
│       ├── docusaurus-plugin-content-docs/  # Translated docs
│       ├── docusaurus-plugin-content-pages/
│       └── docusaurus-theme-classic/
│           ├── navbar.json        # Navbar translations
│           └── footer.json        # Footer translations
│
└── .docusaurus/                   # Build output (auto-generated)
    ├── client-modules.js
    ├── routes.js
    └── ... (other build artifacts)
```

## Core Configuration Files

### 1. docusaurus.config.ts

Main Docusaurus configuration file, containing:
- Website metadata (title, description, URL)
- Theme configuration (navbar, footer)
- Documentation configuration
- Internationalization configuration
- Plugin configuration

### 2. sidebars.ts

Defines the sidebar structure of the documentation, organizing documents into categories:
- Getting Started
- Architecture
- Producer
- Consumer
- Configuration
- FAQ
- Contributing

### 3. package.json

Defines project dependencies and scripts:
- Docusaurus core dependencies
- React and TypeScript
- Build scripts (start, build, serve)

## Tech Stack

### Frameworks and Libraries
- **Docusaurus**: v3.9.2 (static site generator)
- **React**: v18.3.1 (UI framework)
- **TypeScript**: v5.3.3 (type system)
- **Prism**: v2.3.0 (code highlighting)

### Development Tools
- **Node.js**: 18.0+
- **npm**: 9.0+

## Documentation Structure

### Documentation Categories

1. **Getting Started** (3 docs)
   - Installation: Installation guide
   - Quick Start: Quick start tutorial
   - Basic Concepts: Core concepts introduction

2. **Architecture** (3 docs)
   - Overview: System architecture overview
   - Message Model: Message model details
   - Storage: Storage mechanism

3. **Producer** (3 docs)
   - Overview: Producer overview
   - Sending Messages: Message sending
   - Transaction Messages: Transaction messages

4. **Consumer** (4 docs)
   - Overview: Consumer overview
   - Push Consumer: Push consumer
   - Pull Consumer: Pull consumer
   - Message Filtering: Message filtering

5. **Configuration** (3 docs)
   - Broker Config: Broker configuration
   - Client Config: Client configuration
   - Performance Tuning: Performance tuning

6. **FAQ** (3 docs)
   - Common Issues: Common issues
   - Performance: Performance issues
   - Troubleshooting: Troubleshooting

7. **Contributing** (3 docs)
   - Overview: Contributing overview
   - Development Guide: Development guide
   - Coding Standards: Coding standards

## Internationalization (i18n)

### Supported Languages
- **en** (English): Default language
- **zh-CN** (Simplified Chinese): Reserved for expansion

### Translation File Locations
- `i18n/en/`: English translations
- `i18n/zh-CN/`: Chinese translations

### Translation Content
- Theme UI text (navbar, footer, etc.)
- Documentation content
- Homepage content

## Custom Features

### 1. Homepage (Landing Page)
- Gradient background design
- Feature showcase (6 feature cards)
- Quick start button
- GitHub link

### 2. Custom Styles
- Theme color customization
- Dark mode optimization
- Responsive design
- Custom scrollbar

### 3. Mermaid Diagram Support
- Architecture diagrams
- Flowcharts
- Sequence diagrams

## Build Commands

```bash
# Install dependencies
npm install

# Start development server
npm start

# Build production version
npm run build

# Preview production build
npm run serve

# Generate translation files
npm run write-translations

# Type checking
npm run typecheck
```

## Deployment

### GitHub Pages
```bash
npm run deploy
```

### Other Platforms
Deploy the `build/` directory to any static website hosting service (Nginx, CDN, etc.) after building.

## Extension Suggestions

### 1. Multi-language Expansion
- Add more language versions (Japanese, Korean, etc.)
- Use `npm run write-translations -- --locale <locale>` to generate translation templates

### 2. Multi-version Documentation
- Use Docusaurus versioning feature to maintain multiple documentation versions
- Configure `versions.json` to manage versions

### 3. Blog Functionality
- Enable blog feature in `docusaurus.config.ts`
- Create `blog/` directory to store blog posts

### 4. Search Functionality
- Integrate Algolia DocSearch
- Or use local search plugin

## Maintenance Recommendations

1. **Regular dependency updates**: `npm update`
2. **Check build warnings**: Ensure `npm run build` has no warnings
3. **Test multi-language**: Switch languages to verify translations
4. **Performance optimization**: Regularly check Lighthouse scores
5. **Backup data**: Regularly backup documentation content

## Resource Links

- [Docusaurus Official Documentation](https://docusaurus.io/docs)
- [React Official Documentation](https://react.dev)
- [TypeScript Official Documentation](https://www.typescriptlang.org/docs)
- [RocketMQ-Rust GitHub](https://github.com/mxsm/rocketmq-rust)
