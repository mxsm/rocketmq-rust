# RocketMQ-Rust website structure

[简体中文](./PROJECT_STRUCTURE_zh-CN.md) · [Setup](./README.md) · [Writing guide](./DOCUMENTATION.md)

The website is a standalone Docusaurus 3.9 / React 18 / TypeScript project. Its current documentation describes **RocketMQ-Rust 1.0.0 development**. Node **24.13.0** is selected by `.nvmrc`; `package-lock.json` records resolved Node dependencies. Cargo builds do not validate this website.

## Directory responsibilities

| Path | Responsibility |
| --- | --- |
| `docs/` | Current English technical documentation |
| `i18n/zh-CN/docusaurus-plugin-content-docs/current/` | Complete Chinese counterparts with matching IDs |
| `i18n/zh-CN/docusaurus-plugin-content-docs/current.json` | Chinese version and sidebar category labels |
| `releases/` | English historical release posts using the blog plugin; currently shared by the locale builds |
| `src/pages/` | Custom pages, separate from the docs sidebar |
| `src/components/`, `src/theme/`, `src/css/` | Components, theme overrides and styling |
| `static/` | Static images and files copied to the site |
| `examples/` | First-message standalone Rust application and deployment configuration examples |
| `docusaurus.config.ts` | Site URL, locales, docs/blog plugins, version label and theme configuration |
| `sidebars.ts` | Explicit technical-document navigation |
| `i18n/zh-CN/code.json`, `i18n/zh-CN/docusaurus-theme-classic/` | UI, navbar and footer translations |
| `.docusaurus/`, `build/`, `node_modules/` | Generated metadata, static output and installed dependencies; not authored content |

## Technical content organization

The main documentation plan contains 79 logical pages, each with a full English and Chinese file. Supplementary legacy entries and release indexes are additional pages.

| Section | Content |
| --- | --- |
| Introduction and overview | Project identity, capabilities and release scope |
| `getting-started/` | Concepts, installation, local services and first message |
| `architecture/` | Module ownership, message flow, runtime, storage, routing, HA, Proxy, security and design choices |
| `producer/`, `consumer/`, `guides/` | APIs, lifecycle, retries, filtering, order, transactions, delay/recall and request/reply |
| `deployment/`, `operations/` | Topologies, security, provisioning, monitoring, diagnosis, maintenance and recovery |
| `configuration/`, `reference/`, `migration/` | Fields/defaults, build features, errors, CLI, protocol/API contracts and migration |
| `ecosystem/` | Web/native dashboards, read-only MCP, MCP Control and AI SRE |
| `contributing/` | Development, coding, documentation, testing and release engineering |
| `faq/`, `author.md`, `release-notes/` | Preserved supplementary URLs and links to maintained technical/release content |

Use `sidebars.ts` to inspect the actual order. `_category_.json` files do not replace explicit sidebar entries. A new page needs both language files and a reachable navigation or cross-reference entry.

## Routes, versions and translations

English technical pages use `/docs/<route>`; Chinese pages use `/zh-CN/docs/<route>`. By default the route follows the file path, but existing explicit IDs/slugs must be preserved. Release history uses `/releases` and the locale-prefixed route in the Chinese build. There is currently no Chinese blog-content directory, so a Chinese route does not mean the historical post has a Chinese translation. The bilingual technical release index is maintained separately under `docs/release-notes/`. `baseUrl`, trailing-slash behavior and production URL are configured centrally.

The current docs label is `1.0.0 (development)`, translated as `1.0.0（开发版）`, and is visible on technical pages. The site does not create a released 1.0.0 snapshot by changing this label. Retain the existing `current/` translation path; a new `versioned_docs/` tree or `versions.json` is unnecessary for this development document set.

UI translation extraction and document translation are separate. `npm run write-translations -- --locale zh-CN` extracts messages; authors maintain full Chinese Markdown and diagram labels. Historical release text keeps the version it originally described.

Search is configured through the theme's external Algolia integration. Local content builds do not refresh its remote crawler/index. Source-edit links use `editLocalizedFiles: true` so Chinese pages open their Chinese source file.

## Build and maintenance

Use [quick start](./QUICKSTART.md) for dependency installation and local preview. `npm run build` renders both locales; `npm run serve` previews the result. Inspect changed routes, code blocks, diagrams and links, and address problems caused by the content change. Keep generated output out of source commits.

The first-message sample has its own Cargo manifest and path dependencies. Deployment examples are service inputs, not Node application configuration. Verify an example in its owning runtime when executable behavior changes; a Docusaurus build only establishes website rendering.

Publishing follows the existing [deployment workflow](../.github/workflows/deploy.yml). Frontend hosting, remote search indexing and Rust product releases are separate operations from editing this website.
