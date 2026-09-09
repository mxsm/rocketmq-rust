# RocketMQ-Rust website

[简体中文](README-zh_cn.md) · [Documentation authoring](DOCUMENTATION.md)

This directory contains the RocketMQ-Rust documentation site. It is a standalone Node project using Docusaurus 3.9, React 18, TypeScript, and Mermaid. English is the default language; Simplified Chinese has the same document IDs under its translation directory.

## Develop locally

Use Node.js 24.13.0 from `.nvmrc` and npm. The website workflows use the Node 24 release line. `package.json` contains a broader minimum engine declaration; it is not a claim that every older Node release is exercised by this project's workflows.

Run commands from `rocketmq-website/`:

```bash
node --version
npm --version
npm ci
npm run start
```

`npm ci` installs the versions in `package-lock.json`. Reuse installed dependencies during normal editing; reinstall when dependencies are missing or the lockfile changes. The development server uses `http://localhost:3000/` by default and prints its actual address. Stop it with Ctrl+C.

To work on Chinese pages:

```bash
npm run start:zh
```

The Chinese locale uses `/zh-CN/`. A development server serves the selected locale; use a full build to inspect both languages together.

## Build and preview

```bash
npm run build
npm run serve
```

The build includes both configured locales and writes static output to `build/`. The preview command prints the address it serves. Locale-specific builds are also available:

```bash
npm run build -- --locale en
npm run build -- --locale zh-CN
```

Use `npm run clear` when generated Docusaurus data is stale. Do not commit `build/`, `.docusaurus/`, or `node_modules/`. Website builds do not build or test the Rust message services.

## Content layout

| Location | Purpose |
| --- | --- |
| `docs/` | English documentation |
| `i18n/zh-CN/docusaurus-plugin-content-docs/current/` | Complete Chinese counterparts with matching IDs |
| `i18n/zh-CN/code.json` | Translatable component text |
| `i18n/zh-CN/docusaurus-theme-classic/` | Navigation and footer translations |
| `sidebars.ts` | Explicit documentation navigation |
| `releases/` | Release-note blog source |
| `src/` | Pages, components, theme customizations, and styles |
| `static/` | Images and other static assets |
| `docusaurus.config.ts` | Site URL, plugins, locale, search, and version display |

Read [the writing guide](DOCUMENTATION.md) before adding a tutorial, design explanation, operational guide, or reference page. It explains bilingual files, source-based claims, reproducible examples, diagrams, and preservation of existing URLs. Add navigation for pages that contain useful content; avoid publishing empty placeholders.

## Version and release scope

The current documentation is labeled **Next**. The root Cargo source version and the website package version serve different purposes; neither identifies a published downloadable release by itself. Use the project's [GitHub releases](https://github.com/mxsm/rocketmq-rust/releases) for published artifacts and version-specific release information.

Do not mix current source APIs with dependency versions copied from an older tutorial. The source tree, examples, and published release instructions must identify which version they describe.

## Deployment configuration

The existing site configuration uses:

- Site URL: `https://rocketmqrust.com`, base URL `/`.
- GitHub owner/project: `mxsm/rocketmq-rust`.
- Deployment branch: `gh-pages`.
- English and Simplified Chinese locales.

The repository's [deployment workflow](../.github/workflows/deploy.yml) describes the automated publishing process. `npm run deploy` is a publishing action, not a local preview command. Normal documentation editing only needs local development or static preview.

## Contribution and maintenance

Keep English and Chinese content aligned, preserve existing routes, and describe the limits of examples that have not been run. Documentation does not require new metadata validators, file fingerprints, or approval gates. Use the existing build when rendered content or configuration changes, and report the checks actually performed.

See [the project contribution guide](../CONTRIBUTING.md), [issues](https://github.com/mxsm/rocketmq-rust/issues), and [discussions](https://github.com/mxsm/rocketmq-rust/discussions) for contributions and questions.
