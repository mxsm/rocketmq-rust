# Work on the RocketMQ-Rust website

[简体中文](./QUICKSTART_zh-CN.md) · [Full setup](./README.md) · [Writing guide](./DOCUMENTATION.md)

This guide starts the documentation website. To run RocketMQ-Rust **1.0.0 development** services and exchange a message, follow [source installation](./docs/getting-started/installation.md) and [the first-message tutorial](./docs/getting-started/quick-start.md).

## Install and preview

From the repository root, use Node **24.13.0** from `.nvmrc` and npm:

```bash
cd rocketmq-website
node --version
npm ci
npm run start
```

`npm ci` installs the lockfile dependency set. Reuse installed dependencies during normal editing; reinstall when dependencies are missing or the lockfile changes. The server prints its actual URL, normally `http://localhost:3000/`. Stop it with Ctrl+C.

Start the Chinese development site in place of the English server:

```bash
npm run start:zh
```

Development serves one locale at a time. Build and preview both languages with:

```bash
npm run build
npm run serve
```

The static build goes to `build/`, including Chinese content under `zh-CN/`. Confirm the affected pages, links and diagrams in the static preview. A website build does not compile or start the message services.

## Update a page in both languages

1. Edit `docs/<id>.md` and `i18n/zh-CN/docusaurus-plugin-content-docs/current/<id>.md` together. Preserve existing IDs and slugs.
2. For a new page, add its ID to `sidebars.ts` and translate its category label in the Chinese docs translation JSON when needed.
3. Keep commands, configuration keys, units and technical limitations aligned. Mermaid labels and explanatory text have full English and Chinese counterparts.
4. Run the existing build and inspect the affected routes. Use the [writing guide](./DOCUMENTATION.md) for source references, MDX, assets and examples.

`npm run write-translations -- --locale zh-CN` extracts UI translation messages; it does **not** translate Markdown prose or create complete Chinese pages.

## Configuration and publishing

| Change | File or directory |
| --- | --- |
| Site URLs, version label, locales and navbar/footer structure | `docusaurus.config.ts` |
| Documentation navigation | `sidebars.ts` |
| Shared styles | `src/css/custom.css` |
| UI translations | `i18n/zh-CN/code.json` and plugin/theme translation JSON |
| Historical release announcements | `releases/`; currently English posts shared by the locale builds |

The current document set is **1.0.0 development**, not an announcement that 1.0.0 artifacts were released. Historical release posts retain their original versions. The website package's private `0.0.0` version is not the RocketMQ-Rust product version.

Publishing uses the existing [deployment workflow](../.github/workflows/deploy.yml) or an intentionally configured `npm run deploy`; it is separate from local preview. Search uses the configured external index, whose freshness is not established by a local build. See [project structure](./PROJECT_STRUCTURE.md) for ownership and routing.
