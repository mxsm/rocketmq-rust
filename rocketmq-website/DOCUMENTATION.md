# Writing RocketMQ-Rust documentation

[简体中文](DOCUMENTATION-zh-CN.md) · [Website development](README.md)

Write for a reader who needs to build a system, use an API, explain a failure, or understand a design decision. Start with the result and its conditions. A page should make its next action clear without requiring the reader to reconstruct a procedure from several crate READMEs.

## Choose the kind of page

| Kind | Reader's question | Content |
| --- | --- | --- |
| Tutorial | How do I achieve my first working result? | Goal, prerequisites, a complete sequence, expected observations, failure handling, shutdown, next step |
| How-to guide | How do I perform this operation? | Scenario, applicable topology, permissions, affected resources, procedure, observation, recovery |
| Design explanation | Why does the system work this way? | Problem, boundaries, ownership, data flow, invariants, failure behavior, trade-offs, implementation references |
| Reference | What does this option or interface mean? | Exact name, type, default, unit, conditions, constraints, examples, compatibility |

Keep a tutorial focused on one working path. Put alternatives in a comparison or a separate how-to. Explain architecture through responsibilities and state transitions; a list of source files alone does not explain a design.

## Write the English and Chinese pages together

English source and Simplified Chinese translation use matching paths:

```text
docs/architecture/runtime.md
i18n/zh-CN/docusaurus-plugin-content-docs/current/architecture/runtime.md
```

For a new page, the path under the content root supplies its document ID. Inspect existing front matter before editing: preserve an explicit `id` or `slug` and the established URL. Translate the full page, including prerequisites, limitations, recovery, captions, and alternative text. A Chinese summary or an English fallback is not a complete translation.

Use the same API names, configuration keys, command options, values, and example logic in both languages. Keep Broker, NameServer, Controller, Proxy, Topic, Consumer Group, CommitLog, and ConsumeQueue as identifiers; explain unfamiliar terms on first use.

Use normal front matter supported by the site:

```yaml
---
title: Runtime architecture
sidebar_label: Runtime
description: Task ownership, resource budgets, cancellation, and shutdown.
---
```

The Chinese page translates these display fields. Do not add a second language directory under `docs/` or a new metadata registry.

## Establish the scope of a technical claim

Use the implementation, configuration loader, manifest, relevant examples, and documented contracts for the version being described. Module READMEs provide orientation; inspect the actual entry point when a claim depends on a default, feature, protocol registration, or constructor.

Separate these questions:

- **Implemented:** does a concrete implementation exist?
- **Enabled:** which build features, runtime settings, and adapters activate it?
- **Observed:** which scenario was actually compiled, run, or exercised?
- **Released:** does a published release contain the artifact being described?

An exported API is not evidence that every runtime path is wired. A workflow file is not evidence that its scenario ran. A Cargo package version is not a release announcement. State an unresolved condition precisely and continue with the facts available.

The current site labels development documentation **1.0.0 development**. The root source selects Rust 1.95.0 and package version 1.0.0; those values describe the source tree, not a promise that 1.0.0 binaries or crates are published. Link release-specific instructions to the corresponding release, rather than mixing an older crate dependency with current source examples.

## Make commands reproducible

Before each command sequence, identify the working directory, operating system or shell, configuration inputs, and services that must already be running. A complete example includes initialization and shutdown, not just the central send or receive call.

The root Cargo workspace does not contain every product. `rocketmq-example`, several Dashboard projects, and the AI projects have their own manifests or workspaces. Use their own commands and local guides. Do not present a successful root build as proof that those projects build.

For each operational step, distinguish:

1. What the command changes or reads.
2. What output or state the reader should observe.
3. What a missing or unexpected observation means.
4. How to stop or recover without removing unrelated data.

Use placeholder credentials. Explain local development exceptions where they apply. Mark excerpts as excerpts and pseudocode as pseudocode. If a command was checked against source but not run, say so; do not invent command output or production recovery guarantees.

## Explain reliability at the correct boundary

Distinguish acceptance, local durability, replica acknowledgement, derived-index visibility, consumption, and business processing. A timeout can leave the outcome uncertain; it does not necessarily mean that nothing was written. Avoid unqualified “no message loss,” “exactly once,” or throughput claims.

A performance figure needs the workload, environment, configuration, and measurement context. A recovery procedure needs the backup boundary, prerequisites, and known limits. If that information is absent, explain the design or procedure without attaching an unsupported number.

For configuration references, inspect external names and defaults in the parser or Serde implementation. A Rust field name may differ from the configuration key. A field's existence does not imply live reload.

## Use diagrams that can be maintained

Use Mermaid or editable vector diagrams for topology, ownership, sequence, state, and trust boundaries. The site already enables Mermaid. Label process boundaries separately from crate dependencies. Explain arrow meanings and give a short prose description after each diagram.

Keep corresponding diagram labels, captions, and alternative text in English and Chinese. Identifiers such as API names remain unchanged. Shared images without text can use one file; language-specific vector images can use `.en.svg` and `.zh-CN.svg`.

Place image assets under `static/img/docs/<topic>/` and reference them with the site's static-asset conventions. Check readability in light and dark themes and on a narrow screen. Color must not be the only way to distinguish a role or state.

An image model may generate a conceptual illustration or a background without labels. Check its relationships against the article and label it as an illustration when needed. Generated images must not masquerade as a running product screenshot, benchmark, or exact protocol diagram.

## Preserve navigation while content grows

Add completed document IDs to `sidebars.ts`. Translate category and navigation labels in `i18n/zh-CN/`. `_category_.json` does not replace the explicit items in the current handwritten sidebar.

Rewrite an existing page at its current URL where practical. When consolidating content, leave a useful short introduction and a link to the destination. A static GitHub Pages site does not automatically provide server-side HTTP 301 responses.

Keep observability configuration distinct from monitoring procedures. Consolidate overlapping FAQ and performance advice into task-oriented guides as those guides become available. Preserve the author/community entry and route release information through the configured `releases` section.

## Review the change directly

Read both language versions, follow the affected links, and inspect code fences and diagrams. For changes to rendered pages, routes, or site configuration, run the existing website build from this directory:

```bash
npm run build
```

Use `npm run start` or `npm run start:zh` for the relevant language preview. Reuse installed dependencies; run `npm ci` when dependencies are missing or the lockfile changes. Do not commit `.docusaurus/`, `build/`, or `node_modules/`.

Documentation authoring does not require file fingerprints, hashes, a fixed commit, scores, approval stages, or new CI gates. Keep an honest note of checks actually performed and environments not exercised. An unrelated historical failure does not require a repository-wide repair before the article can be completed.

Product authentication, authorization, and audit behavior still belongs in the technical documentation where it affects users. Simplifying the writing process does not change those runtime requirements.
