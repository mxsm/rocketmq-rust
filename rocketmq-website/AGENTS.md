# AGENTS.md

## Scope
This file applies to `rocketmq-website/`.

## Project role
- This directory is the standalone Docusaurus website for RocketMQ Rust.
- It is a Node project, not a Cargo project and not part of the root Rust workspace.
- Root Cargo validation does not validate this website.

## Repository boundaries
- Keep website changes scoped to `rocketmq-website/` unless the user explicitly asks to update source docs, workflows, or shared repository instructions.
- Do not commit generated Docusaurus build output.
- Do not introduce unrelated dependency updates.

## Codex workflow
- Check `git status --short` before editing.
- Treat existing uncommitted changes as user work.
- Use `rg` or `rg --files` for searching.
- Use patch-style edits for manual changes.
- Do not create commits, branches, or pull requests unless the user asks.

## Validation
Run from `rocketmq-website/` before PR submission or final handoff for website changes:

```bash
npm ci
npm run build
```

For iteration when dependencies are already installed:

```bash
npm run build
```

Documentation-only changes outside this directory do not require website validation. Changes inside this directory should be treated as website changes because Docusaurus routing, navigation, MDX rendering, and build configuration can fail independently of Cargo.

## Final response expectations
- Summarize website files changed and why.
- List validation commands and results.
- If validation was skipped because the change is outside website behavior or because only repository instructions changed, say so explicitly.
