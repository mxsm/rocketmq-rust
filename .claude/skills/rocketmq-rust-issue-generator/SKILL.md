---
name: rocketmq-rust-issue-generator
description: Use when the user asks to create, draft, prepare, or publish a GitHub issue for the rocketmq-rust project — bugs, features, enhancements, refactors, docs, unit tests, CI, runtime-model, broker, namesrv, client, store, dashboard, architecture, or performance work — especially when the issue must preserve real ISSUE_TEMPLATE fields, exact title prefix including template emoji, aligned labels, actionable engineering tasks, and no leaked local filesystem paths.
---

# RocketMQ Rust Issue Generator

Generate GitHub issue drafts from the repository's real issue forms. Template-driven — do not output a fixed canonical body unless the selected form has those fields.

## When to Use

- User asks to create, draft, or publish a GitHub issue for rocketmq-rust
- User's rough draft mentions generic sections or non-existent template names — must be converted to real forms
- Issue must preserve exact template title prefix (including emoji), label set, and field order
- Local filesystem paths must be audited and removed before publishing
- Public issue titles and body content must be English-only

**When NOT to use:** Non-rocketmq-rust repositories, pure discussion/questions that should not become GitHub issues, or requests where the user explicitly says not to apply issue templates.

## Quick Reference

| Intent | Template | Title Prefix |
|--------|----------|-------------|
| Bug, panic, incorrect behavior | `bug_report.yml` | `[Bug🐛] ` |
| Missing/improved test coverage | `unit_test.yml` | `[Test🧪] ` |
| Docs, README, examples, comments | `doc.yml` | `[Doc📝] ` |
| New broker/client/store/dashboard capability | `feature_request.yml` | `[Feature🚀] ` |
| CI gate, runtime audit, ergonomics | `enhancement_request.yml` | `[Enhancement✨] ` |
| Internal restructure, module cleanup | `refactor.yml` | `[Refactor♻️] ` |

Select the narrowest template. When ambiguous, state tradeoff and choose narrowest (e.g., "add consume queue recovery tests" → unit test, not feature). Verify files exist in checkout — don't assume draft-only names like `architecture.yml`.

## Workflow

### 1. Locate Templates
```bash
rg --hidden --files -g '.github/ISSUE_TEMPLATE/**' -g 'ISSUE_TEMPLATE/**' -g '!**/target/**'
```
Read `config.yml` for policy. For each form, extract `name`, `title` prefix (with emoji), `labels`, every `body` item in order, and `validations.required`. For Markdown forms, preserve heading order and checklist meaning.

If terminal output corrupts emoji or symbols, do not copy from the noisy terminal. Re-read the template files as UTF-8 and use the exact checked-out `title:` value.

### 2. Fill The Issue

```
Title: <template title prefix including emoji><specific summary>
Template: <filename>
Labels: <labels from template>

<issue body following template field order>

Missing info:
- <None, or concise missing confirmations/data>
```

Rules:
- Every template field, in order. Required fields must have content or be listed as missing.
- Use `Not applicable` only when a field clearly does not apply.
- Never mark prerequisite/contribution checkboxes complete unless confirmed or actually performed.
- Tasks must be actionable: name affected modules, expected behavior, tests, and validation commands.
- Use RocketMQ domain vocabulary (broker, namesrv, commitlog, consume queue, Tokio runtime, etc.) only where it helps the form.
- Write all public issue title/body text in English. Repository-relative paths, code identifiers, commands, issue numbers, Markdown punctuation, and the template emoji are allowed; Chinese or other non-English prose is not allowed.

### 3. Remove Local Paths

Strip Windows/Unix home paths, `file://` links, user-home env vars, local screenshot/temp/IDE paths. Rewrite to repository-relative paths or generic descriptions ("local broker log").

Audit before publishing, including the title when available:
```bash
python <skill-dir>/scripts/audit_issue_paths.py --title "<title>" <draft.md>
```
Fix every finding.

### 4. Publish
1. Confirm required fields complete and path audit passes.
2. Use available GitHub tool/CLI with template labels.
3. Report created issue URL. If publishing unavailable, return ready-to-submit draft.

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Dropping the template emoji from title | Copy `title:` prefix exactly from the template file |
| Inventing template names (e.g., `architecture.yml`) | Verify files exist in checkout; use only real filenames |
| Marking checkboxes complete without confirmation | List unconfirmed items as missing |
| Leaving a local Windows user path in draft | Run `audit_issue_paths.py` before finalizing |
| Writing issue prose in Chinese or mixed language | Rewrite the title/body in English and rerun the audit |
| Using feature_request for test-only work | Unit test additions → `unit_test.yml` |
| Copying mojibake emoji from terminal | Re-read file with UTF-8 or use file content directly |

## Output Modes

- **Title only:** Return candidate titles with exact template prefix.
- **Full draft:** Title, template, labels, body in template order, missing info section.
- **Draft conversion:** Normalize user's rough draft to real form; drop sections not in the selected template.

## Final Check

- Selected template filename is real in the current checkout.
- Title keeps the selected template prefix exactly, including emoji.
- Labels match the selected template.
- Body fields appear in template order.
- Required fields have content or are listed under `Missing info`.
- Title/body are English-only.
- Checkboxes are not falsely marked complete.
- Path and English audit passes with no local absolute paths, machine-specific roots, or non-English prose.
