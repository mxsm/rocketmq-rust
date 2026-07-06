---
name: rocketmq-rust-pr-submitter
description: Use when the user asks to prepare, submit, publish, or optimize a pull request for the rocketmq-rust project, especially when the PR title or commit message must follow the `[ISSUE #issue_id]<emoji><summary>` convention, the `.github/PULL_REQUEST_TEMPLATE.md` body format, and the PR must not be draft unless explicitly requested.
---

# RocketMQ Rust PR Submitter

Prepare RocketMQ Rust PR titles, commit messages, and bodies from `.github/PULL_REQUEST_TEMPLATE.md`. Connects PR to issue via `[ISSUE #id]` title/commit prefix and `Fixes #id` body line.

## When to Use

- User asks to create, prepare, or publish a PR for rocketmq-rust
- PR title or commit message must follow `[ISSUE #issue_id]<emoji><summary>` format
- PR body must follow the repository's real PR template (not a custom format)
- Issue id is available from user request, branch name, or commit history
- Public PR titles, commit messages, and body content must be English-only

**When NOT to use:** Simple branch merges without a linked issue. PRs for repos that don't use the RocketMQ Rust PR template convention.

## Quick Reference

| Change Type | Emoji |
|-------------|-------|
| Bug fix | 🐛 |
| Documentation | 📝 |
| Enhancement / CI / runtime audit | ✨ |
| New feature | 🚀 |
| Refactor | ♻️ |
| Tests | 🧪 |

If unclear, default to ✨ and note the assumption.

## Workflow

### 1. Read the PR Template
```bash
rg --hidden --files -g '*PULL_REQUEST_TEMPLATE*' -g '*pull_request_template*' -g '!**/target/**'
```
Expected: `.github/PULL_REQUEST_TEMPLATE.md` with three headings: `Which Issue(s) This PR Fixes(Closes)`, `Brief Description`, `How Did You Test This Change?`. Remove HTML comments from the generated body. Use the checked-out template if it differs.

### 2. Determine Issue Id
Priority order: user request → branch name (`issue-7544`, `fix-7544`) → commit messages (`[ISSUE #7544]`) → prepared PR text (`Fixes #7544`). Normalize to numeric id. Never publish with `#issue_id` still present.

### 3. Write Title
```
[ISSUE #issue_id]<emoji><summary>
```
Do not put spaces between `]`, the emoji, and the summary. No extra prefixes (conventional commit, branch name). Keep the summary short and action-oriented.

Generate the summary from the actual PR change: affected subsystem, behavior fixed, test coverage added, docs updated, or refactor performed. Do not use sequencing-marker wording such as `task 1`, `stage 1`, `phase 1`, `step 3`, or `part 4`, even if the branch name, issue text, or user draft contains those words.

Write the title and commit-message summary in English only. The required emoji is allowed; Chinese or other non-English prose is not allowed.

If creating a commit as part of the PR workflow, use the exact same compact format for the commit message:

```
[ISSUE #issue_id]<emoji><summary>
```

### 4. Write Body
```markdown
### Which Issue(s) This PR Fixes(Closes)

- Fixes #issue_id

### Brief Description

<Changed modules, behavior, motivation. Keep focused on maintainer review.>

### How Did You Test This Change?

<Commands run and results, or honest note that validation was not run.>
```
Preserve template headings exactly. Keep paths repository-relative. In the test section, list commands and results; say "validation not run" honestly if applicable.

Write all public body prose in English. Repository-relative paths, code identifiers, commands, issue numbers, Markdown punctuation, and the required emoji are allowed.

### 5. Validate and Publish
1. Confirm branch, target branch, and issue id.
2. Validate title/body:
   ```bash
   python <skill-dir>/scripts/validate_pr_content.py --title "<title>" --body <body.md>
   ```
3. Use available GitHub tool/CLI. Create a normal ready-for-review PR — do not pass draft flags unless the user explicitly asked for a draft PR.
4. Report PR URL. If publishing unavailable, provide ready-to-submit title and body.

## Common Mistakes

| Mistake | Fix |
|---------|-----|
| Omitting emoji from title | Always include emoji between `]` and summary |
| Leaving `#issue_id` placeholder | Replace with numeric id in both title and body |
| Creating draft PR by default | Normal ready-for-review PR unless user says "draft PR" |
| Claiming unrun validation passed | State "validation not run" honestly in test section |
| Writing PR prose in Chinese or mixed language | Rewrite the title/body in English and rerun validation |
| Adding spaces or extra prefixes | Only `[ISSUE #id]<emoji><summary>` for PR titles and commit messages |
| Using process labels like `task 1`, `stage 1`, or `phase 1` as the summary | Replace them with a concise summary of the actual code, docs, tests, or behavior change |
| Wrong emoji for change type | Match to linked issue type; default to ✨ |

## Output Format

**Full prepare request:**
```
Title:
[ISSUE #issue_id]<emoji><summary>

Body:
### Which Issue(s) This PR Fixes(Closes)
- Fixes #issue_id
...
```

**Title only:** Return candidate titles in the exact format.

## Final Check

- Title matches `[ISSUE #number]<emoji><summary>`
- Commit message, when created, matches `[ISSUE #number]<emoji><summary>`
- Title and commit summary describe the actual change and do not contain sequencing markers like `task 1`, `stage 1`, or `phase 1`.
- Body includes three PR template headings in order with `- Fixes #number`
- Title/body are English-only.
- Test section doesn't claim unrun commands passed
- No local absolute paths, machine-specific roots, or non-English prose
