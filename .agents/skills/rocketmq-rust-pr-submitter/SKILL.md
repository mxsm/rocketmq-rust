---
name: rocketmq-rust-pr-submitter
description: Use when the user asks to prepare, submit, publish, or optimize a pull request for the rocketmq-rust project, especially when the PR must follow the `[ISSUE #issue_id] <emoji> <summary>` title convention, the `.github/PULL_REQUEST_TEMPLATE.md` body format (Fixes, Brief Description, How Did You Test This Change), and must not be created as a draft PR unless the user explicitly requests one.
---

# RocketMQ Rust PR Submitter

Prepare RocketMQ Rust PR titles and bodies from `.github/PULL_REQUEST_TEMPLATE.md`. Connects PR to issue via `[ISSUE #id]` title prefix and `Fixes #id` body line.

## When to Use

- User asks to create, prepare, or publish a PR for rocketmq-rust
- PR title must follow `[ISSUE #issue_id] <emoji> <summary>` format
- PR body must follow the repository's real PR template (not a custom format)
- Issue id is available from user request, branch name, or commit history

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
[ISSUE #issue_id] <emoji> <summary>
```
Exactly one space after `]` and after emoji. No extra prefixes (conventional commit, branch name). Keep summary short and action-oriented.

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
| Adding extra prefixes (conventional commit, branch name) | Only `[ISSUE #id] <emoji> <summary>` |
| Wrong emoji for change type | Match to linked issue type; default to ✨ |

## Output Format

**Full prepare request:**
```
Title:
[ISSUE #issue_id] <emoji> <summary>

Body:
### Which Issue(s) This PR Fixes(Closes)
- Fixes #issue_id
...
```

**Title only:** Return candidate titles in the exact format.

## Final Check

- Title matches `[ISSUE #number] <emoji> <summary>`
- Body includes three PR template headings in order with `- Fixes #number`
- Test section doesn't claim unrun commands passed
- No local absolute paths or machine-specific roots
