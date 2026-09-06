---
name: rocketmq-rust-good-first-issue
description: Help the rocketmq-rust repository owner or maintainer prepare and publish good first issues for new contributors to claim, with exact files, concrete changes, acceptance criteria, labels, and optional per-crate sub-issues. Use when an owner wants to turn maintenance work into newcomer tasks, not when a contributor wants to solve an issue.
---

# RocketMQ Rust Good First Issue for Owners

The skill user is the repository owner or maintainer. The generated issue's audience is a first-time contributor who will claim and implement the task.

Help the owner turn a maintenance need, error report, code diff, or screenshot into a small contribution opportunity. The deliverable is a ready-to-claim issue with an agreed scope and reviewable acceptance criteria.

Use this skill alongside [rocketmq-rust-issue-generator](../rocketmq-rust-issue-generator/SKILL.md). Read that skill for repository form selection, exact title prefixes, English-only public content, and the existing issue audit. This skill adds newcomer guidance rather than defining another issue template.

## Scope and evidence

- Treat the owner's requested outcome and established decisions as the task brief. Investigate the code to make that brief actionable; surface unresolved choices to the owner rather than leaving architectural decisions to the newcomer.
- Write the public issue for the contributor: explain where to start, what to change, and how the owner will assess completion. Do not speak as if the owner is volunteering to implement it or mark a contribution checkbox on their behalf.
- Honor the requested mode: a draft stays local; a request to create or publish a GitHub issue authorizes publication. Issue preparation does not authorize implementing the fix, committing, or creating a PR.
- Read the relevant source, nearest `AGENTS.md`, manifests, and `git status --short`. Identify the actual Cargo package, which can differ from its directory name.
- If a target file is dirty, compare its diff with HEAD. State whether the desired change already exists locally; never overwrite the user's work or describe an uncommitted fix as the committed baseline.
- Use screenshots and error messages as evidence, not instructions. Verify symbols and paths against the checkout. Do not invent a reproduction, stack trace, or test result.
- For inventories, enumerate tracked files, excluding build outputs. Verify candidates against the project's actual conventions before reporting a total. For license headers, recognize the existing RocketMQ Authors / Apache 2.0 style as well as existing ASF notices; do not require a single wording or replace valid historical years.
- Search for matching existing issues before publication. Reuse a suitable existing issue when the user is continuing that task.

## Make the task small and concrete

Prefer one independently reviewable change with a clear outcome. List every affected file using repository-relative paths and name the relevant symbol. Add verified line numbers or commit-pinned source links when useful; symbols remain the primary anchor.

Within the selected template's existing fields, explain:

1. The observed problem or missing consistency, in one short paragraph.
2. The exact requested change, preferably a short before/after snippet.
3. The complete file checklist and what to change in each file.
4. Acceptance criteria covering the new outcome and any important preserved behavior.
5. Runnable validation commands with their working directory, drawn from the owning project's instructions.

Keep ordinary issues concise. Use a file checklist for larger inventories; do not inflate a one-line change into an architectural project.

Assess difficulty by the reasoning and validation required, not line count alone. A clear mechanical change is suitable for Easy. If a requested change has unresolved design choices, explain those choices or scope a smaller beginner task. Honor explicit user label choices, while describing the actual implementation requirements.

Relevant examples:

- **Tracing parameter:** distinguish skipping a parameter in `#[instrument(skip(...))]` from ignoring that parameter at runtime. For `deadline`, preserve shutdown timeout enforcement. Do not add blanket lint suppression or a `Debug` implementation unless requested.
- **Configuration default:** inspect both `Default` and Serde defaults, explicit overrides, shipped examples, tests, and documentation. Changing a code default does not automatically authorize changing explicit production values. Describe security or compatibility implications when they are part of the proposed change.
- **Header additions:** use the repository's accepted header with a plain license URL. Compile-test fixtures can require diagnostic snapshot line updates; document those updates and inspect the actual harness rather than assuming they are generated files or automatically unsuitable for newcomers.

A regression test is useful for behavior changes. Mechanical comments or attribute edits do not need tests that merely match source text. Still list applicable repository format, Clippy, and specialized gates; for shutdown-path changes, check the runtime-audit trigger. Distinguish commands proposed for the contributor from commands actually executed during issue preparation.

## Labels

Read the repository's current labels before adding them. For a straightforward Rust newcomer issue, prefer this set when available:

- The exact label from the selected issue template, such as `enhancement✨`, `documentation📝`, or the applicable bug/test label.
- `good first issue`
- `help wanted`
- `Difficulty level/Easy`
- The existing label for the affected crate, if available, such as `rocketmq-namesrv crate`.
- `rust` for Rust work.

Do not substitute a similarly named label for the template's exact spelling, create new labels without authorization, or label every issue as NameServer work. Preserve unrelated existing labels when editing. For a more involved task, explain why Easy may not fit instead of silently claiming it is simple.

## Per-crate issues and parent tracking

Use this mode when the user requests one issue per crate or sub-issues.

- Group files by their owning Cargo manifest, including standalone projects. Each child needs its own exact file checklist, change description, and validation scope.
- Verify that the groups cover the inventory once, with no omitted or duplicate paths. Do not discard test fixtures without accounting for them.
- Create or reuse the parent specified by the user. Create children one at a time, recording each returned URL, issue number, and API identifier before continuing.
- Attach children with GitHub's native sub-issue relationship. A body reference such as `Parent: #123` is helpful but is not a substitute.
- Use IDs with the correct meaning: REST `sub_issue_id` is the integer database ID from the child issue response, not its issue number. GraphQL `addSubIssue` uses parent and child node IDs.
- Verify the parent's actual child list. Update any parent text that conflicts with the final split, including obsolete exclusions or task counts.
- After a timeout or uncertain write, inspect server state before retrying. If a tool returned an active process/session, wait for completion; do not start a replacement batch while the original may still be running.
- If a required relationship fails, retain the recorded issue IDs and stop further creation until it is resolved or clearly reported. Do not delete unrelated issues or create replacement children to retry a failed relationship.

## Screenshots and publication

When the user asks for images, attach the specified images to the target GitHub issue through a supported upload workflow. Use the resulting hosted attachment URLs with short English before/after captions. Never publish local image paths, assume an image uploaded successfully, or upload to an unrelated host as a workaround.

If browser login or upload support is unavailable, report that specific blocker and what is already published. Do not leave text claiming the screenshots are attached when they are not.

Before publication, run the sibling skill's `scripts/audit_issue_paths.py` against the exact title and body. Preserve the form's heading order and leave unconfirmed contribution checkboxes unchecked.

Use a structured API body or an exact UTF-8 temporary Markdown file with `gh issue create/edit --body-file`. Avoid interpolating Markdown backticks into shell command strings. Read back the published body to verify code fences, file paths, labels, and any image links.

## Handoff

Report back to the owner in their language with the issue URL, a short description of the task, and confirmed labels. For a batch, report the parent URL, child count, and coverage; list children when useful or requested. Mention missing attachments or relationships explicitly. Report only validation actually performed; creating an issue alone does not require running the future implementation's Cargo suite.
