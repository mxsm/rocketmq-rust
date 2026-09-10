---
title: "Contribute to RocketMQ-Rust"
---

# Contribute to RocketMQ-Rust

Start with a concrete user problem and the component that owns it. A useful contribution can be a reproducible report, a corrected bilingual example, a focused regression test or a behavior change. The [development guide](./development-guide.md) explains environment setup and the local feedback loop; this page explains how to choose and present the work.

## Choose an entry point

| Your goal | First material to read | Useful contribution |
| --- | --- | --- |
| Report a failure | [Troubleshooting](../operations/troubleshooting.md) and the relevant component guide | Exact operation, expected/actual result, version/features and a small reproducer |
| Improve a tutorial or translation | [Documentation guide](./documentation.md) and both language files | Correct commands, prerequisites, expected output and a full counterpart translation |
| Fix a core behavior | [Architecture](../architecture/overview.md) and [module map](../architecture/module-map.md) | A focused change at the owning layer with regression coverage |
| Improve client integration | [Client configuration](../configuration/client-config.md) and [API migration](../migration/rust-api.md) | A public-API example with lifecycle and completion semantics |
| Work on an application | [Ecosystem](../ecosystem/overview.md) and that application's local guide | Product-specific behavior, UI or service integration in its own project |
| Improve performance | [Capacity and performance](../operations/capacity-performance.md) | A defined workload, observed bottleneck and comparable measurements |

Use [GitHub Issues](https://github.com/mxsm/rocketmq-rust/issues) for actionable changes and [Discussions](https://github.com/mxsm/rocketmq-rust/discussions) for usage/design questions. Inspect related work before implementing the same change. Describe the behavior you intend to improve when an issue is broad; avoid expanding a small fix into unrelated refactoring.

## Prepare the correct project

Clone your fork or an authorized checkout, then follow [development setup](./development-guide.md). Read `git status --short` before editing so existing changes stay intact. Use the nearest `AGENTS.md` and current manifest to choose the build root.

The root Cargo workspace, standalone examples, Dashboard applications, MCP, SRE and website have different commands. A directory name is not necessarily a Cargo package name: `rocketmq-client` is selected as `rocketmq-client-rust`. [Release scope](../overview/release-scope.md) explains why workspace membership and product release membership differ.

Preserve page IDs, public APIs and serialized fields unless the change intentionally addresses that contract. Keep dependencies and configuration changes limited to what the problem requires. Do not reformat unrelated files or replace someone else's uncommitted work.

## Report a problem that can be reproduced

Use the repository's issue form for the change type. Include the smallest useful description:

1. Component, source or artifact version, relevant features/backend/mode and platform.
2. Preconditions and exact operation; include a minimal example when possible.
3. Expected behavior and actual observed behavior, including stable error codes and request identifiers.
4. Impact and any workaround already tested.
5. For a proposed fix, the owning files and how the changed behavior will be observed.

Remove credentials, bearer tokens, ACL/TLS material, message bodies and unrelated personal or production details from public reports. Preserve the shape and relevant type of a redacted input so the example remains useful. Do not infer an outage's cause solely from a generic client timeout.

The issue templates distinguish bugs, features, enhancements, refactoring, tests and documentation. Their current fields and title prefixes live in [.github/ISSUE_TEMPLATE](https://github.com/mxsm/rocketmq-rust/tree/main/.github/ISSUE_TEMPLATE); use the actual form instead of inventing extra mandatory fields.

## Implement and check the affected behavior

Keep the change small enough for a reviewer to connect cause, implementation and result. Follow [coding standards](./coding-standards.md), reuse current abstractions and put shared behavior in its owning layer.

For a Rust behavior change, choose a package/target and a focused regression test. A test build that compiles the affected code can also provide compilation evidence. Add checks for directly affected consumers when a shared API or feature changes. A root all-targets build or all-features run is not automatically required for every contribution.

For website content, update English and Chinese together, retain executable examples and technical limits, and run `npm run build` in `rocketmq-website`. Documentation work does not require source fingerprints, a clean-worktree ceremony, a score or a new approval process. Use normal proofreading and the existing preview/build tools.

Record what actually ran and what it demonstrated. If an external cluster, GUI platform or optional tool is unavailable, describe the untested scenario rather than claiming it passed or modifying unrelated dependencies to bypass it.

## Submit a reviewable pull request

Link the issue and use the current [pull request template](https://github.com/mxsm/rocketmq-rust/blob/main/.github/PULL_REQUEST_TEMPLATE.md). The repository convention uses an issue-linked English title, for example:

```text
[ISSUE #1234]📝Clarify consumer offset completion
```

Replace the example number with the actual issue. Describe the concrete problem and final behavior, then summarize relevant validation and limitations. For a semantic change, explain the compatibility or migration effect; for a simple documentation correction, a short description and relevant build result are sufficient.

Review feedback should improve the same scoped result. Update the description if the final implementation changes scope. Leave generated builds, logs, coverage, temporary screenshots and local test data out of the commit. Keep screenshots only when they are intentional documentation assets and accurately represent the product.

A submitted PR does not imply a release or deployment. Maintainers handle integration and release decisions through the applicable project workflow. Community release identity is described in [release scope](../overview/release-scope.md).

Sources: [root engineering agreement](https://github.com/mxsm/rocketmq-rust/blob/main/AGENTS.md), [website guide](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/AGENTS.md), [issue templates](https://github.com/mxsm/rocketmq-rust/tree/main/.github/ISSUE_TEMPLATE), [PR convention](https://github.com/mxsm/rocketmq-rust/blob/main/.agents/skills/rocketmq-rust-pr-submitter/SKILL.md).

## Testing and release work

- [Testing strategy and entry points](./testing.md)
- [Release engineering](./release-engineering.md)
