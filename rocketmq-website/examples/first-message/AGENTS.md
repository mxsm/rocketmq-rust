# AGENTS.md

## Scope and role

- This file applies to `rocketmq-website/examples/first-message/`.
- `rocketmq-doc-first-message` is a standalone Cargo binary with its own `[workspace]` and
  checkout-relative dependencies. Root workspace checks and the Docusaurus build do not compile it.
- It provides the paired `produce` and `consume` modes for the website's first-message tutorial.

## Development validation

Run from this directory for Rust source or manifest changes:

```bash
cargo fmt --all -- --check
cargo check --bin rocketmq-doc-first-message
```

For changed testable behavior, add or reuse focused tests and run
`cargo test --bin rocketmq-doc-first-message <test_name>`.
Instruction-only changes need the repository routing check and `git diff --check`.

## Tutorial consistency

- Keep the client modes, Topic and group names, NameServer address, and the supplied TOML files aligned
  with the English and Chinese getting-started pages.
- Preserve explicit producer/consumer, client runtime, process runtime, and telemetry shutdown on
  success and failure paths, including bounded consumption and Ctrl+C handling.
- When a change needs live validation, follow [local setup](../../docs/getting-started/local-source.md)
  and [the first-message walkthrough](../../docs/getting-started/quick-start.md) from the repository root.
  The sample requires local services and explicitly provisioned Topic/group metadata; its configuration
  resolves data paths beneath `.rocketmq-doc-demo/` relative to the process working directory.
- Keep generated build output and local service data out of commits. Rendered website changes use
  the [website validation guide](../../AGENTS.md).
