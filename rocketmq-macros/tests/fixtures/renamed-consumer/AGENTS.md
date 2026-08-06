# Renamed dependency fixture working agreement

## Scope

- This file applies to the standalone Cargo project in this directory.
- The root repository instructions also apply unless this file is more specific.

## Purpose and boundaries

- Keep `rocketmq-protocol` renamed to `protocol_api`; the fixture proves derive output does not assume the dependency name.
- Keep this project outside the root workspace and suitable for locked, offline validation.
- Do not add production behavior, publishable APIs, or unrelated dependencies here.

## Validation

Run from the repository root:

```bash
cargo check --locked --offline --manifest-path rocketmq-macros/tests/fixtures/renamed-consumer/Cargo.toml
```
