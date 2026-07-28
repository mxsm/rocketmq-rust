# AGENTS.md

## Scope

This file owns the standalone `fuzz/` Cargo project. Root repository
instructions remain cumulative.

## Toolchain and targets

- Keep this project outside the root workspace.
- Build with the fixed `nightly-2026-07-05` toolchain.
- Preserve exactly these four owned targets: `protocol_decode`,
  `raw_broker_config`, `controller_snapshot`, and `store_recovery_record`.
- Keep target features explicit so a target cannot silently compile without
  its owning production dependency.

## Artifacts and corpus

- Curated, minimal regression seeds under `corpus/` are source assets and may
  be committed after review.
- Never commit `target/`, `artifacts/`, crash outputs, generated minimization
  directories, profiler data, or temporary corpus files.
- Do not run long-duration fuzzing as part of the normal local validation
  route. `.github/workflows/fuzz-ci.yml` owns short nightly and longer weekly
  execution, corpus/crash retention, and commit-bound evidence artifacts.

## Validation

Run from `fuzz/`:

```bash
cargo +nightly-2026-07-05 check --locked --all-targets --all-features
```

When `Cargo.toml` or `Cargo.lock` changes, also run:

```bash
cargo audit --file Cargo.lock
```

Changes to the root path dependencies `rocketmq-broker`,
`rocketmq-controller`, `rocketmq-protocol`, or `rocketmq-store-local` require
the fixed-nightly build check.
