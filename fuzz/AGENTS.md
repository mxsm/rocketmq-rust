# AGENTS.md

## Scope

This file owns the standalone `fuzz/` Cargo project. Root repository
engineering rules apply; the local commands below replace the root validation fallback.

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

## Development validation

When a harness or the interface/behavior it consumes changes, check that target with its matching feature.
For example, from `fuzz/`:

```bash
cargo +nightly-2026-07-05 check --locked --bin protocol_decode --features protocol_decode
```

The other target/feature pairs have the same name: `raw_broker_config`, `controller_snapshot`,
and `store_recovery_record`. A local internal edit in a path dependency does not automatically require
every fuzz target to rebuild.

For changes spanning the harness feature setup, or a fuzz integration task, use:

```bash
cargo +nightly-2026-07-05 check --locked --all-targets --all-features
```

Choose `cargo audit --file Cargo.lock` for dependency/security review when relevant. Long fuzzing and
CI evidence collection remain in the fuzz workflow, not the normal development completion condition.
