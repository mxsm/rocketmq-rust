# M09-05 R0/R1/next-major release package evidence

## Result

M09-05 converts the approved release migration policy into a machine-readable plan, an executable guard, CI
enforcement, and three release-facing documents. The overall architecture-refactor inventory advances to **58/82**
work packages completed, with 24 remaining; M09 has one remaining package, PR-M09-06.

## Traceability

| Field | Value |
|---|---|
| Work package | `PR-M09-05` |
| Issue | [#8256](https://github.com/mxsm/rocketmq-rust/issues/8256) |
| Branch | `mxsm/architecture-refactor-r0-r1-next-major-release-package` |
| Design source | `docs/architecture-refactor-design.md` release migration and compatibility strategy |
| Plan | `scripts/architecture-release-plan.json` |
| Guard | `scripts/architecture_release_guard.py` |
| CI | `.github/workflows/rocketmq-rust-ci.yaml` architecture guard job |

## Delivered decisions

- [x] The exact 32-package publish order follows the target DAG and preserves the approved six-stage release chain.
- [x] R0 lists all ten new crates, canonical imports, deprecated compatibility owners, a no behavior change statement,
  and rollback.
- [x] R1 lists the exact 29 ledger edges across 12 consumers and retains the existing Linux/Windows CI no-growth rule.
- [x] External usage collection has four sources, a release cadence, fail-closed uncertainty handling, four notification
  channels, and measurable next-major thresholds.
- [x] next-major records the exact four dependency edges plus admin legacy, common compat, remoting deep path, and Proxy
  optional mode feature removal scopes.
- [x] The two long-term Store composition edges remain outside the deletion plan.
- [x] Human approval covers the relative release windows and notification plan; destructive removal remains pending a
  separate signed next-major evidence gate.

## Automated invariants

`scripts/architecture_release_guard.py` fails when any of these drift:

1. a package is missing, duplicated, or published before a target-DAG dependency;
2. the ten-crate inventory changes;
3. R1 29 / next-major 4 / long-term 2 differs from the compatibility baseline;
4. a planned compatibility edge is removed before its window;
5. Proxy next-major features are activated early;
6. external-usage thresholds, Human approval boundary, release documents, or CI hooks disappear.

## Validation record

| Command | Result |
|---|---|
| `python scripts/architecture_release_guard.py` | passed: topology 32/32, crates 10/10, windows 29/4/2, early activation 0 |
| `python -m unittest scripts.tests.test_architecture_release_guard -v` | passed: 6/6 |
| `python scripts/architecture_dependency_guard.py --mode target` | passed: compatibility 35/35, dev-only 3/3, unauthorized 0 |
| `python scripts/architecture_dependency_guard.py --mode baseline` | passed |
| architecture dependency guard unit/fixture tests | passed: 43/43; built-in fixtures clean 1, expected violations 6 |
| Proxy next-major feature closure tests | passed: 7/7 |
| M09 target/facade/client affected contracts | passed: 16/16 |
| CI `test_*guard.py` discovery | passed: 114/114 |
| `cargo metadata --format-version 1 --locked --no-deps` | passed: exact lockfile metadata resolved |
| `./scripts/check-agents-routing.ps1` | passed: standalone Cargo 4, Node 3, routes 8 |
| `git diff --check` | passed |

This package changes Python policy tooling, CI wiring, and Markdown only. It does not change Rust source, Cargo manifests,
features, public API, wire/storage format, or runtime behavior; root Cargo fmt/Clippy are therefore not triggered by the
documentation-only Rust validation policy. Workflow routing validation remains mandatory and is recorded above.

## Rollback boundary

Revert the plan, guard, CI hook, and release documents together. Do not roll back by deleting canonical crates, restoring
duplicated facade implementations, changing feature defaults, or rewriting persisted/wire data. Any future destructive
removal is a separate next-major work item with a fresh frozen snapshot and approval.
