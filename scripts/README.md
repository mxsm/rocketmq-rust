# Repository scripts

Use Cargo directly for focused Rust tests and Criterion benchmarks. Select the package, target,
and features for the change; these examples are alternatives, not a mandatory checklist.

```bash
cargo test -p rocketmq-namesrv --test namesrv_bootstrap_config
cargo test -p rocketmq-protocol --test request_header_java_compatibility
cargo test -p rocketmq-store --test ha_transfer_engine
cargo bench -p rocketmq-namesrv --bench topic_table_hot_path_bench
cargo bench -p rocketmq-client-rust --features test-support --bench client_hot_path_benchmark
```

The test and benchmark sources remain owned by their crates. Routine development does not need
separate wrappers to enumerate tests, collect prototype reports, or compare historical snapshots.

## Maintained entry points

| Task | Entry point |
| --- | --- |
| Understand PR and integration checks | [CI validation policy](../rocketmq-doc/en/ci-validation-policy.md) |
| Inspect which root CI checks a path selects | `python scripts/ci_scope.py --paths <changed-path> ...` |
| Check project instruction routing | [PowerShell](check-agents-routing.ps1) or [Bash](check-agents-routing.sh) |
| Refresh the generated validation index | `python scripts/architecture_documentation_guard.py --write` |
| Inspect request-header compatibility fixtures | [Protocol fixtures](../rocketmq-protocol/tests/fixtures/request_header_codec/README.md) |
| Generate Admin operation fixtures | [Golden fixture generator](generate_admin_operation_goldens.py) |
| Build release candidates | [Release preparation](run-release-preparation.ps1) |

Scripts used by workflows, release tooling, compatibility fixtures, or documented manual operations
belong here. Keep one-time migration edits and ad hoc report converters out of the maintained toolset.
Before removing a script, check imports, callers, workflow routes, fixture generators, and documentation;
the absence of a CI invocation alone does not make a manual tool obsolete.

## Subdirectories

| Directory | Current purpose |
| --- | --- |
| `interop/` | Live Java/Rust, HA, and storage-fault interoperability checks |
| `java/` | Java client compatibility smoke harness |
| `kubernetes/` | Live cluster fault operations for dedicated qualification runs |
| `fixtures/` | Input corpora and generated compatibility data used by maintained tools |
| `tests/` | Regression tests and fixtures for maintained script behavior |

Retired stage-closeout assertions, module size freezes, implementation fingerprints,
and V2/V3 or remoting refactor baseline collectors are no longer development gates.
Historical review reports remain reference material. Use the maintained crate tests
and benchmark targets when investigating behavior or performance.

The completed M04-M09 migration suites have been retired, including source-text,
fixed-layout, line-count, and exact-ledger assertions. Their `milestone_contract`
runner tier is no longer available. Use the owning crate's behavior tests and the
maintained dependency/API checks when those boundaries change. Release-script and
dynamic-fixture regression suites remain available through the current CI routes.

The completed Store capability and canonical-export source checks, Trait identity
inventory, Rust lint debt inventory, and central debt count gate are also retired,
including their dedicated tests and unused baselines. Keep regression tests for
maintained CI routing, release tooling, generators, runtime/error checks, and
compatibility behavior. Removing an obsolete gate includes its workflow calls,
runner inventory entry, and documentation commands.

## Necessary checks and architecture tools

| Purpose | Maintained command |
| --- | --- |
| Package layering and dependency direction | `python scripts/architecture_dependency_guard.py --scope core-release` |
| Production unsafe and runtime contracts | `python scripts/rust_hygiene_guard.py --scope core-release` |
| Typed errors and safe output | `python scripts/error_architecture_guard.py` |
| Public API intent | `python scripts/public_api_intent_guard.py --scope core-release` |
| Telemetry contracts and generated metrics | `python scripts/telemetry_semantic_guard.py --scope core-release`; `python scripts/generate_metric_catalog.py --check` |
| Relevant property/state tests | `python scripts/run_property_state_suites.py --suite <suite-id>` |
| Runtime ownership review report | `python scripts/runtime_audit.py --scope core-release` (PowerShell/Bash wrappers are also available) |

ArcMut location freezes, full public API snapshots, M10 performance gates,
release-plan snapshots, cross-registry quotas, and runtime escape inventories are
retired with their dedicated tests and data. Dependency checks validate actual
boundaries; they do not freeze dependency versions, feature lists, or package counts.
Rust hygiene keeps safety failures and reports other observations without a baseline.
Runtime auditing now uses one cross-platform report generator; no fingerprints,
per-file migration dispositions, or baseline-enforcement options remain.

Keep operational tools for release packaging, fixture generation, container checks,
interoperability, and live fault/soak testing. Select those tools for the corresponding
task; they are not an additional routine development checklist.
