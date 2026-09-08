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
| Maintain request-header schemas and compatibility fixtures | [Request-header tooling](request-header-codec/README.md) |
| Generate Admin operation fixtures | [Golden fixture generator](generate_admin_operation_goldens.py) |
| Build release candidates | [Release preparation](run-release-preparation.ps1) |

Scripts used by workflows, release tooling, compatibility fixtures, or documented manual operations
belong here. Keep one-time migration edits and ad hoc report converters out of the maintained toolset.
Before removing a script, check imports, callers, workflow routes, fixture generators, and documentation;
the absence of a CI invocation alone does not make a manual tool obsolete.

## Subdirectories

| Directory | Current purpose |
| --- | --- |
| `request-header-codec/` | Offline schema checks, fixture generation, Java compatibility, and shared benchmark inputs |
| `interop/` | Live Java/Rust, HA, and storage-fault interoperability checks |
| `java/` | Java client compatibility smoke harness |
| `kubernetes/` | Live cluster fault operations for dedicated qualification runs |
| `fixtures/` | Input corpora and generated compatibility data used by maintained tools |
| `tests/` | Regression tests and fixtures for maintained script behavior |

Retired stage-closeout assertions, module size freezes, implementation fingerprints,
and V2/V3 or remoting refactor baseline collectors are no longer development gates.
Historical review reports remain reference material. Use the maintained crate tests
and benchmark targets when investigating behavior or performance.
