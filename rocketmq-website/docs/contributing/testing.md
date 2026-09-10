---
title: "Testing strategy and entry points"
---

Select a test from the behavior that could fail. The repository has package tests, independent application tests, protocol interoperability, recovery scenarios, fuzz targets and performance tooling. They answer different questions; the existence of a matrix or a successful website build is not evidence that every scenario has run.

## Match the test to the change

| Test type | Question answered | Existing entry point | Practical limit |
| --- | --- | --- | --- |
| Unit and focused regression | Does a local state transition, mapping or validation behave correctly? | Owning package's inline tests and `tests/` targets | Does not establish a complete cluster path |
| Deterministic property/state suite | Do bounded generated cases preserve a wire, storage or state-machine invariant? | `scripts/property-state-suite-registry.json` and `scripts/run_property_state_suites.py` | Case coverage is bounded, not a proof over every input |
| Component and lifecycle | Do interacting components cancel, drain and release resources correctly? | Client integration targets; Runtime lifecycle and compile-fail tests | In-process peers do not reproduce every network/storage failure |
| Functional cluster test | Can a configured client and services complete the expected message path? | `scripts/run_client_broker_functional_tests.ps1` and `.github/workflows/v1-functional-acceptance.yml` | Record actual topology, feature selection and assertions |
| Java interoperability | Do selected client/server and HA combinations preserve specified semantics? | `scripts/interop/v1-interop-matrix.json` and `scripts/interop/run_v1_interop.py` | The matrix names Java 5.5.0 and explicitly excludes Java Controller, Java AutoSwitchHA and DLedger CommitLog |
| Storage and deployment faults | What survives crash, restart, replica loss or interrupted rollout? | `scripts/interop/v1-storage-fault-matrix.json`; `.github/workflows/kubernetes-fault-matrix.yml` | Requires isolated disposable state and the scenario's actual environment |
| Fuzzing | Can malformed or unusual inputs break a parser/recovery invariant? | Independent `fuzz/` project and `.github/workflows/fuzz-ci.yml` | A finite campaign cannot establish absence of defects |
| Performance and soak | What are throughput, latency, resource use and sustained behavior under a defined load? | Package benchmarks; architecture SLO workflow; [capacity guide](../operations/capacity-performance.md) | Compare like-for-like workloads and report failures as well as successful operations |
| Product and website | Does a standalone application or rendered documentation work in its own project? | Dashboard/MCP/SRE workflows; `rocketmq-website/` | Root Cargo validation does not cover these independent frontends/applications |

The storage fault inventory includes LocalFile, multipath, RocksDB, compaction, POP, timer, tiered storage, Controller and upgrade scenarios. A completed LocalFile test does not establish the other backend or recovery behaviors. See [protocol compatibility](../reference/protocol-compatibility.md) and [backup/recovery](../operations/backup-recovery.md) for interpreting results.

## Run a small, meaningful local check

For a model-only change, run from the repository root:

```bash
cargo fmt -p rocketmq-model -- --check
cargo test -p rocketmq-model --lib
```

The test invocation already compiles that target. A second `cargo check` is unnecessary solely to repeat compilation. Select a named target or test for a narrower change and confirm that the result contains the intended tests, rather than zero matches.

For example, this existing protocol property test runs a single registered deterministic case family:

```bash
cargo test -p rocketmq-protocol --test remoting_wire_golden deterministic_remoting_cases_round_trip_without_trailing_bytes -- --exact
```

Its registry records 32 generated cases and one Rust test result. Case count and test count are different measurements. The registry runner executes all registered suites and rejects zero-test passes; use it for work spanning those suites, not as a routine requirement for an unrelated page edit.

For standalone examples, use `rocketmq-example/` as the working directory and select the exact example. For frontend work, use the frontend's package scripts. Consult the nearest `AGENTS.md` and manifest before selecting commands; [development guide](./development-guide.md) maps these boundaries.

## Test the contract that changed

- **Protocol or persistence:** exercise old and new field/default/layout behavior, malformed input and relevant readers. A round trip through only the new writer and reader can miss compatibility failures.
- **Async ownership:** synchronize startup and cancellation, await owned tasks, and observe resource release. Prefer channels, barriers or virtual time to arbitrary sleeps. A blocking closure may continue after its caller times out.
- **Optional capability:** test the relevant enabled and disabled configurations. `--all-features` alone cannot demonstrate feature absence.
- **Errors and security:** verify stable identity, boundary mapping and redaction; assert that a denied operation did not reach the side-effecting adapter. Do not log secrets or message bodies to make assertions convenient.
- **Consumer behavior:** distinguish listener completion, ACK/offset persistence and business effects; include redelivery or ownership change where relevant.

Use temporary directories and dynamically allocated ports when practical. Keep tests independent of a developer's cluster and preserve unrelated running processes. Put a newly discovered failure into the smallest reproducible regression that exercises the real invariant.

## Work on a fuzz target

The independent project selects `nightly-2026-07-05` and owns four target/feature pairs: `protocol_decode`, `raw_broker_config`, `controller_snapshot` and `store_recovery_record`. When changing the protocol harness or a consumed interface, run from `fuzz/`:

```bash
cargo +nightly-2026-07-05 check --locked --bin protocol_decode --features protocol_decode
```

This checks harness compilation; it does not run a fuzz campaign. The fuzz workflow owns short nightly and longer weekly runs. Preserve small reviewed regression seeds, while keeping crash output, generated corpus, profiling files and build output outside committed source. Choose another matching target only when its interface or behavior is affected.

## Report what was observed

Include the owning project, selected command/target/features, actual result and material environment limitations. For cluster and fault experiments, also record topology, load, failure trigger and recovery observations. A command being configured in CI is not a successful result; an ignored test is not an executed scenario.

Pure documentation changes need relevant content checks. Rendered website changes use `npm run build` from `rocketmq-website/` and inspection of affected pages. Check runnable snippets when they could break. Writing documentation does not require fingerprints, a fixed checkout, all historical findings to be cleared, or a new approval/CI gate.

## Source references

- [Engineering guidance](https://github.com/mxsm/rocketmq-rust/blob/main/AGENTS.md), [property registry](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/property-state-suite-registry.json) and [runner](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/run_property_state_suites.py).
- [Interoperability matrix](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/interop/v1-interop-matrix.json) and [storage fault matrix](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/interop/v1-storage-fault-matrix.json).
- [Fuzz guidance](https://github.com/mxsm/rocketmq-rust/blob/main/fuzz/AGENTS.md), [Client test targets](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-client/Cargo.toml) and [existing workflows](https://github.com/mxsm/rocketmq-rust/tree/main/.github/workflows).
