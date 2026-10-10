# cargo-nextest Evaluation

Tracking issue: [#10945](https://github.com/mxsm/rocketmq-rust/issues/10945).

This document records how [cargo-nextest](https://nexte.st/) was evaluated as a test runner for
the root workspace, what had to change for it to run the existing tests correctly, the measured
result, and the current recommendation. It covers the root Cargo workspace on Linux only.
Coverage, Windows/macOS CI, standalone workspaces, benchmarks, and fuzzing are out of scope.

## Current status

Pilot implemented and measured on Linux (GitHub-hosted `ubuntu-latest`, 4 vCPU, 15 GiB RAM,
nextest 0.9.146). With the changes in this pull request, `cargo nextest run` plus `cargo test --doc`
selects the same tests and reports the same results as `cargo test` for both default features and
`--all-features`. Test execution is about 35% faster, but compilation and the first, cold test run
dominate CI time. The recommendation is to keep `cargo test` as the required CI path for now; see
[Recommendation](#recommendation).

## What is in the repository

| Item | Purpose |
| --- | --- |
| `.config/nextest.toml` | Test groups that keep lock-dependent tests serialized, and a `ci` profile (no retries, no fail-fast, report-only slow-test warning, JUnit output). |
| `Nextest Pilot (ubuntu-latest)` job in `.github/workflows/rocketmq-rust-ci.yaml` | Runs `cargo test` and `cargo nextest run` + `cargo test --doc` on the same build, `PILOT_RUNS` times each, reconciles the selected tests and failures, and writes timings to the job summary. Logs and JUnit files are uploaded as the `nextest-pilot-logs` artifact. |
| `scripts/nextest_pilot_compare.py` and `scripts/tests/test_nextest_pilot_compare.py` | Reconciles every measured round from the pilot's logs and exits non-zero on any divergence. Its tests run in the pilot job before the measurement. It also works on a downloaded `nextest-pilot-logs` artifact. |
| `nextest` output of `scripts/ci_scope.py` | Selects the pilot for scheduled and manual runs, and for changes to `.config/nextest.toml`, the root CI workflow, or the reconciliation script and its tests. Ordinary code changes do not run it. |

The pilot is `continue-on-error` and is not a required check. `Build & Test (ubuntu-latest)` still
runs `cargo test` and remains the required test path. A change to `.config/nextest.toml` also
selects the Rust checks, so the configuration is always validated against a full test run.

## How the comparison is kept equivalent

- **Same build.** The pilot compiles test targets once with `cargo test --no-run`. Both runners
  reuse those artifacts, so compilation is measured separately and is not attributed to either one.
- **Same tests.** `cargo test --workspace` runs unit, integration, binary, and doc tests.
  nextest does not run doctests, so the candidate is `cargo nextest run` followed by
  `cargo test --doc`. For **every** measured round, `scripts/nextest_pilot_compare.py` compares:
  - tests run by `cargo test` (excluding doctests) with tests run by nextest,
  - ignored tests with tests nextest skipped,
  - doctests run and ignored inside `cargo test` with those of `cargo test --doc`, and
  - the **identities** of failed tests (nextest binary ID and test name) and of failed doctests
    (crate and doctest name), as well as their counts.

  `cargo test` output is mapped to nextest binary IDs with `cargo nextest list --message-format json`,
  and nextest failures are read from its JUnit report. A difference in any round, a missing log or
  report, or a test target without a result line fails the pilot job. Failures shared by both runners
  in a round do not, because `Build & Test` already reports them.
- **Same features.** The feature selection matches `Build & Test`: default features on pull
  requests, `--all-features` on scheduled and manual runs and when Cargo manifests change.
- **Warm, alternating runs.** One untimed `cargo test` run warms caches first. Some tests invoke
  Cargo themselves (the trybuild compile-fail suites and the `rocketmq-proxy` feature-closure
  checks), and on a cold `target/tests` cache they dominate whichever runner goes first: locally,
  the first nextest run of `rocketmq-broker` + `rocketmq-proxy` took 304 s, and the same tests then
  took 13 s under `cargo test`. Each runner is then timed `PILOT_RUNS` (3) times, alternating which
  one goes first, so medians and spread can be read from the summary table. nextest installation
  time is reported separately because it is a per-job CI cost.

## Compatibility findings

### Process-local locks and port allocators

nextest runs every test in its own process, so a `static` mutex no longer serializes tests the way
it does inside one `cargo test` binary, and a `static` counter restarts in every test process. Each
such item in test code was checked:

| Item | Shared resource | Handling under nextest |
| --- | --- | --- |
| `BROKER_TEST_LOCK` (`rocketmq-broker/tests/broker_transactional_startup.rs`) | Listener pairs are reserved, released, then bound by a broker | `broker-transactional-startup` group, one test at a time |
| Default HA port 10912 (`MessageStoreConfig::default()` in the same file) | The store's HA listener on all interfaces, shared with any other test binary that starts a store with defaults | The tests now reserve a free HA port. The pilot's all-features run caught this: one test failed in two of three rounds under nextest with a `message_store` start error |
| `CONTROLLER_INTEGRATION_TEST_LOCK` and `NEXT_CONTROLLER_TEST_PORT_BLOCK` (`rocketmq-broker/tests/broker_runtime/unit.rs`) | Blocks of fixed ports between 20000 and 60000 are probed and released, then used by three controllers and two brokers | `cluster-port-blocks` group, one test at a time |
| `NEXT_TEST_BASE_PORT` (`rocketmq-controller/tests/multi_node_cluster_test.rs`) | A port-block counter that starts at 15000; under nextest every test process starts from the same block | `cluster-port-blocks` group, one test at a time |
| Fixed `BASE_PORT` 55000 (`rocketmq-controller/tests/simple_cluster_test.rs`) | gRPC servers on ports 55001 and up, inside the broker controller tests' range | `cluster-port-blocks` group, one test at a time |
| `REMOTING_INGRESS_TEST_LOCK` (`rocketmq-proxy/tests/remoting_ingress.rs`) | Proxy listeners started per test | `proxy-remoting-ingress` group, one test at a time |
| `ENV_LOCK` (`rocketmq-model/src/utils/env_utils.rs`, `rocketmq-protocol/src/protocol/static_topic/topic_queue_mapping_utils.rs`) | Process environment variables | No change needed: each nextest process has its own environment |

The `multi_node_cluster_test` problem was found by the pilot itself: on the first Linux run without
a group, two or three of its tests failed in each round after about 21 s, a different subset each
time, while `cargo test` passed them. Locally, the same two binaries failed 3 of 7 tests without the
group and passed 7 of 7 in two runs with it. Other `rocketmq-controller` tests only use addresses as
data and do not bind fixed ports.

Test-group limits apply only within one `cargo nextest run` invocation. Two concurrent invocations
on the same machine are not coordinated, which matches `cargo test`.

### Other test kinds

- **Tokio tests** run unchanged, including `multi_thread` runtimes.
- **trybuild** compile-fail suites (8 test files) run unchanged. trybuild coordinates test
  processes that share a project directory under `target/tests` with a best-effort file lock.
- **Loom** tests are ordinary `#[test]` functions that call `loom::model` and run unchanged.
- **Custom harnesses:** no `[[test]]` target sets `harness = false`. Only benchmarks do, and
  benchmarks are not run by either runner.
- **Ignored tests** are excluded by both runners by default, and the pilot compares the counts.
- **Tests that re-run their own binary** (the `rocketmq-error` backtrace probes) work under both
  runners. Their child process prints its own `test result:` line into `cargo test` output, so the
  pilot counts only the last result line of each test target.
- **Timeouts:** `cargo test` has no per-test timeout. The `ci` profile reports tests that run
  longer than 120 s but does not terminate them, so the pilot cannot introduce new timeout failures.
- **Cross-binary concurrency:** `cargo test` runs test binaries one after another, while nextest
  interleaves tests from all binaries. This is the main new source of contention for tests that
  bind ports or share directories outside a temporary root. After the fixes in this section, three
  measured rounds for each feature set showed no failures that occurred only under nextest.

### Machine size and serialized groups

The `cluster-port-blocks` group runs its 11 tests one at a time. In the default-features run their
durations add up to 53 s (the longest takes 8 s), while the whole nextest run took 85 s on a 4 vCPU
runner. On a machine with more cores, the other tests finish sooner but this chain does not, so it
sets a floor on nextest's wall time. `cargo test` runs the six `multi_node_cluster_test` tests in
parallel within their binary, so on such a machine nextest can be slower than `cargo test`. The
measurements in this document are from 4 vCPU runners only. Giving those tests port allocation that
is safe across processes would let the group be removed; that is a test change outside this pilot.

## Local commands

Install a pinned nextest release, for example with
`cargo install cargo-nextest --version 0.9.146 --locked` or a
[prebuilt binary](https://nexte.st/docs/installation/pre-built-binaries/). Then, from the
repository root:

```bash
# Baseline
cargo test --workspace

# Candidate with equivalent doctest coverage
cargo nextest run --workspace --profile ci
cargo test --workspace --doc
```

Repeat with `--all-features` on all three commands to cover the full feature set. JUnit output from
the `ci` profile is written to `target/nextest/ci/junit.xml`.

To check CI path selection for the configuration and the reconciliation logic:

```bash
python -m unittest discover -s scripts/tests -p test_ci_scope.py
python -m unittest discover -s scripts/tests -p test_nextest_pilot_compare.py
python scripts/ci_scope.py --paths .config/nextest.toml
```

To reconcile a downloaded `nextest-pilot-logs` artifact:

```bash
python scripts/nextest_pilot_compare.py --logs <artifact-dir> --runs 3
```

## Results

Measured on GitHub-hosted `ubuntu-latest` runners (4 vCPU, 15 GiB RAM) with nextest 0.9.146, on
the commit proposed in this pull request. Each round runs both runners on the same build, after one
untimed warm-up run, alternating which runner goes first. Times are wall-clock seconds.

**Default features**

| Round | First | `cargo test` | `cargo nextest run` | `cargo test --doc` | nextest + doctests |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | cargo test | 157 | 88 | 18 | 106 |
| 2 | nextest | 164 | 88 | 18 | 106 |
| 3 | cargo test | 164 | 87 | 18 | 105 |
| **Median** | | **164** | **88** | **18** | **106** |

**All features**

| Round | First | `cargo test` | `cargo nextest run` | `cargo test --doc` | nextest + doctests |
| --- | --- | ---: | ---: | ---: | ---: |
| 1 | cargo test | 164 | 85 | 17 | 103 |
| 2 | nextest | 159 | 85 | 17 | 103 |
| 3 | cargo test | 153 | 84 | 17 | 101 |
| **Median** | | **159** | **85** | **17** | **103** |

An earlier pair of runs on a previous revision of the pilot gave consistent warm results
(default: 141-146 s versus 98-101 s; all features: 164-172 s versus 105-108 s).

**Test selection (last round)**

| | Default: `cargo test` | Default: nextest + doc | All: `cargo test` | All: nextest + doc |
| --- | ---: | ---: | ---: | ---: |
| Tests run | 10419 | 10419 | 10790 | 10790 |
| Failed | 1 | 1 | 1 | 1 |
| Ignored / skipped | 19 | 19 | 20 | 20 |
| Doctests run | 135 | 135 | 135 | 135 |

The one failure under both runners is
`rocketmq-observability::architecture_guards::metric_name_constants_are_declared_only_in_canonical_or_legacy_files`,
which also fails on `main` at the time of measurement and is unrelated to this evaluation.

**Costs outside test execution (same jobs)**

| | Default features | All features |
| --- | ---: | ---: |
| nextest installation (prebuilt binary) | 1 s | 0 s |
| Shared test build (`cargo test --no-run`) | 2113 s | 2976 s |
| First `cargo test` after the build (untimed warm-up) | 1546 s | 2453 s |

The first test run after a build is far slower than later runs: 1546 s against a 164 s warm median
for default features. Tests that invoke Cargo themselves, such as the trybuild compile-fail suites and
the `rocketmq-proxy` feature-closure checks, compile on a cold `target/tests` cache and are the likely
main cause: locally, three of them took 299 s, 60 s, and 60 s on a cold cache and a few seconds once
warm. A per-test breakdown of the cold run on Linux was not collected. `Build & Test` pays this cold cost on every run, whichever runner is used.

## Recommendation

- **Compatibility:** with three test groups and one isolated HA port, nextest runs the root
  workspace with identical test selection and results for default features and `--all-features`.
- **Benefit:** warm test execution drops from about 160 s to about 105 s including doctests, a saving
  of about 55 s (35%) per run.
- **Limit:** in CI that saving is small next to the 35-50 minute build and the 25-40 minute first,
  cold test run. On the measured `Build & Test` job, it would save well under 5% of the job time.
  On machines with more cores, the serialized `cluster-port-blocks` group can make nextest slower
  than `cargo test` (see [Machine size and serialized groups](#machine-size-and-serialized-groups)).

Recommendation: **narrow the rollout and defer migrating the required job.**

1. Keep `cargo test` in `Build & Test` as the required check.
2. Keep `.config/nextest.toml` and the non-blocking pilot, which runs only on scheduled and manual
   runs and when its configuration changes, so nextest compatibility keeps being checked.
3. Developers can use nextest locally, where builds are warm and the shorter test runs add up.
4. Before any migration, make the cluster tests' port allocation safe across processes so that the
   `cluster-port-blocks` group can be removed, and repeat the measurement on a larger machine.
5. Revisit migration if the build and cold-test costs come down. The first, cold test run is the
   larger lever: it takes about 23 minutes (default features) to 38 minutes (all features) longer
   than a warm run, compared with about one minute saved by the runner. Profiling which
   Cargo-invoking tests account for that time on Linux is a useful follow-up.

During the measurement, one `Build & Test` run with `--all-features` also hung in
`rocketmq-broker` test `structured_send_after_write_completions_are_correlated_by_request_id`
under plain `cargo test` until the 6-hour job limit, while the same test passed in all four
`cargo test` runs of the pilot on the same commit. nextest can terminate such a test with
`slow-timeout = { period = "...", terminate-after = N }`. The pilot leaves this disabled so that it
cannot add failures that `cargo test` would not have; enabling it is a separate decision.

## Fallback

Removing the `nextest-pilot` job, the `nextest` scope output, and `.config/nextest.toml` restores
the previous CI jobs: the required `Build & Test` job never depended on nextest. The HA port change
in `broker_transactional_startup.rs` is independent of the runner and can stay. If nextest is adopted later and a test
regresses only under nextest, run it with `cargo test -p <package> <test_name>` to confirm, then
either fix its shared-resource use or add it to a test group.
