# M06-03o report: CommitLog append values and minimal mapped-file config

## Outcome

Moved four CommitLog append value/contracts and the minimal mapped-file configuration enum from Store to
`rocketmq-store-local`. Store keeps all five existing deep public paths as direct exact re-exports of the Local
canonical owner, so downstream code observes the same Rust types and trait rather than wrappers, aliases, or
conversion seams.

This slice does not move message encoding, `AppendMessageCallback`, Put/Get status or result values, the complete
`MappedFile` trait, `DefaultMappedFile`, per-file flush I/O, the flush manager, or group commit.

## Canonical ownership

`rocketmq-store-local::commit_log::append` is the sole owner of:

- `AppendMessageStatus`
- `AppendMessageResult`
- `PutMessageContext`
- `CompactionAppendMsgCallback`

`rocketmq-store-local::config` is the sole owner of `FlushDiskType`.

Local adds `serde` as an ordinary dependency for the existing manual `FlushDiskType` deserializer and
`serde_json` only as a dev-dependency for vocabulary tests. `bytes` was already a Local dependency. `Cargo.lock`
records the two newly direct dependencies without changing versions.

## Compatibility contract

The five Store compatibility modules contain direct exact `pub use` statements:

- `rocketmq_store::base::message_status_enum::AppendMessageStatus`
- `rocketmq_store::base::message_result::AppendMessageResult`
- `rocketmq_store::base::put_message_context::PutMessageContext`
- `rocketmq_store::base::compaction_append_msg_callback::CompactionAppendMsgCallback`
- `rocketmq_store::config::flush_disk_type::FlushDiskType`

The cross-crate identity fixture proves bidirectional assignment for all value types and trait-object identity for
`CompactionAppendMsgCallback`. `PutMessageStatus`, `GetMessageStatus`, and `PutMessageResult` remain defined in
Store. Existing Store callers, message append code, mapped-file code, configuration deserialization, and public
paths therefore compile without conversion adapters.

The canonical source contract freezes:

- all five `AppendMessageStatus` variants, derives, `PutOk` default, and Java-compatible Display vocabulary;
- all nine public `AppendMessageResult` fields and types, Clone, default values, Display order, `is_ok`, and lazy
  supplier precedence over an eager message id;
- all three private `PutMessageContext` fields and its owned-key, vector, batch, immutable-slice, and mutable-slice
  accessors;
- the exact object-safe `CompactionAppendMsgCallback::do_append` signature;
- `FlushDiskType` derives, `AsyncFlush` default, manual Deserialize implementation, four accepted input tokens,
  and unknown-variant vocabulary.

It also forbids `AppendMessageCallback`, Put/Get status/result owners, `DefaultMappedFile`, `ArcMut`,
`rocketmq-common`, `rocketmq-rust`, and Store facade dependencies from entering Local append ownership.

## TDD and mutation evidence

RED was established before production migration:

- the Local behavior fixture failed with five E0432 imports because `commit_log::append` and `config` did not
  exist, plus the expected missing `serde_json` test dependency;
- the owner contract failed because the canonical definitions still lived in Store;
- the facade mutation scanner passed independently, proving the owner failure was the real missing boundary.

GREEN evidence:

- Local append/config behavior goldens: 5/5;
- Store-to-Local exact identity fixture: 1/1;
- legacy Store status, result, config, compaction, and store-api focused tests: 7/7, 8/8, 8/8, 1/1, and 9/9;
- full Local suite: 54 unit plus 95 integration tests; nine doctests remain intentionally ignored;
- final M06 contract: 87/87.

The 87th contract case mutates each of six canonical classes: Status, Result, Context, compaction trait, Flush,
and forbidden-scope edges. Every mutation is rejected, and the actual canonical files have zero source-contract
violations.

## Feature and configuration truth

Passed:

- Local default, no-default, fast, safe, fast+safe, and all-feature checks;
- Store default and all-feature checks;
- Store no-default checks with the real owner enabled for `local_file_store,fast-load`,
  `local_file_store,safe-load`, and `local_file_store,fast-load,safe-load`.

The four raw Store combinations without a backend owner remain an explicitly non-passing pre-existing baseline:

- no default features;
- fast only;
- safe only;
- fast+safe only.

Every raw probe exits 101 with 124 E0004 non-exhaustive-match errors and one unused `ArcMut` warning. BASE has
zero diff for root `Cargo.toml`, `rocketmq-store/Cargo.toml`, and `rocketmq-store/src/message_store.rs`, proving
M06-03o neither introduced nor concealed that baseline.

## Dependency and consumer audit

The Local normal dependency tree contains 67 packages and no `rocketmq-common`, `rocketmq-remoting`,
`rocketmq-store`, `rocketmq-rust`, `rocketmq-broker`, or Store RocksDB edge. Architecture source and manifest
guards independently enforce the same closure.

The four standalone Cargo projects have no direct `rocketmq-store-local` or `rocketmq-store` dependency.
`rocketmq-store-inspect`, which consumes Store, is confirmed by Cargo metadata to be a root-workspace member and
is covered by root workspace Clippy.

## Platform evidence

Passed under WSL/Linux with `CARGO_TARGET_DIR=target/wsl-m06-03o`:

- Local append/config focused tests: 5/5;
- Store exact identity fixture: 1/1;
- Local default check;
- Store default check.

The isolated build used Rust/Cargo 1.98 nightly. `cargo clean --target-dir target/wsl-m06-03o` removed 8,480
files (4.4 GiB), and the target directory was verified absent afterward.

## ArcMut audit

ArcMut unit tests pass 63/63 and fixtures pass 24/24. Canonical initial and final guards pass, and
`scripts/arc-mut-baseline.json` is unchanged from BASE
`1da5f8d638b5e84fc6808e31a53c6d994b630fe9`.

A detached BASE worktree was scanned and promoted to a transient candidate independently of the current tree.
Direct BASE-scan-to-current-candidate comparison reports 1,232 identities and 3,375 occurrences on both sides,
zero NEW, zero STALE, zero line-only changes, and zero semantic changes. The detached worktree was removed and
pruned; the transient current candidate under `target/` was also removed before commit.

The canonical baseline-file-to-current diagnostic still shows the nine historical `commit_log_loader.rs`
line-only updates recorded by M06-03n. That diagnostic is not the direct M06-03o BASE comparison; it introduced
no new identity, occurrence, fingerprint, item, or relocation approval.

## Validation

Passed on Windows:

- `cargo fmt --all -- --check`
- Local append/config focused 5/5 and full Local suite
- Store exact identity 1/1 plus legacy focused status/result/config/compaction/store-api suites
- `python -m unittest scripts.tests.test_m06_store_local_contract`: 87/87
- Local six-case and supported Store five-case feature matrices
- Local and Store package Clippy plus root workspace all-target/all-feature Clippy with `-D warnings`
- strict Local Rustdoc; normal Store Rustdoc with exactly four unrelated pre-existing invalid-HTML warnings
- architecture guard 35 unit tests, fixtures, and baseline mode
- AGENTS routing (`standalone_cargo=4`, `node_projects=3`, `routes=8`)
- ArcMut guard 63 unit tests, 24 fixtures, initial/final guard, promotion, canonical comparison, and direct BASE scan
- Local normal dependency tree and standalone consumer audit
- `git diff --check`, checklist consistency, final staged-diff review, and clean post-commit worktree

Root/package Clippy results were produced against the final Rust candidate. Only Python contract and Markdown
evidence changed afterward, so the Rust candidate did not require a repeated Clippy build after documentation.

Typed-error, runtime-ownership, and observability specialized gates were not triggered: this slice changes no
typed error mapping, task/runtime/blocking path, observability feature, or telemetry wiring.

## Scope and handoff

M06-03o is a base-value ownership slice, not the full mapped-file migration. `AppendMessageCallback`, common
broker/batch message types, select results, transient pools, memory-lock/platform helpers, `MappedFile`,
`DefaultMappedFile`, and flush orchestration remain outside Local canonical ownership.

Before the complete mapped-file owner can move, the architecture still requires two hard prerequisites:

1. remove or contain `ArcMut` without creating a Local-to-`rocketmq-rust` dependency;
2. move the common broker/batch message model to `rocketmq-model` or introduce a neutral model/store-api bridge,
   without a temporary Local-to-common dependency.

PR-M06-03, its Exit Checklist, and M06-04 through M06-12 remain open.
