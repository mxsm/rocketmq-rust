# M06-03q report: TransientStorePool ownership

## Outcome

Moved `TransientStorePool` from Store to the canonical
`rocketmq-store-local::base::transient_store_pool` module. The old
`rocketmq-store::base::transient_store_pool::TransientStorePool` path is a direct exact re-export, preserving
public type identity without a wrapper or conversion.

No Cargo manifest, dependency, or feature changed. This slice reuses the Local memory-lock manager and platform
syscall boundary delivered by M06-03p.

## Compatibility and preserved semantics

The public constructor, lifecycle, queue, statistics, and real-commit APIs retain their signatures. Compile-time
Store-to-Local and Local-to-Store assignments prove exact type identity. Clone continues to share the available
queue, real-commit flag, and memory-lock manager through the same three `Arc` values; real commit starts as true.

This mechanical move deliberately preserves the existing lifecycle behavior, including behavior that might be
changed only by a separately reviewed fix:

- repeated initialization appends another `pool_size` buffers and accumulates lock statistics;
- borrowing pops the front; returning accepts arbitrary buffer lengths, may exceed configured capacity, and
  pushes to the front;
- the low-buffer warning threshold remains exactly `pool_size / 10 * 4`;
- destroy drains only the currently available queue, ignores borrowed buffers, and directly invokes
  `munlock(buffer.as_ptr(), file_size)` for each available buffer, including buffers whose lock failed or was
  skipped because of budget;
- the first unlock error stops later syscalls while dropping `VecDeque::Drain` still removes the remaining queue;
- destroy does not use memory-lock handles and does not decrement manager statistics.

`init_with_locker` remains crate-private and the new deterministic `destroy_with_unlocker` seam has no `pub`
visibility of any kind.
Moving the final Store production caller also lets `MemoryLockManager::lock_buffer_with` narrow from its temporary
`#[doc(hidden)] pub` M06-03p visibility to `pub(crate)`. The Local canonical pool is its sole production caller
outside the manager's public syscall wrapper. `lock_region_with` and `unlock_region_with` remain temporarily
public only because Store production adapters still need them. No cross-crate public test seam or implicit `Drop`
cleanup was introduced.

## TDD and contract evidence

RED was recorded before the production move. The focused Python owner/facade contract failed because the Local
canonical file was absent, and the public Rust fixture failed with E0432 because the Local module did not exist.

GREEN passes:

- Local public Clone/shared-state and deterministic concurrent-return contract: 2/2;
- Local internal lifecycle and memory-lock edge cases: 8/8;
- Store-to-Local exact type identity: 1/1;
- existing Store mapped-file transient-pool round trip: 1/1;
- full Local suite: 69 unit plus 102 integration tests; nine existing doctests remain ignored;
- full M06 source and mutation contract: 95/95.

An independent review found that the first destroy-seam contract rejected only plain `pub fn` and did not freeze
its caller surfaces. Contract-first RED proved that `pub(crate)`, `pub(super)`, `pub(in crate::base)`, an extra
canonical production caller, an extra module test caller, and external Local production/integration-test callers
were incorrectly accepted. GREEN parses the declaration visibility and initially constrained the corresponding
direct-call surfaces.

A second independent review found that the direct-call pattern required a following `(` and therefore accepted
function-item aliases such as `let seam = Self::destroy_with_unlocker`. Contract-first RED covered canonical
production and module-test aliases plus external Local production/integration aliases using both
`.destroy_with_unlocker` and `::destroy_with_unlocker`. GREEN now counts every active-Rust prefixed reference
without counting the unqualified function declaration: production has exactly one reference in public `destroy`,
each of the four named module tests has exactly one reference, and every other Local/Store production or test file
has zero.

The source contract freezes the single Local owner, field schema, public API, exact queue/lifecycle bodies,
manager projections, private injection seams, the crate-private `lock_buffer_with` boundary, and exact Store
facade. Memory-lock call counts prove Store no longer invokes `lock_buffer_with` and the Local pool is its only
production caller outside the manager. Separate per-file production/test reference counts prove
`destroy_with_unlocker` cannot be referenced or called outside its exact public-destroy/module-test surfaces.
Negative mutations reject copied owners,
wrapper/type-alias/brace/glob facades, Clone/shared-field drift, real-commit default changes, iteration instead of
drain, removed unlock calls, return-order changes, threshold reassociation, `pub`, `pub(crate)`, `pub(super)`,
`pub(self)`, and `pub(in ...)` destroy-seam widening, extra direct callers, function-item/alias references, other
widened injection seams, and `Drop`.

## Feature, platform, and architecture evidence

Local and Store each pass seven feature checks covering default/no-default or local-file, fast, safe,
fast+safe, observability, and all features. Both packages pass default and all-feature all-target Clippy with
`-D warnings`. Local all-feature Rustdoc passes with `RUSTDOCFLAGS=-D warnings`; Store normal Rustdoc succeeds
while retaining four unrelated existing invalid-HTML warnings.

WSL/Linux uses isolated `target/wsl-m06-03q` and passes the Local public fixture 2/2, Local internal lifecycle
8/8, Store identity 1/1, and Local/Store observability checks. The isolated target is then cleaned (9,872 files,
4.8 GiB) and verified absent.

Architecture dependency tests pass 35/35; clean/violation fixtures and baseline mode pass. ArcMut tests pass
63/63, all 24 fixtures pass, and the final guard reports no regression. AGENTS routing reports
`standalone_cargo=4`, `node_projects=3`, and `routes=8`.

## Validation and known baseline

The exact root formatting and workspace Clippy commands, `git diff --check`, focused/full tests, feature checks,
package Clippy, Rustdoc, M06 contract, architecture guard, ArcMut guard, routing, and WSL checks pass.

The error-hygiene gate reproduces only pre-existing findings outside this slice: source stringification in
`rocketmq-broker/src/auth/auth_admin_service.rs:326`; anyhow results in MCP `app.rs:45/112`, `main.rs:24/46`,
`transport/stdio.rs:20`, and `transport/streamable_http.rs:42/64/136`; and missing
`docs/07-error-hygiene-allowlist.md` plus `docs/error-codes.md`.

## Scope and handoff

M06-03q completes only `TransientStorePool` ownership. It does not repair lifecycle/accounting behavior or move
`MappedFile`, `DefaultMappedFile`, CommitLog orchestration, flush/group commit, CQ/Index, HA, Timer/POP, runtime
ownership, or persisted formats.

PR-M06-03, its Exit Checklist, and M06-04 through M06-12 remain open.
