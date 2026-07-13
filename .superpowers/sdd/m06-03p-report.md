# M06-03p report: memory-lock manager and platform syscalls

## Outcome

Moved `MemoryLockManager`, `MemoryLockCategory`, `MemoryLockHandle`, and the `mlock`/`munlock` platform
boundary from Store to `rocketmq-store-local`. Store preserves the old public paths as direct exact re-exports,
so callers retain type and function identity without wrappers or conversions.

This slice deliberately does not move or repair `TransientStorePool`. Store also keeps page-size, madvise,
prefetch, mincore, and its remaining platform constants and helpers.

## Canonical ownership

The sole canonical owners are:

- `rocketmq-store-local::base::memory_lock_manager` for `MemoryLockManager`, `MemoryLockCategory`, and
  `MemoryLockHandle`;
- `rocketmq-store-local::utils::ffi` for `mlock` and `munlock`.

Local adds `rocketmq-error` as a normal dependency. Its optional `rocketmq-observability` dependency is enabled
only by `observability = ["dep:rocketmq-observability", "rocketmq-observability/otel-metrics"]`. Store forwards
its existing observability feature to `rocketmq-store-local/observability`.

## Compatibility contract

The old Store modules contain direct exact re-exports for only the migrated symbols. Cross-crate tests prove
bidirectional identity for all three types and function-pointer identity for both syscalls.

At the M06-03p delivery snapshot, the three injection seams, `lock_buffer_with`, `lock_region_with`, and
`unlock_region_with`, remained `#[doc(hidden)] pub` because Store production adapters still crossed the crate
boundary for `TransientStorePool` initialization, `DefaultMappedFile` range locking/unlocking, and the CommitLog
active-lock lifecycle. M06-03q later moves `TransientStorePool` to Local and narrows `lock_buffer_with` to
`pub(crate)` after its final Store production caller disappears. The other two seams remain temporarily public
for their Store production adapters. Their Rustdoc identifies every current production/test caller surface and
documents errors.

The migration preserves:

- category label vocabulary and observability metric names;
- strict versus warn-only failure handling and exact legacy error text;
- byte-budget reservation, rollback, unlock, and concurrent counter semantics;
- atomic ordering and syscall argument behavior;
- platform-specific error mapping and all `mlock`/`munlock` error strings.

## TDD and mutation evidence

RED was established before the production move: the focused contract failed because both Local canonical owner
files and the Local observability feature/dependencies were absent.

GREEN evidence includes:

- Local behavior and concurrent-budget goldens: 5/5;
- Store-to-Local type and function identity: 2/2;
- existing Store transient-pool, range-lock, and active-lock focused tests: 4/4, 2/2, and 2/2;
- full Local suite: 61 unit plus 100 integration tests; nine doctests remain intentionally ignored;
- final M06 source and mutation contract: 92/92.

Reviewer follow-up TDD exposed a weakness in the first contract rather than in runtime behavior. RED injected a
second production `lock_buffer_with` call into an already allowlisted Store file; the old whole-file allowlist
incorrectly returned no violation. GREEN replaced it with separate production/test call-site counts and exact
delegation checks. Focused owner/documentation/delegation/mutation checks pass 3/3, and the full contract passes
92/92. The review candidate also passes strict Local Rustdoc, Local all-target/all-feature Clippy, and Store
focused transient-pool 4/4, range-lock 2/2, and memory-lock 14/14 unit tests; the memory-lock filter additionally
passes the Store-to-Local identity fixture 2/2.

The contract freezes canonical ownership, field schemas, enum order, labels, error text, atomic order, exact Store
facades, feature wiring, dependency closure, and the temporary seam callers. Production and test calls are
counted separately per file; 11 production delegation functions across the canonical manager and Store adapters
have constrained receiver, argument, and delegation forms. Negative fixtures reject copied owners, wrapper
facades, field or label drift, reordered accounting, undocumented seams, extra production/test calls, wrong
receivers, direct callback/syscall bypasses of the canonical manager, and feature/dependency mutations.

## Feature and dependency evidence

Passed Local checks for default, no-default, fast, safe, fast+safe, no-default+observability, and all features.
Passed Store checks for default, all features, local-file fast, local-file safe, local-file fast+safe,
local-file+observability, and default+observability.

Cargo inverse trees prove that Local does not select `rocketmq-observability` with observability disabled. With
the feature enabled, Local selects exactly the observability dependency's `otel-metrics` feature; Store's
observability feature reaches the Local feature.

The seven observability CI feature states passed workspace checks and `rocketmq-observability` tests. The
default plus six explicitly selected observability-feature Clippy probes each reproduced only two pre-existing
Broker test unused imports of `DataVersionExt`; neither file is changed by this slice. This matrix is distinct
from the repository's required all-feature gate. Local and Store package Clippy pass with `-D warnings` under
default and all features.

## Platform evidence

Windows passes the focused behavior, identity, feature, package-Clippy, and documentation checks. WSL/Linux with
an isolated target passes the Local lock behavior fixture, Store identity fixture, and Local/Store observability
checks. The Linux build exposed a Store import used only by its Windows prefetch implementation; the final source
correctly narrows that import with `#[cfg(windows)]`.

The isolated WSL target is cleaned and verified absent after the final Linux check.

## Architecture and ArcMut evidence

Architecture dependency guard tests pass 35/35; clean/violation fixtures and baseline mode pass. AGENTS routing
reports `standalone_cargo=4`, `node_projects=3`, and `routes=8`.

ArcMut unit tests pass 63/63, all 24 fixtures pass, and the final guard reports no boundary regression. This
slice neither moves nor modifies `DefaultMappedFile` or the `ArcMut` ownership model.

## Validation and known baselines

Passed:

- `cargo fmt --all -- --check`;
- Local full tests and focused Store compatibility tests;
- Local/Store feature matrices and package Clippy with `-D warnings`;
- `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings`;
- strict Local Rustdoc and normal Store Rustdoc;
- M06 contract, architecture guard, AGENTS routing, and ArcMut guard;
- observability checks/tests, dependency-tree audit, WSL/Linux checks, and `git diff --check`.

The observability CI default plus six explicitly selected feature Clippy probes are not described as passed:
they are blocked by two existing unused `DataVersionExt` imports in Broker tests. In contrast, the exact required
root all-feature Clippy command passes. The error-hygiene gate reproduces only existing Broker auth/MCP findings
and two already-missing error-hygiene documents. The current diff does not touch any reported baseline path.
Store Rustdoc retains four unrelated existing invalid-HTML warnings.

## Scope and handoff

M06-03p completes only memory-lock manager and syscall ownership. `TransientStorePool`, `MappedFile`,
`DefaultMappedFile`, CommitLog orchestration, flush/group commit, CQ/Index, HA, Timer/POP, runtime ownership, and
persisted formats remain outside this slice.

PR-M06-03, its Exit Checklist, and M06-04 through M06-12 remain open.
