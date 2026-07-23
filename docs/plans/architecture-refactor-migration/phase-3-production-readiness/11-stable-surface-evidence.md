# M11-12 Stable Rust surface evidence

> Snapshot: Issue #8641 / M11-12bc110 candidate
> Status: partial PASS; baseline guard passes, stable target remains fail closed
> Scope: R22 nightly feature removal and stable-default preparation

## Outcome

This slice removes four obsolete nightly feature declarations without changing the public
`RequestProcessorV2::Fut` GAT contract:

- `rocketmq-remoting` no longer enables `impl_trait_in_assoc_type` in its crate root, integration
  test, or complete example.
- The three built-in core processor examples use concrete `Ready` futures, preserving the
  allocation-free static-dispatch path.
- Integration and documentation-only processors use explicit boxed futures; this does not add
  allocation to the production core path.
- `rocketmq-controller` no longer enables `arbitrary_self_types`; its existing `Arc<Self>`
  lifecycle receivers compile unchanged.

The repository nightly feature-attribute inventory decreases from eight to four. R22 remains open
because the following registered debt is still present:

| Path | Feature | Exit owner |
|---|---|---|
| `rocketmq/src/lib.rs` | `sync_unsafe_cell` | R18 next-major public compatibility removal |
| `rocketmq-runtime/src/lib.rs` | `async_fn_traits` | R22 stable Runtime scheduler boundary |
| `rocketmq-runtime/src/lib.rs` | `unboxed_closures` | R22 stable Runtime scheduler boundary |
| `rocketmq-broker/benches/syncunsafecell_mut.rs` | `sync_unsafe_cell` | R18 compatibility benchmark removal |

## Guard contract

`scripts/stable_surface_guard.py` scans every repository Rust source outside generated/vendor
directories and compares feature attributes with `scripts/stable-surface-policy.json`.

- Baseline mode fails for unregistered features, stale policy entries, duplicate entries,
  malformed paths/features, or an unreadable source/policy.
- Target mode additionally fails until the actual nightly feature set is empty.
- `scripts/tests/test_stable_surface_guard.py` covers baseline PASS, clean target PASS,
  unregistered/stale/remaining-target failures, retired source gates, and the three concrete
  zero-allocation core futures.
- The root architecture CI executes the baseline guard on Windows and Linux.

## Verification

| Evidence | Result |
|---|---|
| Remoting/Controller compile | all-target/all-feature checks passed after removing the four attributes |
| Processor V2 behavior | integration 7 passed / 1 manual allocation check ignored; library dispatcher 1/1 passed; complete example check passed |
| Public API path comparison | same-toolchain main/candidate rustdoc JSON comparison reports 0 named public-path differences for both Remoting and Controller |
| Stable surface guard | baseline reports exactly 4 registered features; 7/7 guard/source-contract tests passed |
| Stable target | expected failure lists exactly the four registered entries above |
| Stable Cargo attempt | with the host nightly-only `-Zthreads=4` override neutralized, compilation advances to `rocketmq-runtime` and fails at its registered `async_fn_traits` feature; Remoting/Controller add no earlier stable error |
| Final repository gates | format check, AGENTS routing drift guard, workspace all-target/all-feature strict Clippy, and diff check passed |

## Remaining work

1. Replace the lending `ScheduledTaskManager` nightly signature without weakening cancellation,
   serialization, or public compatibility.
2. Complete R18 after the required next-major and HUMAN/Release Manager approval, removing ArcMut
   and its comparison benchmark.
3. Run the complete stable feature matrix on one frozen commit, then proceed to Miri/Loom and
   soak/SLO evidence.

This evidence does not mark R22, M11, or Phase 3 complete and does not substitute a fixture for a
stable target PASS.

## Rollback

Reverting this slice restores the four feature attributes and opaque associated future aliases.
The public GAT trait shape is unchanged, so no consumer migration or persisted-data rollback is
required. The stable-surface policy and CI step must be reverted together to avoid a stale
baseline.
