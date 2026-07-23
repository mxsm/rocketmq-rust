# M11-12 Stable Rust surface evidence

> Snapshot: Issue #8643 / M11-12bc111 candidate
> Status: partial PASS; baseline guard passes, stable target remains fail closed
> Scope: R22 nightly feature removal and stable-default preparation

## Outcome

The bc110 and bc111 stable-surface slices remove six obsolete nightly feature declarations while
preserving the named Remoting and Runtime compatibility entry points:

- `rocketmq-remoting` no longer enables `impl_trait_in_assoc_type` in its crate root, integration
  test, or complete example.
- The three built-in core processor examples use concrete `Ready` futures, preserving the
  allocation-free static-dispatch path.
- Integration and documentation-only processors use explicit boxed futures; this does not add
  allocation to the production core path.
- `rocketmq-controller` no longer enables `arbitrary_self_types`; its existing `Arc<Self>`
  lifecycle receivers compile unchanged.
- `rocketmq-runtime` no longer enables `async_fn_traits` or `unboxed_closures`. Its four public
  `_async` scheduler methods remain available and now accept an owned `Send + 'static` future.
- The compatibility adapter holds its inner Tokio mutex until each returned future completes,
  preserving the prior exclusive invocation behavior while reusing the stable scheduler.
- Runtime, Client, and Broker callers clone their owned capabilities before returning
  `async move` futures; no complete Store or runtime root capture is introduced.

The repository nightly feature-attribute inventory decreases from eight to two. R22 remains open
because the following registered debt is still present:

| Path | Feature | Exit owner |
|---|---|---|
| `rocketmq/src/lib.rs` | `sync_unsafe_cell` | R18 next-major public compatibility removal |
| `rocketmq-broker/benches/syncunsafecell_mut.rs` | `sync_unsafe_cell` | R18 compatibility benchmark removal |

## Guard contract

`scripts/stable_surface_guard.py` scans every repository Rust source outside generated/vendor
directories and compares feature attributes with `scripts/stable-surface-policy.json`.

- Baseline mode fails for unregistered features, stale policy entries, duplicate entries,
  malformed paths/features, or an unreadable source/policy.
- Target mode additionally fails until the actual nightly feature set is empty.
- `scripts/tests/test_stable_surface_guard.py` covers baseline PASS, clean target PASS,
  unregistered/stale/remaining-target failures, retired source gates, and the three concrete
  zero-allocation core futures. Its Runtime source contract also rejects `AsyncFnMut` and both
  retired feature gates while requiring the stable owned-future bounds and serialized adapter.
- The root architecture CI executes the baseline guard on Windows and Linux.

## Verification

| Evidence | Result |
|---|---|
| Remoting/Controller compile | all-target/all-feature checks passed after removing the four attributes |
| Processor V2 behavior | integration 7 passed / 1 manual allocation check ignored; library dispatcher 1/1 passed; complete example check passed |
| Public API path comparison | same-toolchain main/candidate rustdoc JSON comparison reports 0 named public-path differences for both Remoting and Controller |
| Runtime scheduler compile | Runtime, Client, and Broker all-target checks pass; Broker and Client cover every migrated repository call site |
| Runtime scheduler behavior | owned-future serialization 1/1 and fixed-rate/fixed-delay/no-overlap 3/3 pass |
| Stable Runtime | `RUSTFLAGS=-Cdebuginfo=0 cargo +stable check -p rocketmq-runtime --all-targets` passes after neutralizing the host nightly-only `-Zthreads=4` setting |
| Stable surface guard | baseline reports exactly 2 registered features; 8/8 guard/source-contract tests pass |
| Stable target | expected failure lists exactly the two R18 `sync_unsafe_cell` entries above |
| Runtime ownership | enforcing runtime audit passes after the scheduler adapter and caller migration |
| Proxy recursion regression | `cargo check -p rocketmq-proxy-local --all-targets --all-features` passes with the existing `recursion_limit = "256"` budget |
| Final repository gates | format check, workspace all-target/all-feature strict Clippy, and diff check pass |

## Remaining work

1. Complete R18 after the required next-major and HUMAN/Release Manager approval, removing ArcMut
   and its comparison benchmark.
2. Run the complete stable feature matrix on one frozen commit, then proceed to Miri/Loom and
   soak/SLO evidence.

This evidence does not mark R22, M11, or Phase 3 complete and does not substitute a fixture for a
stable target PASS.

## Rollback

Reverting bc111 restores the duplicate lending scheduler and both Runtime feature attributes; its
Runtime, Client, and Broker caller changes must be reverted together. Reverting bc110 restores the
four Remoting/Controller feature attributes and opaque associated future aliases. Neither slice
changes persisted data. The stable-surface policy must be reverted with the matching source slice
to avoid a stale baseline.
