# M11-12 Stable Rust surface evidence

> Snapshot: Issue #8645 / M11-12bc112 candidate
> Status: PASS; nightly surface is empty and the stable workspace matrix passes
> Scope: R22 nightly feature removal and stable-default/full-feature verification

## Outcome

The bc110 through bc112 stable-surface slices remove all eight obsolete nightly feature
declarations while preserving the named Remoting, Runtime, and ArcMut compatibility entry points:

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
- `ArcMut`, `WeakArcMut`, and `SyncUnsafeCellWrapper` use the workspace's existing stable
  `parking_lot::RwLock` backing. The legacy reference escapes remain documented compatibility
  debt; this implementation change does not claim that R18 public removal is complete.
- `ArcMut::get_inner()` exposes the stable backing cell so new callers can use read/write guards.
  There are no repository callers of the prior explicit nightly return type.
- The obsolete `syncunsafecell_mut` comparison benchmark is removed instead of introducing a new
  governed ArcMut test/bench caller.

The repository nightly feature-attribute inventory decreases from eight to zero. Both stable
surface modes now pass:

| Mode | Result |
|---|---|
| baseline | PASS, exactly 0 registered/observed feature attributes |
| target | PASS, no nightly feature remains |

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
  The ArcMut source contract rejects `SyncUnsafeCell`, requires the stable `Arc<RwLock<T>>`
  backing, and requires the obsolete benchmark to stay absent.
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
| ArcMut compatibility | `cargo check -p rocketmq-rust --all-targets` and 7/7 ArcMut facade tests pass; Broker all-target/all-feature check proves the retired benchmark manifest is clean |
| ArcMut governance | reviewed baseline passes at 20 identities / 58 occurrences: production 6/12, test 1/7, compatibility 13/39; 27 fixtures and all 79 ArcMut guard tests pass |
| Stable surface guard | baseline and target both pass with 0 features; 9/9 stable guard/source-contract tests pass |
| Stable default workspace | `RUSTFLAGS=-Cdebuginfo=0 cargo +stable check --workspace` passes, including Proxy Local |
| Stable full workspace matrix | `RUSTFLAGS=-Cdebuginfo=0 CARGO_INCREMENTAL=0 cargo +stable check --workspace --all-targets --all-features` passes on the same candidate snapshot |
| Runtime ownership | enforcing runtime audit passes after the scheduler adapter and caller migration |
| Proxy recursion regression | `cargo check -p rocketmq-proxy-local --all-targets --all-features` passes with the existing `recursion_limit = "256"` budget |
| Final repository gates | format check, workspace all-target/all-feature strict Clippy, and diff check pass |

## Remaining work

1. Complete R18 after the required next-major and HUMAN/Release Manager approval, removing the
   remaining public ArcMut compatibility facade. The comparison benchmark is already retired.
2. R23 Miri/Loom audit is complete in
   [`11-arc-mut-soundness-evidence.md`](11-arc-mut-soundness-evidence.md). R24 has since delivered
   the soak/SLO, dashboard/alerts/runbook, rollback, and evidence contract documented in
   [`11-slo-release-evidence.md`](11-slo-release-evidence.md); real dynamic execution remains part
   of the frozen candidate gate. R22 itself remains complete.

This evidence marks R22 complete. The separate R23 evidence does not mark R18, M11, or Phase 3
complete and does not substitute bounded Miri/Loom results for dynamic fault or HUMAN approval
evidence.

## Rollback

Reverting bc112 restores the unstable ArcMut backing, the two final feature attributes, and the
obsolete benchmark; its Cargo dependency/lockfile, stable policy, guard contract, and ArcMut
baseline changes must be reverted together. Reverting bc111 restores the duplicate lending
scheduler and both Runtime feature attributes; its Runtime, Client, and Broker caller changes must
be reverted together. Reverting bc110 restores the four Remoting/Controller feature attributes and
opaque associated future aliases. None of the slices changes persisted data. The stable-surface
policy must be reverted with the matching source slice to avoid a stale baseline.
