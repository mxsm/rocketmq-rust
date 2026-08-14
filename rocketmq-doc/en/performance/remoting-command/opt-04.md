# OPT-04: immutable RPC hook snapshots

## Production audit

The transport already uses a copy-on-write `HookRegistry` backed by `ArcSwap<HookSnapshot>`. Registration and clearing serialize only the update, copy the hook slice, and publish a new immutable generation. The request path loads one snapshot before the before-hook callback and retains that same `Arc` through processing and the after-hook callback. Client retry paths likewise pass an explicit snapshot to each paired callback.

This satisfies the required visibility contract:

- a request sees one hook generation for both callbacks;
- registering or clearing hooks affects later snapshots only;
- a retained snapshot remains valid while a new generation is published;
- the empty fast path avoids an `ArcSwap` load after the atomic hook-count check.

The behavior predates this evaluation, so no production rewrite is necessary. The focused registry test retains the first generation, publishes another, clears the registry, and verifies all three views independently.

## Formal baseline

The formal ten-process baseline compares the production immutable snapshot with the removed per-request `Vec<Arc<dyn RPCHook>>` clone shape.

| Hooks | Legacy vector clone median | Immutable snapshot median | Delta |
|---:|---:|---:|---:|
| 0 | 1.96 ns | 1.20 ns | -38.5% |
| 1 | 38.88 ns | 19.29 ns | -50.4% |
| 4 | 64.63 ns | 19.09 ns | -70.5% |

The immutable load stays approximately constant for non-empty sets while vector cloning grows with hook count. For four hooks the process-median ranges do not overlap: 62.72–71.80 ns for cloning versus 18.77–19.92 ns for snapshots.

## Decision and rollback

**ACCEPT the existing immutable snapshot design.** It removes per-hook reference-count churn from the request hot path and preserves callback visibility. No further representation change is justified by the current profile.

If a future change reintroduces mutable in-place hook lists or independently loads before and after snapshots, revert that change or restore the explicit request-scoped `Arc<HookSnapshot>`. Keep the benchmark comparison as the regression reference.
