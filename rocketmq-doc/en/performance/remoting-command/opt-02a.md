# OPT-02A: direct static request ID storage

## Change

The process-wide outbound request ID sequence no longer uses `LazyLock<Arc<AtomicI32>>`. It is a direct `static AtomicI32`, and all three allocation entry points delegate to one private `next_request_id` helper. This change intentionally retains `Ordering::AcqRel`; any ordering change is evaluated separately.

## Semantics and happens-before audit

The generated value is copied into `RemotingCommand::opaque` and used as a correlation key on the remoting wire. Searches of protocol production code show one atomic declaration, one atomic fetch in `next_request_id_from`, and three callers of `next_request_id`. No consumer performs an acquire load from the request-ID atomic, derives object visibility from its ordering, or treats the old `Arc` identity as state. Removing the lazy and shared-pointer wrappers therefore changes ownership and indirection only.

Tests use an isolated local atomic rather than mutating the process-global sequence. Eight threads generate 8,192 values with no duplicates and the exact 0 through 8,191 range. A boundary test proves the existing signed wrap from `i32::MAX` to `i32::MIN` remains unchanged. A source contract prevents reintroducing lazy/shared storage.

## Diagnostic construction curve

The candidate curve used a 1 second warmup, 1 second measurement, and 10 samples. The baseline column is the earlier formal 10-process baseline, so the directional delta is not a same-session acceptance result.

| Threads | Formal baseline M commands/s | Candidate diagnostic M commands/s | Directional delta |
|---:|---:|---:|---:|
| 1 | 31.49 | 38.16 | +21.2% |
| 2 | 37.05 | 56.87 | +53.5% |
| 4 | 34.17 | 57.85 | +69.3% |
| 8 | 29.76 | 42.97 | +44.4% |
| 16 | 28.98 | 38.29 | +32.1% |
| 32 | 29.05 | 37.40 | +28.7% |

The direction is consistent across every concurrency point, but session and profile differences prevent attributing the percentages to the storage change. The implementation is retained because it removes unnecessary lifetime machinery, preserves all semantics, and has no negative directional signal. This report makes no release-level performance claim.

## Risk and rollback

The risk is accidental divergence between request-ID entry points or wrap behavior. The shared helper and tests cover both. Revert the static declaration, helper, and tests together if any correlation behavior changes. Do not combine this result with a future memory-ordering result when reporting performance.
