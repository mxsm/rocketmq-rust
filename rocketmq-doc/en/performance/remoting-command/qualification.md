# Remoting command refactor qualification

## Outcome

The remoting command plan is complete. Correctness work established explicit defaults and response intent, endpoint-owned frame limits, bounded `GO_AWAY` retry, lazy validated extension fields, and a reproducible Rust/Java baseline. Performance work retained only candidates with a direct invariant or acceptable evidence and stopped speculative or footprint-negative candidates.

## Candidate decisions

| Candidate | Decision | Evidence |
|---|---|---|
| Binary wire-only construction | Accept | Removes outbound request-ID side effect; golden and malformed-input behavior unchanged |
| Direct static request-ID storage | Accept | Removes lazy/shared indirection; all diagnostic concurrency points positive; uniqueness and signed wrap preserved |
| Relaxed request-ID ordering | Stop | 0/6 concurrency points improved; production remains `AcqRel` |
| Explicit body length | Accept | Removes one `Bytes` clone/drop; 12/12 short A/B points non-negative, approximately 0.5%–22.4% faster |
| Direct `Arc<dyn Header>` ownership | Stop | Saves one typed-header allocation but increases command size and 100,000-object RSS by about 5% |
| Immutable RPC hook snapshot | Accept existing design | Four-hook snapshot 19.09 ns versus 64.63 ns vector clone, about 70.5% lower |
| Lazy cached fields and redacted display | Accept | Display performs zero raw-map materializations; unique read-then-mutate reuses the cached map |
| Global encode capacity buckets | Stop | No production-weighted length distribution; JSON and ROCKETMQ allocation shapes differ materially |
| Zero/one-field canonicalization | Accept | One-field path removes the temporary sort allocation; canonical multi-field path unchanged |
| Additional direct codecs | Stop | No production request-code ranking or CPU attribution |
| Raw field passthrough | Stop | Canonical/preserve contract, mutation/signing proof, and forwarding profile absent |
| Validated frame descriptor | Stop | No instruction-level duplicate-scan hotspot; trusted-boundary risk outweighs unproven benefit |

Diagnostic percentages are not promoted to release-level throughput claims unless their report uses a same-session A/B. The formal baseline remains the absolute reference and all stop decisions state their reopen condition.

## Focused final qualification

The final time-boxed gate ran against the merged protocol and transport scope:

- `cargo fmt -p rocketmq-protocol -p rocketmq-transport -- --check`
- `cargo test --locked -p rocketmq-protocol --lib`: 1,490 passed
- `cargo test --locked -p rocketmq-protocol --test remoting_wire_golden`: 2 passed
- `cargo test --locked -p rocketmq-transport --lib`: 197 passed
- `cargo clippy --locked -p rocketmq-protocol -p rocketmq-transport --no-deps --all-targets --all-features -- -D warnings`

All commands exited zero. The final drive check reported 46.9 GiB free, so no Cargo cleanup was necessary.

At the request of the operator, the final gate intentionally omitted a full-workspace replay, long soak, and fuzz run. Earlier focused boundary, TLS, segmented output, Java oracle, request-ID, hook, codec, and wire tests remain recorded in the individual reports and pull requests. This scope reduction trades broad integration confidence for completion time; it does not weaken the protocol tests that directly cover the changed code.

## Release and rollback

The merged changes are suitable for normal CI and canary rollout. Observe remoting decode/encode p95 and p99, request timeout and unmatched-response counts, `GO_AWAY` retry outcomes, frame-limit rejections, and process RSS. No stopped experiment is enabled or hidden behind a runtime flag.

Each accepted optimization is independently reversible by its squash commit. Revert the smallest owning commit if its invariant or metric regresses; keep the correctness foundations and formal baseline unless their own compatibility tests fail. The machine must remain powered on after completion.
