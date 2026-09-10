---
title: "Coding standards"
---

# Coding standards

Follow the existing implementation at the owning layer and the nearest project guide. These conventions preserve public contracts, lifecycle ownership and diagnosable failures. They summarize current repository rules rather than defining a separate contribution process.

## Rust structure and public contracts

Use the selected toolchain, package edition, `rustfmt.toml` and `.clippy.toml`. The root defaults to Rust 2021; Admin CLI and several standalone applications use Rust 2024. Preserve those manifest choices. Use snake_case modules/functions, PascalCase types and SCREAMING_SNAKE_CASE constants, and retain the Apache 2.0 header on new Rust files.

Keep implementation modules private and exports deliberate. Prefer enums, configuration/request structs, builders and newtypes to positional flags or long argument lists. Exhaustively match project-owned compatibility-sensitive enums. Keep optional features additive and test the relevant default/disabled behavior when a change affects it.

Request codes, response codes, headers, Serde fields/defaults and persisted layouts are compatibility surfaces. A rename that preserves an internal Rust call can still break a wire or storage contract. Explain an intended semantic change and its migration path; consult [protocol compatibility](../reference/protocol-compatibility.md).

Split modules by cohesive behavior when useful. File length is a review signal, not a reason to split unrelated logic into arbitrary fragments. Web backend and GPUI local guides use a flat module layout without `mod.rs`; do not impose that local rule on unrelated modules without checking their guide.

## Errors and documentation

Use typed errors for recoverable configuration, input, I/O, transport, storage and lifecycle failures. At public boundaries, map to the established component error model and stable descriptor. Do not replace meaningful errors with a generic string or match a human-readable message to make a retry decision.

Avoid production `todo!`, `unimplemented!` and recoverable-path `unwrap`/`expect`/panic. Test assertions are appropriate, and an intentional infallible facade needs a documented invariant or a fallible companion. Narrow any lint allowance and explain why it exists.

Rustdoc should describe non-obvious invariants and applicable `# Errors`, `# Panics` and `# Safety` contracts. Comments explain why a choice is required; do not narrate every line. Keep unsafe blocks minimal with an immediately preceding `// SAFETY:` explanation and a clear safe-wrapper or caller contract.

## Async execution and shutdown

| Concern | Required design property |
| --- | --- |
| Background tasks | Own work through `ServiceContext`, `TaskGroup` or the established lifecycle owner; cancel and await it during shutdown. |
| Blocking operations | Use `BlockingExecutor` or an established top-level boundary; do not introduce raw `spawn_blocking`, nested `block_on` or ad hoc runtimes. |
| Synchronization | Do not hold a synchronous lock guard across `.await`; keep lock scopes small. |
| Admission | Preserve bounded task, byte and blocking budgets. Timeout does not necessarily stop underlying blocking work. |
| Async traits | Prefer native async trait methods; do not introduce `#[async_trait]`. |
| Completion | Distinguish accepted, written, durable, replicated and business-complete outcomes. |

The [runtime design](../architecture/runtime.md) describes ownership and budget boundaries. A shutdown implementation that drops a handle without awaiting owned work is not equivalent to a completed shutdown. Runtime changes need focused cancellation/resource tests where those behaviors change, without a fingerprint or baseline ceremony.

## Observability and sensitive data

Prefer `#[tracing::instrument(skip_all, ...)]` with explicit low-cardinality fields. Never log credentials, ACL/TLS material, tokens, message bodies or entire request/configuration objects. Avoid unsampled per-message spans and unbounded topic/group labels.

Use the established error redaction and telemetry ownership paths. A safe `Debug` implementation on one wrapper does not make arbitrary strings safe to log. [Errors and observability](../architecture/errors-observability.md) explains signal ownership and boundary mappings.

## Frontend and desktop code

| Project | Local convention |
| --- | --- |
| Website | Docusaurus/MDX, existing components and page IDs; paired English/Chinese content and translated navigation. |
| Web frontend | React/TypeScript/Vite, shared tokens/components and unified `src/api/` client; operational tables with loading, empty, error, search, pagination and refresh states. |
| Web backend | Thin Axum handlers, service orchestration, separate API DTOs and internal models, explicit local error mapping; reusable logic in Dashboard common. |
| GPUI | Deterministic render paths, state changes through Context/Window, stable element IDs, owned subscriptions and nonblocking UI work. |
| Tauri | Frontend commands from the app root; Rust commands from `src-tauri`. Frontend asset compilation is separate from desktop packaging. |

Preserve accessibility, focus, keyboard operation, light/dark consistency and usable resizing. Product confirmation dialogs for destructive actions remain part of the application behavior; they are unrelated to the documentation-writing workflow. Do not expose internal migration or API-parity implementation notes as user-facing product controls.

## Select relevant checks

Use the smallest target that demonstrates the change. For example, a model change can use:

```bash
cargo fmt -p rocketmq-model -- --check
cargo test -p rocketmq-model --lib
```

These are examples for that package, not commands to run for every edit. Reuse a meaningful test and inspect how many tests ran. A compiling test can replace a redundant `cargo check`; add package-scoped Clippy or consumer validation when it addresses an actual concern. Do not run a mutating workspace formatter over unrelated dirty Rust files.

For rendered website changes, run `npm run build` in `rocketmq-website`. Standalone projects use their own local profiles. Full feature/platform matrices, interoperability, long-running fault tests and release qualification belong to tasks that need that evidence. Do not claim an unrun scenario passed.

Sources: [root rules](https://github.com/mxsm/rocketmq-rust/blob/main/AGENTS.md), [Web frontend](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-web/frontend/AGENTS.md), [Web backend](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-web/backend/AGENTS.md), [GPUI](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-gpui/AGENTS.md), [Tauri](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-dashboard/rocketmq-dashboard-tauri/AGENTS.md), [website](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/AGENTS.md).
