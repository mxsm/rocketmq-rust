# AGENTS.md

## Scope

- This file applies to `rocketmq-dashboard/rocketmq-dashboard-gpui/`.

## Project role and build boundary

- This directory is the native RocketMQ Dashboard desktop application built with GPUI and `gpui-component`.
- It is a standalone Cargo workspace and is not a member of the repository-root Cargo workspace.
- Run Cargo commands from this directory. Do not rely on root-workspace `-p rocketmq-dashboard-gpui` commands or
  root-workspace validation to cover it.
- Use the repository-pinned Rust 1.95.0 toolchain, Rust 2024 edition, resolver 3, root `rustfmt.toml`, and root
  `.clippy.toml`. Do not lower the MSRV or change these settings merely to bypass a local failure.
- Keep `Cargo.lock` committed and synchronized when dependencies change. Avoid unrelated dependency upgrades.
- `src/main.rs` owns process startup, observability bootstrap, GPUI initialization, the application window, and
  orderly shutdown. `src/ui.rs` declares UI modules, and `src/ui/*.rs` owns views and presentation state.
- Keep the existing flat module layout and do not introduce `mod.rs` files.
- Several view modules are already large; prefer extracting cohesive components or state when extending them.

## Repository boundaries and shared code

- Keep GPUI-specific rendering, window behavior, event handling, and view state in this project.
- Put reusable Dashboard models, client contracts, and UI-independent service logic in
  `../rocketmq-dashboard-common/` when practical; do not duplicate shared domain behavior in views.
- The direct repository path dependencies are `rocketmq-dashboard-common` and `rocketmq-observability`. A change
  to either must follow the root instructions for that crate and must revalidate this standalone consumer.
- Changes to shared RocketMQ crates consumed through those dependencies may also require GPUI revalidation. Use
  the current manifests and the root shared-code rules to determine the actual consumer scope.
- Do not modify the Tauri or Web Dashboard implementations merely to mirror a GPUI change unless the user asks
  for a cross-implementation change or a shared compatibility surface requires it.
- Do not add this project to the root workspace without an explicit repository architecture decision.

## GPUI architecture and UI rules

- Call `gpui_component::init` before creating or using `gpui-component` widgets, and keep the first window-level
  view wrapped in `gpui_component::Root`.
- Perform UI and entity mutations through GPUI `Context`/`Window` APIs. Notify the context after state changes
  that must trigger rendering, and use stable element IDs for interactive controls.
- Keep `Render` implementations deterministic and responsive. Do not perform RocketMQ calls, file I/O, sleeps,
  blocking synchronization, or expensive data transformation in a render path.
- Use `cx.listener` or the established GPUI callback pattern for event handlers. Capture only the owned data the
  handler needs, and avoid reference cycles between entities, subscriptions, and callbacks.
- Keep long-running or fallible work outside the UI thread. Return results to the owning GPUI entity and model
  explicit loading, success, empty, and error states instead of blocking or panicking.
- Preserve ownership of subscriptions and tasks for as long as their callbacks are required. New detached work
  must be an intentional application-lifetime operation with documented shutdown behavior; do not introduce
  raw `tokio::spawn`, `std::thread`, ad hoc runtimes, or nested `block_on` calls.
- Do not hold synchronous mutex or RwLock guards across `.await`. Prefer message passing or short, explicit lock
  scopes when background work communicates with UI state.
- Keep navigation state and page identity centralized. Reuse shared visual helpers and components instead of
  copying style chains across pages when the reuse remains readable.
- Preserve keyboard accessibility, readable contrast, focus behavior, scrolling, resizing, and loading/error
  feedback when changing interactive UI. Do not assume the default 1440x900 window is the only usable size.
- Keep OS-specific code behind the narrowest `cfg` boundary and preserve Linux, macOS, and Windows behavior.
  Validate platform-specific window, input, or rendering changes on every affected OS when practical.

## Errors, observability, and sensitive data

- Use typed errors for recoverable domain, configuration, network, and UI-operation failures. `anyhow` is
  acceptable at the binary and build-script boundaries, but it must not replace meaningful component or service
  error types.
- Preserve observability initialization before the GPUI application starts, keep the telemetry guard alive for
  the application lifetime, and shut it down after the GPUI run loop exits.

## Testing policy

- Prefer testing pure state transitions, filtering, pagination, view-model conversion, and event decisions
  without requiring a GPU, display server, live RocketMQ cluster, fixed port, or external network.
- During iteration, run the smallest useful command first, such as:

```bash
cargo check
cargo test test_name
cargo test --bin rocketmq-dashboard-gpui test_name
```

- Run `cargo test` when behavior is broad, shared state changes, startup/shutdown changes, or the final validation
  profile requires it.
- For visual, focus, input, window, or platform behavior that automated tests cannot prove, perform a manual
  `cargo run` smoke test on a supported graphical environment and report the platform and scenarios checked.

## Validation

Run all commands from `rocketmq-dashboard/rocketmq-dashboard-gpui/`.

Before PR submission or final handoff for Rust code, manifest, build-script, or dependency changes, run the full
project profile used by `.github/workflows/dashboard-gpui-ci.yml`:

```bash
cargo fmt --all -- --check
cargo clippy --all-targets --all-features -- -D warnings
cargo check --all-targets --all-features
cargo test
```

On Linux, compilation requires the native UI/build dependencies used by CI:

```bash
sudo apt-get install clang cmake make ninja-build pkg-config protobuf-compiler \
  libfontconfig1-dev libfreetype6-dev libx11-dev libxcb1-dev \
  libxkbcommon-dev libxkbcommon-x11-dev
```

- GUI execution is not a substitute for the non-interactive validation profile, and the validation profile does
  not replace a manual smoke test when the change is inherently visual or OS-specific.

## Build script and generated artifacts

- Keep `build.rs` deterministic, fallible through `Result`, and free of unnecessary network access or machine-
  specific assumptions. Emit precise Cargo rerun directives if it begins consuming files or environment values.
- Do not commit `target/`, packaged binaries, local logs, screenshots used only for inspection, runtime audit
  artifacts, or other generated build output.
