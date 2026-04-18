# RocketMQ Admin TUI

`rocketmq-admin-tui` is the terminal UI adapter for RocketMQ admin operations. It uses Ratatui for UI rendering and depends on `rocketmq-admin-core` for admin capabilities.

This crate must not depend on `rocketmq-admin-cli`.

## Role in the Admin Stack

```text
keyboard/UI event
  -> TUI state or action
  -> TuiAdminFacade
  -> rocketmq-admin-core request DTO
  -> rocketmq-admin-core service
  -> TUI view model
```

The TUI should reuse the same core service layer as the CLI instead of reimplementing admin RPC logic or calling CLI command modules.

## Responsibilities

- Own Ratatui layout, state, input handling, and rendering.
- Convert user interactions into facade calls or core request DTOs.
- Convert structured core results into TUI view models.
- Keep async admin operations separated from UI event handling.
- Provide compile-level guardrails that TUI depends on core, not CLI.

## Non-Responsibilities

- CLI parsing with `clap`.
- Shell completion generation.
- CLI table/color/prompt/progress rendering.
- Shared admin domain logic that belongs in `rocketmq-admin-core`.
- Duplicate implementation of RocketMQ admin RPC orchestration.

## Current Facade Coverage

`TuiAdminFacade` exposes compile-checked access to core service paths for:

| Domain | Coverage |
|---|---|
| topic | list, route, status, create/update, permissions, delete, order config, allocation. |
| nameserver | config query/update, KV config update/delete, write permission operations. |
| broker | config query/update plan/apply, runtime stats, consume stats. |
| cluster | cluster list, broker names, send-message RT diagnostics. |
| connection | consumer and producer connection inspection. |
| consumer | config, running info, progress, delete subscription group, consume mode. |
| offset | clone, consumer status, skip accumulated messages, reset by time. |
| queue | consume queue query and RocksDB CQ write progress. |
| HA | HA status and sync-state-set query. |
| stats | stats-all query. |
| producer | producer info query. |

Other domains can be added incrementally when a real TUI screen needs them. The boundary is already established: add facade/view-model support around core DTOs and services, not CLI command calls.

## Run

From the repository root:

```bash
cargo run -p rocketmq-admin-tui
```

## Development Rules

- Do not add a dependency on `rocketmq-admin-cli`.
- Do not parse CLI command structs in TUI code.
- Do not hold UI state locks across `.await`.
- Keep blocking or network admin work behind async service/facade calls.
- Prefer TUI-specific view models over rendering core DTOs directly in widgets.
- Add facade tests when exposing a new core service path.

## Validation

Run targeted TUI tests after changing this crate:

```bash
cargo test -p rocketmq-admin-tui
```

For root workspace Rust changes, also run:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Design Asset

![](../../../resources/rocketmq-cli-ui.png)

