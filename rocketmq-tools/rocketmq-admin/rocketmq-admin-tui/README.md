# rocketmq-admin-tui

[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-admin-tui` is the interactive terminal administration panel for
RocketMQ Rust. It uses Ratatui and crossterm for the terminal experience, while
all RocketMQ administration behavior is delegated to `rocketmq-admin-core`
through `TuiAdminFacade`.

The crate is designed for operators who want a searchable, keyboard-driven
management surface without reimplementing CLI parsing or RocketMQ RPC logic.
It currently exposes 100 facade-backed admin commands across 17 RocketMQ
management domains.

[中文文档](README-zh_cn.md)

## Architecture

![rocketmq-admin-tui architecture](../../../resources/admin-tui-architecture.svg)

The stable runtime flow is:

```text
terminal event -> app action -> state/form validation -> TuiAdminFacade -> admin-core DTO/service -> result view model -> Ratatui renderer
```

The TUI owns interaction, state, layout, and rendering. Core administration
requests, validation, RPC orchestration, and structured results stay in
`rocketmq-admin-core`.

## Preview

![rocketmq-admin-tui preview](../../../resources/rocketmq-admin-tui.png)

## Capabilities

- Searchable command tree grouped by RocketMQ admin domain, with a risk marker on
  every command and groups that fold.
- Five focus areas: NameServer, Search, Commands, Parameters, and Result. A key bar
  lists the keys of the focused area, and `F1` opens the full reference.
- Keyboard and mouse: every action has a key; a click focuses a pane, selects a
  command, picks a choice, or presses Run, and the wheel scrolls whatever is under
  the pointer. `F2` hands the mouse back to the terminal for text selection.
- Typed argument model for strings, optional strings, numbers, booleans, enums,
  key/value maps, and millisecond timestamps. Text fields edit at the cursor,
  choices switch with the arrows or `Space`, credentials are masked, and every
  command remembers its form.
- Form-level validation before a command can run; the first invalid field takes
  the focus.
- Risk-aware execution model:
  - safe commands run directly;
  - mutating commands require typing `confirm`;
  - dangerous commands require typing the target value when available.
- The main thread only draws the interface and reads input. Each command runs as a
  task of the process-owned runtime (`RuntimeOwner`), on a worker thread whose stack
  is sized for the admin call graph, and uses a separate client runtime beneath the
  application's client scope. Cancelling a command stops its operation; its task then
  closes the command's connections and background tasks before it reports. Exiting
  waits for this cleanup. Cancellation does not undo requests already accepted by a
  broker or NameServer.
- Progress updates for long-running workflows such as monitoring and message
  pull operations.
- Structured result rendering as tables, key/value rows, JSON, text, or operation
  summaries. Tables keep a row cursor, scroll by column, right-align numbers, and
  open any row in a detail view; documents wrap or pan, and JSON and debug output
  are syntax-colored. `z` zooms the result pane.
- Motion that explains state: focus, command, and result changes are animated, a
  running command carries a travelling highlight, and outcomes flash in their color.
  Decorative effects fade out when the application is idle, an idle screen is not
  redrawn at all, and `F3` switches motion off.
- Adapts to the terminal: 24-bit color with a 256-color fallback, one pane at a time
  below 96 columns, and a readable notice below 48x12.
- Boundary tests that enforce `rocketmq-admin-tui -> rocketmq-admin-core` and
  reject dependencies on the CLI adapter.

## Quick Start

Run from the repository root in an interactive terminal:

```bash
cargo run -p rocketmq-admin-tui
```

The TUI starts without requiring a NameServer address. Set one from the
NameServer focus area before executing cluster-backed commands.

Common keys (single letters act outside text fields, where they are typed instead):

| Key | Action |
|---|---|
| `Tab` / `Shift+Tab` | Move focus through NameServer, Search, Commands, Parameters, and Result. |
| `/` or `Ctrl+F` | Search commands. `Ctrl+F` also works while typing in a field. |
| `n` / `p` / `r` | Jump to the NameServer field, the parameter form, or the result. |
| Arrows or `j` / `k` | Move through commands, fields, or result rows. `PgUp`, `PgDn`, `Home`, and `End` move further. |
| `Left` / `Right` | Fold command groups, switch a choice, or scroll result columns. |
| `Space` | Switch a boolean or enum parameter. |
| `Enter` | Open a command, run it from the form, confirm, or show a result row in full. |
| `Ctrl+R` or `F5` | Run the selected command from anywhere. |
| `Esc` | Cancel a running command, otherwise step back one level. At the command list a second press quits. |
| `Ctrl+C` | Cancel a running command, or quit when none is running. |
| `Ctrl+L` | Clear the current result. |
| `z` / `w` | Zoom the result pane; wrap or unwrap long lines. |
| `F1` or `?` | Toggle help. |
| `F2` / `F3` | Switch mouse capture or animations on and off. |
| `q` or `Ctrl+Q` | Quit. |

## Command Coverage

The command catalog is generated in `src/commands/catalog.rs` and protected by
tests. Current coverage:

| Domain | Commands | Examples |
|---|---:|---|
| Auth | 12 | user and ACL get/list/create/update/delete/copy. |
| Broker | 15 | config, runtime stats, consume stats, epoch, cleanup, cold data flow control, commitlog read-ahead, timer engine. |
| Cluster | 3 | cluster list, broker names, send-message RT diagnostics. |
| Connection | 2 | consumer and producer connection inspection. |
| Consumer | 8 | config, running info, progress, monitoring, subscription group, consume mode. |
| Controller | 5 | config, metadata, elect master, clean metadata. |
| Export | 6 | configs, metrics, metadata, RocksDB metadata, RocksDB RPC export, POP records. |
| HA | 2 | HA status and sync-state-set query. |
| Lite | 6 | broker, parent topic, lite topic, group, client, dispatch. |
| Message | 12 | decode, query, trace, direct consume, dump compaction log, print, consume. |
| NameServer | 6 | config, KV config, write permission. |
| Offset | 5 | clone, consumer status, skip accumulated, reset by time. |
| Producer | 4 | producer info, send message, send status, send RT. |
| Queue | 2 | consume queue and RocksDB CQ write progress. |
| Static Topic | 2 | update and remap static topic. |
| Stats | 1 | stats-all query. |
| Topic | 9 | list, cluster, route, status, update, permission, delete, order config, allocate MQ. |

## Runtime Model

`RocketmqTuiApp` owns the event loop on the main thread. It ticks 30 times a
second, reads crossterm events, applies internal actions, and draws the current
`AppState`. A frame is drawn only when it is due: on every tick during a
transition, on every other tick while only ambient effects or a running command
are on screen, and not at all on an idle screen, which therefore writes nothing
to the terminal.

Command execution is separated from UI handling:

1. The selected `CommandSpec` defines arguments, result view kind, and risk
   level.
2. `CommandFormState` validates the typed form values.
3. `execute_command_with_progress` dispatches by command ID and returns a `Send`
   future.
4. The future runs as a `TaskGroup` task of the application's client scope, on a
   runtime worker thread. The main thread never polls it.
5. `TuiAdminFacade` converts form values into `rocketmq-admin-core` request DTOs.
6. Core services execute the admin operation.
7. `CommandResultViewModel` converts structured results into TUI-friendly
   tables, JSON, text, key/value rows, or summaries.
8. Late results from cancelled tasks are ignored by execution ID.

Runtime threads are created with a 16 MiB stack. The admin, client, and transport
call graph that a command awaits needs about 1.3 MiB in an unoptimized Windows
build, which is more than the 1 MiB main-thread stack of that platform. Keeping
commands off the main thread is what makes their stack budget a runtime setting
instead of a platform default.

## Boundary Contract

`rocketmq-admin-tui` must remain a terminal UI adapter:

- It depends on `rocketmq-admin-core`, not `rocketmq-admin-cli`.
- It does not use `clap`, `clap_complete`, `tabled`, `colored`, `dialoguer`, or
  `indicatif`.
- It does not call CLI command modules or parse CLI command structs.
- Shared admin request/result/service behavior belongs in `rocketmq-admin-core`.
- TUI-only concerns belong here: layout, focus, command catalog, forms, result
  view models, keyboard actions, progress display, and terminal rendering.

These rules are enforced by `tests/no_cli_dependency.rs`.

## Crate Layout

```text
rocketmq-admin-tui/
├── src/
│   ├── main.rs                 # Runtime ownership and app startup
│   ├── rocketmq_tui_app.rs     # Event loop, action handling, command tasks, frame pacing
│   ├── rocketmq_tui_app/       # Keyboard, mouse, and paste handling
│   ├── state.rs                # App state, form state, validation, focus model, motion clock
│   ├── motion.rs               # Tick-driven animation primitives
│   ├── result_view.rs          # Prepared results: grids, documents, viewports, wrapping
│   ├── terminal.rs             # Terminal modes and synchronized frame output
│   ├── text.rs                 # Display width, truncation, and wrapping
│   ├── text_input.rs           # Line editing for text inputs
│   ├── ui.rs                   # Frame layout, hit map, and render entry point
│   ├── ui/                     # Pane painters, theme, effects, and widgets
│   ├── action.rs               # Internal action messages
│   ├── event.rs                # Keyboard helpers
│   ├── admin_facade.rs         # TUI-to-admin-core facade
│   ├── admin_facade/           # Core request builders and async operations
│   ├── commands.rs             # Command metadata surface
│   ├── commands/               # Catalog and executor dispatch
│   └── view_model/             # Result conversion for terminal rendering
└── tests/
    └── no_cli_dependency.rs    # Adapter boundary guardrails
```

## Adding a TUI Command

1. Add or reuse the admin request/result/service in `rocketmq-admin-core`.
2. Add request-builder and async operation methods to `TuiAdminFacade`.
3. Add a `CommandSpec` in the appropriate catalog domain.
4. Wire the command ID in `execute_command_with_progress`.
5. Convert the result into a `CommandResultViewModel`.
6. Add focused tests for catalog coverage, argument validation, facade mapping,
   and result rendering.

## Validation

For documentation-only changes, local Markdown/SVG checks are usually enough.
For Rust changes in this crate, run:

```bash
cargo test -p rocketmq-admin-tui
```

For relevant Rust changes, select package-scoped checks from the repository root:

```bash
cargo fmt -p rocketmq-admin-tui -- --check
cargo clippy -p rocketmq-admin-tui --no-deps -- -D warnings
```

## Related Crates

- [`rocketmq-admin-core`](../rocketmq-admin-core) - reusable admin request, service, and result layer.
- [`rocketmq-admin-cli`](../rocketmq-admin-cli) - command-line adapter that shares the same core layer.
- [`rocketmq-transport`](../../../rocketmq-transport) - RocketMQ remoting protocol and RPC types.
- [`rocketmq-client`](../../../rocketmq-client) - RocketMQ client APIs used by admin services.

## License

Licensed under the [Apache License, Version 2.0](../../../LICENSE-APACHE).
