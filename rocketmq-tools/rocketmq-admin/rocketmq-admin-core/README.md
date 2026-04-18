# RocketMQ Admin Core

`rocketmq-admin-core` is the reusable admin capability layer for RocketMQ Rust tools.

## Responsibilities

- Defines admin request/response DTOs and service APIs.
- Owns RocketMQ admin RPC orchestration and domain validation.
- Provides reusable helpers for broker, cluster, topic, consumer, offset, queue, HA, stats, export, auth, controller, and related admin domains.
- Exposes structured results for adapters such as CLI and TUI.

## Non-Responsibilities

- CLI argument parsing.
- Shell completion generation.
- Terminal prompt, progress bar, color, or table rendering.
- TUI state, layout, event handling, or view-model rendering.

## Adapter Boundary

The intended call chain is:

```text
adapter args or UI state
  -> core request DTO
  -> core service
  -> structured result
  -> adapter renderer or view model
```

`rocketmq-admin-cli` owns `clap`, completion, terminal rendering, confirmation prompts, and command-line output. `rocketmq-admin-tui` owns Ratatui UI state and calls core services through its facade.

## Features

- Default build avoids the RocksDB dependency.
- `rocksdb-export` enables RocksDB metadata export support for CLI commands that need it.

