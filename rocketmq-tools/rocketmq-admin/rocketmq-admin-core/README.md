# RocketMQ Admin Core

`rocketmq-admin-core` is the reusable capability layer for RocketMQ admin operations. It contains admin domain models, request/response DTOs, service orchestration, validation, and shared helpers used by `rocketmq-admin-cli` and `rocketmq-admin-tui`.

This crate is not a command-line or terminal UI crate.

## Role in the Admin Stack

```text
rocketmq-admin-cli      rocketmq-admin-tui
        |                       |
        | adapter args/UI state |
        v                       v
        rocketmq-admin-core service/DTO layer
                     |
                     v
       RocketMQ admin client and remoting APIs
```

The stable call chain is:

```text
adapter input
  -> core request DTO
  -> core service
  -> structured result
  -> adapter renderer or view model
```

## Responsibilities

- Define admin request and result types for each command domain.
- Validate domain inputs without depending on CLI parser objects.
- Orchestrate RocketMQ admin RPC calls through reusable service methods.
- Return structured data that can be rendered by CLI, TUI, tests, or future adapters.
- Provide shared resolver/helper logic for cluster, broker, topic, consumer, offset, queue, HA, stats, export, auth, controller, and related admin domains.

## Non-Responsibilities

- CLI argument parsing with `clap`.
- Shell completion generation.
- Terminal table, color, progress bar, prompt, or confirmation rendering.
- TUI state management, layout, event handling, or Ratatui view rendering.
- Any dependency on `rocketmq-admin-cli`.

## Public API Shape

Each domain should expose explicit request/result structs and service methods:

```rust
let request = SomeDomainRequest::try_new(/* domain values */)?;
let result = SomeDomainService::some_operation_by_request_with_rpc_hook(request, rpc_hook).await?;
```

Do not pass CLI parameter structs into core. If an operation needs to support both CLI and TUI, the shared input belongs in a core DTO.

## Feature Flags

| Feature | Default | Purpose |
|---|---:|---|
| `rocksdb-export` | No | Enables RocksDB metadata export support for CLI commands that need direct RocksDB reading. |

The default build intentionally avoids pulling RocksDB into every core consumer.

## Development Rules

- Keep this crate free of `clap`, `clap_complete`, `tabled`, `colored`, `indicatif`, `dialoguer`, and `ratatui`.
- Put display formatting in adapter crates, not here.
- Add unit tests for request validation and helper behavior when adding a new service.
- Keep service methods async where they perform admin RPC or other I/O.
- Prefer concrete request/result structs over broad command registries or CLI-specific trait objects.

## Validation

Run targeted core tests after changing this crate:

```bash
cargo test -p rocketmq-admin-core
```

For root workspace Rust changes, also run the repository-required formatting and clippy commands from the workspace root:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

