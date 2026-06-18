# rocketmq-admin-cli

[![Crates.io](https://img.shields.io/crates/v/rocketmq-admin-cli.svg)](https://crates.io/crates/rocketmq-admin-cli)
[![License](https://img.shields.io/badge/license-Apache--2.0-blue.svg)](../../../LICENSE-APACHE)

`rocketmq-admin-cli` is the command-line administration adapter for the
[rocketmq-rust](https://github.com/mxsm/rocketmq-rust) workspace. It provides a
Java-compatible RocketMQ admin command surface, shell completion generation,
confirmation prompts, progress display, terminal rendering, and table/JSON/YAML
output while delegating reusable admin behavior to
[`rocketmq-admin-core`](../rocketmq-admin-core).

The crate is intended for operators who need a CLI for RocketMQ cluster
inspection and maintenance, and for contributors migrating or extending
RocketMQ admin commands without coupling terminal UI concerns to shared admin
services.

[中文文档](README-zh_cn.md)

## Architecture

![rocketmq-admin-cli architecture](../../../resources/admin-cli-architecture.svg)

The CLI is intentionally thin:

- **Entry point**: `main.rs` starts a dedicated Tokio runtime thread and sets the
  remoting version used by admin requests.
- **Root parser**: `RocketMQCli` owns `clap` parsing, shell completion
  generation, and Java-compatible argument normalization such as `-bn` to
  `--brokerName`.
- **Command domains**: `commands.rs` exposes admin domains and a shared
  `CommandExecute` trait. Current tests assert 96 Java-registered commands are
  reachable under their expected Rust domains.
- **Command adapters**: subcommands convert CLI arguments into
  `rocketmq-admin-core` request DTOs, call core services, and render structured
  results.
- **Core services**: `rocketmq-admin-core` owns reusable admin request models,
  services, routing, and RPC-facing behavior used by CLI and TUI adapters.
- **Terminal UX**: formatters, colored status output, confirmation prompts,
  progress bars, and completion scripts stay in this crate.

Future TUI or GUI adapters should depend on `rocketmq-admin-core`, not on
`rocketmq-admin-cli`.

## Capabilities

- Java-compatible admin command names for cluster, broker, topic, consumer,
  message, auth, HA, export, and controller operations.
- Root help, per-domain help, and a categorized `show` command table.
- Shell completion generation for `bash`, `zsh`, and `fish`.
- NameServer address input through `-n` / `--namesrvAddr` on commands that need
  cluster access.
- Compatibility handling for selected Java admin flags and command aliases.
- Terminal rendering with table, JSON, and YAML formatter infrastructure.
- Confirmation helpers for dangerous operations and `--yes` support on common
  arguments.
- Progress bar and spinner helpers for long-running operations.
- Integration with `rocketmq-admin-core`, `rocketmq-remoting`,
  `rocketmq-common`, and the shared admin RPC stack.

## Quick Start

Run local help without a RocketMQ cluster:

```bash
cargo run -p rocketmq-admin-cli -- --help
cargo run -p rocketmq-admin-cli -- topic --help
cargo run -p rocketmq-admin-cli -- show
```

Generate shell completion:

```bash
cargo run -p rocketmq-admin-cli -- --generate-completion bash
cargo run -p rocketmq-admin-cli -- --generate-completion zsh
cargo run -p rocketmq-admin-cli -- --generate-completion fish
```

Build a release binary:

```bash
cargo build --release -p rocketmq-admin-cli
```

The binary is written to:

```text
target/release/rocketmq-admin-cli
```

On Windows, Cargo adds the `.exe` suffix.

## Cluster Examples

The commands below require a reachable RocketMQ NameServer and the relevant
cluster permissions.

```bash
# Topic commands
rocketmq-admin-cli topic topicList -n 127.0.0.1:9876
rocketmq-admin-cli topic topicRoute -t MyTopic -n 127.0.0.1:9876
rocketmq-admin-cli topic updateTopic -t MyTopic -c DefaultCluster -r 8 -w 8 -n 127.0.0.1:9876
rocketmq-admin-cli topic deleteTopic -t MyTopic -c DefaultCluster -n 127.0.0.1:9876

# NameServer commands
rocketmq-admin-cli nameserver getNamesrvConfig -n 127.0.0.1:9876
rocketmq-admin-cli nameserver updateKvConfig -s namespace -k key -v value -n 127.0.0.1:9876
rocketmq-admin-cli nameserver deleteKvConfig -s namespace -k key -n 127.0.0.1:9876

# Broker commands
rocketmq-admin-cli broker getBrokerConfig -c DefaultCluster -n 127.0.0.1:9876
rocketmq-admin-cli broker updateBrokerConfig -c DefaultCluster -k flushDiskType -v ASYNC_FLUSH -n 127.0.0.1:9876

# Container and export commands
rocketmq-admin-cli container addBroker -c 127.0.0.1:10911 -b ./conf/broker.conf
rocketmq-admin-cli export exportMetrics -c DefaultCluster -f /tmp/rocketmq/export -n 127.0.0.1:9876
rocketmq-admin-cli export rocksDBConfigToJson -p /tmp/rocketmq/config -t topics -j true
```

Most cluster-facing commands accept `-n` or `--namesrvAddr`. Where the command
path supports the standard RocketMQ environment variable, you can also set:

```bash
set ROCKETMQ_NAMESRV_ADDR=127.0.0.1:9876
rocketmq-admin-cli topic topicList
```

On Unix shells:

```bash
export ROCKETMQ_NAMESRV_ADDR=127.0.0.1:9876
rocketmq-admin-cli topic topicList
```

## Command Domains

| Domain | Purpose |
| --- | --- |
| `auth` | User and ACL create, update, delete, list, get, and copy operations. |
| `broker` | Broker config, status, cleanup, cold-data flow control, timer, epoch, and CommitLog operations. |
| `cluster` | Cluster listing and cluster-wide send latency diagnostics. |
| `connection` | Consumer and producer connection inspection. |
| `consumer` | Subscription group, consumer progress, running info, consume mode, and monitoring operations. |
| `container` | Broker container add and remove operations. |
| `controller` | Controller config, metadata, master election, and broker metadata cleanup. |
| `export` | Config, metadata, metrics, POP record, and RocksDB metadata export. |
| `ha` | HA runtime status and sync-state-set inspection. |
| `lite` | Lite topic, group, client, broker, parent-topic, and dispatch operations. |
| `message` | Message query, decode, print, consume, trace, send, and compaction-log diagnostics. |
| `nameserver` | NameServer config, KV config, and broker write-permission operations. |
| `offset` | Clone, reset, skip, and inspect consumer offsets. |
| `producer` | Producer connection and status inspection. |
| `queue` | ConsumeQueue query and RocksDB ConsumeQueue write-progress checks. |
| `stats` | Topic and consumer TPS statistics. |
| `topic` | Topic create, update, delete, query, permission, static-topic, route, and allocation operations. |

Use `rocketmq-admin-cli show` to print the complete categorized command table.

## Output and UX

This crate owns terminal-specific behavior:

- `formatters/` contains table, JSON, and YAML formatting infrastructure.
- `ui/output.rs` contains colored status, headers, summaries, and empty-result
  helpers.
- `ui/prompt.rs` contains confirmation and input helpers for interactive flows.
- `ui/progress.rs` contains spinner and progress-bar helpers.
- `--generate-completion` writes shell completion scripts to stdout.

Shared request, result, and service behavior belongs in `rocketmq-admin-core` so
other adapters can reuse it.

## Adding or Migrating a Command

Use this flow for new CLI commands and Java command migration:

1. Add or reuse request/result DTOs in `rocketmq-admin-core/src/core/<domain>.rs`.
2. Add or reuse the service method in `rocketmq-admin-core`.
3. Add CLI argument parsing under
   `rocketmq-admin-cli/src/commands/<domain>/`.
4. Convert CLI args into the core request DTO.
5. Call the core service and render the structured result.
6. Add core tests for request/service behavior.
7. Add CLI tests for help output, parsing, aliases, or command smoke behavior.

Keep shared admin RPC orchestration out of CLI command files when it can be
reused by TUI or future adapters.

## Crate Layout

```text
rocketmq-admin-cli/
  src/main.rs             runtime setup and CLI entry point
  src/rocketmq_cli.rs     root parser, completion generation, Java-compatible args
  src/commands.rs         command domain enum, CommandExecute, show table
  src/commands/           domain subcommands and CLI-to-core adapters
  src/formatters/         table, JSON, and YAML output formatters
  src/ui/                 output, prompt, progress, and style helpers
  src/validators.rs       CLI validation helpers
  tests/cli_help.rs       help, completion, aliases, and parser smoke tests
  tests/java_parity_inventory.rs  Java command inventory reachability tests
```

## Validation

Run targeted checks after documentation or CLI changes:

```bash
cargo test -p rocketmq-admin-cli
cargo run -p rocketmq-admin-cli -- --help
cargo run -p rocketmq-admin-cli -- show
```

For Rust behavior changes inside this workspace, also run the required root
workspace checks:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Design Boundaries

- This crate is a command-line adapter. Reusable admin behavior belongs in
  `rocketmq-admin-core`.
- Command modules should convert CLI options into core request DTOs and render
  core results; they should not duplicate admin business logic.
- Terminal-only concerns such as prompts, progress, colors, output format, and
  shell completion belong in this crate.
- TUI and future GUI adapters should depend on `rocketmq-admin-core`, not on the
  CLI crate.
- Cluster examples require a running RocketMQ cluster; help, completion, and
  parser tests run locally.

## Related Crates

- [`rocketmq-admin-core`](../rocketmq-admin-core): reusable admin request,
  service, and operation layer.
- [`rocketmq-admin-tui`](../rocketmq-admin-tui): terminal UI adapter built on
  the core services.
- [`rocketmq-remoting`](../../../rocketmq-remoting): RocketMQ remoting protocol
  and transport foundation used by admin operations.

## License

Licensed under the Apache License, Version 2.0. See
[`LICENSE-APACHE`](../../../LICENSE-APACHE) for details.
