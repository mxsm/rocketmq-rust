# RocketMQ Admin CLI

`rocketmq-admin-cli` is the command-line adapter for RocketMQ admin operations. It owns CLI parsing, command grouping, shell completion, confirmation prompts, progress display, and terminal rendering. Shared admin behavior is delegated to `rocketmq-admin-core`.

## Role in the Admin Stack

```text
shell command
  -> clap args in rocketmq-admin-cli
  -> rocketmq-admin-core request DTO
  -> rocketmq-admin-core service
  -> structured result
  -> CLI renderer
```

Future TUI or GUI adapters should depend on `rocketmq-admin-core`, not on this crate.

## Responsibilities

- Define CLI commands and subcommands with `clap`.
- Preserve Apache RocketMQ tool-compatible command names and flags where supported.
- Convert CLI args into `rocketmq-admin-core` request DTOs.
- Call core services and render structured results for terminal users.
- Own shell completion generation.
- Own confirmation prompts, progress bars, table/JSON/YAML output, and command-line error messages.

## Non-Responsibilities

- RocketMQ admin RPC orchestration that can be shared by other frontends.
- Domain request/result types shared with TUI or tests.
- Ratatui UI state or view models.
- Direct implementation of new admin business logic when a core service should exist.

## Build and Run

From the repository root:

```bash
cargo run -p rocketmq-admin-cli -- --help
cargo run -p rocketmq-admin-cli -- topic --help
```

Build a release binary:

```bash
cargo build --release -p rocketmq-admin-cli
```

The release binary is written to:

```text
target/release/rocketmq-admin-cli
```

## Common Commands

```bash
# Show top-level help
rocketmq-admin-cli --help

# Show categorized command table
rocketmq-admin-cli show

# Generate shell completion
rocketmq-admin-cli --generate-completion bash
rocketmq-admin-cli --generate-completion zsh
rocketmq-admin-cli --generate-completion fish

# Topic commands
rocketmq-admin-cli topic topicList -n 127.0.0.1:9876
rocketmq-admin-cli topic topicRoute -t MyTopic -n 127.0.0.1:9876
rocketmq-admin-cli topic updateTopic -t MyTopic -c DefaultCluster -r 8 -w 8 -n 127.0.0.1:9876
rocketmq-admin-cli topic deleteTopic -t MyTopic -c DefaultCluster -n 127.0.0.1:9876

# NameServer commands
rocketmq-admin-cli nameserver getKVConfig -s namespace -k key -n 127.0.0.1:9876
rocketmq-admin-cli nameserver updateKVConfig -s namespace -k key -v value -n 127.0.0.1:9876

# Broker config commands
rocketmq-admin-cli broker getBrokerConfig -c DefaultCluster -n 127.0.0.1:9876
rocketmq-admin-cli broker updateBrokerConfig -c DefaultCluster -k flushDiskType -v ASYNC_FLUSH -n 127.0.0.1:9876
```

## Command Domains

| Domain | Purpose |
|---|---|
| `topic` | Topic create/update/delete/query and static topic operations. |
| `nameserver` | NameServer config, KV config, and write permission operations. |
| `broker` | Broker config, status, cleanup, cold-data, timer, and commit-log operations. |
| `cluster` | Cluster listing and cluster send-RT diagnostics. |
| `consumer` | Subscription group, consumer progress, running info, and consume mode operations. |
| `connection` | Consumer and producer connection inspection. |
| `offset` | Clone, reset, skip, and query consumer offsets. |
| `queue` | ConsumeQueue query and RocksDB CQ write-progress checks. |
| `message` | Message query, decode, print, consume, and send diagnostics. |
| `producer` | Producer info and send diagnostic operations. |
| `controller` | Controller config and metadata operations. |
| `auth` | User and ACL management. |
| `ha` | HA status and sync-state-set inspection. |
| `stats` | Cluster-wide topic and consumer statistics. |
| `export` | Config, metadata, pop-record, and optional RocksDB metadata export. |
| `lite` | Lite topic/group/client inspection and dispatch operations. |

## Environment

Most commands accept `-n` or `--namesrvAddr`. You can also set the NameServer address through the standard RocketMQ environment variable when supported by the command path:

```bash
set ROCKETMQ_NAMESRV_ADDR=127.0.0.1:9876
rocketmq-admin-cli topic topicList
```

On Unix shells:

```bash
export ROCKETMQ_NAMESRV_ADDR=127.0.0.1:9876
rocketmq-admin-cli topic topicList
```

## Adding or Migrating a Command

Use this flow for new commands and for continuing old command migration:

1. Add or reuse a request/result DTO in `rocketmq-admin-core/src/core/<domain>.rs`.
2. Add or reuse a service method in `rocketmq-admin-core`.
3. Add CLI args and rendering in `rocketmq-admin-cli/src/commands/<domain>/`.
4. Convert CLI args into the core request DTO.
5. Call the core service and render the structured result.
6. Add core request/service tests and CLI parse/help/smoke tests.

Do not put shared admin RPC orchestration directly in CLI command files if the operation can be reused by TUI or future adapters.

## Validation

Run targeted CLI tests after changing this crate:

```bash
cargo test -p rocketmq-admin-cli
```

For root workspace Rust changes, also run:

```bash
cargo fmt --all
cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings
```

## Related Crates

- [`rocketmq-admin-core`](../rocketmq-admin-core): reusable admin capability layer.
- [`rocketmq-admin-tui`](../rocketmq-admin-tui): terminal UI adapter built on core services.
