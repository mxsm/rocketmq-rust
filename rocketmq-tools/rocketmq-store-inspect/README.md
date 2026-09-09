# rocketmq-store-inspect

Offline CommitLog inspection, downgrade checks and multipath consolidation for RocketMQ-Rust.
The Cargo package is `rocketmq-store-inspect`; its executable is `rocketmq-cli-rust`.
This tool operates on local files and does not connect to a running cluster.

## Build and help

Use the repository's [pinned toolchain](../../rust-toolchain.toml), from the workspace root:

```bash
cargo build -p rocketmq-store-inspect --bin rocketmq-cli-rust
cargo run -p rocketmq-store-inspect --bin rocketmq-cli-rust -- --help
cargo run -p rocketmq-store-inspect --bin rocketmq-cli-rust -- read-message-log --help
cargo run -p rocketmq-store-inspect --bin rocketmq-cli-rust -- downgrade-preflight --help
cargo run -p rocketmq-store-inspect --bin rocketmq-cli-rust -- consolidate-multipath --help
```

Cargo adds `.exe` on Windows. Commands after `cargo run` require the `--` separator.
The tool also supports `--version` and `--verbose` diagnostics.

## Read message IDs

```bash
rocketmq-cli-rust read-message-log -c /data/commitlog/00000000000000000000 -f 0 -t 2
```

The command displays file size and a table with `message_id` and `client_message_id`.
It skips message bodies and scans records sequentially. `--to 2` limits scanning to the first
two records. The current `--from` comparison uses a counter incremented before filtering:
both `0` and `1` include the first record, while `2` starts at the second.
These are record counters, not byte offsets.

Use a stable copy or a stopped store for consistent inspection. This reader does not acquire
the Broker's exclusive Store lock and is not a corruption or checksum certification tool:
truncated or invalid frame sizes can end the scan without a complete integrity report.
See [the reader](src/content_show.rs) and the shared [record inspector](../../rocketmq-store/src/inspection.rs).

## Downgrade preflight

Stop the Broker first. This command acquires the exclusive Store lock and evaluates
Rust-owned storage formats against the requested target version:

```bash
rocketmq-cli-rust downgrade-preflight \
  --target-version 0.9.0 \
  --config /etc/rocketmq-rust/broker.toml \
  --output downgrade-report.json
```

It writes a structured report to `--output`, or stdout when no output file is given.
A denied downgrade exits with code `2`; other failures use typed CLI error codes.
An allowed report concerns the inspected storage formats and is not evidence that a complete
cluster rollback has been qualified. Keep a compatible inspection tool through the rollback window.

## Consolidate multipath CommitLog

Run against a stopped Broker and specify the actual Store root whose lock fences that Broker:

```bash
rocketmq-cli-rust consolidate-multipath \
  --source-root /data-a/commitlog \
  --source-root /data-b/commitlog \
  --target /data-consolidated/commitlog \
  --mapped-file-size 1073741824 \
  --store-root /var/lib/rocketmq-rust/store
```

The target must not exist; its parent and the Store root must exist.
The tool validates segment ownership, continuity, frame structure and free space,
copies into staging, checks byte equality, synchronizes files and publishes the new destination
with a rename. It leaves source files intact and prints a JSON report.
See [consolidation](src/multipath_consolidate.rs) for the exact supported layout checks.

## Source and validation

[CLI arguments](src/command_line.rs), [entrypoint](src/bin/rocketmq_cli.rs),
[downgrade policy](src/downgrade_preflight.rs) and [tests](tests).

```bash
cargo fmt -p rocketmq-store-inspect -- --check
cargo test -p rocketmq-store-inspect
```

[Apache License 2.0](../../LICENSE-APACHE).
