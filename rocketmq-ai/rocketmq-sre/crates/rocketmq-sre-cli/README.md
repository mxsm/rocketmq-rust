# rocketmq-sre-cli

Operator CLI for the [AI SRE workspace](../../README.md). The package is
`rocketmq-sre-cli`; its binary is **`rocketmq-sre`**.

## Usage

Run Cargo commands from `rocketmq-ai/rocketmq-sre`:

```bash
cargo run --locked -p rocketmq-sre-cli --bin rocketmq-sre -- --help
cargo run --locked -p rocketmq-sre-cli --bin rocketmq-sre -- --url http://127.0.0.1:8090 status
```

Options must precede the command. `--url` overrides `ROCKETMQ_SRE_URL`.
Protected reads resolve the token from `ROCKETMQ_SRE_TOKEN`, or the variable
named by `--token-env`. There is no `--token` option. Use
`--allow-cluster <UUID>` repeatedly to narrow cluster scope and `--compact`
for compact JSON.

| Commands | Behavior |
| --- | --- |
| `status`, `readiness` | Liveness/dependency reads; no token required by the CLI |
| `openapi`, `clusters`, `cluster <UUID>` | Authenticated API/cluster reads |
| `incident <UUID>`, `inspection <UUID>`, `plan <UUID>` | Authenticated workflow reads |
| `draft-plan <JSON_FILE>`, `draft-runbook <JSON_FILE>` | Validate and print a local typed draft without network access |

Local drafts are bounded to 256 KiB and carry no execution authority. Remote
commands use the fixed GET-only [Rust client](../rocketmq-sre-client).
There are no approve, execute, apply, reset, raw Admin or arbitrary request
commands. The process uses a current-thread Tokio runtime for these bounded
CLI operations.

Successful output is JSON except for help. Usage errors exit with code 2;
other operational/output failures exit with code 1 and print a fixed public
message to stderr. See [src/lib.rs](src/lib.rs) and [src/main.rs](src/main.rs).

## Validation

```bash
cargo fmt -p rocketmq-sre-cli -- --check
cargo test --locked -p rocketmq-sre-cli
```

[Apache License 2.0](../../../../LICENSE-APACHE).
