# Log Filter Governance Release Evidence

## Release identity

| Field | Value |
|---|---|
| Commit/release | `<required>` |
| Candidate cluster | `<required>` |
| Operator | `<required>` |
| Change window | `<required>` |
| Rollback owner | `<required>` |

## Static and test gates

| Gate | Command | Exit code | Evidence link | Result |
|---|---|---:|---|---|
| Observability tests | `cargo test -p rocketmq-observability` |  |  |  |
| Four core services | `cargo test -p <package>` |  |  |  |
| Broker tests | `cargo test -p rocketmq-broker` |  |  |  |
| MCP gates | See repository `AGENTS.md` |  |  |  |
| Format | `cargo fmt --all -- --check` |  |  |  |
| Workspace Clippy | `cargo clippy --workspace --no-deps --all-targets --all-features -- -D warnings` |  |  |  |
| Runtime audit | `.\scripts\runtime-audit.ps1 -SkipBaseline -EnforceBoundaryBaseline` |  |  |  |
| Architecture guards | `cargo test -p rocketmq-observability --test architecture_guards` |  |  |  |

## Startup behavior

Attach each generated summary and raw stdout/stderr. Do not mark pass from visual inspection alone.

| Service | Default DEBUG | Default INFO | Target DEBUG only | Invalid filter failed before lifecycle | `filter_source` | Result |
|---|---:|---:|---|---|---|---|
| NameServer |  |  |  |  |  |  |
| Broker |  |  |  |  |  |  |
| Controller |  |  |  |  |  |  |
| Proxy |  |  |  |  |  |  |

## Broker reload and audit

| Field | Value |
|---|---|
| Request ID |  |
| Operator AccessKey identifier |  |
| Reason/ticket |  |
| Effective target filter |  |
| TTL |  |
| `intent` audit durable |  |
| `success` audit durable |  |
| Internal reload latency (limit 100 ms) |  |
| Restore mode | TTL / explicit |
| Restore audit successful |  |
| Expiry gauge returned to zero |  |
| Sensitive data absent from evidence |  |

## Performance canary

Baseline and candidate observation windows must each be at least 15 minutes under comparable traffic.

| Metric | Baseline average | Candidate average | Change | Budget | Result |
|---|---:|---:|---:|---:|---|
| Broker throughput |  |  |  | no worse than -1% |  |
| Broker P99 |  |  |  | no worse than +2% |  |

Attach the raw CSV, both JSON summaries, PromQL expressions, traffic profile, and any data-quality exclusions.

## Alerts and rollback

| Check | Evidence | Result |
|---|---|---|
| Reload failure alert evaluated |  |  |
| Audit failure alert evaluated |  |  |
| TTL overdue alert evaluated |  |  |
| Sustained runtime override alert evaluated |  |  |
| Automatic restore failure alert evaluated |  |  |
| INFO rollback completed within 5 minutes |  |  |
| Post-rollback `DEBUG=0`, `INFO>0` |  |  |

## Go/No-Go

- Decision: `<GO | NO-GO>`
- Approver: `<required>`
- Decision time: `<required>`
- Residual risks: `<required>`
- Evidence gaps: `<none or required>`

The implementation target is 96/100, but the score must remain provisional until real target-cluster canary, alert, and rollback evidence is attached. Missing evidence is a No-Go, not an assumed pass.
