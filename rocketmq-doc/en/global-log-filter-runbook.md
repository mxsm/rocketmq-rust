# Global Log Filter Operations Runbook

## Contract

NameServer, Broker, Controller, Proxy, MCP, and the selected Dashboard backends use the shared `rocketmq-observability` resolver. The startup precedence is:

```text
authorized runtime override > --log-filter > RUST_LOG > logging.filter/logFilter > info
```

Runtime override is currently exposed only by Broker. NameServer, Controller, and Proxy retain an in-process reload handle when reload is enabled, but do not expose an unauthenticated management protocol. MCP and the Dashboard applications use the same startup resolver and default to `info`.

Blank values, invalid directives, non-Unicode `RUST_LOG`, and conflicting `logging.filter`/`logFilter` values fail before the business lifecycle starts. All console, file, and OTLP log layers share one outer filter. Build scripts must not inject a logging default.

## Configuration

YAML:

```yaml
logging:
  filter: info,rocketmq_broker=debug
  reload:
    enabled: false
```

TOML:

```toml
[logging]
filter = "info,rocketmq_broker=debug"

[logging.reload]
enabled = false
```

Java properties compatibility:

```properties
logFilter=info,rocketmq_broker=debug
```

The four core service executables also accept `--log-filter <DIRECTIVE>`. `RUST_LOG` remains the portable process-level override. MCP temporarily accepts `server.log_level` as a deprecated alias and emits one warning; new MCP configuration must use `logging.filter`. Dashboard GPUI and Dashboard Web backend currently use `RUST_LOG` or the default only.

At startup, check the structured event fields `service`, `effective_filter`, `filter_source`, `subscriber_installed`, and `reload_enabled`. Do not approve a rollout when the subscriber is not installed or the source differs from the intended deployment source.

## Broker remote reload

Remote reload is disabled unless all of these conditions hold:

- `logging.reload.enabled=true`;
- Broker authentication and authorization are both enabled;
- the request passes the existing `Cluster/Update` authorization check;
- ACL credentials, reason, request ID, and a TTL from 60 through 7200 seconds are present;
- `${store_path_root_dir}/logs/log-filter-audit.jsonl` is writable;
- the owned TTL restore task was mounted successfully.

A normal operator must keep an explicit global `info` baseline and may elevate only `rocketmq_*` targets. Global DEBUG/TRACE and span/field directives require a super-user break-glass account. A new override replaces the previous one and resets its TTL. Expiry or `logFilterRestore=true` restores the startup baseline; there is no nested override stack.

Use ACL environment variables instead of placing secrets in shell history:

```text
ROCKETMQ_ACL_ACCESS_KEY
ROCKETMQ_ACL_SECRET_KEY
ROCKETMQ_ACL_SECURITY_TOKEN (optional)
```

Apply a scoped override:

```powershell
rocketmq-admin-cli.exe -n 127.0.0.1:9876 broker updateBrokerConfig -b 127.0.0.1:10911 `
  -p "logFilter=info,rocketmq_broker=debug" `
  -p "logFilterReason=INC-42 diagnose dispatch latency" `
  -p "logFilterTtlSeconds=600" `
  -p "logFilterRequestId=INC-42" --yes
```

Restore early:

```powershell
rocketmq-admin-cli.exe -n 127.0.0.1:9876 broker updateBrokerConfig -b 127.0.0.1:10911 `
  -p "logFilterRestore=true" `
  -p "logFilterReason=INC-42 investigation completed" `
  -p "logFilterRequestId=INC-42-restore" --yes
```

The audit sequence is `intent`, then `success`; automatic expiry adds `ttl_restore`. Every append is executed through the bounded blocking executor and followed by `sync_data`. If the success audit fails, the Broker attempts an immediate rollback to the startup baseline.

## Verification and release gates

Build service binaries first, then run the startup probes for each core service:

```powershell
.\scripts\verify-log-filter-governance.ps1 -Service broker -Executable .\target\release\rocketmq-broker-rust.exe -CommonArguments @("-c", ".\conf\broker.toml")
```

Run the authenticated Broker reload and restore probe on a canary whose audit path is locally readable:

```powershell
.\scripts\verify-broker-log-filter-reload.ps1 `
  -AdminExecutable .\target\release\rocketmq-admin-cli.exe `
  -BrokerAddress 127.0.0.1:10911 `
  -AuditPath C:\rocketmq\store\logs\log-filter-audit.jsonl `
  -NamesrvAddress 127.0.0.1:9876
```

The probe enforces `DEBUG=0` and `INFO>0` by default, target-only DEBUG, fail-closed invalid directives, an internal reload interval of at most 100 ms, durable audit evidence, and successful TTL or explicit restoration.

Collect 15-minute baseline and candidate Prometheus samples. The queries must each return exactly one scalar series:

```powershell
.\scripts\collect-log-filter-canary.ps1 -PrometheusBaseUrl http://prometheus:9090 `
  -ThroughputQuery '<broker throughput PromQL>' -P99Query '<broker P99 PromQL>' -Phase baseline

.\scripts\collect-log-filter-canary.ps1 -PrometheusBaseUrl http://prometheus:9090 `
  -ThroughputQuery '<broker throughput PromQL>' -P99Query '<broker P99 PromQL>' -Phase candidate `
  -BaselineSummary .\target\log-filter-evidence\log-filter-canary-baseline-<timestamp>.json
```

The candidate gate allows at most 1% average throughput loss and 2% average P99 growth. Repository automation cannot certify a real production canary; attach target-cluster evidence before assigning the final 96/100 production-readiness score.

## Metrics and alerts

- `rocketmq_observability_log_filter_reload_total{service,result,source}`
- `rocketmq_observability_log_filter_active{service,source}`
- `rocketmq_observability_log_filter_expiry_timestamp_seconds{service}`
- audit, automatic-restore, and rollback failure counters under the same observability namespace

Prometheus rules in `distribution/config/prometheus-log-filter-alerts.yaml` cover reload failures, audit failures, overdue TTLs, long-running runtime overrides, and restore failures. The Broker Grafana dashboard includes reload success, active source, TTL, and governance failure panels.

## Failure matrix

| Symptom | Expected fail-safe behavior | Operator action |
|---|---|---|
| Invalid or blank startup filter | Process exits before business lifecycle | Correct the highest-precedence source; do not add a fallback |
| `logging.filter` conflicts with `logFilter` | Process exits before business lifecycle | Remove the legacy alias or make both values identical |
| Default startup emits DEBUG | Release gate fails | Check `filter_source`, deployment environment, and stale binaries |
| Reload disabled or TTL task unavailable | Broker returns `NoPermission` | Enable reload deliberately and restart after validating task ownership |
| Authentication or authorization disabled | Broker returns `NoPermission` | Do not bypass the gate; enable both security layers |
| Audit intent cannot be persisted | Filter remains unchanged | Repair store permissions/capacity and retry with a new request ID |
| Reload fails | Last-known-good filter remains active | Inspect reload failure metric and failure audit |
| Success audit fails | Broker rolls back to startup baseline | Treat as an incident; repair audit storage before another override |
| TTL restore fails or becomes overdue | Critical alert fires | Use authenticated explicit restore; restart Broker if baseline cannot be restored |
| Candidate performance exceeds budget | Canary gate fails | Restore INFO, retain evidence, and stop promotion |

## Rollback

For an active override, issue `logFilterRestore=true` and verify a successful audit record plus an expiry gauge of zero. For a bad startup configuration, remove the CLI/environment/config override and restart with default `info`. A rollback is complete only after default probes show `DEBUG=0`, `INFO>0`, the active source is no longer `runtime`, and the Broker has remained stable for the observation window.
