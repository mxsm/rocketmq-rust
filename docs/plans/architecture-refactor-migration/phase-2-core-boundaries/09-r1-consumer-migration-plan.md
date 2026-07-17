# M09-05 R1 consumer migration plan

## Scope

R1 migrates internal consumers away from compatibility facades while keeping external deprecated paths compilable.
The authoritative inventory is the 29 `remove_by = "R1"` edges in
`scripts/architecture-dependency-baseline.json`; `scripts/architecture-release-plan.json` independently records the
same consumer set and the release guard rejects any difference.

## Exact 29-edge consumer list

| Consumer | Manifest | Compatibility targets | Edge count | R1 action |
|---|---|---|---:|---|
| `rocketmq-admin-cli` | `rocketmq-tools/rocketmq-admin/rocketmq-admin-cli/Cargo.toml` | Common, Remoting, legacy Runtime | 3 | use Admin/Core canonical DTO, transport, runtime and error paths |
| `rocketmq-admin-core` | `rocketmq-tools/rocketmq-admin/rocketmq-admin-core/Cargo.toml` | Common | 1 | isolate legacy facade behind its compatibility surface; keep canonical core independent |
| `rocketmq-admin-tui` | `rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/Cargo.toml` | Common, Remoting, legacy Runtime | 3 | consume Admin/Core and canonical owners |
| `rocketmq-auth` | `rocketmq-auth/Cargo.toml` | Common, Remoting | 2 | use Security API, Protocol and Transport owners |
| `rocketmq-broker` | `rocketmq-broker/Cargo.toml` | Common, Remoting, legacy Runtime | 3 | use Model/Protocol/Transport/Runtime owners; keep approved Store composition separately |
| `rocketmq-client-rust` | `rocketmq-client/Cargo.toml` | Common, Remoting, legacy Runtime | 3 | use Model/Protocol/Transport/Runtime owners |
| `rocketmq-common` | `rocketmq-common/Cargo.toml` | legacy Runtime | 1 | forward runtime compatibility directly to `rocketmq-runtime` |
| `rocketmq-controller` | `rocketmq-controller/Cargo.toml` | Common, Remoting, legacy Runtime | 3 | use Model/Protocol/Transport/Runtime owners |
| `rocketmq-namesrv` | `rocketmq-namesrv/Cargo.toml` | Common, Remoting, legacy Runtime | 3 | use Model/Protocol/Transport/Runtime owners |
| `rocketmq-proxy` | `rocketmq-proxy/Cargo.toml` | Common, Remoting | 2 | compose Proxy adapters and canonical contracts only |
| `rocketmq-remoting` | `rocketmq-remoting/Cargo.toml` | Common, legacy Runtime | 2 | forward deprecated paths to Protocol/Transport/Runtime owners |
| `rocketmq-store` | `rocketmq-store/Cargo.toml` | Common, Remoting, legacy Runtime | 3 | compose Store API/Local/Rocks/Tiered and canonical Runtime owners |
| **Total** |  |  | **29** | all edges reach zero during R1; external deprecated paths remain until next-major gates pass |

The two `long-term` Store composition edges and four `next-major` compatibility edges are not part of the 29-edge R1
burn-down and must not be silently counted as R1 work.

## CI no-growth rule

CI runs `python scripts/architecture_dependency_guard.py --mode baseline` on Linux and Windows. The baseline guard
matches caller, target, dependency kind, manifest path, alias, and count; therefore it rejects a new facade edge, a
renamed bypass, or growth of an existing allowance. CI also runs `python scripts/architecture_release_guard.py`, which
requires the R1 plan to remain exactly equal to the 29-edge ledger.

R1 work removes an entry only after the corresponding source imports, feature closure, tests, and standalone consumers
have migrated. It never “fixes” a finding by widening the allowlist.

## External usage collection

The external usage snapshot is collected at R0, refreshed on every R1 minor release, and signed before next-major:

1. crates.io reverse-dependency inventory and download trend for compatibility and canonical crates;
2. GitHub code search for old crate/module paths, with repository and last-activity deduplication;
3. project issues/discussions labeled `architecture-migration`, including blocked downstream builds;
4. release deprecation feedback and opt-in telemetry where such telemetry is available and privacy-safe.

Each snapshot records query/time, raw count, deduplicated active consumers, known migration owner, blocker severity, and
canonical replacement. Absence of telemetry is recorded as unknown evidence, never as zero usage.

## Notifications and exit criteria

External notification uses R0/R1 release notes, a versioned migration guide, a GitHub tracking issue/discussion, and
Rust deprecation notes containing the canonical replacement. R1 is complete when all 29 internal edges are zero, root
and standalone verification passes, CI still rejects new facade edges, and remaining external consumers have an owner
or a documented next-major disposition.

## R1 checklist

- [x] The consumer table totals exactly 29 edges and matches the machine-readable plan.
- [x] The CI rule forbids new or expanded facade edges.
- [x] External usage sources, cadence, uncertainty handling, and notification channels are defined.
- [x] R1 is separated from the four next-major and two long-term edges.
- [x] Removing an allowlist entry requires real consumer migration and validation.
