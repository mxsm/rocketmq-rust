# Tauri dashboard implementation acceptance

Date: 2026-09-10. Scope: the desktop implementation plan S01-S25 and its
F01-F22 / A01-A03 parity requirements. The storage design is fresh SQLite;
older data formats are intentionally not migrated or supported.

## Delivered steps

Each implementation step has its own Issue and squash-merged PR. The final
integration step records validation and any remaining runtime coverage limits.

| Step | Delivery | PR |
| --- | --- | --- |
| Development cluster | Locally built RocketMQ Rust NameServer, two Brokers, Proxy, isolated ACL fixture | [10359](https://github.com/mxsm/rocketmq-rust/pull/10359) |
| S01 | Owned, versioned local storage and shutdown | [10358](https://github.com/mxsm/rocketmq-rust/pull/10358) |
| S02 | Persistent expiring sessions and account-wide revocation | [10363](https://github.com/mxsm/rocketmq-rust/pull/10363) |
| S03 | Owned audit lifecycle and truthful mutation receipts | [10365](https://github.com/mxsm/rocketmq-rust/pull/10365) |
| S04 | Connection revisions, endpoint identities, atomic NameServer replacement | [10369](https://github.com/mxsm/rocketmq-rust/pull/10369) |
| S05 | Shared environment credentials for managed admin connections | [10374](https://github.com/mxsm/rocketmq-rust/pull/10374) |
| S06 | Exact entity navigation and bounded return history | [10376](https://github.com/mxsm/rocketmq-rust/pull/10376) |
| S07 | Configured Proxy scopes for Consumer queries | [10379](https://github.com/mxsm/rocketmq-rust/pull/10379) |
| S08 | Topic protection, create/update semantics, send receipts | [10381](https://github.com/mxsm/rocketmq-rust/pull/10381) |
| S09 | Per-target Topic results and manual failed-target review | [10383](https://github.com/mxsm/rocketmq-rust/pull/10383) |
| S10 | Consumer protection and per-target mutation outcomes | [10385](https://github.com/mxsm/rocketmq-rust/pull/10385) |
| S11 | Reviewed Broker configuration writes and readback | [10387](https://github.com/mxsm/rocketmq-rust/pull/10387) |
| S12 | Producer directory and stale query invalidation | [10389](https://github.com/mxsm/rocketmq-rust/pull/10389) |
| S13 | Real scoped ACL user management | [10391](https://github.com/mxsm/rocketmq-rust/pull/10391) |
| S14 | ACL policy management and typed resource deletion | [10393](https://github.com/mxsm/rocketmq-rust/pull/10393) |
| S15 | Multi-Broker Consumer configuration and explicit edit source | [10395](https://github.com/mxsm/rocketmq-rust/pull/10395) |
| S16 | Reviewed Consumer offset reset and readback; correct skip semantics | [10397](https://github.com/mxsm/rocketmq-rust/pull/10397) |
| S17 | Explicit Consumer runtime and stack diagnostics | [10399](https://github.com/mxsm/rocketmq-rust/pull/10399) |
| S18 | DLQ Key/ID/client scope, retained receipts and CSV export | [10401](https://github.com/mxsm/rocketmq-rust/pull/10401) |
| S19 | Exact combined Topic Broker/Cluster filters | [10403](https://github.com/mxsm/rocketmq-rust/pull/10403) |
| S20 | Dashboard business metrics with observation quality | [10405](https://github.com/mxsm/rocketmq-rust/pull/10405) |
| S21 | Environment-scoped persisted history and local-date charts | [10407](https://github.com/mxsm/rocketmq-rust/pull/10407) |
| S22 | Audited Consumer Monitor rule revisions | [10409](https://github.com/mxsm/rocketmq-rust/pull/10409) |
| S23 | Read-only storage and collector diagnostics | [10411](https://github.com/mxsm/rocketmq-rust/pull/10411) |
| S24 | Consistent backup, verification, empty-target restore and session revocation | [10413](https://github.com/mxsm/rocketmq-rust/pull/10413) |
| S25 | Integration acceptance and Windows shutdown stack fix | Current integration PR |

## Parity mapping

| Requirements | Implementation |
| --- | --- |
| F01, F02, F03 | S13 ACL users, S14 ACL policies, S05 credentials |
| F04, F05 | S11 Broker configuration, S12 Producer discovery |
| F06, F07 | S20 current overview, S21 actual stored history |
| F08, F09, F10 | S15 configuration summary, S16 offset reset, S07 Proxy scope |
| F11, F12, F13 | S09 Topic receipts, S10 Consumer receipts, S18 DLQ receipts |
| F14, F15, F22 | S08/S10 protection, S08 existence and send-status semantics |
| F16, F17, F18, F19 | S22 monitors, S03 audit, S02 sessions, S23 storage |
| F20, F21 | S19 exact filters, S06 entity navigation |
| A01, A02, A03 | S17 diagnostics, S18 DLQ parameters, S04 atomic replacement |
| D01-D04 desktop scope | S01/S04 local configuration, S21 history, S23 diagnostics, S24 recovery |

MySQL/PostgreSQL, shared multi-node storage, distributed collection leases, HTTP
health listeners, alert evaluation/notifications, and automatic recovery are not
part of the selected desktop scope. Consumer diagnostics through Proxy explicitly
report unsupported because the current admin diagnostic interface has no Proxy
target parameter. Unknown or partial observations are not reported as healthy.

## Automated validation

- Frontend production build passed; all 41 tests across 13 Vitest files passed.
- Backend full library suite passed: 140 tests, with three live ACL tests selected
  separately and also passing against the rebuilt local images. The registration coverage test includes every non-auth command and
  checks its dashboard authorization boundary instead of a historical fixed count.
- The storage CLI passed its independent binary check. Three storage operation
  tests cover committed WAL data, restored data, revoked sessions, source
  preservation, corrupt/unsupported backups, occupied targets, and failed restore
  publication. Persistence tests cover accepted I/O and owned background shutdown.
- Relevant shared admin/client behavior tests were run with their implementation
  steps. No full root-workspace feature matrix or cross-platform packaging claim
  is made. Vite reports the existing large-bundle advisory.

## Desktop and cluster verification

The Windows Tauri executable was started with an isolated data directory. Its real
WebView and authenticated IPC were used; no mock backend supplied these results.

| Verification | Observed result |
| --- | --- |
| Visible account flow | Sign-in, required initial password change, sign-in again, Sessions list, confirmed account-wide revocation and return to sign-in |
| Visible Monitor / Storage pages | Rule create and confirmed delete; available SQLite, actual page capacity, unknown filesystem free space, collector status; consecutive diagnostics refreshes did not change the write timestamp |
| Connection IPC | Address add/switch/delete, stale revision rejection, invalid atomic replacement rollback, environment-isolated rules, configured Proxy Consumer query |
| Topic and message IPC | Two-Broker create/update, route, SEND_OK receipt, ID/Key lookup, message detail, single-Broker delete and whole-Topic delete |
| Consumer / Broker IPC | Two-Broker group writes, complete configuration summary, detected differing configuration, reset of four queues, skip to latest, per-target group deletion; Broker configuration write/readback and restoration of the original value |
| Directory and history IPC | Producer directory query without a supplied group, audit filtering/cursor, Broker/Topic stored samples and pagination; history remained readable after restart |
| Storage CLI / restored desktop | Live backup, verify, restore; restored cached session rejected, new login successful, rules/history retained, restore audit present |
| New local Rust images | NameServer, Broker, Proxy, and admin CLI rebuilt from local source; ordinary and ACL Compose projects healthy (six containers); two-Broker listing and desktop SEND_OK after replacement |
| Isolated ACL integration | Valid/invalid/anonymous credential query behavior, normal/transactional sends, user CRUD, typed policy deletion preserving other entries: all three live tests passed |
| Exit | The initial real exit exposed a Windows main-thread stack overflow. Heap-pinning the joined SDK shutdown futures fixed it. A 1 MiB-stack lifecycle regression passed; two subsequent real desktop exits returned code 0 |

Temporary Topic, Consumer group, and Monitor smoke resources were deleted. Both
Docker clusters remain running for development. The desktop and its temporary
Vite process were closed after checking graceful shutdown.

### Remaining runtime coverage

- TLS certificate/handshake integration was not exercised; the local fixtures use
  plaintext remoting. Configuration and credential boundaries have automated tests.
- No live consumer implementing RunningInfo/JStack was attached. The no-client
  query returned a safe unavailable result; Proxy diagnostics reported unsupported.
  Offline, unsupported, bounded and truncated response branches have unit coverage.
- No populated DLQ with an online recipient or recorded Trace fixture was present.
  The empty DLQ route reported topic-not-found. Actual DLQ resend/CSV and Trace
  round trips remain unverified; receipt identity, failure retention, Key/client
  parameter handling, and export behavior are covered by focused tests.
- Topic/Consumer modal interactions were not exhaustively clicked. Their real
  service/IPC mutations were exercised above, with focused receipt and navigation
  tests covering result branches. This is not a complete visual or cross-platform
  packaging certification.

These limits are not counted as successful live coverage. They do not add SQL
server backends, notifications, distributed leases, or HTTP deployment to the
desktop scope.

## Operating references

- [Local Rust cluster](../deploy/dev/README.md) and [isolated ACL fixture](../deploy/dev/acl/README.md)
- [History](HISTORY.md), [Monitor rules](MONITOR_RULES.md), [storage diagnostics](STORAGE_DIAGNOSTICS.md)
- [Backup and restore](STORAGE_OPERATIONS.md)

Databases, snapshots, credentials, screenshots, logs, and build outputs are local
validation artifacts and are not included in this change.
