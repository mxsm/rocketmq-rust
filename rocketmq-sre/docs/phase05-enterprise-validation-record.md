# Phase 5 Enterprise Validation Record

Date: 2026-07-29  
Result: Passed  
Evidence schema: `rocketmq-sre.phase05-enterprise-smoke.v1`

## Scope

The validation used Docker PostgreSQL and the dedicated
`rocketmq-sre-phase00` Kind cluster. It exercised logical multi-region
isolation, a 100-cluster Fleet, representative enterprise integrations,
current/N-1 component compatibility, a two-cluster Fleet release, Control
Plane database restore, and a supervised RocketMQ test-cluster rebuild.

Run the complete scenario from the repository worktree:

```powershell
.\rocketmq-sre\scripts\phase05-enterprise-smoke.ps1
```

The machine-readable, redacted result is written outside the repository to:

```text
D:\BuildCache\rocketmq-sre-temp\phase05-enterprise-smoke.json
```

No token, secret, database password, private key, ACL material, message body,
or full service configuration is included in the result.

## Results

| Scenario | Result | Evidence |
| --- | --- | --- |
| Fleet scale | Passed | 100 clusters, four 25-item pages, no duplicates, worst health visible, inspection concurrency 8, quota backpressure enforced |
| Multi-region | Passed | Two logical regions, cluster allowlist isolation, residency filtering, local-region disconnect degradation |
| Runtime compatibility | Passed | Connector, Execution Agent, and MCP: current is full, N-1 is read-only degraded, incompatible protocol/capability is denied |
| Enterprise integration | Passed | ITSM, ChatOps, Pager, CMDB, GitOps, and CI/CD; idempotency and stale outbox claim recovery verified |
| Fleet release | Passed | Two regions and two targets; one readiness denial, out-of-order batch denial, canary regression, pause, and rollback |
| Control Plane restore | Passed | PostgreSQL custom-format backup restored into an isolated database; all sampled counts matched; current Control Plane health and readiness succeeded |
| RocketMQ test-cluster DR | Passed | Broker Pod was replaced, deterministic configuration/topic rebuild completed, pre/post synthetic probe both reached 10/10/10 |

## Control Plane restore sample

The final run restored the following source projection without count drift:

| Projection | Source | Restored |
| --- | ---: | ---: |
| SQLx migrations | 47 | 47 |
| Clusters | 437 | 437 |
| Incidents | 311 | 311 |
| Fleet releases | 3 | 3 |

The restore database was ephemeral and was dropped after validation. The
restored Control Plane returned `healthy` and `ready=true` before cleanup.

## RocketMQ recovery boundary

The Kind Broker uses an ephemeral test volume. The exercise therefore proves
deterministic Broker and synthetic-topic rebuild plus post-recovery data-plane
operation; it does not claim restoration of historical message data. The
evidence explicitly records `message_history_restore_claimed=false`.

One observed run required a single bounded-probe retry immediately after the
Broker became Ready. The retry succeeded with 10 sent, 10 received, and 10
acknowledged messages. This is treated as a startup stabilization observation,
not hidden as a first-attempt success.

Production message-history RPO requires persistent Broker storage, replicated
commit logs, and a separately verified backup/restore policy outside this Kind
test exercise.

## Cleanup verification

- The isolated restore database and dump were removed.
- The temporary restored Control Plane process was stopped.
- The Kind Broker StatefulSet returned to one Ready replica.
- The AI SRE Control Plane remained at one Ready replica.
- Generated evidence and Cargo output remained on `G:`.

## Final repository validation

The final source revision was also checked independently from the scenario
smoke:

| Validation | Result |
| --- | --- |
| Rust 2024 source layout | Passed, 0 `mod.rs` files |
| Rust formatting | Passed for all 11 SRE crates |
| Rust Clippy | Passed for workspace, all targets, all features, `-D warnings` |
| Rust tests | Passed for workspace and all features; external PostgreSQL/Kind scenarios were exercised separately by the enterprise smoke |
| OpenAPI generation and TypeScript drift check | Passed |
| UI lint | Passed with zero warnings |
| UI tests | 26 files and 71 tests passed |
| UI production build | Passed |
| Runtime ownership audit | Passed |
| AGENTS routing drift control | Passed |
| `git diff --check` | Passed |

On Windows, the single `cargo fmt --all -- --check` invocation exceeds the OS
command-line/path length limit (`os error 206`). The equivalent per-package
format check was therefore run for every SRE crate and all 11 passed. Cargo
home, target output, and temporary compilation files were kept on `G:`.
