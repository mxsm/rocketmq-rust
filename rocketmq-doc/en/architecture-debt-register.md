# Architecture debt register

Review index for `scripts/architecture-debt-registry.json`. Update this summary when
ownership, status, or evidence changes. Historical counts and source-token snapshots are
not development gates. Trait and lint inventory freezes have been retired; the trait
design guide, scoped Clippy checks, and source-level review remain applicable.

Release planning boundary: `2.0.0`.

| ID | Class | Owner | Status | Removal condition | Evidence |
|---|---|---|---|---|---|
| `ARC-ALLOW-001` | `allow` | `architecture-maintainers` | `active` | typed error paths replace the registered error boundary exceptions | `scripts/error_architecture_guard.py`<br>`rocketmq-doc/en/error-hygiene-allowlist.md`<br>`scripts/tests/test_error_architecture_guard.py` |
| `ARC-COMP-001` | `compatibility` | `broker` | `active` | broker production paths depend only on store capabilities and selected backends | `scripts/architecture_dependency_guard.py`<br>`rocketmq-doc/en/message-store-capability-migration.md`<br>`rocketmq-store/tests/capability_conformance_tests.rs` |
| `ARC-COMP-002` | `compatibility` | `store-tools` | `active` | store inspection uses store-api and backend-specific inspection capabilities | `scripts/architecture_dependency_guard.py` |
| `ARC-FACADE-001` | `facade` | `store` | `active` | no production consumer requires aggregate MessageStore or store re-exports | `scripts/architecture-dependency-policy.json`<br>`rocketmq-doc/en/message-store-capability-migration-adr.md`<br>`rocketmq-store/tests/capability_conformance_tests.rs` |
| `ARC-FACADE-002` | `facade` | `proxy` | `active` | consumers use proxy-core and mode-specific packages without compatibility re-exports | `scripts/architecture-dependency-policy.json`<br>`rocketmq-proxy/Cargo.toml` |
| `ARC-PANIC-001` | `panic` | `architecture-maintainers` | `active` | production panic-surface inventory reaches zero | `scripts/rust_hygiene_guard.py` |
| `ARC-RUNTIME-001` | `runtime_adapter` | `runtime-maintainers` | `active` | runtime work remains owned, bounded, cancellable, and covered by shutdown tests | `scripts/runtime-audit.ps1`<br>`scripts/rust_hygiene_guard.py` |
| `ARC-RUNTIME-002` | `runtime_adapter` | `runtime-maintainers` | `resolved` | the ClientRuntime::new API remains absent | `rocketmq-client/src/runtime.rs` |
| `ARC-RUNTIME-003` | `runtime_adapter` | `proxy` | `resolved` | fixed Cluster lane count and hash-routing helpers remain absent | `rocketmq-proxy-cluster/src/cluster_admission.rs`<br>`rocketmq-proxy-cluster/src/cluster_behavior_tests.rs` |
| `ARC-RUNTIME-004` | `runtime_adapter` | `broker` | `resolved` | the transaction service continues to own the bridge directly and keeps coordination locks narrow | `rocketmq-broker/src/transaction/queue/default_transactional_message_service.rs`<br>`rocketmq-broker/src/transaction/queue/transactional_message_bridge.rs` |
| `ARC-RUNTIME-005` | `runtime_adapter` | `broker` | `resolved` | Broker response paths do not restore process-wide wakeup writer shards | `rocketmq-broker/src/processor/pull_message_processor.rs`<br>`rocketmq-transport/tests/session_concurrency.rs` |
| `ARC-RUNTIME-006` | `runtime_adapter` | `dashboard` | `resolved` | the Dashboard lifecycle slot never owns a mutable AdminGuard across remote RPC awaits | `rocketmq-dashboard/rocketmq-dashboard-web/backend/src/admin/dashboard_admin_client.rs`<br>`rocketmq-tools/rocketmq-admin/rocketmq-admin-core/src/core/dashboard.rs` |
| `ARC-RUNTIME-007` | `runtime_adapter` | `runtime-maintainers` | `resolved` | fixed entrypoint blocking constants and the TUI unbounded action channel remain absent | `rocketmq-runtime/src/config.rs`<br>`rocketmq-tools/rocketmq-admin/rocketmq-admin-tui/src/rocketmq_tui_app.rs` |
| `ARC-UNSAFE-001` | `unsafe` | `architecture-maintainers` | `active` | unsafe operations are removed or remain covered by documented invariants and focused tests | `scripts/rust_hygiene_guard.py`<br>`scripts/tests/test_rust_hygiene_guard.py` |

Removed internal crates, facade re-exports, old module paths, and historical migration
evidence are not compatibility surfaces. Protocol, wire, persisted-layout, and implemented
behavior contracts remain covered by their maintained tests.
