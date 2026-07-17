# M09-05 R0 compatibility release notes

## Release intent

R0 is the first compatibility release after the Phase 2 gate. It publishes the ten canonical boundary crates while
keeping the current public paths, default features, wire formats, storage formats, service behavior, and deployment
modes available. The package version of every workspace crate remains lockstep.

**No behavior change / no behavior change:** R0 is an ownership and package-boundary release. It does not authorize a
behavior change, a public-item removal, a default-feature change, a wire/storage migration, or early activation of the
Proxy mode features.

## Release topology

The conceptual chain approved by the architecture design is:

`model/error/runtime/security/store-api → protocol/observability → transport/auth/local/tiered → rocks → facade → service/tool`

The executable order is recorded in `scripts/architecture-release-plan.json`. It refines the conceptual chain by
publishing `rocketmq-error`, `rocketmq-macros`, and `rocketmq-runtime` before packages that compile against them, and it
checks all 32 packages against `scripts/architecture-dependency-policy.json`. A facade or service/tool release must not
reference a canonical owner version that has not been published.

## Ten new canonical crates

| Canonical package/import | Canonical responsibility | R0 deprecated compatibility owner |
|---|---|---|
| `rocketmq-model` / `rocketmq_model` | model and message DTOs | matching `rocketmq-common` model/message paths |
| `rocketmq-protocol` / `rocketmq_protocol` | protocol headers, codes, codecs, and wire contracts | matching `rocketmq-remoting` protocol and `rocketmq-common` boundary paths |
| `rocketmq-transport` / `rocketmq_transport` | network transport contracts and implementation | matching `rocketmq-remoting` transport paths |
| `rocketmq-security-api` / `rocketmq_security_api` | security-neutral contracts and principals | matching Common/Remoting security contract paths |
| `rocketmq-store-api` / `rocketmq_store_api` | public store contracts | matching `rocketmq-store` contract paths |
| `rocketmq-store-local` / `rocketmq_store_local` | local-file store implementation | matching `rocketmq-store` local implementation paths |
| `rocketmq-store-rocksdb` / `rocketmq_store_rocksdb` | RocksDB store implementation | matching `rocketmq-store` RocksDB paths |
| `rocketmq-proxy-core` / `rocketmq_proxy_core` | mode-neutral Proxy contracts | matching `rocketmq-proxy` core paths |
| `rocketmq-proxy-cluster` / `rocketmq_proxy_cluster` | remote-cluster adapter and client lifecycle | matching `rocketmq-proxy` cluster paths |
| `rocketmq-proxy-local` / `rocketmq_proxy_local` | embedded Broker/Store adapter | matching `rocketmq-proxy` local paths |

“Deprecated” identifies a migration path, not an R0 deletion. Existing compatibility owners continue forwarding or
composing canonical owners. Consumers should use the canonical import in new code and migrate existing imports during
R1.

## Compatibility guarantees

- Public API: canonical and legacy compile fixtures remain green; no unapproved breaking diff is allowed.
- Features: existing default and no-default behavior remains unchanged; the next-major Proxy fixture is documentation,
  not the active `rocketmq-proxy/Cargo.toml` feature set.
- Wire/storage: JSON/binary protocol goldens, Serde names/defaults, CommitLog/CQ/Index and RocksDB semantics remain
  unchanged.
- Runtime: no new detached background work or blocking boundary is introduced by this release package.

## Rollback

If R0 release validation fails, stop the facade/service publication, withdraw the affected version, and republish the
previous lockstep workspace version. Keep both canonical crates and deprecated forwarding paths in source while the
failure is corrected. The rollback must not delete persisted data, rewrite wire/storage formats, or reactivate copied
implementations inside compatibility facades.

## R0 checklist

- [x] Ten new crates, canonical imports, and deprecated compatibility owners are listed.
- [x] The exact 32-package publication order is machine-readable and dependency-checked.
- [x] The no behavior change statement covers API, features, wire, storage, and runtime behavior.
- [x] The rollback preserves compatibility paths and persisted/wire data.
- [x] Early next-major deletion and Proxy mode activation are explicitly forbidden.
