---
title: "Release engineering"
---

The repository defines a community distribution, a core package scope and deployment assets. This page explains how those pieces fit together and how documentation should describe them. It does not announce a published release or execute publication. Start with [release scope](../overview/release-scope.md) for product identity and exclusions.

## Follow the declared scope

The root workspace contains 28 members. `scripts/core-release-scope.json` classifies 27 core packages: 24 registry-publish packages and three binary-only packages. The Dashboard common member is not part of that core package inventory. Registry classification and executable service count are separate concepts. Dashboard, MCP and SRE are independent products; their own source and deployment documentation apply.

The identity is `RocketMQ Rust Community Distribution` with `unofficial-community` identity and `official_apache_release: false`. Use that identity consistently in release notes and downloads. Apache 2.0 licensing does not make an artifact an official Apache project release.

## Package registry crates with Cargo

The repository has no separate candidate-preparation, archive or crate staging tooling; registry packages are prepared with Cargo directly. To check that the registry-publish packages can be packaged, run from the repository root:

```bash
cargo package --workspace --locked --no-verify --exclude rocketmq-admin-cli --exclude rocketmq-admin-tui --exclude rocketmq-store-inspect --exclude rocketmq-dashboard-common
```

The excluded packages are the three binary-only packages and the Dashboard common member. A packaging result checks each package boundary; it is not a successful `cargo publish`. Test-only sibling dev-dependencies are declared path-only, so Cargo drops them from the published manifests and publication order depends only on normal and build dependencies.

## Maintain release and website documentation together

1. **Choose the documented surface.** Identify source-based setup, registry packages or a standalone product. Keep the version and feature assumptions explicit.
2. **Describe actual changes.** Cover behavior, configuration defaults, public APIs, wire/storage compatibility and migration effects. Reuse [Java migration](../migration/java-to-rust.md), [Rust API migration](../migration/rust-api.md) and [upgrade/rollback](../operations/upgrade-rollback.md) for those boundaries.
3. **Separate capabilities from observations.** A registered command, declared target or matrix scenario is an implementation fact. A successful runtime or recovery result needs an actual observation and its scope.
4. **Update both languages.** Keep page IDs, commands, configuration keys, units and diagram semantics aligned. 1.0.0 development describes the current development documentation; do not silently relabel it as a released version.
5. **Refresh installation links when artifacts exist.** Point to the actual artifact and its instructions, then check the affected website routes and examples. Avoid speculative download URLs or blanket production-readiness claims.

Documentation work requires no fingerprint ceremony, fixed checkout, new approval stage or additional CI gate.

## Source references

- [Core package scope](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/core-release-scope.json) and [release identity](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/release-identity.json).
