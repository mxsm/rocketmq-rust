---
title: "Release scope and distribution identity"
---

# Release scope and distribution identity

RocketMQ-Rust contains more products than one core release. Use the root Cargo manifest to identify workspace members, the core scope manifest to identify release classifications, and the selected artifact's release information to identify what was actually published. These are separate sources with different purposes.

## Source workspace, core release and standalone products

| Set | Current source definition | What membership means |
| --- | --- | --- |
| Root Cargo workspace | `Cargo.toml`, 28 members | Cargo can select these packages from the repository root. |
| Core release package set | `scripts/core-release-scope.json`, 27 packages | Packages classified for the core release process. This does not prove registry publication. |
| Core services | NameServer, Broker, Controller, Proxy | Service products named by the core release scope. They still have different build features, configuration and launch commands. |
| Dashboard common | A root workspace member, explicitly excluded from core release | Shared dashboard models/services can be built with the root workspace without making the desktop/Web products part of that release. |
| Independent Cargo products | Examples, GPUI, Tauri backend, Web backend, MCP, MCP Control, SRE and specialized fixtures/fuzzing | Use each local manifest and guide; a root build does not cover them. |
| Node projects | Website, Dashboard frontends, SRE UI and TypeScript SDK | Each has its own package manifest and commands. |

The scope lists repository exclusions for Dashboard, MCP and SRE. MCP Control is also a separate project and is absent from the core package list; absence does not mean it inherits the core release's status. The [module map](../architecture/module-map.md) and [ecosystem overview](../ecosystem/overview.md) identify these owners.

## Read package classifications literally

| Classification | Meaning in the release model | Current examples |
| --- | --- | --- |
| `registry-publish` | Selected for registry package planning | Client, model, protocol, transport, runtime, security, storage, service libraries and Admin Core |
| `binary-only` | Distributed as a binary product rather than a registry library in this classification | `rocketmq-admin-cli`, `rocketmq-admin-tui`, `rocketmq-store-inspect` |
| `internal-only` | Allowed schema classification for internal packages | No current core entry uses it |
| `non-publish` | Allowed schema classification for packages outside publication | No current core entry uses it |

The current core list has 24 `registry-publish` and three `binary-only` entries. A service package may include a library and a binary; its classification does not remove its executable. Conversely, a package version in Cargo.toml does not tell you whether a downloadable archive or registry version exists.

## Distribution identity

The checked-in `distribution/release-identity.json` declares:

| Field | Declared value |
| --- | --- |
| Distribution name | RocketMQ Rust Community Distribution |
| Identity kind | `unofficial-community` |
| Official Apache release | `false` |
| Source project | `mxsm/rocketmq-rust` |
| Registry owner / package prefix | `mxsm` / `rocketmq-` on crates.io |
| OCI namespace | `ghcr.io/mxsm/rocketmq-rust` |
| Helm chart name | `rocketmq-rust` |
| License identifier | `Apache-2.0` |

These are declared release destinations and identity metadata, not a statement that every image, chart or package is currently available. “Official project documentation” refers to documentation maintained for this project; it must not be read as an official Apache Software Foundation release designation. The identity file explicitly identifies an unofficial community distribution.

Use artifact-specific documentation for [containers](../deployment/containers.md) and [Kubernetes](../deployment/kubernetes.md). Keep the image tag, chart values, service binary and corresponding documentation together; do not substitute an unverified “latest” artifact for a named release.

## Version numbers and the 1.0.0 development documentation

The root workspace currently declares version `1.0.0`, while independent applications can declare their own versions, such as `0.1.0`. These are source manifest values. They do not establish a publication date, support period, or cross-product release lockstep.

The Docusaurus current documentation is labeled **1.0.0 development** and uses English plus `zh-CN` content. 1.0.0 development describes the current source family and can contain behavior newer than a published artifact. A Chinese page is a full counterpart with the same page ID, not a separate product version.

For an incident or compatibility question, record the actual executable/package version, build features, deployment mode and relevant configuration. Then compare the matching source/API documentation. The [capability matrix](./capability-matrix.md) and [compatibility reference](../reference/protocol-compatibility.md) explain why a version string alone is insufficient.

## Select the right build and delivery unit

1. Find the owning package/application and its manifest.
2. Decide whether the deliverable is a library, service binary, desktop bundle, frontend assets, chart or container.
3. Use that product's build entry and supported feature/platform combination.
4. Associate the result with its matching configuration, operating procedure and known limitations.

For example, building the Tauri React frontend does not produce a desktop installer; building the root workspace does not build the Web Dashboard backend. Publishing core messaging packages does not publish MCP or establish its HTTP authentication configuration.

This page is a source-scope reference. It does not announce a new release or assert that release workflows have run.

Sources: [workspace manifest](https://github.com/mxsm/rocketmq-rust/blob/main/Cargo.toml), [core release scope](https://github.com/mxsm/rocketmq-rust/blob/main/scripts/core-release-scope.json), [distribution identity](https://github.com/mxsm/rocketmq-rust/blob/main/distribution/release-identity.json), [website version configuration](https://github.com/mxsm/rocketmq-rust/blob/main/rocketmq-website/docusaurus.config.ts).
