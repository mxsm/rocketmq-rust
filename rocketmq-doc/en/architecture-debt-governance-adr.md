# Architecture debt governance decision

- Status: Accepted
- Date: 2026-07-29
- Updated: 2026-09-08 (retire historical inventory and count gates)
- Decision owners: RocketMQ Rust maintainers

## Context

Architecture debt was recorded in several baselines, source allowlists, and
historical migration documents. Their release names and removal windows did
not agree, and some tests attempted to preserve packages and re-export paths
that no longer exist.

## Decision

`scripts/architecture-debt-registry.json` is the single index for active
architecture debt and resolved decisions. Entries record ownership, reasons,
removal conditions, release planning, and relevant evidence. The risk-test matrix
continues to map those records to maintained tests.

The central count-synchronization guard, Trait identity inventory, lint exception
inventory, and completed Store migration gate are retired. The debt register is
a review summary; updating historical counts or matching source tokens is not a
prerequisite for development. Existing specialist tools keep their own supported
inputs without duplicating their counts in the central registry.

The current architecture-debt planning boundary is `2.0.0`. Maintainers review
removal targets when the corresponding work changes.

Removed internal crates, facade re-exports, old module paths, and historical
migration evidence are not compatibility surfaces. They may be deleted or
refactored when doing so preserves implemented behavior and the real
compatibility boundaries:

- RocketMQ request and response codes;
- remoting and gRPC wire formats;
- persisted message, queue, index, and checkpoint layouts;
- observable broker, client, proxy, controller, and store behavior.

Compatibility tests therefore target canonical protocol, transport, ingress,
capability, and storage-layout boundaries. They must not recreate removed
packages merely to preserve source compatibility.

## Consequences

- Maintain risk ownership and relevant test evidence when changing a boundary.
  The evidence tooling checks risk-to-test mappings; there is no central
  exact-count or source-snapshot gate.
- The two remaining `rocketmq-store` composition dependencies cannot grow and
  must be removed by `2.0.0`.
- Runtime, unsafe, and typed-error tooling retains its relevant regression tests.
  Trait design and lint allowances are reviewed in source, with scoped compiler
  and Clippy validation instead of historical inventory maintenance.
- Public API baselines may accept deliberate breaking cleanup for `2.0.0`;
  protocol and persisted-data compatibility still require explicit golden
  tests and review.
