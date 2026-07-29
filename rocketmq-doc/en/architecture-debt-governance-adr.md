# Architecture debt governance decision

- Status: Accepted
- Date: 2026-07-29
- Decision owners: RocketMQ Rust maintainers

## Context

Architecture debt was recorded in several baselines, source allowlists, and
historical migration documents. Their release names and removal windows did
not agree, and some tests attempted to preserve packages and re-export paths
that no longer exist.

## Decision

`scripts/architecture-debt-registry.json` is the single index for active
architecture debt. An active entry must name an owner, reason, accepted
decision, removal condition, release boundary, and executable evidence.
Detailed identities may remain in their specialist machine-readable baseline,
but that baseline must be referenced by a registry entry and remain governed
by its non-growth guard.

The only architecture-debt release boundary is `2.0.0`. Historical milestone
names and open-ended values such as `long-term` or `next-major` are not valid
removal windows.

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

- CI fails when a registry entry loses ownership or evidence, an ADR target is
  missing, a resolved source check regresses, or the generated debt register
  drifts.
- The two remaining `rocketmq-store` composition dependencies cannot grow and
  must be removed by `2.0.0`.
- Panic, trait, allowlist, and runtime-adapter inventories remain
  non-growth baselines and are burned down under their specialist guards.
- Public API baselines may accept deliberate breaking cleanup for `2.0.0`;
  protocol and persisted-data compatibility still require explicit golden
  tests and review.
