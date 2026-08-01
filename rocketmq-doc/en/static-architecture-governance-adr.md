# Static Architecture Governance ADR

- Status: Accepted
- Date: 2026-07-31
- Owners: Architecture Governance, Client, Admin, and SRE maintainers

## Context

The live `pr_static` inventory contained 18 modules. Five modules failed for
independent reasons: release topology mixed root-workspace and standalone
projects, the PR tier required stale production-attestation hashes, accepted
error-hygiene debt referenced a missing evidence document, narrow Rust lint
allows lacked local intent, and the trait inventory treated approved native
async methods as debt while allowing empty classification traits to survive.

The MessageStore capability, public API intent, and module maintainability
contracts were already green after their owning migrations. Reopening those
baselines would have hidden regressions instead of addressing the five live
findings.

## Decision

1. Release discovery exposes three different sets: root workspace members,
   standalone Cargo projects, and the union of governance targets. A standalone
   project such as RocketMQ MCP is never represented as a root workspace member.
2. Pull requests use a lightweight candidate record containing the commit,
   execution environment, command status, and known failures. SHA, image digest,
   signing, promotion, soak, and disaster-recovery evidence remain dynamic
   release validation.
3. Accepted error-hygiene debt must link to repository evidence with an owner,
   rationale, and removal condition.
4. Narrow item-level lint exceptions with an inline Rust `reason` are governed
   at the code site. Crate-wide, module-wide, and unreasoned item exceptions
   remain in the central debt registry.
5. Native `async fn` in a statically dispatched trait is compliant. Macro-backed
   async traits and empty marker traits remain inventoried. The Lite Pull
   Consumer exposes direct native traits, and the SRE/Admin empty classifiers
   are removed because they provided no behavioral or authority guarantee.
6. The CI and local PR gate execute the same `pr_static` inventory. A guard may
   move to another tier only when its semantics are documented, as with the
   production-attestation SLO guard.

## Consequences

- The PR gate is deterministic and does not depend on mutable release hashes.
- Standalone projects remain visible to governance without corrupting Cargo
  workspace facts.
- Central lint debt decreases from 151 to 102 entries; no ceiling increases.
- Trait debt reports only actionable macro or empty-marker design choices.
- Heavy production proof remains available without blocking code-focused
  pre-GA development.

## Verification

The decision is accepted only when the 18-module `pr_static` tier passes twice
consecutively, the release/debt/lint/trait unit modules pass, and repository
format, Clippy, routing, runtime-boundary, error-hygiene, and diff checks report
their actual results.
