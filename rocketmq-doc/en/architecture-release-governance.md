# Architecture Release Governance

This document defines the maintained release boundary for the root Cargo
workspace. Cargo manifests and `cargo metadata` remain the source of truth;
the machine-readable release plan records only active resources.

## Release topology

The publish order in `scripts/architecture-release-plan.json` is a
topological order of the target dependency graph plus every unexpired,
exactly identified transition debt edge. A package is published only after
all internal dependencies required by the current transition state are
available at the same release version.

The baseline and transition dependency modes are required gates. Strict
target mode remains visible while the P2.1 and P2.2 ledger is non-empty and
becomes required as soon as that ledger reaches zero. A transition entry
must name one manifest edge, owner, reason, removal phase, and ISO deadline;
directory wildcards and permanent exceptions are not valid debt.

Long-term facade composition edges are recorded separately from transition
debt. Their manifest identities must exist exactly as recorded and do not
permit new callers, aliases, dependency kinds, or duplicate edges.

Invalid JSON, a missing design source, a missing manifest, an absent Cargo
section, or an unknown package is a structured release-guard failure. Such
input must never produce a Python traceback.
