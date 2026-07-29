# Architecture public API and compatibility evidence

This record binds the current workspace surface and compatibility commands to
the repository-owned guards. It replaces evidence that referenced crates and
paths removed by the architecture migration.

## Public API snapshot

- Scope: every current workspace library target (`library_targets=28`)
- Feature profile: default
- Snapshot comparison: `differences=0`
- Accepted source-level cleanup relative to the previous compatibility surface:
  - additive: 1 (`ClusterExecutionDiagnostics` and its client snapshot
    accessor)
  - deprecated: 0
  - breaking: 2
    - `ClientRuntime::new` removed; callers use fallible
      `ClientRuntime::try_new`
    - `ClusterConfig` gained mandatory bounded-execution fields for Rust
      struct literals; Serde configuration remains backward-readable through
      defaults

The package count is derived from `cargo metadata`; the guard rejects a
baseline that is missing a current library target or retains a removed one.

The repository owner explicitly approved removal of obsolete internal crates,
facades, module paths, and source-level contracts on 2026-07-29. That decision
is the compatibility classification authority for this change; it does not
waive protocol, wire, persisted-layout, or implemented-behavior compatibility.
The same authority explicitly approved the `ClusterConfig` source-shape change:
the obsolete fixed-lane execution contract is not retained.

## Compatibility matrix

- Result: `40/40`
- Feature profiles: `feature=24/24`
- Wire and ingress contracts: `wire=6/6`
- Storage layouts and engines: `storage=10/10`

The wire group is owned by the current canonical boundaries:
`rocketmq-protocol`, `rocketmq-transport`, `rocketmq-proxy`, and
`rocketmq-runtime`. Storage checks exercise current capability and component
contracts rather than old re-export facades. The matrix intentionally does not
recreate the removed `rocketmq-common`, `rocketmq-remoting`, or workspace
facade packages.
