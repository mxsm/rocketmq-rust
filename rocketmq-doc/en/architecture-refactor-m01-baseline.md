# Architecture Refactor M01 Dependency Baseline

## Purpose

This document freezes the repository facts consumed by the M01 dependency guard. It is a migration baseline, not
the desired architecture. Existing exceptions may only be removed or narrowed. Adding an exception requires an
architecture decision, an owner, and a removal milestone.

## M01 Entry Baseline

| Fact | Frozen value |
|---|---|
| Git commit | `ea04025d397e87f3364211c4aa997982658e58a7` |
| Root workspace packages | 22 |
| Target workspace packages | 32 |
| Planned packages | 10 |
| Root workspace Client consumers | 5 |
| Standalone Client consumers | 3 |

The original 22 root workspace packages and ten planned additions form the immutable M01 entry baseline. The
current governed snapshot is recorded in `scripts/architecture-dependency-baseline.json`. The original additions
were:

- `rocketmq-model`
- `rocketmq-protocol`
- `rocketmq-transport`
- `rocketmq-security-api`
- `rocketmq-store-api`
- `rocketmq-store-local`
- `rocketmq-store-rocksdb`
- `rocketmq-proxy-core`
- `rocketmq-proxy-cluster`
- `rocketmq-proxy-local`

The five current workspace consumers of `rocketmq-client-rust` are Broker, NameServer, Proxy, Admin Core, and
MCP. The three standalone consumers are `rocketmq-example`, the Tauri backend, and the Web Dashboard backend.
The root `[workspace.dependencies]` declaration and the Client package itself are declarations, not consumers.
All four standalone Cargo roots are scanned; GPUI is the fourth root and is currently not a Client consumer.

The M01 entry baseline also recorded one existing package cycle:

```text
rocketmq-common -> rocketmq-rust -> rocketmq-observability -> rocketmq-common
```

The Phase 1 observability migration removed that cycle; the current baseline no longer carries the exception.

## Phase 1 Closeout Candidate

| Fact | Candidate value |
|---|---|
| Source parent | `70ec1a177b17be3dfa61af50e29337711a0c58e1` |
| Root workspace packages | 24 |
| Target workspace packages | 32 |
| Remaining planned packages | 8 |
| Normalized metadata SHA-256 | `846d01c77db84a4374c378c17c47574b621e86753a5acefd476720947ca1c4b1` |
| Delivery topology | One approved Phase 1 issue/branch/PR containing M01–M03 slices |

`rocketmq-model` and `rocketmq-security-api` are now real workspace packages. The eight remaining planned
packages are `rocketmq-protocol`, `rocketmq-transport`, `rocketmq-store-api`, `rocketmq-store-local`,
`rocketmq-store-rocksdb`, `rocketmq-proxy-core`, `rocketmq-proxy-cluster`, and `rocketmq-proxy-local`.

## Policy and Baseline Files

- `scripts/architecture-dependency-policy.json` describes the versioned roots, current 24/32 package counts, planned
  packages, all 32 target DAG entries, direct and transitive forbidden edges, Client policy, facade ledger, and
  milestone order. The facade ledger is executable: canonical owners cannot depend back on their facade.
- `scripts/architecture-dependency-baseline.json` records the commit, workspace package names, and reviewed
  manifest/source exceptions. Client manifest entries freeze caller, target, dependency kind, normalized path,
  alias, and count. Source entries freeze a concrete relative file, alias, and occurrence count. Every exception
  has an owner and `remove_by` milestone; directory-wide source exceptions are invalid.
- The independent compatibility manifest ledger freezes every current edge into `rocketmq-common`,
  `rocketmq-remoting`, `rocketmq-store`, or `rocketmq-rust`, plus the reviewed R0 Common-to-Protocol and
  Common-to-Observability edges at count zero. It includes root-workspace metadata and all workspace/path
  dependencies from the four standalone Cargo roots. Entries include an owner, reason, removal milestone, and ADR.
- `scripts/architecture_dependency_guard.py` checks normal, build, and dev dependencies, cycles, forbidden target
  edges and reachability, Client manifest dependencies, and Rust source imports including renamed Client
  dependencies. Aliases are scoped to their manifest caller. A Rust-aware lexer excludes nested comments,
  strings, byte strings, raw strings, byte raw strings, and character literals without confusing lifetimes.
  Unterminated lexical input fails closed.

The baseline file is intentionally hand-reviewed. The guard never regenerates or expands it. A normal migration
removes entries; an expansion needs an ADR and Human Architect approval.

## Commands and Exit Contract

Run from the repository root:

```powershell
python scripts/architecture_dependency_guard.py --mode baseline
python scripts/architecture_dependency_guard.py --mode target --allow-missing-planned-crates
python scripts/architecture_dependency_guard.py --fixtures
```

Exit codes are stable:

| Exit code | Meaning |
|---|---|
| `0` | The selected mode is complete and compliant. |
| `1` | One or more architecture violations were found, or target mode is explicitly incomplete. |
| `2` | Policy, baseline, metadata, source root, output, or CLI input is invalid. |

Target mode without `--allow-missing-planned-crates` is an input error until all eight remaining planned packages exist.
With the flag, missing planned packages are reported as `TARGET_INCOMPLETE`, JSON status is `incomplete`, and the
command exits 1 even when there are no other findings. It never prints a successful target result while packages
are missing. M09 removes this flag from the final acceptance command.

Baseline mode accepts gradual addition of only the eight remaining planned packages and applies their target rules
immediately. It rejects any unplanned workspace package. Existing manifest/source occurrences may decrease;
adding an edge, changing its kind/path/alias, moving an import, or increasing an occurrence count is rejected.

For reproducible or synthetic evaluation, pass pre-generated metadata and an alternate source tree:

```powershell
python scripts/architecture_dependency_guard.py `
  --mode target `
  --metadata-file target/architecture-refactor/M01/metadata.json `
  --source-root . `
  --output target/architecture-refactor/M01/dependency-report.json `
  --allow-missing-planned-crates
```

Each violation reports `rule`, `caller`, `target`, `path`, and dependency `kind`. The fixture suite proves a clean
case and rejects cycles, Protocol-to-Transport, Store-API-to-backend, Proxy-Local-to-Client, and
foundation-to-facade edges. Unit tests additionally prove renamed Client source imports are detected.

The reproducibility record contains the exact Cargo metadata command, normalized metadata SHA-256, Cargo and
Rust compiler versions, and `generated_output_path`. Baseline mode writes the normalized, non-versioned metadata
evidence under `target/architecture-refactor/M01/`, reloads it, and verifies its SHA-256. Absolute manifest paths
are normalized to repository-relative forward-slash paths before hashing or comparison.

Direct normal/build/dev edges are diagnosed with their own `kind`. Production reachability uses only normal
edges and reports `graph=normal`; dev or build dependencies never contaminate the production closure.

## Updating the Baseline

1. Confirm the change removes or narrows an existing exception.
2. Update the relevant owner and removal milestone only when an approved ADR changes responsibility.
3. Run unit tests, built-in fixtures, baseline mode, and target mode.
4. Review the JSON diff manually; never copy guard findings into the baseline automatically.
5. Store generated metadata and reports under `target/architecture-refactor/`; do not commit them.

## ArcMut Usage Baseline

`scripts/arc-mut-baseline.json` freezes 3,452 independently identified occurrences in 1,277 governed ledger
entries. An entry groups only the same path, symbol, finding kind, and category; its `occurrences` array retains
an independent semantic ID, token-context fingerprint, enclosing item, and evidence line for every occurrence.
The line is diagnostic only and is deliberately excluded from identity, so inserting blank lines cannot create
baseline churn. Moving a usage to another file or item, renaming an alias, changing its context, or adding an
identical duplicate produces a new occurrence instead of inheriting an exemption.

| Category | Occurrences |
|---|---:|
| Production | 2,317 |
| Test | 1,095 |
| Compatibility | 40 |
| **Total** | **3,452** |

Compatibility is intentionally exact: only `rocketmq/src/arc_mut.rs` and the public re-exports in
`rocketmq/src/lib.rs` are compatibility surfaces. Path components named `tests`, `benches`, or `examples` and
inline `#[cfg(test)]` items are classified as tests; the standalone `rocketmq-example/src` tree remains
production. Everything else is production.

| Owner | Occurrences | Removal milestone |
|---|---:|---|
| Auth | 7 | M08 |
| Broker | 1,049 | M05 |
| Client | 878 | M05 |
| Common | 10 | M05 |
| Controller | 228 | M06 |
| NameServer | 124 | M06 |
| Proxy | 22 | M08 |
| Remoting | 233 | M02 |
| Runtime foundation | 51 | M02 |
| Store | 823 | M02 |
| Tools | 27 | M08 |

### Phase 1 ArcMut Closeout

The frozen M01 entry inventory above is retained for auditability. After the M02 safe ownership slices and M03
foundation migration, the governed closeout baseline is:

| Owner | Identities | Occurrences | Removal milestone |
|---|---:|---:|---|
| Auth | 6 | 7 | M08 |
| Broker | 410 | 1,049 | M05 |
| Client | 223 | 878 | M05 |
| Common | 9 | 10 | M05 |
| Controller | 109 | 228 | M06 |
| NameServer | 62 | 124 | M06 |
| Proxy | 7 | 22 | M08 |
| Remoting | 121 | 220 | M11 |
| Runtime foundation | 16 | 48 | M11 |
| Store | 288 | 817 | M11 |
| Tools | 15 | 27 | M08 |
| **Total** | **1,266** | **3,430** | |

The closeout is a strict reduction from the frozen M01 entry (1,277 identities and 3,452 occurrences) and
contains no new identity or occurrence. Twenty-seven existing occurrences changed token-context fingerprints
while remaining in the same governed item; their one-to-one review record is
`scripts/arc-mut-relocation-approvals.json`. Additional occurrences discovered during review were removed or
folded into existing governed test items rather than approved.

The initial M02 deadline for every Remoting, Runtime Foundation, and Store entry contradicted M02's first-slice
scope and its explicit facade compatibility requirement. ADR-013 corrects their finite ultimate deadline to M11,
the Phase 3 gate that requires ArcMut to leave production and public compatibility APIs. M05 and M06 still own
the canonical Transport and Store burn-down; the corrected deadline cannot be used to add or move debt.

Every ledger entry cites `ADR-002` or the reviewed `ADR-013` correction, has a concrete owner, explains the legacy
shared-mutation debt, and has a finite `remove_by` milestone. The scanner recognizes `ArcMut`, `WeakArcMut`, and
`SyncUnsafeCellWrapper`; builds a repository-wide, crate/module-scoped symbol graph for `use`, grouped/nested
use trees, `pub use`, and `type` aliases and follows it to a fixed point; distinguishes
imports, re-exports, aliases, constructors, and type references; and reports `mut_from_ref` definitions/calls,
dangerous `AsMut`/`DerefMut` implementations, and named/tuple shared-cell wrappers with an `unsafe impl Sync` or
a safe `&self -> &mut` escape. Unit structs and ordinary local `UnsafeCell` fields are not classified. Rust comments,
normal/byte/raw strings, character literals, and lifetimes are tokenized rather than searched as text.
Malformed lexical input exits 2 instead of silently skipping a file.

Run the guard and its 24-case built-in fixture matrix from the repository root:

```powershell
python scripts/arc_mut_guard.py
python scripts/arc_mut_guard.py --fixtures
python scripts/arc_mut_guard.py `
  --baseline scripts/arc-mut-baseline.json `
  --compare-baseline scripts/arc-mut-baseline.json
```

The default comparison rejects new, stale, moved, changed, and expired debt. A migration must first prove the
proposed baseline is a strict subset (or advances a removal deadline) with `--compare-baseline`; ordinary changes
cannot add occurrences, change governance metadata, or extend a deadline. Baseline promotion applies the same
identity and occurrence checks. A token-context relocation requires `--relocation-approvals` with a reviewed,
one-to-one ADR record; unused approvals fail closed. `--bootstrap` writes only below `target/` and deliberately
emits `UNASSIGNED` review placeholders, so it cannot overwrite the governed baseline.

The increase from the earlier 2,624-candidate inventory to 3,035 is intentional: the original scanner stopped at
file boundaries. The governed inventory now includes propagated uses of aliases and re-exports such as
`ConnectionHandlerContext`, `ArcConsumeQueue`, and wrappers exposing shared mutation from Local/Rocks message
stores, mapped files, and RPC responses. It is a scope correction, not newly introduced source debt.

Scanner limitation: this is a fail-closed lexical policy guard, not rustc or a whole-program type resolver.
Module resolution covers checked-in Cargo package/lib names, `crate`/`self`/`super`, grouped and nested use trees,
qualified paths, Rust visibility (`private`, `pub`, `pub(crate)`, and `pub(super)`), inline module scopes, and
chained glob imports/re-exports. Procedural macro expansion and dependencies outside checked-in Rust source are not visible.
Custom shared-`UnsafeCell` analysis is brace-aware but requires the struct and corresponding impl to be in the
same file, and `mut_from_ref` is conservatively reported regardless of receiver type. These limitations may
cause reviewable false positives, but they do not permit comments, strings, simple aliases, re-exports, file
moves, or symbol renames to hide existing debt.

The visibility/glob refinement raised the reviewed inventory from 3,035 to 3,163. The additional 128 test
occurrences are explicit `use super::*`/module glob imports that make a governed symbol visible. Private aliases
now remain confined to their defining module and descendants, safe sibling declarations shadow hazardous parent
names, and glob chains propagate only names visible from their source module. No existing removal milestone was
extended during this baseline regeneration. Milestone validation accepts the complete M01–M12 plan range;
ordinary baseline comparison still rejects extending an existing entry to a later milestone.

The module-identity wrapper refinement raised the inventory from 3,163 to 3,452. The 289 additional governed
occurrences are propagated identities of the five already detected shared-cell wrappers, primarily
`LocalFileMessageStore` (+112), `DefaultMappedFile` (+104), `RpcResponse` (+26), and
`RocksDBMessageStore` (+23), plus their aliases, imports, and glob visibility. Wrapper evidence now follows
visible type-alias chains and qualified paths back to the exact struct identity. Two prior sibling-scope false
positives were also removed (one Client and one Controller occurrence). Inline modules with the same type name
cannot exchange evidence, and an explicit safe declaration shadows a hazardous glob import. This is another
scanner coverage correction over unchanged Rust source; no removal deadline was extended.

Repository analysis is ordered deliberately: all normal alias edges and visible glob imports/re-exports reach a
fixed point first; shared-cell `unsafe impl Sync` and safe `&self -> &mut` evidence is then resolved over that
completed graph; finally the hazard set reaches its own fixed point. This ordering detects wrappers imported by
direct or chained globs without treating an ordinary globbed `UnsafeCell` type as hazardous in the absence of
shared-mutation evidence. The ordering refinement did not change the reviewed 3,452-occurrence inventory.

## Compatibility Baseline Index

M01 does not change runtime behavior or a compatibility surface. It freezes the following surfaces so later
milestones must attach a focused diff or golden result instead of relying on source review alone:

| Surface | Canonical evidence | Required migration proof |
|---|---|---|
| Public Rust paths and type identity | Compatibility manifest ledger plus crate-owned API tests | Canonical/legacy path compilation and API diff |
| Request/response codes and headers | Existing remoting request-code and serialization tests | Golden and differential codec corpus |
| Serde configuration envelopes | Existing config tests in each owning service | Old/new round trip and unknown-field policy tests |
| CommitLog, ConsumeQueue, and Index formats | Existing store recovery and RocksDB foundation suites | Dirty-tail, flush, HA, 20-byte record, and backend parity fixtures |
| Cargo default and optional features | Root and standalone manifests captured by the dependency baseline | Exact changed feature checks in addition to `--all-features` |
| Runtime ownership and shutdown | `scripts/runtime-audit-baseline.json` and the enforcing runtime audit | Deterministic task, cancellation, deadline, and drain tests |
| Error mapping and redaction | Error architecture guard and hygiene scripts | Typed mapping, severity, retry, and sensitive-output tests |

The compatibility manifest ledger in `scripts/architecture-dependency-baseline.json` is executable and
monotonic. A later milestone may remove a legacy edge or occurrence, but it cannot add one without an ADR,
owner, removal milestone, and Human Architect approval.

## Performance Baseline Contract

Performance evidence is generated below `target/architecture-refactor/M01/<run-id>/` and is not committed. Every
record must include the Git commit, toolchain, OS/kernel, CPU, memory, filesystem, Cargo profile, enabled
features, message size, queue/topic count, connection count, TLS ratio, duration, warm-up, and sampling method.
The normalized result schema records throughput, p50/p99/p999 latency, RSS, allocation rate, CPU, disk/network
bytes, and workload-specific I/O amplification.

For the same frozen profile, a later change fails the default regression gate when throughput, p99 latency, or
RSS degrades by more than 5% without an approved architecture exception. Absolute scale claims, including the
mostly-idle one-million-connection target, require their own named hardware profile and resource budget; they
cannot be inferred from the general 5% regression threshold. M10 owns the executable performance comparison
guard and the production benchmark/soak results.

## CI Enforcement

`.github/workflows/rocketmq-rust-ci.yaml` runs the architecture guard suite on both Ubuntu and Windows. The job
executes all dependency/ArcMut unit tests, built-in positive and negative fixtures, dependency baseline mode,
and the shared-mutation baseline. Target mode remains intentionally incomplete until M09 creates all ten
planned packages and removes the reviewed compatibility debt; it is not reported as a successful CI gate in
M01.
