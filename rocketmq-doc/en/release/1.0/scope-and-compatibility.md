# RocketMQ Rust 1.0 Scope and Compatibility

## Release objective

RocketMQ Rust 1.0 targets the user-visible core capabilities of Apache RocketMQ Java 5.5.0. Equivalent behavior does not require an identical internal implementation. A Rust-native implementation is acceptable when its externally observable behavior, durability, error handling, and operational recovery meet the same contract and have functional-system evidence.

The machine-readable source of truth is `scripts/v1-capability-manifest.json`. It assigns each capability a DRI, an independent reviewer, a release approver, a target phase, a target release candidate, dependencies, implementation status, and evidence status. `scripts/v1_capability_guard.py` rejects missing or inconsistent assignments.

## Supported core boundary

The core denominator contains the 27 packages classified by `scripts/core-release-scope.json`. It covers the nameserver, broker, client, remoting protocol, transport, authentication, stores, Proxy, Controller, and core Admin surfaces. Repository-global findings for excluded products remain visible, but they do not alter the core capability result.

The following products or implementations are explicitly outside the 1.0 denominator:

- OpenMessaging;
- BrokerContainer, including its two mqadmin commands;
- DLedger CommitLog;
- Java Controller internal DLedger and JRaft protocols, including request codes 1014 through 1018.

Raw protocol and Admin inventories retain excluded symbols so unknown or excluded requests can fail predictably. Their presence in a raw inventory is not a claim of behavioral support.

## Pure Rust Controller

The supported Controller topology is a Pure Rust Controller and Rust Broker deployment. It must provide the same user-visible election, fencing, failover, recovery, and administration functions as Java 5.5. It does not promise Java Controller wire compatibility, mixed Java/Rust quorum membership, JRaft component reuse, DLedger component reuse, or Java AutoSwitchHA framing.

Controller parity is therefore evaluated as a Rust-native alternative: functional-system tests must prove single-writer authority, epoch and lease fencing, committed-frontier recovery, stale-master rejection, tail truncation, and safe rejoin. Java-internal request codes remain recognizable but not applicable to the supported topology.

## Rust-native Timer and POP

Rust-native Timer and POP implementations may use storage layouts that differ from Java. Their compatibility obligation is semantic: delivery time, scan and recall behavior, acknowledgement and invisibility changes, retry limits, restart recovery, lag reporting, HA behavior, and failure handling must match the approved Java-visible contract.

Rust-native Timer and POP alternatives do not imply support for Java TimerRocksDB directories or binary-compatible POP state. Format ownership and activation markers must be explicit. Unsupported downgrade attempts are rejected by the 1.0 preflight tool before an older reader starts.

## Conditional Java data migration

Conditional Java data migration is not an automatic 1.0 requirement. Rust-to-Rust upgrades, restart recovery, and rollback within documented activation boundaries are mandatory. Java-to-Rust data-directory migration or direct takeover becomes mandatory only for a product profile that explicitly declares it.

When such a profile is declared, acceptance uses semantic records and upgrade/rollback behavior. It does not require Rust and Java to share the same implementation, database engine, or in-memory architecture. A profile that is not declared must not advertise direct Java data takeover.

## Dependency and ownership rules

Cross-domain capabilities follow their declared dependency edges. Important chains include:

- query pagination: Protocol to Client to Broker to Store;
- advanced Timer: Store to Broker to Admin to HA;
- Proxy ingress: Protocol and Transport to Proxy policy to local and cluster backends;
- Controller mode: Controller authority to Broker control plane to Store write admission and HA;
- Admin parity: protocol inventory to Admin Core to CLI and TUI presentation.

The DRI implements and maintains the capability. The reviewer independently evaluates the domain contract. The release approver decides whether the evidence is sufficient for the declared target. These roles must be distinct in each manifest record. Active F-01 through F-18 and G-01 through G-06 target `1.0.0-rc.1`; G-07 and G-08 are explicitly deferred.

## Evidence and readiness language

Short deterministic tests, compatibility inventories, focused integration tests, and bounded fault scenarios are required before the first complete release candidate. Long-running fuzzing, soak, burn-in, large-capacity, and independent security qualification are tracked separately as deferred G-07 and G-08 work.

While those long-running items are deferred, the release is not production-certified. Passing the short-check matrix means that the declared functional candidate gates passed; it must not be described as proof of long-duration capacity, disaster-recovery timing, or production certification.
