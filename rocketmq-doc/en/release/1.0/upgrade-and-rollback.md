# RocketMQ Rust 1.0 Upgrade and Rollback

This guide covers upgrades between Rust Broker releases. It does not promise
direct takeover of a Java Broker data directory, DLedger CommitLog support, or
Java Controller/AutoSwitchHA wire compatibility.

## Before upgrading

1. Stop all writers and take a recoverable backup of the configuration and
   Store roots.
2. Retain the old Rust Broker binary and the 1.0 `rocketmq-cli-rust` tool.
3. Record every writable and read-only CommitLog root. Do not remove a root that
   still owns historical segments.
4. Run the new Broker first with the existing Store backend and compatibility
   modes. Enable a new owner only after its shadow/compare checks pass.
5. Verify ordinary, retry, transaction, delay, FIFO, batch, recall, and LMQ
   message flows before changing a persisted format owner.

## Mandatory downgrade preflight

Always stop the Broker and run the 1.0 preflight before starting an older Rust
binary:

```shell
rocketmq-cli-rust downgrade-preflight \
  --target-version 0.9.0 \
  --config /etc/rocketmq-rust/broker.toml \
  --output downgrade-report.json
```

Exit code `0` permits the inspected transition. Exit code `2` is a hard fence.
Do not bypass it by invoking an older Broker directly. The tool opens RocksDB
column families read-only and rejects missing, corrupt, or incompatible state
that the configuration or format inventory declares as initialized.

## Persisted-format boundaries

### Multipath CommitLog

Returning to a single-path configuration is safe only before the first segment
is allocated outside the primary root. After that point, keep every historical
root mounted or consolidate offline into a new directory:

```shell
rocketmq-cli-rust consolidate-multipath \
  --source-root /data-a/commitlog \
  --source-root /data-b/commitlog \
  --target /data-consolidated/commitlog \
  --mapped-file-size 1073741824 \
  --store-root /var/lib/rocketmq-rust/store
```

The target must be new. The command holds the Store lock, rejects duplicate or
gapped segment ownership, validates CommitLog frames, compares every copied
byte, and leaves all source roots unchanged. Point the Broker at the target only
after the report succeeds.

### POP retry and consumer profiles

Rust 1.0 uses dual-read/v2-write retry semantics and stores a versioned POP
consumer-profile marker. A configuration rollback may select the dual reader;
a pre-v2 binary cannot safely acknowledge v2 state. Drain POP inflight work and
profiles before such a downgrade. Missing state is accepted only when persistent
POP was never declared; declared-but-missing state is corruption.

### Timer ownership

`java_compat` is the default Rust timer mode. `extended_timeline` is a Rust-owned
format and is unrelated to Java TimerRocksDB. Move through shadow comparison
before formal activation. Once the extended owner marker is committed, rollback
requires quiescing admissions, draining outstanding timers, and recording a
clean checkpoint. An old binary must not be started against an active extended
owner.

### Compaction and Tiered metadata

Compaction `CURRENT` and Tiered metadata carry explicit versions. Rust 1.0 reads
the supported legacy generation and rejects future or corrupt versions without
rewriting them. Retain the 1.0 reader until any v2 Compaction generation and
Tiered v1 metadata have been migrated or retired.

### Transactions, query cursors, and configuration

Transaction metrics use atomic checkpoints with restart recovery. Legacy query
requests remain valid while Java-compatible RocksDB `lastKey` cursors are
accepted where that backend defines continuation. Java properties conversion is
an import step: save and use the generated canonical TOML for subsequent Rust
restarts.

## Controller and HA

Controller mode is Rust-native. Upgrade Controller and Broker components using
their Rust protocol and fencing contract. Do not form a mixed Java/Rust
Controller quorum or connect Java AutoSwitchHA peers. DefaultHA remains the
bounded Java interoperability profile documented separately.

## Recovery rule

If any check reports an unknown version, corrupt marker, missing declared state,
or incomplete consolidation, keep the Broker stopped. Restore the backup or use
the retained 1.0 reader/tool to repair or migrate the state, then rerun the
preflight. Never treat an inspection failure as permission to truncate data.
