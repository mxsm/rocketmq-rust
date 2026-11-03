# Native Dashboard storage

This crate provides native Dashboard persistence without GPUI or an HTTP backend. It is a repository-root
workspace member. GPUI startup and its configuration, environment, Monitor, History and Audit adapters use
this authority. The desktop enables drivers through its optional `storage-sqlite`, `storage-mysql` and
`storage-postgres` features. Visible storage selection/workspace initialization/import, engine-aware OPS
status and persistent Sessions UI remain subsequent deliveries. E01/E02 remain incomplete until those
workflows and native application verification are delivered.

The default feature set uses files and does not link SQLx. `sqlite`, `mysql` and `postgres` independently
enable only their optional SQLx drivers. Selecting an unavailable driver or failing to connect returns an
explicit typed error. A database connection is external input with redacted Debug formatting; the serializable
`StorageSelection` contains only the non-sensitive engine choice. There is no automatic file fallback.

Configuration, environments, Monitor rules, local operation records and the P11 audit journal retain the
native schema-four canonical document. One whole-document CAS commits them together. The storage revision
advances independently of the configuration revision, including for Audit-only and Monitor writes. Clients
load authoritative state on every read. A conflicting audit finish can be reapplied to a newly loaded journal;
the business operation must not be replayed. Authorization and business operation ownership belong to the
calling native service.

File mode keeps `config.json`, schema-two `history.json`, the directory's `.dashboard-writer.lock` and the
existing schema-one through schema-three migrations. Session metadata uses `sessions.json` under the same
single writer. SQL uses only `native_dashboard_*` tables, independently of Web backend migrations. SQLite
uses WAL, a bounded connection pool, busy timeout and immediate write transactions. MySQL uses InnoDB and
case-sensitive ASCII lease keys. PostgreSQL and MySQL lock the ownership and document rows in that order.

History keeps the captured environment identity, bounds retained series and observations globally, and
requires a fenced collection lease checked in the commit transaction. Lease acquisition and expiry use the
database clock in SQL mode. `HistoryAppendOutcome` distinguishes an actually retained submitted sample from
an empty, invalid or fully evicted sample. A real write can succeed without retaining a submitted observation.
`HistoryAppendReport` records that distinction explicitly, and `HistoryReadReport` distinguishes missing,
existing and actually migrated file data. Legacy observations with no environment use a separate unassigned
lease and retain `None` attribution; they cannot appear in a current-environment chart.
Session records preserve expiry, last access and revocation; a sequence cursor excludes later creations even
when their timestamps are equal. Session operations and the P11 journal use the same authority, while the
explicit atomic config/Monitor + Audit contract remains the canonical document transaction.

Opening SQL creates or resumes its native schema but leaves a new database uninitialized. Applications must
explicitly initialize it or import. `ImportSnapshot::read_files` acquires the source directory writer and decodes
every source before returning a candidate. Missing configuration, invalid History and unsupported schema are
errors. Reading an import never rewrites the source. Importing commits all native data documents in one SQL
transaction and rejects an existing destination; a failed later insert rolls back earlier inserts.

The caller supplies `ChildServiceContext`; file I/O and History retention use its storage executor. The caller
must finish its owned storage users and await `DashboardStore::close` before stopping its runtime. This crate
does not create a runtime or a detached task. Status performs a fresh read and reports actual engine, schema,
initialization, successful-write time and applicable pool counts. An unavailable authority returns an error.

Run focused checks from the repository root:

```text
cargo fmt -p rocketmq-dashboard-storage -- --check
cargo test -p rocketmq-dashboard-storage
cargo test -p rocketmq-dashboard-storage --features sqlite
```

The ignored MySQL/PostgreSQL contracts require disposable dedicated databases supplied through
`ROCKETMQ_DASHBOARD_NATIVE_MYSQL_TEST_URL` / `ROCKETMQ_DASHBOARD_NATIVE_POSTGRES_TEST_URL`. They reset only
this crate's native tables, so these variables must never point at a database containing user native data.
Connection values should be injected by the local fixture or secret provider and must not be printed:

```text
cargo test -p rocketmq-dashboard-storage --features mysql mysql_native_contract -- --ignored
cargo test -p rocketmq-dashboard-storage --features postgres postgres_native_contract -- --ignored
```

The common contract runs against file, SQLite and each explicitly enabled server fixture. It covers independent
CAS conflicts, authoritative peer visibility, atomic state/Audit documents, bounded environment-specific History,
competing collection leases and stale fences, persistent sessions, stable pagination and peer revocation. SQL
tests additionally cover import rollback, source preservation, restart after interrupted schema setup and future
schema rejection. Skipped server tests do not constitute real database verification.
