# Storage diagnostics UI

Storage & diagnostics is a read-only view of the local SQLite database, storage
activity, history collector and saved connection context. It does not establish
Broker reachability or provide cleanup, vacuum, backup or restore operations.

The environment toolbar refreshes the existing storage and history status reads.
Each region retains its own last successful observation when another read fails.
Failures and stale observations remain visible rather than being converted into
zero measurements or current availability. Shared read ownership prevents an
unmounted page's response from updating a replacement page.

Allocated and reusable values describe database pages; they exclude WAL and
filesystem overhead. Display units use decimal KB/MB/GB, with exact byte counts
available on the measurement. Null capacity values mean not measured, including
filesystem free space. The page does not derive a disk-usage percentage.

Activity timestamps come from the backend. Rendering or refreshing diagnostics
does not substitute the current time for the latest committed write. Null sample
and write timestamps mean not observed. A status check at least one minute old
is marked stale; an invalid or future observation time has unknown age.

Collector intervals, retention, sample/write times and errors come from the
collector status. No error being reported is not proof of a running sampler or
healthy Brokers. The status DTO does not identify the sampled environment, so
the view does not assign its activity to the currently selected NameServer.

Connection context uses the current connection store and retains NameServer
settings and Cluster navigation. Database availability, saved configuration and
remote connectivity are separate observations.

Frontend validation runs from the app directory:

```text
npm run build
npm test -- --run src/pages/storage src/features/dashboard/readResource.test.ts src/app/layout/pageToolbar.test.ts src/stores/navigation.test.ts
```

Existing backend contract tests in `src-tauri/src/ops.rs` verify that reads and
rolled-back writes do not advance committed-write activity, that missing
databases are not created by a read, and that unavailable capacity stays unknown.

```text
cargo test --locked --lib ops::tests
```

Visual acceptance still requires comparison with the selected Storage reference,
keyboard and narrow-window checks, and inspection of normal, unavailable, stale,
unmeasured and collector-error states.
