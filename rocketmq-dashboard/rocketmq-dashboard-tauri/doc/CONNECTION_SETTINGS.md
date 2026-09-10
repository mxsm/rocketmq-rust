# Revisioned desktop connection settings

All exported NameServer/Proxy additions, switches, deletions, VIP/TLS changes,
and full NameServer replacement share one transaction manager. Read the current
`ConnectionSettingsView` with `get_connection_settings`. It includes revision,
stable endpoints, current endpoint IDs, environment ID, and NameServer/Proxy
snapshots. It contains no RocketMQ credentials.

Every configuration mutation requires `expectedRevision`; replacement carries it
inside `request`. The transaction compares the stored revision, validates the full
candidate configuration, writes both snapshots and stable identities, records the
successful audit, increments the revision, and commits. Runtime publication occurs
once after commit. Conflicting drafts are rejected with
`dashboard.configuration_conflict`; the frontend asks the user to refresh and
review instead of resubmitting automatically.

Endpoint UUIDs are associated with `(kind, canonical address)` and survive removal
and later readdition. Each saved logical NameServer address group has its own UUID
environment. Switching groups changes environment; VIP/TLS or Proxy changes do
not. No address hashing or guessing that different groups belong to the same
cluster is involved.

Accepted remote mutations carry the frontend's revision and hold a read lease
through the RPC. A configuration switch waits for those mutations. A stale mutation
is rejected before RPC dispatch. Read commands check revision before and after
querying; the frontend also rejects old results and remounts business views when
the shared revision changes. A completed write receipt is preserved even if the
user changes views. Configuration pages retain drafts and require review when a
poll detects a newer revision.

## Atomic NameServer replacement (A03)

The backend command and typed frontend service are implemented. A bulk editor is
an optional follow-up UI; the command is registered and available to import tools.

```typescript
await replaceNameServers(
    ['127.0.0.1:9876', '127.0.0.2:9876'],
    { kind: 'address', value: '127.0.0.2:9876' },
    settings.revision,
);
```

`currentEndpoint` can also be `{ kind: 'existing_id', value: endpointId }`. Empty
entries are removed and canonical duplicates are collapsed. Every nonempty entry
must be valid and the selected endpoint must belong to the replacement list.
Replacing with an empty list requires `currentEndpoint: null`. An intentionally
empty configuration stays empty after restart. Each list accepts at most 256
entries.

The fresh database format is schema version 4. Older development databases are
rejected without modification; use a new `DASHBOARD_TAURI_DATA_DIR` as needed.

## Validation

From the standalone backend: `cargo test --lib connection::`, plus affected
NameServer, Proxy, audit, and error tests. Tests cover competing revisions, invalid
replacement rollback, stable IDs, empty-list reopening, write leases, and atomic
audit failure. From the frontend: `npm run build` and the focused auth/session
service tests, including stale reads and preservation of completed writes.

## RocketMQ administration credentials

Set `DASHBOARD_TAURI_ROCKETMQ_ACCESS_KEY` and
`DASHBOARD_TAURI_ROCKETMQ_SECRET_KEY` in the process environment before starting
the desktop app. `DASHBOARD_TAURI_ROCKETMQ_SECURITY_TOKEN` is optional. Missing
half of a credential pair, empty keys, or a token without keys fails startup with
a safe configuration error. With none configured, the client remains anonymous.

Credentials stay in backend memory and are shared by all administration sessions
and NameServer probes. They are not saved in SQLite or returned to the frontend.
Connection settings expose only `credentialsConfigured`; the NameServer page
shows that status. Environment changes require restarting the app. Desktop login
credentials and RocketMQ administration credentials serve separate purposes.
The current admin-core client signs using HMAC-SHA256. Configure the target Rust
Broker with `signatureAlgorithm = "HmacSHA256"`; its default HMAC-SHA1 setting
does not match this client.

See the [isolated ACL fixture](../deploy/dev/acl/README.md) for a reproducible local
authentication query with the same builder used by the desktop app.
