# Storage diagnostics (S23)

The Storage page reports SQLite availability, schema version, observation start,
check time, and the latest committed write. Database page and freelist bytes are
read from SQLite; filesystem free space is unknown, not zero. These sizes exclude
WAL and filesystem overhead. No connection pool metrics or HTTP listener exist.

Business table triggers update a singleton activity row inside the same
transaction. Rollbacks also roll back that timestamp. The time identifies a write
in the latest committed transaction, not the precise disk flush instant. Status
and collector diagnostics use read-only authentication and connections, so refresh
does not advance session activity or manufacture a successful write.

Ordinary health responses contain no database path or credentials. Missing files
are not created by status checks. A failed read reports unavailable. Snapshots
older than a minute or retained after failed refresh are visibly marked stale.
Local database availability does not establish Broker reachability.
