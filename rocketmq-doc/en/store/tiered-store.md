# Tiered Store read and recovery contract

Tiered Store is a derived copy of LocalFile data. A successful Tiered dispatch
does not replace the LocalFile append or replication acknowledgement boundary,
and Tiered Store is not an independent disaster-recovery replica.

## Read selection

The `storage_level` setting uses one shared policy for message, query, timestamp,
and offset-oriented reads:

- `Disable` selects LocalFile unless the record is explicitly remote-only.
- `NotInDisk` selects Tiered Store when LocalFile no longer owns the requested
  range.
- `NotInMem` selects Tiered Store for disk-resident or remote-only records.
- `Force` selects Tiered Store unless an internal operation explicitly requires
  LocalFile.

Timeout, network, and storage-read failures may fall back only when a LocalFile
candidate is known to exist. Not-found results remain misses. Corruption and all
unclassified failures are fatal and are never converted into an empty result.

## Restart and cleanup

The JSON metadata envelope records its format version and the selected provider
configuration and persistence format. Startup rejects incompatible persisted
data before queue, segment, index, or dispatcher recovery begins. Recovery may
rebuild derived in-memory indexes and cursors, but it must not modify committed
provider payloads.

Cleanup considers only committed, sealed segments. It removes a provider object
before durably publishing its metadata tombstone, and CommitLog segments remain
retained while any live ConsumeQueue unit references them.
