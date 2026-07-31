-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- The MCP manifest digest pins the immutable tool/resource surface, while
-- source availability can recover or degrade without changing that manifest.
-- Keep every distinct source-state transition append-only. The repository
-- serializes handshakes on the cluster row and treats an identical latest
-- manifest plus source state as idempotent.
ALTER TABLE cluster_capability_snapshots
    DROP CONSTRAINT IF EXISTS cluster_capability_snapshots_cluster_id_manifest_digest_key;

DROP INDEX IF EXISTS cluster_capability_snapshots_cluster_id_manifest_digest_key;

CREATE INDEX IF NOT EXISTS cluster_capability_snapshots_manifest_history
    ON cluster_capability_snapshots (cluster_id, manifest_digest, created_at DESC);
