-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

UPDATE evidence_snapshots
SET expires_at = collected_at + INTERVAL '30 days'
WHERE expires_at IS NULL;

CREATE INDEX evidence_active_scope
    ON evidence_snapshots (tenant_id, cluster_id, expires_at, observed_at DESC, id);
