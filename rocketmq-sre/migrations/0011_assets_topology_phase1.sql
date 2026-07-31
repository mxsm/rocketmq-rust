-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- A collection identifies one coherent inventory observation.  The Phase 00
-- asset and edge tables remain append-only and retain their per-source
-- timestamps; this table adds the boundary required for deterministic diffs.
CREATE TABLE asset_inventory_snapshots (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    sources JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    freshness_seconds BIGINT NOT NULL CHECK (freshness_seconds >= 0),
    partial BOOLEAN NOT NULL,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX asset_inventory_snapshots_latest
    ON asset_inventory_snapshots (tenant_id, cluster_id, observed_at DESC, id DESC);

ALTER TABLE asset_snapshots
    ADD COLUMN inventory_snapshot_id UUID REFERENCES asset_inventory_snapshots(id);

ALTER TABLE topology_edges
    ADD COLUMN inventory_snapshot_id UUID REFERENCES asset_inventory_snapshots(id);

CREATE UNIQUE INDEX asset_snapshots_collection_identity
    ON asset_snapshots (inventory_snapshot_id, kind, external_key)
    WHERE inventory_snapshot_id IS NOT NULL;

CREATE UNIQUE INDEX topology_edges_collection_identity
    ON topology_edges (inventory_snapshot_id, from_key, to_key, relation)
    WHERE inventory_snapshot_id IS NOT NULL;

CREATE INDEX asset_snapshots_collection
    ON asset_snapshots (tenant_id, cluster_id, inventory_snapshot_id, kind, external_key);

CREATE INDEX topology_edges_collection
    ON topology_edges (
        tenant_id,
        cluster_id,
        inventory_snapshot_id,
        from_key,
        to_key,
        relation
    );

ALTER TABLE topology_diffs
    ADD COLUMN previous_snapshot_id UUID REFERENCES asset_inventory_snapshots(id),
    ADD COLUMN current_snapshot_id UUID REFERENCES asset_inventory_snapshots(id),
    ADD COLUMN partial BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN suppressed_removals INTEGER NOT NULL DEFAULT 0
        CHECK (suppressed_removals >= 0),
    ADD COLUMN content_hash TEXT;

ALTER TABLE topology_diffs
    ADD CONSTRAINT topology_diffs_content_hash
        CHECK (
            content_hash IS NULL
            OR content_hash ~ '^sha256:[0-9A-Fa-f]{64}$'
        );

CREATE UNIQUE INDEX topology_diffs_current_snapshot
    ON topology_diffs (current_snapshot_id)
    WHERE current_snapshot_id IS NOT NULL;
