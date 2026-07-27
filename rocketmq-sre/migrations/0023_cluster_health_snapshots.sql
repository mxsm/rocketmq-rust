-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE cluster_health_snapshots (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    score SMALLINT CHECK (score IS NULL OR score BETWEEN 0 AND 100),
    status TEXT NOT NULL CHECK (
        status IN ('healthy', 'degraded', 'critical', 'unknown')
    ),
    data_quality TEXT NOT NULL CHECK (
        data_quality IN ('complete', 'partial', 'stale', 'missing')
    ),
    operational_state TEXT NOT NULL CHECK (
        operational_state IN ('normal', 'maintenance', 'fault_drill')
    ),
    algorithm_version TEXT NOT NULL,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    report JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (tenant_id, cluster_id, observed_at, algorithm_version)
);

CREATE INDEX cluster_health_snapshots_latest
    ON cluster_health_snapshots (tenant_id, cluster_id, observed_at DESC, id DESC);

CREATE INDEX cluster_health_snapshots_fleet
    ON cluster_health_snapshots (tenant_id, status, observed_at DESC, cluster_id);
