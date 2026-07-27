-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE evidence_snapshots (
    id UUID PRIMARY KEY,
    query_id UUID NOT NULL,
    correlation_id UUID NOT NULL,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    investigation_id UUID REFERENCES investigations(id),
    incident_id UUID REFERENCES sre_incidents(id),
    schema_family TEXT NOT NULL,
    schema_major INTEGER NOT NULL CHECK (schema_major > 0),
    schema_minor INTEGER NOT NULL CHECK (schema_minor >= 0),
    source TEXT NOT NULL,
    resource TEXT NOT NULL,
    time_range_start TIMESTAMPTZ NOT NULL,
    time_range_end TIMESTAMPTZ NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    collected_at TIMESTAMPTZ NOT NULL,
    freshness_seconds BIGINT NOT NULL CHECK (freshness_seconds >= 0),
    coverage TEXT NOT NULL,
    sensitivity TEXT NOT NULL,
    partial BOOLEAN NOT NULL,
    warnings JSONB NOT NULL DEFAULT '[]'::JSONB,
    inline_content JSONB,
    content_uri TEXT,
    content_size_bytes BIGINT,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    query_hash TEXT NOT NULL CHECK (query_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    expires_at TIMESTAMPTZ,
    CHECK (
        (inline_content IS NOT NULL AND content_uri IS NULL)
        OR (inline_content IS NULL AND content_uri IS NOT NULL AND content_size_bytes IS NOT NULL)
    )
);

CREATE INDEX evidence_scope_observed
    ON evidence_snapshots (tenant_id, cluster_id, observed_at DESC, id);
CREATE INDEX evidence_incident
    ON evidence_snapshots (tenant_id, cluster_id, incident_id, observed_at DESC);
CREATE INDEX evidence_dedup
    ON evidence_snapshots (tenant_id, cluster_id, query_hash, time_range_start, time_range_end);

CREATE TABLE asset_snapshots (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    kind TEXT NOT NULL,
    external_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    source TEXT NOT NULL,
    attributes JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    freshness_seconds BIGINT NOT NULL CHECK (freshness_seconds >= 0),
    partial BOOLEAN NOT NULL,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^sha256:[0-9A-Fa-f]{64}$')
);

CREATE INDEX asset_snapshots_latest
    ON asset_snapshots (tenant_id, cluster_id, kind, external_key, observed_at DESC);

CREATE TABLE topology_edges (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    from_key TEXT NOT NULL,
    to_key TEXT NOT NULL,
    relation TEXT NOT NULL,
    source TEXT NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    freshness_seconds BIGINT NOT NULL CHECK (freshness_seconds >= 0),
    partial BOOLEAN NOT NULL,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^sha256:[0-9A-Fa-f]{64}$')
);

CREATE INDEX topology_edges_latest
    ON topology_edges (tenant_id, cluster_id, from_key, to_key, relation, observed_at DESC);

CREATE TABLE topology_diffs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    previous_observed_at TIMESTAMPTZ,
    current_observed_at TIMESTAMPTZ NOT NULL,
    additions JSONB NOT NULL DEFAULT '[]'::JSONB,
    removals JSONB NOT NULL DEFAULT '[]'::JSONB,
    changes JSONB NOT NULL DEFAULT '[]'::JSONB,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX topology_diffs_scope
    ON topology_diffs (tenant_id, cluster_id, current_observed_at DESC);

CREATE TABLE connector_sessions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    subject TEXT NOT NULL,
    schema_family TEXT NOT NULL,
    schema_major INTEGER NOT NULL,
    capability JSONB NOT NULL,
    connected_at TIMESTAMPTZ NOT NULL,
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    disconnected_at TIMESTAMPTZ,
    UNIQUE (tenant_id, cluster_id, subject, connected_at)
);

CREATE INDEX connector_sessions_active
    ON connector_sessions (tenant_id, cluster_id, last_heartbeat_at DESC)
    WHERE disconnected_at IS NULL;

CREATE TABLE connector_queries (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    session_id UUID NOT NULL REFERENCES connector_sessions(id),
    correlation_id UUID NOT NULL,
    query_id UUID NOT NULL UNIQUE,
    query_payload JSONB NOT NULL,
    deadline TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'running', 'completed', 'failed', 'cancelled')),
    response_payload JSONB,
    error_code TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX connector_queries_pending
    ON connector_queries (session_id, sequence_id)
    WHERE status IN ('pending', 'running');
