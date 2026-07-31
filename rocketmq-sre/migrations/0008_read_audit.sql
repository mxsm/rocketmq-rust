-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE read_audit (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    audit_id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID,
    actor_subject TEXT NOT NULL,
    operation TEXT NOT NULL,
    resource_type TEXT NOT NULL,
    resource_id TEXT,
    correlation_id UUID NOT NULL,
    request_hash TEXT NOT NULL CHECK (request_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    outcome TEXT NOT NULL CHECK (outcome IN ('success', 'partial', 'denied', 'failed')),
    error_code TEXT,
    row_count BIGINT,
    byte_count BIGINT,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX read_audit_scope
    ON read_audit (tenant_id, cluster_id, occurred_at DESC, sequence_id);

CREATE TABLE source_capability_history (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    connector_session_id UUID REFERENCES connector_sessions(id),
    source TEXT NOT NULL,
    schema_major INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('queryable', 'degraded', 'missing', 'unsupported')),
    limits JSONB NOT NULL,
    last_success_at TIMESTAMPTZ,
    latency_millis BIGINT,
    freshness_seconds BIGINT,
    observed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX source_capability_history_latest
    ON source_capability_history (tenant_id, cluster_id, source, observed_at DESC);
