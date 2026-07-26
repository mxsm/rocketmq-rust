-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE IF NOT EXISTS clusters (
    id UUID PRIMARY KEY,
    tenant_id TEXT NOT NULL,
    external_cluster_key TEXT NOT NULL,
    environment TEXT NOT NULL,
    region TEXT NOT NULL,
    rocketmq_version TEXT NOT NULL,
    deployment_mode TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    requested_access_profile TEXT NOT NULL DEFAULT 'read_only',
    effective_access_profile TEXT NOT NULL DEFAULT 'read_only'
        CHECK (effective_access_profile = 'read_only'),
    onboarding_state TEXT NOT NULL
        CHECK (
            onboarding_state IN (
                'pending',
                'handshaking',
                'ready_read_only',
                'read_only_degraded',
                'rejected',
                'offboarded'
            )
        ),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    offboarded_at TIMESTAMPTZ,
    UNIQUE (tenant_id, external_cluster_key)
);

CREATE TABLE IF NOT EXISTS cluster_capability_snapshots (
    id UUID PRIMARY KEY,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    manifest_digest TEXT NOT NULL,
    protocol_version TEXT NOT NULL,
    schema_version TEXT NOT NULL,
    mutation_supported BOOLEAN NOT NULL CHECK (mutation_supported = FALSE),
    manifest JSONB NOT NULL,
    data_sources JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (cluster_id, manifest_digest)
);

CREATE INDEX IF NOT EXISTS cluster_capability_snapshots_latest
    ON cluster_capability_snapshots (cluster_id, observed_at DESC);

CREATE TABLE IF NOT EXISTS cluster_onboarding_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    event_type TEXT NOT NULL,
    actor_subject TEXT NOT NULL,
    correlation_id UUID NOT NULL,
    event_payload JSONB NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS cluster_onboarding_events_by_cluster
    ON cluster_onboarding_events (cluster_id, sequence_id);

CREATE TABLE IF NOT EXISTS connector_identities (
    id UUID PRIMARY KEY,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    subject TEXT NOT NULL,
    issuer TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    revoked_at TIMESTAMPTZ,
    UNIQUE (cluster_id, subject, issuer)
);

CREATE INDEX IF NOT EXISTS connector_identities_active
    ON connector_identities (cluster_id, subject)
    WHERE revoked_at IS NULL;
