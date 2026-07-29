-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE fleets (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (name)
);

CREATE TABLE fleet_tenants (
    id UUID PRIMARY KEY,
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    name TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (fleet_id, name)
);

CREATE TABLE fleet_regions (
    id UUID PRIMARY KEY,
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    region_key TEXT NOT NULL,
    display_name TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    residency_tags JSONB NOT NULL DEFAULT '[]'::JSONB,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (fleet_id, region_key)
);

CREATE TABLE fleet_cluster_registrations (
    cluster_id UUID PRIMARY KEY REFERENCES clusters(id),
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    region_id UUID NOT NULL REFERENCES fleet_regions(id),
    environment TEXT NOT NULL
        CHECK (environment IN ('development', 'test', 'staging', 'production', 'other')),
    owner_name TEXT NOT NULL,
    lifecycle_state TEXT NOT NULL
        CHECK (
            lifecycle_state IN (
                'pending',
                'onboarding',
                'active',
                'read_only_degraded',
                'offboarding',
                'retired'
            )
        ),
    residency_tags JSONB NOT NULL DEFAULT '[]'::JSONB,
    lifecycle_revision BIGINT NOT NULL DEFAULT 1 CHECK (lifecycle_revision > 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (fleet_id, tenant_id, region_id, cluster_id)
);

CREATE INDEX fleet_cluster_registrations_scope
    ON fleet_cluster_registrations (fleet_id, tenant_id, region_id, environment, lifecycle_state);

CREATE TABLE fleet_quota_policies (
    id UUID PRIMARY KEY,
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    region_id UUID REFERENCES fleet_regions(id),
    cluster_id UUID REFERENCES clusters(id),
    policy_version BIGINT NOT NULL CHECK (policy_version > 0),
    queries_per_minute INTEGER NOT NULL CHECK (queries_per_minute >= 0),
    model_tokens_per_hour BIGINT NOT NULL CHECK (model_tokens_per_hour >= 0),
    concurrent_workflows INTEGER NOT NULL CHECK (concurrent_workflows >= 0),
    concurrent_inspections INTEGER NOT NULL CHECK (concurrent_inspections >= 0),
    evidence_bytes_per_hour BIGINT NOT NULL CHECK (evidence_bytes_per_hour >= 0),
    notifications_per_hour INTEGER NOT NULL CHECK (notifications_per_hour >= 0),
    automatic_actions_per_hour INTEGER NOT NULL CHECK (automatic_actions_per_hour >= 0),
    owner_name TEXT NOT NULL,
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CHECK (cluster_id IS NULL OR region_id IS NOT NULL)
);

CREATE UNIQUE INDEX fleet_quota_policies_active_scope
    ON fleet_quota_policies (
        fleet_id,
        tenant_id,
        COALESCE(region_id, '00000000-0000-0000-0000-000000000000'::UUID),
        COALESCE(cluster_id, '00000000-0000-0000-0000-000000000000'::UUID)
    )
    WHERE active;

CREATE TABLE fleet_quota_usage_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    event_id UUID NOT NULL UNIQUE,
    quota_policy_id UUID NOT NULL REFERENCES fleet_quota_policies(id),
    tenant_id UUID NOT NULL,
    region_id UUID,
    cluster_id UUID,
    resource_kind TEXT NOT NULL
        CHECK (
            resource_kind IN (
                'query',
                'model_token',
                'workflow',
                'inspection',
                'evidence_byte',
                'notification',
                'automatic_action'
            )
        ),
    amount BIGINT NOT NULL CHECK (amount >= 0),
    occurred_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX fleet_quota_usage_events_window
    ON fleet_quota_usage_events (quota_policy_id, resource_kind, occurred_at DESC);

CREATE TABLE regional_endpoints (
    id TEXT PRIMARY KEY,
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    region_id UUID NOT NULL REFERENCES fleet_regions(id),
    cluster_id UUID REFERENCES clusters(id),
    endpoint_kind TEXT NOT NULL
        CHECK (endpoint_kind IN ('connector', 'executor', 'execution_agent', 'mcp')),
    component_version TEXT NOT NULL,
    protocol_version TEXT NOT NULL,
    schema_digest TEXT NOT NULL CHECK (schema_digest ~ '^sha256:[0-9A-Fa-f]{64}$'),
    capabilities JSONB NOT NULL,
    residency_tags JSONB NOT NULL,
    capacity INTEGER NOT NULL CHECK (capacity >= 0),
    health TEXT NOT NULL
        CHECK (health IN ('healthy', 'degraded', 'disconnected', 'incompatible')),
    last_heartbeat_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX regional_endpoints_routing
    ON regional_endpoints (tenant_id, region_id, cluster_id, endpoint_kind, health, last_heartbeat_at DESC);

CREATE TABLE fleet_asset_index (
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    region_id UUID NOT NULL REFERENCES fleet_regions(id),
    environment TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    component TEXT NOT NULL,
    component_version TEXT NOT NULL,
    image_digest TEXT,
    feature_digest TEXT,
    configuration_digest TEXT,
    health TEXT NOT NULL,
    attributes JSONB NOT NULL DEFAULT '{}'::JSONB,
    observed_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (cluster_id, component)
);

CREATE INDEX fleet_asset_index_query
    ON fleet_asset_index (fleet_id, tenant_id, region_id, environment, component_version, health);

CREATE TABLE fleet_compliance_findings (
    id UUID PRIMARY KEY,
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    region_id UUID NOT NULL REFERENCES fleet_regions(id),
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    category TEXT NOT NULL,
    expected_digest TEXT NOT NULL CHECK (expected_digest ~ '^sha256:[0-9A-Fa-f]{64}$'),
    live_digest TEXT NOT NULL CHECK (live_digest ~ '^sha256:[0-9A-Fa-f]{64}$'),
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    severity TEXT NOT NULL CHECK (severity IN ('info', 'warning', 'error', 'critical')),
    owner_name TEXT NOT NULL,
    recommendation TEXT NOT NULL,
    finding_state TEXT NOT NULL
        CHECK (finding_state IN ('open', 'acknowledged', 'resolved', 'accepted_exception')),
    observed_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (cluster_id, category, expected_digest, live_digest)
);

CREATE INDEX fleet_compliance_findings_scope
    ON fleet_compliance_findings (fleet_id, tenant_id, region_id, severity, finding_state, observed_at DESC);

CREATE TABLE fleet_inspection_runs (
    id UUID PRIMARY KEY,
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    region_ids UUID[] NOT NULL,
    cluster_ids UUID[] NOT NULL,
    pack_ids TEXT[] NOT NULL,
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency > 0 AND max_concurrency <= 32),
    timeout_seconds INTEGER NOT NULL CHECK (timeout_seconds > 0 AND timeout_seconds <= 86400),
    model_token_budget BIGINT NOT NULL CHECK (model_token_budget >= 0),
    evidence_byte_budget BIGINT NOT NULL CHECK (evidence_byte_budget >= 0),
    inspection_state TEXT NOT NULL
        CHECK (
            inspection_state IN (
                'pending',
                'running',
                'completed',
                'partially_completed',
                'failed',
                'cancelled'
            )
        ),
    completed_clusters INTEGER NOT NULL DEFAULT 0 CHECK (completed_clusters >= 0),
    failed_clusters INTEGER NOT NULL DEFAULT 0 CHECK (failed_clusters >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    completed_at TIMESTAMPTZ
);

CREATE INDEX fleet_inspection_runs_scope
    ON fleet_inspection_runs (fleet_id, tenant_id, inspection_state, created_at DESC);

-- Preserve existing single-cluster installations under one deterministic
-- default Fleet while retaining their tenant and region identity.
INSERT INTO fleets (id, name, owner_name)
VALUES (
    '00000000-0000-4000-8000-000000000005'::UUID,
    'default',
    'messaging-platform'
)
ON CONFLICT (id) DO NOTHING;

INSERT INTO fleet_tenants (id, fleet_id, name, owner_name)
SELECT DISTINCT
    cluster.tenant_id::UUID,
    '00000000-0000-4000-8000-000000000005'::UUID,
    cluster.tenant_id,
    COALESCE(NULLIF(cluster.owner_name, ''), 'messaging-platform')
FROM clusters cluster
ON CONFLICT (id) DO NOTHING;

INSERT INTO fleet_regions (id, fleet_id, region_key, display_name, owner_name, residency_tags)
SELECT DISTINCT
    (
        SUBSTRING(MD5('rocketmq-sre-region:' || cluster.region), 1, 8) || '-' ||
        SUBSTRING(MD5('rocketmq-sre-region:' || cluster.region), 9, 4) || '-' ||
        '4' || SUBSTRING(MD5('rocketmq-sre-region:' || cluster.region), 14, 3) || '-' ||
        '8' || SUBSTRING(MD5('rocketmq-sre-region:' || cluster.region), 18, 3) || '-' ||
        SUBSTRING(MD5('rocketmq-sre-region:' || cluster.region), 21, 12)
    )::UUID,
    '00000000-0000-4000-8000-000000000005'::UUID,
    cluster.region,
    cluster.region,
    'messaging-platform',
    JSONB_BUILD_ARRAY('region:' || cluster.region)
FROM clusters cluster
ON CONFLICT (fleet_id, region_key) DO NOTHING;

INSERT INTO fleet_cluster_registrations (
    cluster_id,
    fleet_id,
    tenant_id,
    region_id,
    environment,
    owner_name,
    lifecycle_state,
    residency_tags,
    lifecycle_revision,
    created_at,
    updated_at
)
SELECT
    cluster.id,
    '00000000-0000-4000-8000-000000000005'::UUID,
    cluster.tenant_id::UUID,
    region.id,
    CASE LOWER(cluster.environment)
        WHEN 'dev' THEN 'development'
        WHEN 'development' THEN 'development'
        WHEN 'test' THEN 'test'
        WHEN 'staging' THEN 'staging'
        WHEN 'prod' THEN 'production'
        WHEN 'production' THEN 'production'
        ELSE 'other'
    END,
    COALESCE(NULLIF(cluster.owner_name, ''), 'messaging-platform'),
    CASE cluster.onboarding_state
        WHEN 'pending' THEN 'pending'
        WHEN 'handshaking' THEN 'onboarding'
        WHEN 'ready_read_only' THEN 'active'
        WHEN 'read_only_degraded' THEN 'read_only_degraded'
        WHEN 'rejected' THEN 'read_only_degraded'
        WHEN 'offboarded' THEN 'retired'
    END,
    JSONB_BUILD_ARRAY('region:' || cluster.region),
    1,
    cluster.created_at,
    cluster.updated_at
FROM clusters cluster
JOIN fleet_regions region
  ON region.fleet_id = '00000000-0000-4000-8000-000000000005'::UUID
 AND region.region_key = cluster.region
ON CONFLICT (cluster_id) DO NOTHING;
