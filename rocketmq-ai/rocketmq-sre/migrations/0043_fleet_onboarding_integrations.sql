-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE fleet_onboarding_assessments (
    id UUID PRIMARY KEY,
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    region_id UUID NOT NULL REFERENCES fleet_regions(id),
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    requested_access TEXT NOT NULL
        CHECK (requested_access IN ('read_only', 'supervised', 'bounded_autonomy')),
    effective_access TEXT NOT NULL
        CHECK (effective_access IN ('read_only', 'supervised', 'bounded_autonomy')),
    connector_tls_verified BOOLEAN NOT NULL,
    schema_compatible BOOLEAN NOT NULL,
    missing_capabilities TEXT[] NOT NULL DEFAULT '{}',
    signal_gaps TEXT[] NOT NULL DEFAULT '{}',
    excessive_scopes TEXT[] NOT NULL DEFAULT '{}',
    incompatibilities TEXT[] NOT NULL DEFAULT '{}',
    eligible BOOLEAN NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (cluster_id, observed_at)
);

CREATE INDEX fleet_onboarding_assessments_scope
    ON fleet_onboarding_assessments (tenant_id, cluster_id, observed_at DESC);

CREATE TABLE fleet_quota_decisions (
    id UUID PRIMARY KEY,
    policy_id UUID NOT NULL REFERENCES fleet_quota_policies(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    cluster_id UUID REFERENCES clusters(id),
    work_kind TEXT NOT NULL
        CHECK (
            work_kind IN (
                'active_incident',
                'verification',
                'rollback',
                'audit',
                'interactive_query',
                'workflow',
                'inspection',
                'model_explanation',
                'notification',
                'automatic_action'
            )
        ),
    resource_kind TEXT NOT NULL
        CHECK (
            resource_kind IN (
                'query',
                'model_token',
                'concurrent_workflow',
                'concurrent_inspection',
                'evidence_byte',
                'notification',
                'automatic_action'
            )
        ),
    amount BIGINT NOT NULL CHECK (amount > 0),
    allowed BOOLEAN NOT NULL,
    reason_code TEXT NOT NULL,
    observed BIGINT NOT NULL CHECK (observed >= 0),
    quota_limit BIGINT NOT NULL CHECK (quota_limit >= 0),
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX fleet_quota_decisions_scope
    ON fleet_quota_decisions (tenant_id, cluster_id, allowed, occurred_at DESC);

-- Extend the closed Phase 3 adapter list with signed, inbound-only
-- representatives for CMDB, GitOps, and CI/CD.
DO $$
DECLARE
    constraint_record RECORD;
BEGIN
    FOR constraint_record IN
        SELECT conname
        FROM pg_constraint
        WHERE conrelid = 'integration_targets'::REGCLASS
          AND contype = 'c'
          AND pg_get_constraintdef(oid) LIKE '%adapter_kind%'
    LOOP
        EXECUTE format(
            'ALTER TABLE integration_targets DROP CONSTRAINT %I',
            constraint_record.conname
        );
    END LOOP;
END;
$$;

ALTER TABLE integration_targets
    ADD CONSTRAINT integration_targets_adapter_kind_v2_check
    CHECK (
        adapter_kind IN (
            'mock_itsm',
            'signed_webhook_itsm',
            'chatops_webhook',
            'pager',
            'email',
            'mock_cmdb',
            'mock_gitops',
            'signed_release_webhook'
        )
    );

ALTER TABLE integration_targets
    ADD CONSTRAINT integration_targets_notification_boundary_v2_check
    CHECK (
        (
            adapter_kind IN (
                'mock_itsm',
                'signed_webhook_itsm',
                'mock_cmdb',
                'mock_gitops',
                'signed_release_webhook'
            )
            AND notification_target_id IS NULL
        )
        OR
        (
            adapter_kind IN ('chatops_webhook', 'pager', 'email')
            AND notification_target_id IS NOT NULL
        )
    );

CREATE TABLE enterprise_integration_events (
    id UUID PRIMARY KEY,
    target_id UUID NOT NULL REFERENCES integration_targets(id),
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    event_kind TEXT NOT NULL
        CHECK (
            event_kind IN (
                'cmdb_snapshot',
                'gitops_snapshot',
                'release_started',
                'release_canary',
                'release_promoted',
                'release_rolled_back'
            )
        ),
    external_event_id TEXT NOT NULL
        CHECK (char_length(external_event_id) BETWEEN 1 AND 256),
    source_version TEXT NOT NULL
        CHECK (char_length(source_version) BETWEEN 1 AND 128),
    nonce TEXT NOT NULL CHECK (char_length(nonce) BETWEEN 16 AND 128),
    payload_digest TEXT NOT NULL
        CHECK (payload_digest ~ '^sha256:[0-9A-Fa-f]{64}$'),
    payload JSONB NOT NULL CHECK (jsonb_typeof(payload) = 'object'),
    signature_verified BOOLEAN NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL,
    followup_kind TEXT
        CHECK (followup_kind IS NULL OR followup_kind IN ('upgrade_readiness')),
    followup_id UUID,
    UNIQUE (target_id, external_event_id),
    UNIQUE (target_id, nonce),
    CHECK (received_at >= occurred_at - INTERVAL '5 minutes')
);

CREATE INDEX enterprise_integration_events_scope
    ON enterprise_integration_events (tenant_id, cluster_id, event_kind, received_at DESC);

CREATE TABLE integration_health_observations (
    id UUID PRIMARY KEY,
    target_id UUID NOT NULL REFERENCES integration_targets(id),
    health_status TEXT NOT NULL
        CHECK (health_status IN ('unknown', 'healthy', 'degraded', 'unavailable', 'disabled')),
    config_valid BOOLEAN NOT NULL,
    secret_available BOOLEAN NOT NULL,
    endpoint_valid BOOLEAN NOT NULL,
    last_delivery_at TIMESTAMPTZ,
    last_error_code TEXT,
    observed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX integration_health_observations_latest
    ON integration_health_observations (target_id, observed_at DESC);
