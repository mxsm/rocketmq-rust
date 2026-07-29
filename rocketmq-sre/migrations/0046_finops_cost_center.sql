-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE finops_cost_ledger (
    id UUID PRIMARY KEY,
    idempotency_key TEXT NOT NULL
        CHECK (char_length(idempotency_key) BETWEEN 1 AND 256),
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    region_id UUID NOT NULL REFERENCES fleet_regions(id),
    cluster_id UUID REFERENCES clusters(id),
    source_kind TEXT NOT NULL
        CHECK (
            source_kind IN (
                'model_invocation',
                'control_plane',
                'connector',
                'execution_agent',
                'observability',
                'object_storage',
                'synthetic_probe'
            )
        ),
    workload_kind TEXT NOT NULL
        CHECK (
            workload_kind IN (
                'incident',
                'diagnostic_pack',
                'workflow',
                'inspection',
                'verification',
                'rollback',
                'audit',
                'system'
            )
        ),
    provider_profile TEXT,
    model_family TEXT,
    incident_id UUID REFERENCES sre_incidents(id),
    pack_id TEXT,
    workflow_id TEXT,
    request_count BIGINT NOT NULL CHECK (request_count >= 0),
    input_tokens BIGINT NOT NULL CHECK (input_tokens >= 0),
    output_tokens BIGINT NOT NULL CHECK (output_tokens >= 0),
    latency_millis BIGINT NOT NULL CHECK (latency_millis >= 0),
    error_count BIGINT NOT NULL CHECK (error_count >= 0),
    quantity_millis BIGINT NOT NULL CHECK (quantity_millis >= 0),
    cost_micros BIGINT NOT NULL CHECK (cost_micros >= 0),
    currency TEXT NOT NULL DEFAULT 'USD' CHECK (currency = 'USD'),
    occurred_at TIMESTAMPTZ NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, idempotency_key),
    CHECK (error_count <= request_count OR request_count = 0),
    CHECK (
        source_kind = 'model_invocation'
        OR (provider_profile IS NULL AND model_family IS NULL)
    )
);

CREATE INDEX finops_cost_ledger_window
    ON finops_cost_ledger (tenant_id, occurred_at DESC);
CREATE INDEX finops_cost_ledger_dimensions
    ON finops_cost_ledger (
        tenant_id,
        region_id,
        cluster_id,
        source_kind,
        provider_profile,
        model_family,
        occurred_at DESC
    );

CREATE TABLE finops_budgets (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    scope_kind TEXT NOT NULL
        CHECK (
            scope_kind IN (
                'tenant',
                'provider',
                'model',
                'region',
                'cluster',
                'incident',
                'diagnostic_pack',
                'workflow'
            )
        ),
    scope_key TEXT NOT NULL CHECK (char_length(scope_key) BETWEEN 1 AND 256),
    budget_version BIGINT NOT NULL CHECK (budget_version > 0),
    period_kind TEXT NOT NULL CHECK (period_kind IN ('hourly', 'daily', 'monthly')),
    soft_limit_micros BIGINT NOT NULL CHECK (soft_limit_micros >= 0),
    hard_limit_micros BIGINT NOT NULL CHECK (hard_limit_micros > 0),
    owner_name TEXT NOT NULL CHECK (char_length(owner_name) BETWEEN 1 AND 256),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (soft_limit_micros <= hard_limit_micros),
    UNIQUE (tenant_id, scope_kind, scope_key, budget_version)
);

CREATE UNIQUE INDEX finops_budgets_active_scope
    ON finops_budgets (tenant_id, scope_kind, scope_key)
    WHERE active;

CREATE TABLE finops_budget_decisions (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    cluster_id UUID REFERENCES clusters(id),
    budget_id UUID NOT NULL REFERENCES finops_budgets(id),
    work_class TEXT NOT NULL
        CHECK (
            work_class IN (
                'safety_check',
                'audit',
                'verification',
                'rollback',
                'active_incident',
                'interactive',
                'background'
            )
        ),
    requested_cost_micros BIGINT NOT NULL CHECK (requested_cost_micros >= 0),
    observed_cost_micros BIGINT NOT NULL CHECK (observed_cost_micros >= 0),
    projected_cost_micros BIGINT NOT NULL CHECK (projected_cost_micros >= 0),
    soft_limit_micros BIGINT NOT NULL CHECK (soft_limit_micros >= 0),
    hard_limit_micros BIGINT NOT NULL CHECK (hard_limit_micros > 0),
    allowed BOOLEAN NOT NULL,
    degradation TEXT NOT NULL
        CHECK (
            degradation IN (
                'none',
                'prefer_lower_cost_model',
                'reduce_sampling',
                'defer_low_priority',
                'deny_low_priority'
            )
        ),
    reason_code TEXT NOT NULL,
    protected_controls TEXT[] NOT NULL,
    evaluated_at TIMESTAMPTZ NOT NULL,
    CHECK (soft_limit_micros <= hard_limit_micros),
    CHECK (
        work_class NOT IN ('safety_check', 'audit', 'verification', 'rollback')
        OR (allowed AND degradation = 'none')
    )
);

CREATE INDEX finops_budget_decisions_scope
    ON finops_budget_decisions (tenant_id, budget_id, evaluated_at DESC);

CREATE TABLE finops_allocation_policies (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    policy_version BIGINT NOT NULL CHECK (policy_version > 0),
    allocation_mode TEXT NOT NULL CHECK (allocation_mode IN ('showback', 'chargeback')),
    allocation_keys JSONB NOT NULL CHECK (jsonb_typeof(allocation_keys) = 'array'),
    organization_confirmed BOOLEAN NOT NULL,
    owner_name TEXT NOT NULL CHECK (char_length(owner_name) BETWEEN 1 AND 256),
    active BOOLEAN NOT NULL DEFAULT TRUE,
    created_at TIMESTAMPTZ NOT NULL,
    CHECK (
        allocation_mode = 'showback'
        OR (organization_confirmed AND jsonb_array_length(allocation_keys) > 0)
    ),
    UNIQUE (tenant_id, policy_version)
);

CREATE UNIQUE INDEX finops_allocation_policies_active
    ON finops_allocation_policies (tenant_id)
    WHERE active;

CREATE FUNCTION reject_finops_append_only_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'FinOps ledger and decision history are append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER finops_cost_ledger_append_only
BEFORE UPDATE OR DELETE ON finops_cost_ledger
FOR EACH ROW EXECUTE FUNCTION reject_finops_append_only_mutation();

CREATE TRIGGER finops_budget_decisions_append_only
BEFORE UPDATE OR DELETE ON finops_budget_decisions
FOR EACH ROW EXECUTE FUNCTION reject_finops_append_only_mutation();
