-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE capacity_forecasts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    resource JSONB NOT NULL,
    metric TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('ready', 'insufficient_data', 'stale', 'unstable_trend', 'unsupported')
    ),
    quality TEXT NOT NULL CHECK (quality IN ('low', 'medium', 'high')),
    algorithm_version TEXT NOT NULL,
    sample_start TIMESTAMPTZ NOT NULL,
    sample_end TIMESTAMPTZ NOT NULL,
    coverage_ratio DOUBLE PRECISION NOT NULL CHECK (coverage_ratio BETWEEN 0.0 AND 1.0),
    slope_per_hour DOUBLE PRECISION,
    volatility DOUBLE PRECISION,
    threshold DOUBLE PRECISION,
    exhaustion_at TIMESTAMPTZ,
    points JSONB NOT NULL,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    observed_at TIMESTAMPTZ NOT NULL,
    CHECK (sample_end >= sample_start)
);

CREATE INDEX capacity_forecasts_latest
    ON capacity_forecasts (tenant_id, cluster_id, metric, observed_at DESC, id);

CREATE TABLE backlog_eta_forecasts (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    resource JSONB NOT NULL,
    backlog_kind TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('ready', 'insufficient_data', 'stale', 'unstable_trend', 'unsupported')
    ),
    current_value DOUBLE PRECISION NOT NULL,
    arrival_rate_per_second DOUBLE PRECISION,
    drain_rate_per_second DOUBLE PRECISION,
    estimated_clear_at TIMESTAMPTZ,
    coverage_ratio DOUBLE PRECISION NOT NULL CHECK (coverage_ratio BETWEEN 0.0 AND 1.0),
    algorithm_version TEXT NOT NULL,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    observed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX backlog_eta_latest
    ON backlog_eta_forecasts (tenant_id, cluster_id, backlog_kind, observed_at DESC, id);

CREATE TABLE anomaly_baselines (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    resource JSONB NOT NULL,
    metric TEXT NOT NULL,
    period_seconds BIGINT NOT NULL CHECK (period_seconds > 0),
    median DOUBLE PRECISION NOT NULL,
    median_absolute_deviation DOUBLE PRECISION NOT NULL CHECK (median_absolute_deviation >= 0),
    sample_count INTEGER NOT NULL CHECK (sample_count > 0),
    coverage_ratio DOUBLE PRECISION NOT NULL CHECK (coverage_ratio BETWEEN 0.0 AND 1.0),
    algorithm_version TEXT NOT NULL,
    valid_from TIMESTAMPTZ NOT NULL,
    valid_until TIMESTAMPTZ NOT NULL,
    CHECK (valid_until > valid_from)
);

CREATE INDEX anomaly_baselines_latest
    ON anomaly_baselines (tenant_id, cluster_id, metric, valid_until DESC, id);

CREATE TABLE change_points (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    resource JSONB NOT NULL,
    metric TEXT NOT NULL,
    detected_at TIMESTAMPTZ NOT NULL,
    before_value DOUBLE PRECISION NOT NULL,
    after_value DOUBLE PRECISION NOT NULL,
    score DOUBLE PRECISION NOT NULL,
    algorithm_version TEXT NOT NULL,
    evidence_ids UUID[] NOT NULL DEFAULT '{}'
);

CREATE INDEX change_points_scope
    ON change_points (tenant_id, cluster_id, detected_at DESC, id);

CREATE TABLE what_if_simulations (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    simulation_kind TEXT NOT NULL CHECK (
        simulation_kind IN (
            'broker_offline',
            'proxy_offline',
            'traffic_increase',
            'broker_scale_out',
            'proxy_scale_out',
            'topic_queue_expand',
            'version_upgrade',
            'configuration_diff'
        )
    ),
    status TEXT NOT NULL CHECK (status IN ('completed', 'insufficient_data', 'unsupported')),
    input JSONB NOT NULL,
    assumptions JSONB NOT NULL DEFAULT '[]'::JSONB,
    projected_utilization JSONB NOT NULL,
    bottlenecks JSONB NOT NULL DEFAULT '[]'::JSONB,
    blast_radius JSONB NOT NULL DEFAULT '[]'::JSONB,
    missing_assumptions JSONB NOT NULL DEFAULT '[]'::JSONB,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    algorithm_version TEXT NOT NULL,
    created_by TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX what_if_simulations_scope
    ON what_if_simulations (tenant_id, cluster_id, created_at DESC, id);

CREATE TABLE upgrade_readiness_reports (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    target_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('ready', 'ready_with_warnings', 'blocked', 'insufficient_data')
    ),
    findings JSONB NOT NULL,
    pack_versions JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > observed_at)
);

CREATE INDEX upgrade_readiness_latest
    ON upgrade_readiness_reports (tenant_id, cluster_id, observed_at DESC, id);

CREATE TABLE dr_readiness_reports (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    target_region TEXT,
    requested_rto_seconds BIGINT NOT NULL CHECK (requested_rto_seconds >= 0),
    requested_rpo_seconds BIGINT NOT NULL CHECK (requested_rpo_seconds >= 0),
    status TEXT NOT NULL CHECK (
        status IN ('ready', 'ready_with_warnings', 'blocked', 'insufficient_data')
    ),
    findings JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL,
    CHECK (expires_at > observed_at)
);

CREATE INDEX dr_readiness_latest
    ON dr_readiness_reports (tenant_id, cluster_id, observed_at DESC, id);
