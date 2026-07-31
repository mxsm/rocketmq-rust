-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE capacity_forecasts
    ADD COLUMN report JSONB;

ALTER TABLE backlog_eta_forecasts
    ADD COLUMN report JSONB;

ALTER TABLE anomaly_baselines
    ADD COLUMN report JSONB;

ALTER TABLE change_points
    ADD COLUMN report JSONB;

CREATE TABLE anomaly_assessments (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    metric TEXT NOT NULL,
    seasonality TEXT NOT NULL CHECK (seasonality IN ('hourly', 'daily', 'weekly')),
    anomaly BOOLEAN NOT NULL,
    report JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX anomaly_assessments_latest
    ON anomaly_assessments (tenant_id, cluster_id, metric, seasonality, observed_at DESC, id);

ALTER TABLE what_if_simulations
    ADD COLUMN report JSONB,
    ADD COLUMN execution_eligible BOOLEAN NOT NULL DEFAULT FALSE
        CHECK (execution_eligible = FALSE);

ALTER TABLE upgrade_readiness_reports
    ADD COLUMN report JSONB,
    ADD COLUMN execution_eligible BOOLEAN NOT NULL DEFAULT FALSE
        CHECK (execution_eligible = FALSE);

ALTER TABLE dr_readiness_reports
    ADD COLUMN report JSONB,
    ADD COLUMN execution_eligible BOOLEAN NOT NULL DEFAULT FALSE
        CHECK (execution_eligible = FALSE);

CREATE TABLE forecast_actual_outcomes (
    forecast_id UUID NOT NULL REFERENCES capacity_forecasts(id),
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    metric TEXT NOT NULL,
    forecast_window TEXT NOT NULL CHECK (
        forecast_window IN ('seven_days', 'thirty_days')
    ),
    projected_at TIMESTAMPTZ NOT NULL,
    predicted_value DOUBLE PRECISION NOT NULL,
    actual_value DOUBLE PRECISION NOT NULL,
    absolute_error DOUBLE PRECISION NOT NULL CHECK (absolute_error >= 0),
    signed_error DOUBLE PRECISION NOT NULL,
    covered_by_interval BOOLEAN,
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (forecast_id, projected_at)
);

CREATE INDEX forecast_actual_outcomes_scope
    ON forecast_actual_outcomes (
        tenant_id, cluster_id, metric, forecast_window, projected_at DESC
    );
