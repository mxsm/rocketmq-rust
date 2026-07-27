-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE model_profiles
    ADD COLUMN endpoint_url TEXT NOT NULL DEFAULT '',
    ADD COLUMN dialect TEXT NOT NULL DEFAULT 'open_ai',
    ADD COLUMN allowed_data_classes JSONB NOT NULL DEFAULT '[]'::JSONB,
    ADD COLUMN estimated_cost_microusd_per_1k_tokens BIGINT,
    ADD COLUMN preserve_reasoning_content BOOLEAN NOT NULL DEFAULT FALSE,
    ADD COLUMN kimi_mfjs_enabled BOOLEAN NOT NULL DEFAULT FALSE;

ALTER TABLE model_profiles
    DROP CONSTRAINT model_profiles_health_check;

ALTER TABLE model_profiles
    ADD CONSTRAINT model_profiles_health_check
    CHECK (health IN ('unknown', 'healthy', 'degraded', 'unavailable', 'quarantined', 'disabled'));

ALTER TABLE model_invocations
    ADD COLUMN correlation_id UUID,
    ADD COLUMN actual_model TEXT NOT NULL DEFAULT '';

CREATE INDEX model_invocations_correlation
    ON model_invocations (tenant_id, correlation_id, started_at DESC);
