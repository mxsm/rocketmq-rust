-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE model_profiles
    ADD CONSTRAINT model_profiles_id_tenant_unique UNIQUE (id, tenant_id);

ALTER TABLE model_profile_lifecycle
    ADD CONSTRAINT model_profile_lifecycle_profile_tenant_fk
    FOREIGN KEY (profile_id, tenant_id)
    REFERENCES model_profiles(id, tenant_id);

ALTER TABLE provider_smoke_results
    ADD COLUMN tenant_id UUID;

UPDATE provider_smoke_results smoke
SET tenant_id = profile.tenant_id
FROM model_profiles profile
WHERE profile.id = smoke.profile_id;

ALTER TABLE provider_smoke_results
    ALTER COLUMN tenant_id SET NOT NULL,
    ADD CONSTRAINT provider_smoke_results_profile_tenant_fk
        FOREIGN KEY (profile_id, tenant_id)
        REFERENCES model_profiles(id, tenant_id),
    ADD CONSTRAINT provider_smoke_results_snapshot_bound
        CHECK (octet_length(result_snapshot::TEXT) <= 65536);

CREATE INDEX provider_smoke_results_profile_observed
    ON provider_smoke_results (tenant_id, profile_id, observed_at DESC, sequence_id DESC);

CREATE TABLE model_profile_lifecycle_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    profile_id UUID NOT NULL,
    from_state TEXT,
    to_state TEXT NOT NULL CHECK (
        to_state IN ('draft', 'certified', 'promoted', 'quarantined', 'retired')
    ),
    revision BIGINT NOT NULL CHECK (revision > 0),
    rollback_profile_id UUID,
    reason_code TEXT NOT NULL CHECK (char_length(reason_code) BETWEEN 1 AND 128),
    operator_confirmed BOOLEAN NOT NULL,
    changed_by TEXT NOT NULL CHECK (char_length(changed_by) BETWEEN 1 AND 256),
    correlation_id UUID NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    CHECK (
        from_state IS NULL
        OR from_state IN ('draft', 'certified', 'promoted', 'quarantined', 'retired')
    ),
    FOREIGN KEY (profile_id, tenant_id)
        REFERENCES model_profiles(id, tenant_id),
    FOREIGN KEY (rollback_profile_id, tenant_id)
        REFERENCES model_profiles(id, tenant_id)
);

CREATE INDEX model_profile_lifecycle_events_profile
    ON model_profile_lifecycle_events (tenant_id, profile_id, revision DESC, sequence_id DESC);

CREATE TRIGGER model_profile_lifecycle_events_append_only
    BEFORE UPDATE OR DELETE ON model_profile_lifecycle_events
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
