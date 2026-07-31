-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE fleet_releases (
    id UUID PRIMARY KEY,
    fleet_id UUID NOT NULL REFERENCES fleets(id),
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    correlation_id UUID NOT NULL,
    release_ref TEXT NOT NULL CHECK (char_length(release_ref) BETWEEN 1 AND 256),
    artifact_digest TEXT NOT NULL CHECK (artifact_digest ~ '^sha256:[0-9A-Fa-f]{64}$'),
    target_version TEXT NOT NULL CHECK (char_length(target_version) BETWEEN 1 AND 128),
    owner_name TEXT NOT NULL CHECK (char_length(owner_name) BETWEEN 1 AND 256),
    maintenance_window_start TIMESTAMPTZ NOT NULL,
    maintenance_window_end TIMESTAMPTZ NOT NULL,
    rollback_artifact_digest TEXT NOT NULL
        CHECK (rollback_artifact_digest ~ '^sha256:[0-9A-Fa-f]{64}$'),
    slo_policy_id TEXT NOT NULL CHECK (char_length(slo_policy_id) BETWEEN 1 AND 256),
    release_status TEXT NOT NULL
        CHECK (
            release_status IN (
                'planned',
                'readiness_checking',
                'ready',
                'canary_running',
                'batch_running',
                'paused',
                'verifying',
                'rolling_back',
                'rolled_back',
                'completed',
                'manual_takeover',
                'failed'
            )
        ),
    active_batch INTEGER CHECK (active_batch IS NULL OR active_batch >= 0),
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, release_ref),
    CHECK (maintenance_window_end > maintenance_window_start)
);

CREATE INDEX fleet_releases_scope
    ON fleet_releases (tenant_id, fleet_id, updated_at DESC);

CREATE TABLE fleet_release_batches (
    fleet_release_id UUID NOT NULL REFERENCES fleet_releases(id) ON DELETE CASCADE,
    batch_sequence INTEGER NOT NULL CHECK (batch_sequence >= 0),
    region_id UUID NOT NULL REFERENCES fleet_regions(id),
    cluster_ids UUID[] NOT NULL,
    max_concurrency INTEGER NOT NULL CHECK (max_concurrency BETWEEN 1 AND 32),
    canary BOOLEAN NOT NULL,
    PRIMARY KEY (fleet_release_id, batch_sequence),
    CHECK (cardinality(cluster_ids) BETWEEN 1 AND 100),
    CHECK (NOT canary OR cardinality(cluster_ids) = 1)
);

CREATE TABLE fleet_release_targets (
    fleet_release_id UUID NOT NULL REFERENCES fleet_releases(id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    region_id UUID NOT NULL REFERENCES fleet_regions(id),
    batch_sequence INTEGER NOT NULL,
    canary BOOLEAN NOT NULL,
    target_state TEXT NOT NULL
        CHECK (
            target_state IN (
                'pending',
                'readiness_checking',
                'ready',
                'ineligible',
                'canary_running',
                'batch_running',
                'paused',
                'rolling_back',
                'rolled_back',
                'completed',
                'skipped',
                'failed'
            )
        ),
    release_id UUID REFERENCES release_workflows(id),
    readiness_reason_codes TEXT[] NOT NULL DEFAULT '{}',
    regression_detected BOOLEAN NOT NULL DEFAULT FALSE,
    sanitized_outcome TEXT,
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (fleet_release_id, cluster_id),
    UNIQUE (release_id),
    FOREIGN KEY (fleet_release_id, batch_sequence)
        REFERENCES fleet_release_batches(fleet_release_id, batch_sequence),
    CHECK (cardinality(readiness_reason_codes) <= 32),
    CHECK (sanitized_outcome IS NULL OR char_length(sanitized_outcome) BETWEEN 1 AND 1024),
    CHECK ((canary AND batch_sequence = 0) OR NOT canary),
    CHECK (
        target_state NOT IN ('ready', 'canary_running', 'batch_running', 'completed')
        OR release_id IS NOT NULL
    )
);

CREATE INDEX fleet_release_targets_scope
    ON fleet_release_targets (tenant_id, fleet_release_id, batch_sequence, target_state);

CREATE TABLE fleet_release_events (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    fleet_release_id UUID NOT NULL REFERENCES fleet_releases(id) ON DELETE CASCADE,
    tenant_id UUID NOT NULL REFERENCES fleet_tenants(id),
    cluster_id UUID REFERENCES clusters(id),
    from_release_status TEXT,
    to_release_status TEXT NOT NULL,
    from_target_state TEXT,
    to_target_state TEXT,
    reason_code TEXT NOT NULL CHECK (char_length(reason_code) BETWEEN 1 AND 128),
    actor_subject TEXT NOT NULL CHECK (char_length(actor_subject) BETWEEN 1 AND 256),
    details JSONB NOT NULL CHECK (jsonb_typeof(details) = 'object'),
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX fleet_release_events_history
    ON fleet_release_events (tenant_id, fleet_release_id, sequence_id);

CREATE FUNCTION reject_fleet_release_event_mutation()
RETURNS TRIGGER AS $$
BEGIN
    RAISE EXCEPTION 'Fleet release events are append-only';
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER fleet_release_events_append_only
BEFORE UPDATE OR DELETE ON fleet_release_events
FOR EACH ROW EXECUTE FUNCTION reject_fleet_release_event_mutation();

CREATE FUNCTION protect_fleet_release_identity()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.fleet_id <> OLD.fleet_id
       OR NEW.tenant_id <> OLD.tenant_id
       OR NEW.correlation_id <> OLD.correlation_id
       OR NEW.release_ref <> OLD.release_ref
       OR NEW.artifact_digest <> OLD.artifact_digest
       OR NEW.target_version <> OLD.target_version
       OR NEW.rollback_artifact_digest <> OLD.rollback_artifact_digest
       OR NEW.slo_policy_id <> OLD.slo_policy_id
       OR NEW.created_at <> OLD.created_at THEN
        RAISE EXCEPTION 'Fleet release identity and immutable definition cannot change';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER fleet_releases_protect_identity
BEFORE UPDATE ON fleet_releases
FOR EACH ROW EXECUTE FUNCTION protect_fleet_release_identity();

CREATE FUNCTION protect_fleet_release_target_scope()
RETURNS TRIGGER AS $$
BEGIN
    IF NEW.fleet_release_id <> OLD.fleet_release_id
       OR NEW.tenant_id <> OLD.tenant_id
       OR NEW.cluster_id <> OLD.cluster_id
       OR NEW.region_id <> OLD.region_id
       OR NEW.batch_sequence <> OLD.batch_sequence
       OR NEW.canary <> OLD.canary THEN
        RAISE EXCEPTION 'Fleet release target scope cannot change';
    END IF;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER fleet_release_targets_protect_scope
BEFORE UPDATE ON fleet_release_targets
FOR EACH ROW EXECUTE FUNCTION protect_fleet_release_target_scope();
