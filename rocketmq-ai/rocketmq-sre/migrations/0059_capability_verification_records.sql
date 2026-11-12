-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- These attestations describe diagnostic capability validation. They neither
-- authorize execution nor reuse the outcome of an execution as validation.
CREATE TABLE capability_verification_records (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    pack_id TEXT NOT NULL CHECK (char_length(pack_id) BETWEEN 1 AND 128),
    pack_version TEXT NOT NULL CHECK (char_length(pack_version) BETWEEN 1 AND 64),
    level TEXT NOT NULL CHECK (level IN ('test', 'production')),
    outcome TEXT NOT NULL CHECK (outcome IN ('passed','failed','inconclusive','revoked')),
    observed_at TIMESTAMPTZ NOT NULL,
    report_digest TEXT NOT NULL,
    resource TEXT CHECK (char_length(resource) BETWEEN 1 AND 512),
    investigation_id UUID,
    revision_id UUID,
    record_snapshot JSONB NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    CHECK ((investigation_id IS NULL) = (revision_id IS NULL))
);

CREATE INDEX capability_verification_records_scope
    ON capability_verification_records (tenant_id, cluster_id, pack_id, pack_version, observed_at DESC);

CREATE UNIQUE INDEX capability_verification_report_identity
    ON capability_verification_records (tenant_id, cluster_id, pack_id, pack_version, level,
        COALESCE(resource,''), COALESCE(investigation_id,'00000000-0000-0000-0000-000000000000'::uuid),
        COALESCE(revision_id,'00000000-0000-0000-0000-000000000000'::uuid), report_digest);

CREATE TRIGGER capability_verification_records_append_only
    BEFORE UPDATE OR DELETE ON capability_verification_records
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
