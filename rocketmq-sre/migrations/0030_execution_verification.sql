-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Immutable before/during/after Evidence captured by the descriptor-driven
-- verifier. Evidence remains scoped to the execution and can be linked into
-- the incident timeline by correlation_id without exposing target secrets.
CREATE TABLE execution_verification_evidence (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES executions(id),
    step_id UUID NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    phase TEXT NOT NULL CHECK (phase IN ('pre', 'during', 'post', 'rollback_post')),
    evidence_id UUID NOT NULL,
    evidence_snapshot JSONB NOT NULL,
    observed_at TIMESTAMPTZ NOT NULL,
    UNIQUE (execution_id, step_id, attempt, phase, evidence_id)
);

CREATE INDEX execution_verification_evidence_timeline
    ON execution_verification_evidence (execution_id, step_id, sequence_id);

-- One deterministic verification decision per forward or compensation
-- attempt. The full result keeps the descriptor conditions and Evidence IDs.
CREATE TABLE execution_verifications (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    execution_id UUID NOT NULL REFERENCES executions(id),
    step_id UUID NOT NULL,
    attempt INTEGER NOT NULL CHECK (attempt > 0),
    compensation BOOLEAN NOT NULL DEFAULT FALSE,
    outcome TEXT NOT NULL CHECK (outcome IN ('succeeded', 'failed', 'inconclusive')),
    result_snapshot JSONB NOT NULL,
    started_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ NOT NULL,
    CHECK (completed_at >= started_at),
    UNIQUE (execution_id, step_id, attempt, compensation)
);

CREATE INDEX execution_verifications_timeline
    ON execution_verifications (execution_id, sequence_id);

CREATE TRIGGER execution_verification_evidence_append_only
    BEFORE UPDATE OR DELETE ON execution_verification_evidence
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE TRIGGER execution_verifications_append_only
    BEFORE UPDATE OR DELETE ON execution_verifications
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
