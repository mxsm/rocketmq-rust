-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Run counters remain canonical decimal strings, preserving the D13 u64 wire
-- contract without narrowing revisions to PostgreSQL's signed BIGINT range.
CREATE TABLE investigation_runs (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    investigation_id UUID NOT NULL REFERENCES investigations(id),
    run_snapshot JSONB NOT NULL CHECK (jsonb_typeof(run_snapshot) = 'object'),
    revision TEXT NOT NULL CHECK (revision ~ '^(0|[1-9][0-9]{0,19})$'),
    status TEXT NOT NULL CHECK (status IN (
        'queued','running','pausing','paused','needs_evidence',
        'completed','inconclusive','cancelling','cancelled','failed'
    )),
    event_sequence BIGINT NOT NULL DEFAULT 0 CHECK (event_sequence >= 0),
    event_floor BIGINT NOT NULL DEFAULT 1 CHECK (event_floor >= 1),
    worker_owner UUID,
    worker_epoch BIGINT NOT NULL DEFAULT 0 CHECK (worker_epoch >= 0),
    worker_lease_until TIMESTAMPTZ,
    recovery_checked_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, id),
    CHECK ((worker_owner IS NULL) = (worker_lease_until IS NULL)),
    CHECK (run_snapshot->>'id' = id::text),
    CHECK (run_snapshot #>> '{initial_scope,tenant_id}' = tenant_id::text),
    CHECK (run_snapshot #>> '{initial_scope,cluster_id}' = cluster_id::text),
    CHECK (run_snapshot #>> '{initial_scope,investigation_id}' = investigation_id::text),
    CHECK (run_snapshot->>'revision' = revision),
    CHECK (run_snapshot->>'status' = status)
);

CREATE INDEX investigation_runs_parent
    ON investigation_runs (tenant_id, investigation_id, created_at DESC, id);
CREATE INDEX investigation_runs_recovery
    ON investigation_runs (worker_lease_until, id)
    WHERE worker_owner IS NOT NULL;

CREATE TABLE investigation_run_creations (
    tenant_id UUID NOT NULL,
    run_id UUID NOT NULL,
    actor_subject TEXT NOT NULL CHECK (char_length(actor_subject) BETWEEN 1 AND 255),
    idempotency_key TEXT NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 128),
    response_snapshot JSONB NOT NULL CHECK (jsonb_typeof(response_snapshot) = 'object'),
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, run_id, actor_subject, idempotency_key),
    FOREIGN KEY (tenant_id, run_id) REFERENCES investigation_runs(tenant_id, id)
);

CREATE TABLE investigation_run_commands (
    tenant_id UUID NOT NULL,
    run_id UUID NOT NULL,
    actor_subject TEXT NOT NULL CHECK (char_length(actor_subject) BETWEEN 1 AND 255),
    idempotency_key TEXT NOT NULL CHECK (char_length(idempotency_key) BETWEEN 1 AND 128),
    command_id UUID NOT NULL,
    request_snapshot JSONB NOT NULL CHECK (jsonb_typeof(request_snapshot) = 'object'),
    receipt_snapshot JSONB NOT NULL CHECK (jsonb_typeof(receipt_snapshot) = 'object'),
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, run_id, actor_subject, idempotency_key),
    UNIQUE (tenant_id, run_id, command_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES investigation_runs(tenant_id, id)
);

-- Logical event sequence is per run and advances in the snapshot transaction.
-- Retention removes only a bounded prefix; receipts and attempts remain durable.
CREATE TABLE investigation_run_events (
    tenant_id UUID NOT NULL,
    run_id UUID NOT NULL,
    sequence BIGINT NOT NULL CHECK (sequence > 0),
    event_snapshot JSONB NOT NULL CHECK (jsonb_typeof(event_snapshot) = 'object'),
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, run_id, sequence),
    FOREIGN KEY (tenant_id, run_id) REFERENCES investigation_runs(tenant_id, id),
    CHECK (event_snapshot->>'run_id' = run_id::text),
    CHECK (event_snapshot->>'tenant_id' = tenant_id::text),
    CHECK (event_snapshot->>'sequence' = sequence::text)
);

CREATE TRIGGER investigation_run_creations_append_only
    BEFORE UPDATE OR DELETE ON investigation_run_creations
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER investigation_run_commands_append_only
    BEFORE UPDATE OR DELETE ON investigation_run_commands
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
CREATE TRIGGER investigation_run_events_immutable
    BEFORE UPDATE ON investigation_run_events
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

-- Keep the original bounded worker report independently of the canonical
-- distinct-Evidence charge derived while holding the run row lock.
CREATE TABLE investigation_run_results (
    tenant_id UUID NOT NULL,
    run_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    submitted_result JSONB NOT NULL CHECK (jsonb_typeof(submitted_result) = 'object'),
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, run_id, attempt_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES investigation_runs(tenant_id, id),
    CHECK (submitted_result->>'run_id' = run_id::text),
    CHECK (submitted_result->>'attempt_id' = attempt_id::text)
);
CREATE TRIGGER investigation_run_results_append_only
    BEFORE UPDATE OR DELETE ON investigation_run_results
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();
