-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Run diagnoses have no synthetic conversation, turn, or answer foreign key.
CREATE TABLE investigation_run_revisions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL,
    investigation_id UUID NOT NULL,
    run_id UUID NOT NULL,
    step_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    revision_snapshot JSONB NOT NULL CHECK (jsonb_typeof(revision_snapshot) = 'object'),
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, run_id, attempt_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES investigation_runs(tenant_id, id),
    CHECK (revision_snapshot->>'id' = id::text),
    CHECK (revision_snapshot->>'run_id' = run_id::text),
    CHECK (revision_snapshot #>> '{scope,tenant_id}' = tenant_id::text),
    CHECK (revision_snapshot #>> '{scope,cluster_id}' = cluster_id::text),
    CHECK (revision_snapshot #>> '{scope,investigation_id}' = investigation_id::text),
    CHECK (revision_snapshot->>'step_id' = step_id::text),
    CHECK (revision_snapshot->>'attempt_id' = attempt_id::text)
);
CREATE TRIGGER investigation_run_revisions_append_only
    BEFORE UPDATE OR DELETE ON investigation_run_revisions
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

-- Written in the admission transaction before a query may reach a connector.
-- History establishes the original primary requirement after pack refinement.
CREATE TABLE investigation_run_reads (
    tenant_id UUID NOT NULL,
    run_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    read_snapshot JSONB NOT NULL CHECK (jsonb_typeof(read_snapshot) = 'object'),
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, run_id, attempt_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES investigation_runs(tenant_id, id),
    CHECK (read_snapshot #>> '{scope,tenant_id}' = tenant_id::text),
    CHECK (read_snapshot #>> '{query,tenant_id}' = tenant_id::text),
    CHECK (read_snapshot #>> '{query,query_id}' IS NOT NULL),
    CHECK (read_snapshot #>> '{query,correlation_id}' IS NOT NULL)
);
CREATE UNIQUE INDEX investigation_run_reads_query_identity ON investigation_run_reads ((read_snapshot #>> '{query,query_id}'));
CREATE UNIQUE INDEX investigation_run_reads_correlation_identity ON investigation_run_reads ((read_snapshot #>> '{query,correlation_id}'));
CREATE TRIGGER investigation_run_reads_append_only
    BEFORE UPDATE OR DELETE ON investigation_run_reads
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE TABLE investigation_run_model_requests (
    tenant_id UUID NOT NULL,
    run_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    correlation_id UUID NOT NULL UNIQUE,
    response_byte_limit BIGINT NOT NULL CHECK (response_byte_limit = 65536),
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, run_id, attempt_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES investigation_runs(tenant_id, id)
);
CREATE TRIGGER investigation_run_model_requests_append_only
    BEFORE UPDATE OR DELETE ON investigation_run_model_requests
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

CREATE TABLE investigation_run_explanations (
    tenant_id UUID NOT NULL,
    run_id UUID NOT NULL,
    attempt_id UUID NOT NULL,
    invocation_id UUID NOT NULL REFERENCES model_invocations(id),
    summary TEXT CHECK (summary IS NULL OR char_length(summary) <= 8000),
    recorded_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (tenant_id, run_id, attempt_id),
    FOREIGN KEY (tenant_id, run_id) REFERENCES investigation_runs(tenant_id, id)
);
CREATE TRIGGER investigation_run_explanations_append_only
    BEFORE UPDATE OR DELETE ON investigation_run_explanations
    FOR EACH ROW EXECUTE FUNCTION rocketmq_sre_reject_append_only_change();

ALTER TABLE investigation_runs ADD COLUMN dispatch_checked_at TIMESTAMPTZ;
CREATE INDEX investigation_runs_dispatch ON investigation_runs (dispatch_checked_at, id)
    WHERE status IN ('queued', 'running') AND worker_owner IS NULL;
