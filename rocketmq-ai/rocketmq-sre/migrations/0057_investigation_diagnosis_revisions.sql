-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE investigation_diagnosis_revisions (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    investigation_id UUID NOT NULL REFERENCES investigations(id),
    conversation_id UUID NOT NULL REFERENCES conversations(id),
    turn_id UUID NOT NULL REFERENCES conversation_turns(id),
    answer_revision_id UUID NOT NULL REFERENCES conversation_answer_revisions(id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    pack_id TEXT NOT NULL CHECK (char_length(pack_id) BETWEEN 1 AND 128),
    pack_version TEXT NOT NULL CHECK (char_length(pack_version) BETWEEN 1 AND 64),
    status TEXT NOT NULL CHECK (status IN ('healthy', 'fault', 'inconclusive', 'unsupported')),
    rule_result JSONB NOT NULL,
    hypotheses JSONB NOT NULL,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    primary_model_invocation_id UUID REFERENCES model_invocations(id),
    execution_eligible BOOLEAN NOT NULL DEFAULT FALSE CHECK (execution_eligible = FALSE),
    partial BOOLEAN NOT NULL,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (investigation_id, revision),
    UNIQUE (turn_id),
    UNIQUE (answer_revision_id)
);

CREATE INDEX investigation_diagnosis_revisions_scope
    ON investigation_diagnosis_revisions (tenant_id, cluster_id, investigation_id, revision DESC);
