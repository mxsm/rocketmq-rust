-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE model_invocations
    ADD COLUMN conversation_id UUID REFERENCES conversations(id),
    ADD COLUMN investigation_id UUID REFERENCES investigations(id);

ALTER TABLE model_invocations
    DROP CONSTRAINT model_invocations_purpose_check;

ALTER TABLE model_invocations
    ADD CONSTRAINT model_invocations_purpose_check
    CHECK (
        purpose IN (
            'primary_diagnosis',
            'schema_repair',
            'critic',
            'planner',
            'summary',
            'eval',
            'conversation_tool_selection',
            'conversation_answer'
        )
    );

CREATE INDEX model_invocations_conversation
    ON model_invocations (tenant_id, cluster_id, conversation_id, started_at DESC);

CREATE TABLE conversation_turns (
    id UUID PRIMARY KEY,
    conversation_id UUID NOT NULL REFERENCES conversations(id),
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    sequence INTEGER NOT NULL CHECK (sequence > 0),
    question TEXT NOT NULL CHECK (char_length(question) BETWEEN 1 AND 8192),
    resource TEXT,
    status TEXT NOT NULL CHECK (
        status IN ('collecting', 'answered', 'needs_scope', 'needs_evidence', 'cancelled', 'failed')
    ),
    query_intent JSONB,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    correlation_id UUID NOT NULL,
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ,
    UNIQUE (conversation_id, sequence)
);

CREATE INDEX conversation_turns_scope
    ON conversation_turns (tenant_id, cluster_id, conversation_id, sequence);

CREATE TABLE conversation_answer_revisions (
    id UUID PRIMARY KEY,
    conversation_id UUID NOT NULL REFERENCES conversations(id),
    turn_id UUID NOT NULL REFERENCES conversation_turns(id),
    revision INTEGER NOT NULL CHECK (revision > 0),
    answer TEXT NOT NULL CHECK (char_length(answer) BETWEEN 1 AND 12000),
    mode TEXT NOT NULL CHECK (mode IN ('model_assisted', 'rules_only')),
    citations JSONB NOT NULL DEFAULT '[]'::JSONB,
    evidence_ids UUID[] NOT NULL DEFAULT '{}',
    model_invocation_id UUID REFERENCES model_invocations(id),
    partial BOOLEAN NOT NULL,
    warnings JSONB NOT NULL DEFAULT '[]'::JSONB,
    created_at TIMESTAMPTZ NOT NULL,
    UNIQUE (turn_id, revision)
);

CREATE INDEX conversation_answer_revisions_conversation
    ON conversation_answer_revisions (conversation_id, turn_id, revision DESC);

CREATE TABLE conversation_evidence_links (
    id UUID PRIMARY KEY,
    turn_id UUID NOT NULL REFERENCES conversation_turns(id),
    evidence_id UUID NOT NULL REFERENCES evidence_snapshots(id),
    linked_at TIMESTAMPTZ NOT NULL,
    UNIQUE (turn_id, evidence_id)
);

CREATE INDEX conversation_evidence_links_evidence
    ON conversation_evidence_links (evidence_id, turn_id);
