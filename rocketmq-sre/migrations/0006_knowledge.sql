-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

CREATE TABLE knowledge_items (
    id UUID PRIMARY KEY,
    tenant_id UUID NOT NULL,
    cluster_id UUID REFERENCES clusters(id),
    title TEXT NOT NULL,
    component TEXT NOT NULL,
    rocketmq_version_range TEXT NOT NULL,
    source_uri TEXT NOT NULL,
    source_version TEXT NOT NULL,
    owner_name TEXT NOT NULL,
    review_status TEXT NOT NULL CHECK (
        review_status IN ('draft', 'in_review', 'validated', 'deprecated', 'expired')
    ),
    review_due_at TIMESTAMPTZ NOT NULL,
    sensitivity TEXT NOT NULL,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    conflict BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL,
    UNIQUE (tenant_id, source_uri, source_version, content_hash)
);

CREATE INDEX knowledge_items_scope
    ON knowledge_items (tenant_id, cluster_id, component, review_status, updated_at DESC);

CREATE TABLE knowledge_chunks (
    id UUID PRIMARY KEY,
    knowledge_item_id UUID NOT NULL REFERENCES knowledge_items(id) ON DELETE CASCADE,
    ordinal INTEGER NOT NULL CHECK (ordinal >= 0),
    heading TEXT,
    content TEXT NOT NULL,
    content_hash TEXT NOT NULL CHECK (content_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    search_document TSVECTOR GENERATED ALWAYS AS (
        setweight(to_tsvector('simple', COALESCE(heading, '')), 'A')
        || setweight(to_tsvector('simple', content), 'B')
    ) STORED,
    UNIQUE (knowledge_item_id, ordinal)
);

CREATE INDEX knowledge_chunks_search
    ON knowledge_chunks USING GIN (search_document);

CREATE TABLE knowledge_feedback (
    id UUID PRIMARY KEY,
    knowledge_item_id UUID NOT NULL REFERENCES knowledge_items(id),
    tenant_id UUID NOT NULL,
    cluster_id UUID REFERENCES clusters(id),
    kind TEXT NOT NULL CHECK (kind IN ('useful', 'incorrect', 'outdated')),
    comment TEXT,
    created_by_subject TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL
);

CREATE TABLE knowledge_review_tasks (
    id UUID PRIMARY KEY,
    knowledge_item_id UUID NOT NULL REFERENCES knowledge_items(id),
    reason TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('open', 'completed', 'dismissed')),
    created_at TIMESTAMPTZ NOT NULL,
    completed_at TIMESTAMPTZ
);

CREATE INDEX knowledge_review_tasks_open
    ON knowledge_review_tasks (knowledge_item_id, created_at DESC)
    WHERE status = 'open';
