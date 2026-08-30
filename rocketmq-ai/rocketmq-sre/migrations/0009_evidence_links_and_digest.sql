-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE evidence_snapshots
    ADD COLUMN content_digest TEXT;

UPDATE evidence_snapshots
SET content_digest = content_hash
WHERE content_digest IS NULL;

ALTER TABLE evidence_snapshots
    ALTER COLUMN content_digest SET NOT NULL;

ALTER TABLE evidence_snapshots
    ADD CONSTRAINT evidence_snapshots_content_digest_format
    CHECK (content_digest ~ '^sha256:[0-9A-Fa-f]{64}$');

CREATE TABLE evidence_links (
    id UUID PRIMARY KEY,
    evidence_id UUID NOT NULL REFERENCES evidence_snapshots(id),
    investigation_id UUID REFERENCES investigations(id),
    incident_id UUID REFERENCES sre_incidents(id),
    linked_at TIMESTAMPTZ NOT NULL,
    CHECK (investigation_id IS NOT NULL OR incident_id IS NOT NULL)
);

CREATE UNIQUE INDEX evidence_links_unique_target
    ON evidence_links (
        evidence_id,
        COALESCE(investigation_id, '00000000-0000-0000-0000-000000000000'::UUID),
        COALESCE(incident_id, '00000000-0000-0000-0000-000000000000'::UUID)
    );
CREATE INDEX evidence_links_investigation
    ON evidence_links (investigation_id, evidence_id);
CREATE INDEX evidence_links_incident
    ON evidence_links (incident_id, evidence_id);
