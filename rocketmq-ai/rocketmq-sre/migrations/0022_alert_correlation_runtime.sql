-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE sre_incidents
    ADD COLUMN alert_correlation_key TEXT,
    ADD COLUMN severity TEXT CHECK (
        severity IS NULL OR severity IN ('info', 'warning', 'error', 'critical')
    ),
    ADD COLUMN owner_name TEXT NOT NULL DEFAULT 'unassigned',
    ADD COLUMN occurrence_count INTEGER NOT NULL DEFAULT 0
        CHECK (occurrence_count >= 0),
    ADD COLUMN last_alert_at TIMESTAMPTZ,
    ADD COLUMN reopened_from_incident_id UUID REFERENCES sre_incidents(id);

CREATE UNIQUE INDEX sre_incidents_alert_correlation_key
    ON sre_incidents (tenant_id, cluster_id, alert_correlation_key)
    WHERE alert_correlation_key IS NOT NULL;

CREATE INDEX sre_incidents_alert_inbox
    ON sre_incidents (
        tenant_id,
        cluster_id,
        status,
        severity,
        last_alert_at DESC,
        id
    );

CREATE INDEX sre_incidents_recurrence
    ON sre_incidents (tenant_id, cluster_id, reopened_from_incident_id)
    WHERE reopened_from_incident_id IS NOT NULL;

ALTER TABLE notification_outbox
    ADD COLUMN claimed_at TIMESTAMPTZ,
    ADD COLUMN claim_token UUID;

CREATE INDEX notification_outbox_claim
    ON notification_outbox (status, claimed_at)
    WHERE status = 'delivering';
