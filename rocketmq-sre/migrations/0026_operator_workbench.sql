-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE inspection_runs
    DROP CONSTRAINT inspection_runs_template_check,
    ADD CONSTRAINT inspection_runs_template_check CHECK (
        template IN (
            'cluster_health',
            'consumer',
            'broker',
            'telemetry',
            'full_cluster',
            'producer_consumer',
            'store_ha',
            'routing_proxy',
            'security',
            'upgrade',
            'disaster_recovery'
        )
    );

ALTER TABLE sre_incidents
    ADD COLUMN acknowledged_at TIMESTAMPTZ,
    ADD COLUMN acknowledged_by TEXT,
    ADD COLUMN assigned_at TIMESTAMPTZ,
    ADD COLUMN suppressed_until TIMESTAMPTZ,
    ADD COLUMN suppression_reason TEXT,
    ADD COLUMN merged_into_incident_id UUID REFERENCES sre_incidents(id),
    ADD COLUMN sla_ack_due_at TIMESTAMPTZ,
    ADD COLUMN sla_resolve_due_at TIMESTAMPTZ;

UPDATE sre_incidents
SET sla_ack_due_at = created_at + CASE severity
        WHEN 'critical' THEN INTERVAL '15 minutes'
        WHEN 'error' THEN INTERVAL '30 minutes'
        WHEN 'warning' THEN INTERVAL '2 hours'
        ELSE INTERVAL '8 hours'
    END,
    sla_resolve_due_at = created_at + CASE severity
        WHEN 'critical' THEN INTERVAL '4 hours'
        WHEN 'error' THEN INTERVAL '8 hours'
        WHEN 'warning' THEN INTERVAL '24 hours'
        ELSE INTERVAL '72 hours'
    END;

ALTER TABLE sre_incidents
    ALTER COLUMN sla_ack_due_at SET NOT NULL,
    ALTER COLUMN sla_resolve_due_at SET NOT NULL,
    ALTER COLUMN sla_ack_due_at SET DEFAULT (NOW() + INTERVAL '8 hours'),
    ALTER COLUMN sla_resolve_due_at SET DEFAULT (NOW() + INTERVAL '72 hours'),
    ADD CONSTRAINT sre_incidents_suppression_window CHECK (
        suppressed_until IS NULL OR suppressed_until > created_at
    ),
    ADD CONSTRAINT sre_incidents_merge_target CHECK (
        merged_into_incident_id IS NULL OR merged_into_incident_id <> id
    ),
    ADD CONSTRAINT sre_incidents_sla_order CHECK (
        sla_resolve_due_at >= sla_ack_due_at
    );

CREATE INDEX sre_incidents_operator_inbox
    ON sre_incidents (
        tenant_id,
        cluster_id,
        acknowledged_at,
        suppressed_until,
        sla_ack_due_at,
        sla_resolve_due_at
    )
    WHERE status NOT IN ('resolved', 'escalated');

CREATE TABLE incident_operations (
    sequence_id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    operation_id UUID NOT NULL UNIQUE,
    tenant_id UUID NOT NULL,
    cluster_id UUID NOT NULL REFERENCES clusters(id),
    incident_id UUID NOT NULL REFERENCES sre_incidents(id),
    operation_kind TEXT NOT NULL CHECK (
        operation_kind IN ('acknowledge', 'assign', 'merge', 'split', 'suppress', 'reopen')
    ),
    actor_subject TEXT NOT NULL,
    reason TEXT,
    related_incident_id UUID REFERENCES sre_incidents(id),
    details JSONB NOT NULL DEFAULT '{}'::JSONB,
    correlation_id UUID NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL
);

CREATE INDEX incident_operations_history
    ON incident_operations (tenant_id, cluster_id, incident_id, sequence_id);
