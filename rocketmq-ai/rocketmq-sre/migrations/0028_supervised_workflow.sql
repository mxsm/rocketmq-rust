-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Approval grants are service-issued capabilities. Keeping the exact grant
-- beside the immutable human decision allows a later Executor submission to
-- prove the plan/precondition binding without accepting a user-supplied grant.
ALTER TABLE approvals
    ADD COLUMN precondition_hash TEXT
        CHECK (
            precondition_hash IS NULL
            OR precondition_hash ~ '^sha256:[0-9A-Fa-f]{64}$'
        ),
    ADD COLUMN approval_grant_snapshot JSONB;

ALTER TABLE approvals
    ADD CONSTRAINT approvals_grant_matches_decision
    CHECK (
        (
            decision = 'approved'
            AND precondition_hash IS NOT NULL
            AND approval_grant_snapshot IS NOT NULL
        )
        OR (
            decision = 'rejected'
            AND precondition_hash IS NULL
            AND approval_grant_snapshot IS NULL
        )
    );

CREATE INDEX approvals_current_grant
    ON approvals (tenant_id, cluster_id, plan_id, decided_at DESC)
    WHERE decision = 'approved';
