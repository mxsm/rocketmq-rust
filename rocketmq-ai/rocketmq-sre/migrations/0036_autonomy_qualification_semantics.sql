-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- New qualification records use kind-specific immutable identities. Historical
-- rows remain available for reports, while the NOT VALID constraints prevent
-- older non-authoritative fixtures from blocking this forward-only migration.
ALTER TABLE autonomy_qualification_samples
    ADD CONSTRAINT autonomy_sample_execution_binding
    CHECK (
        (
            sample_kind = 'shadow_outcome'
            AND execution_id IS NULL
        )
        OR (
            sample_kind = 'supervised_success'
            AND execution_id IS NOT NULL
        )
    ) NOT VALID;

ALTER TABLE autonomy_qualification_samples
    ADD CONSTRAINT autonomy_sample_qualification_facts
    CHECK (
        qualified = (
            human_outcome_linked
            AND evidence_complete
            AND stable_window_passed
            AND cardinality(reason_codes) = 0
        )
    ) NOT VALID;

-- A supervised execution contributes at most once to one exact Autonomous
-- cohort, regardless of retries or alternate incident/plan request payloads.
CREATE UNIQUE INDEX autonomy_supervised_execution_sample
    ON autonomy_qualification_samples (cohort_id, execution_id)
    WHERE sample_kind = 'supervised_success' AND execution_id IS NOT NULL;

CREATE INDEX autonomy_samples_reconciled_window
    ON autonomy_qualification_samples (
        cohort_id, sample_kind, reconciled_at DESC, observed_at DESC
    );
