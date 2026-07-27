-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

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
            'eval'
        )
    );
