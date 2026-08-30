-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

ALTER TABLE critic_reviews
    ADD COLUMN diagnosis_revision_id UUID REFERENCES diagnosis_revisions(id),
    ADD COLUMN fallback_chain TEXT[] NOT NULL DEFAULT '{}',
    ADD COLUMN prompt_version TEXT,
    ADD COLUMN schema_version TEXT,
    ADD COLUMN payload_hash TEXT;

UPDATE critic_reviews AS review
SET diagnosis_revision_id = plan.diagnosis_revision_id,
    fallback_chain = ARRAY(
        SELECT jsonb_array_elements_text(
            COALESCE(review.review_snapshot -> 'fallback_chain', '[]'::JSONB)
        )
    ),
    prompt_version = COALESCE(review.review_snapshot ->> 'prompt_version', 'legacy-unknown'),
    schema_version = COALESCE(review.review_snapshot ->> 'schema_version', 'legacy-unknown'),
    payload_hash = COALESCE(review.review_snapshot ->> 'payload_hash', review.review_hash)
FROM action_plans AS plan
WHERE plan.id = review.plan_id;

ALTER TABLE critic_reviews
    ALTER COLUMN diagnosis_revision_id SET NOT NULL,
    ALTER COLUMN prompt_version SET NOT NULL,
    ALTER COLUMN schema_version SET NOT NULL,
    ALTER COLUMN payload_hash SET NOT NULL,
    ALTER COLUMN critic_invocation_id DROP NOT NULL,
    ALTER COLUMN critic_model_family DROP NOT NULL,
    ALTER COLUMN critic_provider DROP NOT NULL,
    ALTER COLUMN critic_profile DROP NOT NULL,
    ALTER COLUMN critic_model_revision DROP NOT NULL,
    ALTER COLUMN endpoint_instance DROP NOT NULL,
    ADD CONSTRAINT critic_reviews_payload_hash_format
        CHECK (payload_hash ~ '^sha256:[0-9A-Fa-f]{64}$'),
    ADD CONSTRAINT critic_reviews_invocation_identity_complete
        CHECK (
            (critic_invocation_id IS NULL
                AND critic_model_family IS NULL
                AND critic_provider IS NULL
                AND critic_profile IS NULL
                AND critic_model_revision IS NULL
                AND endpoint_instance IS NULL)
            OR
            (critic_invocation_id IS NOT NULL
                AND critic_model_family IS NOT NULL
                AND critic_provider IS NOT NULL
                AND critic_profile IS NOT NULL
                AND critic_model_revision IS NOT NULL
                AND endpoint_instance IS NOT NULL)
        );

CREATE INDEX critic_reviews_exact_gate
    ON critic_reviews (
        plan_id,
        plan_hash,
        diagnosis_revision_id,
        primary_invocation_id,
        created_at DESC
    );
