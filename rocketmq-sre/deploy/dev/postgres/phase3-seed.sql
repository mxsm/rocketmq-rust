-- Copyright 2026 The RocketMQ Rust Authors
-- Licensed under the Apache License, Version 2.0.

-- Development-only identity graph for supervised planning. Run this after the
-- Control Plane has applied migrations through 0027. It creates no approval,
-- lease, execution, credential, or cluster mutation.

INSERT INTO clusters (
    id,
    tenant_id,
    external_cluster_key,
    environment,
    region,
    rocketmq_version,
    deployment_mode,
    owner_name,
    requested_access_profile,
    effective_access_profile,
    onboarding_state
) VALUES (
    '03000000-0000-4000-8000-000000000001',
    '03000000-0000-4000-8000-000000000002',
    'phase3-dev-cluster',
    'development',
    'local',
    'dev',
    'docker-compose',
    'rocketmq-sre-dev',
    'read_only',
    'read_only',
    'ready_read_only'
) ON CONFLICT (id) DO NOTHING;

INSERT INTO sre_incidents (
    id,
    tenant_id,
    cluster_id,
    title,
    resource,
    symptom_family,
    fingerprint,
    status,
    workflow_checkpoint,
    created_by_subject,
    created_at,
    updated_at
) VALUES (
    '03000000-0000-4000-8000-000000000003',
    '03000000-0000-4000-8000-000000000002',
    '03000000-0000-4000-8000-000000000001',
    'Phase 3 supervised execution fixture',
    'deployment/default/proxy',
    'proxy_saturation',
    'sha256:0303030303030303030303030303030303030303030303030303030303030303',
    'diagnosing',
    '{}'::JSONB,
    'phase3-dev-seed',
    NOW(),
    NOW()
) ON CONFLICT (id) DO NOTHING;

INSERT INTO diagnosis_revisions (
    id,
    incident_id,
    revision,
    status,
    rule_result,
    hypotheses,
    evidence_ids,
    primary_model_invocation_id,
    execution_eligible,
    partial,
    created_at
) VALUES (
    '03000000-0000-4000-8000-000000000004',
    '03000000-0000-4000-8000-000000000003',
    1,
    'confirmed',
    '{"fixture":true}'::JSONB,
    '[]'::JSONB,
    '{}',
    NULL,
    FALSE,
    FALSE,
    NOW()
) ON CONFLICT (id) DO NOTHING;

INSERT INTO model_profiles (
    id,
    tenant_id,
    profile_name,
    provider_family,
    protocol_family,
    model_family,
    model_name,
    model_revision,
    endpoint_instance,
    region,
    data_residency,
    data_classes,
    capabilities,
    priority,
    credential_ref,
    credential_owner,
    enabled,
    health,
    created_at,
    updated_at
) VALUES (
    '03000000-0000-4000-8000-000000000005',
    '03000000-0000-4000-8000-000000000002',
    'phase3-dev-fixture',
    'openai-compatible',
    'openai-compatible',
    'fixture-family',
    'fixture-model',
    'fixture-r1',
    'fixture-endpoint',
    'local',
    'local',
    '[]'::JSONB,
    '{"structured_output":true}'::JSONB,
    100,
    'development-fixture-no-secret',
    'gateway',
    TRUE,
    'healthy',
    NOW(),
    NOW()
) ON CONFLICT (id) DO NOTHING;

INSERT INTO model_invocations (
    id,
    tenant_id,
    cluster_id,
    incident_id,
    diagnosis_revision_id,
    parent_invocation_id,
    purpose,
    requested_profile_id,
    actual_profile_id,
    provider_family,
    model_family,
    model_revision,
    endpoint_instance,
    fallback_chain,
    prompt_version,
    schema_version,
    rationale,
    started_at,
    completed_at
) VALUES (
    '03000000-0000-4000-8000-000000000006',
    '03000000-0000-4000-8000-000000000002',
    '03000000-0000-4000-8000-000000000001',
    '03000000-0000-4000-8000-000000000003',
    '03000000-0000-4000-8000-000000000004',
    NULL,
    'primary_diagnosis',
    '03000000-0000-4000-8000-000000000005',
    '03000000-0000-4000-8000-000000000005',
    'openai-compatible',
    'fixture-family',
    'fixture-r1',
    'fixture-endpoint',
    '{}',
    'phase3-dev',
    'rocketmq-sre.model.v1',
    'development-only fixture invocation',
    NOW(),
    NOW()
) ON CONFLICT (id) DO NOTHING;

INSERT INTO model_invocations (
    id,
    tenant_id,
    cluster_id,
    incident_id,
    diagnosis_revision_id,
    parent_invocation_id,
    purpose,
    requested_profile_id,
    actual_profile_id,
    provider_family,
    model_family,
    model_revision,
    endpoint_instance,
    fallback_chain,
    prompt_version,
    schema_version,
    rationale,
    started_at,
    completed_at
) VALUES (
    '03000000-0000-4000-8000-000000000007',
    '03000000-0000-4000-8000-000000000002',
    '03000000-0000-4000-8000-000000000001',
    '03000000-0000-4000-8000-000000000003',
    '03000000-0000-4000-8000-000000000004',
    '03000000-0000-4000-8000-000000000006',
    'critic',
    '03000000-0000-4000-8000-000000000005',
    '03000000-0000-4000-8000-000000000005',
    'openai-compatible',
    'fixture-family',
    'fixture-r1',
    'fixture-endpoint',
    '{}',
    'phase3-dev-critic',
    'rocketmq-sre.critic.v1',
    'development-only heterogeneous critic fixture',
    NOW(),
    NOW()
) ON CONFLICT (id) DO NOTHING;

UPDATE diagnosis_revisions
SET primary_model_invocation_id = '03000000-0000-4000-8000-000000000006',
    execution_eligible = TRUE
WHERE id = '03000000-0000-4000-8000-000000000004';
