import type {
  ModelProfileLifecyclePage,
  ProviderSmokeResult,
} from "@/api/types";

const OBSERVED_AT = "2026-07-29T01:42:00Z";

function passingSmoke(
  id: string,
  profileId: string,
  latencyMs: number,
): ProviderSmokeResult {
  return {
    id,
    profile_id: profileId,
    connectivity_ok: true,
    structured_output_ok: true,
    tool_arguments_ok: true,
    evidence_citation_ok: true,
    overall_ok: true,
    latency_ms: latencyMs,
    failure_codes: [],
    result_snapshot: {
      calls_attempted: 3,
      bounded_response: true,
      cluster_mutation_performed: false,
    },
    observed_at: OBSERVED_AT,
  };
}

export const phase4ModelLifecycle: ModelProfileLifecyclePage = {
  schema_version: "rocketmq-sre.model-profile-lifecycle.v1",
  observed_at: OBSERVED_AT,
  items: [
    {
      profile_id: "70000000-0000-4000-8000-000000000002",
      profile_name: "deepseek-prod",
      provider_family: "deepseek",
      model_family: "deepseek-reasoner",
      model_revision: "deepseek-reasoner-2026-07",
      state: "promoted",
      revision: 7,
      rollback_profile_id: "70000000-0000-4000-8000-000000000003",
      reason_code: "operator.promoted",
      operator_confirmed: true,
      updated_by: "sre-oncall@example.com",
      updated_at: "2026-07-29T01:45:00Z",
      latest_smoke: passingSmoke(
        "74000000-0000-4000-8000-000000000001",
        "70000000-0000-4000-8000-000000000002",
        842,
      ),
      automation_eligible: true,
    },
    {
      profile_id: "70000000-0000-4000-8000-000000000003",
      profile_name: "zhipu-glm-prod",
      provider_family: "zhipu-glm",
      model_family: "glm",
      model_revision: "glm-4-plus-2026-06",
      state: "certified",
      revision: 4,
      reason_code: "smoke.certified",
      operator_confirmed: true,
      updated_by: "model-governance@example.com",
      updated_at: "2026-07-28T23:18:00Z",
      latest_smoke: passingSmoke(
        "74000000-0000-4000-8000-000000000002",
        "70000000-0000-4000-8000-000000000003",
        1_126,
      ),
      automation_eligible: false,
    },
    {
      profile_id: "70000000-0000-4000-8000-000000000004",
      profile_name: "kimi-prod",
      provider_family: "kimi-moonshot",
      model_family: "moonshot",
      model_revision: "moonshot-v1-128k",
      state: "quarantined",
      revision: 3,
      reason_code: "provider_smoke_failed",
      operator_confirmed: false,
      updated_by: "provider-smoke-worker",
      updated_at: "2026-07-29T01:42:00Z",
      latest_smoke: {
        id: "74000000-0000-4000-8000-000000000003",
        profile_id: "70000000-0000-4000-8000-000000000004",
        connectivity_ok: false,
        structured_output_ok: false,
        tool_arguments_ok: false,
        evidence_citation_ok: false,
        overall_ok: false,
        latency_ms: 5_000,
        failure_codes: ["provider_timeout"],
        result_snapshot: {
          calls_attempted: 1,
          bounded_response: true,
          cluster_mutation_performed: false,
        },
        observed_at: OBSERVED_AT,
      },
      automation_eligible: false,
    },
  ],
};
