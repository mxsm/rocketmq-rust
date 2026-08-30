import { ApiError } from "@/api/client";
import type {
  ModelProfileLifecyclePage,
  ModelProfileLifecycleTransitionRequest,
  ModelProfileLifecycleView,
  ModelProfileRollbackRequest,
  ProviderSmokeResult,
} from "@/api/types";
import {
  allowedLifecycleTransitions,
  hasPassingSmoke,
} from "@/features/models/modelLifecycle";

import { phase4ModelLifecycle } from "./phase4ModelDemo";

export interface ModelLifecycleMock {
  list: () => ModelProfileLifecyclePage;
  transition: (
    id: string,
    input: ModelProfileLifecycleTransitionRequest,
    actor: string,
  ) => ModelProfileLifecycleView;
  rollback: (
    id: string,
    input: ModelProfileRollbackRequest,
    actor: string,
  ) => ModelProfileLifecycleView;
  smoke: (id: string) => ProviderSmokeResult;
}

export function createModelLifecycleMock(): ModelLifecycleMock {
  const page = structuredClone(phase4ModelLifecycle);

  const find = (id: string) => {
    const profile = page.items.find((item) => item.profile_id === id);
    if (!profile) {
      throw new ApiError(
        404,
        "source_unavailable",
        "model profile is unavailable",
      );
    }
    return profile;
  };

  const checkRevision = (
    profile: ModelProfileLifecycleView,
    expected: number,
  ) => {
    if (profile.revision !== expected) {
      throw new ApiError(
        409,
        "model_profile_revision_conflict",
        "model profile lifecycle revision changed",
      );
    }
  };

  const update = (
    profile: ModelProfileLifecycleView,
    actor: string,
    reasonCode: string,
  ) => {
    profile.revision += 1;
    profile.reason_code = reasonCode;
    profile.operator_confirmed = true;
    profile.updated_by = actor;
    profile.updated_at = new Date().toISOString();
    profile.automation_eligible =
      profile.state === "promoted" && hasPassingSmoke(profile);
    page.observed_at = profile.updated_at;
    return structuredClone(profile);
  };

  return {
    list: () => structuredClone(page),
    transition: (id, input, actor) => {
      const profile = find(id);
      checkRevision(profile, input.expected_revision);
      if (!input.operator_confirmed) {
        throw new ApiError(
          400,
          "operator_confirmation_required",
          "operator confirmation is required",
        );
      }
      if (
        !allowedLifecycleTransitions(profile.state).includes(
          input.target_state,
        )
      ) {
        throw new ApiError(
          409,
          "invalid_model_lifecycle_transition",
          "model profile lifecycle transition is not allowed",
        );
      }
      if (
        ["certified", "promoted"].includes(input.target_state) &&
        !hasPassingSmoke(profile)
      ) {
        throw new ApiError(
          409,
          "provider_smoke_required",
          "a passing smoke is required",
        );
      }
      if (input.target_state === "promoted" && input.rollback_profile_id) {
        const rollback = find(input.rollback_profile_id);
        if (
          rollback.profile_id === profile.profile_id ||
          !["certified", "promoted"].includes(rollback.state) ||
          !hasPassingSmoke(rollback)
        ) {
          throw new ApiError(
            409,
            "model_rollback_target_unavailable",
            "rollback target is unavailable",
          );
        }
        if (rollback.state === "promoted") {
          rollback.state = "certified";
          update(rollback, actor, "superseded_by_profile");
        }
      }
      profile.state = input.target_state;
      profile.rollback_profile_id =
        input.target_state === "promoted"
          ? input.rollback_profile_id
          : undefined;
      return update(profile, actor, input.reason_code);
    },
    rollback: (id, input, actor) => {
      const profile = find(id);
      checkRevision(profile, input.expected_revision);
      if (!input.operator_confirmed) {
        throw new ApiError(
          400,
          "operator_confirmation_required",
          "operator confirmation is required",
        );
      }
      if (profile.state !== "promoted" || !profile.rollback_profile_id) {
        throw new ApiError(
          409,
          "model_rollback_target_missing",
          "model rollback target is unavailable",
        );
      }
      const target = find(profile.rollback_profile_id);
      if (!hasPassingSmoke(target)) {
        throw new ApiError(
          409,
          "model_rollback_target_unavailable",
          "model rollback target is not healthy",
        );
      }
      const sourceId = profile.profile_id;
      profile.state = "quarantined";
      update(profile, actor, input.reason_code);
      target.state = "promoted";
      target.rollback_profile_id = sourceId;
      return update(target, actor, input.reason_code);
    },
    smoke: (id) => {
      const profile = find(id);
      const observedAt = new Date().toISOString();
      const smoke: ProviderSmokeResult = {
        id: crypto.randomUUID(),
        profile_id: profile.profile_id,
        connectivity_ok: true,
        structured_output_ok: true,
        tool_arguments_ok: true,
        evidence_citation_ok: true,
        overall_ok: true,
        latency_ms: 735,
        failure_codes: [],
        result_snapshot: {
          calls_attempted: 3,
          bounded_response: true,
          cluster_mutation_performed: false,
        },
        observed_at: observedAt,
      };
      profile.latest_smoke = smoke;
      profile.updated_at = observedAt;
      profile.automation_eligible = profile.state === "promoted";
      page.observed_at = observedAt;
      return structuredClone(smoke);
    },
  };
}
