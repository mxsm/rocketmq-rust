import type { ModelProfileLifecycleView } from "@/api/types";

import {
  allowedLifecycleTransitions,
  canGovernModels,
  lifecycleBadgeVariant,
  summarizeModelGovernance,
} from "./modelLifecycle";

describe("model lifecycle presentation", () => {
  it("keeps retired terminal and quarantine recovery progressive", () => {
    expect(allowedLifecycleTransitions("retired")).toEqual([]);
    expect(allowedLifecycleTransitions("quarantined")).toEqual([
      "certified",
      "retired",
    ]);
    expect(allowedLifecycleTransitions("promoted")).not.toContain(
      "certified",
    );
    expect(lifecycleBadgeVariant("quarantined")).toBe("destructive");
  });

  it("requires a dedicated governance role", () => {
    expect(canGovernModels(["operator", "approver"])).toBe(false);
    expect(canGovernModels(["model-governance"])).toBe(true);
    expect(canGovernModels(["rocketmq:model-governance"])).toBe(true);
    expect(canGovernModels(["sre-admin"])).toBe(true);
  });

  it("summarizes actual smoke and routing eligibility", () => {
    const profiles = [
      profile("promoted", true, true),
      profile("certified", true, false),
      profile("quarantined", false, false),
      profile("draft", false, false),
    ];

    expect(summarizeModelGovernance(profiles)).toEqual({
      promoted: 1,
      healthy: 2,
      quarantined: 1,
      automationEligible: 1,
    });
  });
});

function profile(
  state: ModelProfileLifecycleView["state"],
  smokeOk: boolean,
  automationEligible: boolean,
): ModelProfileLifecycleView {
  return {
    profile_id: crypto.randomUUID(),
    profile_name: `${state}-profile`,
    provider_family: "deepseek",
    model_family: "deepseek-reasoner",
    model_revision: "v1",
    state,
    revision: 1,
    reason_code: "test",
    operator_confirmed: false,
    updated_by: "test",
    updated_at: "2026-07-29T00:00:00Z",
    latest_smoke: {
      id: crypto.randomUUID(),
      profile_id: crypto.randomUUID(),
      connectivity_ok: smokeOk,
      structured_output_ok: smokeOk,
      tool_arguments_ok: smokeOk,
      evidence_citation_ok: smokeOk,
      overall_ok: smokeOk,
      failure_codes: smokeOk ? [] : ["provider_timeout"],
      result_snapshot: {},
      observed_at: "2026-07-29T00:00:00Z",
    },
    automation_eligible: automationEligible,
  };
}
