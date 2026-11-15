import { describe, expect, it } from "vitest";

import { readinessMatches, readinessOperation } from "./actionReadinessPresentation";
import { actionReadinessFixture, READINESS_NOW } from "./test-fixtures/actionReadiness";

describe("exact action readiness binding", () => {
  it("accepts the exact candidate with canonical-equivalent JSON key order", () => {
    const { view, request, owner } = actionReadinessFixture({ prepared: true });
    view.binding.parameters = { ttl_seconds: 60, level: "DEBUG", component: "broker", logger: "rocketmq_broker::processor" };
    expect(readinessMatches(view, request, owner)).toBe(true);
    expect(readinessOperation(view, "create_plan", Date.parse(READINESS_NOW))).toBe(true);
    expect(view.stages.qualification.state).toBe("unknown");
    expect(view.execution_readiness).toBe("unknown");
  });

  it.each([
    "tenant_id", "cluster_id", "incident_id", "diagnosis_revision_id", "actor_subject",
    "resource", "action_id", "descriptor_version",
  ] as const)("rejects a foreign %s before releasing projection metadata", (key) => {
    const { view, request, owner } = actionReadinessFixture();
    view.binding[key] = "foreign-scope-marker";
    expect(readinessMatches(view, request, owner)).toBe(false);
  });

  it("rejects changed desired parameters and an unrelated same-resource proof", () => {
    const { view, request, owner } = actionReadinessFixture({ prepared: true });
    view.binding.parameters = { component: "broker", logger: "rocketmq_broker::processor", level: "INFO", ttl_seconds: 60 };
    expect(readinessMatches(view, request, owner)).toBe(false);
    view.binding.parameters = request.parameters;
    view.binding.precondition_evidence!.evidence_id = "unrelated-proof";
    expect(readinessMatches(view, request, owner)).toBe(false);
  });

  it("permits only diagnosis facts when a candidate has no validated parameter echo", () => {
    const { view, request, owner } = actionReadinessFixture({ confirmed: false });
    request.resource = "";
    view.binding.resource = "";
    view.binding.parameters = null;
    view.binding.parameters_digest = null;
    expect(readinessMatches(view, request, owner)).toBe(true);
    view.operations.prepare_precondition = { allowed: true, reason_codes: [] };
    expect(readinessMatches(view, request, owner)).toBe(false);
  });

  it("fails closed on missing stages, private reason text, or a contradictory satisfied summary", () => {
    const fixture = actionReadinessFixture();
    const missing = structuredClone(fixture.view) as unknown as { stages: Record<string, unknown> };
    delete missing.stages.qualification;
    expect(readinessMatches(missing, fixture.request, fixture.owner)).toBe(false);
    const privateReason = structuredClone(fixture.view) as unknown as { stages: { target: { reason_codes: string[] } } };
    privateReason.stages.target.reason_codes = ["private-agent-error-text"];
    expect(readinessMatches(privateReason, fixture.request, fixture.owner)).toBe(false);
    fixture.view.execution_readiness = "satisfied";
    expect(readinessMatches(fixture.view, fixture.request, fixture.owner)).toBe(false);
  });

  it("does not allow an expired or timeless affordance", () => {
    const { view, request, owner } = actionReadinessFixture();
    expect(readinessOperation(view, "prepare_precondition", Date.parse(view.valid_until!))).toBe(false);
    view.valid_until = null;
    expect(readinessOperation(view, "prepare_precondition", Date.parse(READINESS_NOW))).toBe(false);
    expect(readinessMatches(view, request, owner)).toBe(false);
  });

  it("does not turn a target-scope declaration gap into an allowed readonly preparation", () => {
    const { view, request, owner } = actionReadinessFixture();
    view.binding.target_scope_digest = null;
    view.stages.target = {
      state: "blocked", source: "target_scope_configuration", reason_codes: ["target_scope_unverified"],
      evidence_ids: [], observed_at: null, valid_until: null,
    };
    view.operations.prepare_precondition = { allowed: false, reason_codes: ["target_scope_unverified"] };
    expect(readinessMatches(view, request, owner)).toBe(true);
    expect(readinessOperation(view, "prepare_precondition", Date.parse(READINESS_NOW))).toBe(false);
    view.operations.prepare_precondition = { allowed: true, reason_codes: [] };
    expect(readinessMatches(view, request, owner)).toBe(false);
  });

  it("binds a Plan view to its immutable step while keeping the current reader distinct from its author", () => {
    const { view, request, owner, plan } = actionReadinessFixture({ prepared: true });
    if (plan.kind !== "action_plan") throw new Error("expected action plan fixture");
    const step = plan.plan.steps[0];
    const planRequest = { ...request, plan_id: plan.plan.id, plan_step_id: step.id };
    view.binding.plan_id = plan.plan.id;
    view.binding.plan_step_id = step.id;
    view.binding.plan_hash = plan.plan.plan_hash;
    view.binding.actor_subject = "independent-approver";
    const reader = { ...owner, subject: "independent-approver", evidenceIds: step.evidence_ids, planHash: plan.plan.plan_hash };
    expect(readinessMatches(view, planRequest, reader)).toBe(true);
    view.binding.plan_hash = `sha256:${"0".repeat(64)}`;
    expect(readinessMatches(view, planRequest, reader)).toBe(false);
  });
});
