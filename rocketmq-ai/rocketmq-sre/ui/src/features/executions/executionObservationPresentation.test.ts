import { describe, expect, it } from "vitest";
import { executionLookupMatches, executionObservationMatches, planExecutionsMatch, unsignedDecimal } from "./executionObservationPresentation";
import { observationFixture } from "./test-fixtures/observation";

describe("execution read projections", () => {
  it("accepts exact recorded history with a lossless epoch and event sequence", () => {
    const { view, owner } = observationFixture();
    expect(executionObservationMatches(view, owner)).toBe(true);
    expect(unsignedDecimal("18446744073709551615")).toBe(true);
    expect(unsignedDecimal("18446744073709551616")).toBe(false);
    expect(unsignedDecimal("09007199254740993")).toBe(false);
  });

  it.each(["tenant", "cluster", "execution", "schema", "expiry", "sequence", "duplicate attempt", "duplicate step"])("rejects a foreign or malformed %s before rendering", (field) => {
    const { view, owner } = observationFixture();
    const other = "10000000-0000-4000-8000-000000000099";
    if (field === "tenant") view.binding.tenant_id = other;
    if (field === "cluster") view.binding.cluster_id = other;
    if (field === "execution") view.binding.execution_id = other;
    if (field === "schema") view.schema_version = "legacy-observation";
    if (field === "expiry") view.valid_until = "2026-11-17T09:00:00Z";
    if (field === "sequence") view.timeline.push({ ...view.timeline[0], sequence: "9007199254740992" });
    if (field === "duplicate attempt") view.steps[0].attempts.push({ ...view.steps[0].attempts[0] });
    if (field === "duplicate step") view.steps.push({ ...view.steps[0] });
    expect(executionObservationMatches(view, owner)).toBe(false);
  });

  it("does not promote a step-journal success into independent business recovery", () => {
    const { view, owner } = observationFixture();
    view.binding.state = "succeeded";
    view.business_recovery = { state: "satisfied", source: "step_journal", reason_codes: ["result_recorded"] };
    expect(executionObservationMatches(view, owner)).toBe(false);
    view.business_recovery.source = "independent_business_observation";
    expect(executionObservationMatches(view, owner)).toBe(true);
  });

  it("keeps legacy and invalid verification records visible without inventing proof", () => {
    const { view, verification, owner } = observationFixture();
    view.steps[0].verifications = [{ ...verification, proof_state: "legacy_missing", provenance: "recorded_only", observed_stable_seconds: null }];
    expect(executionObservationMatches(view, owner)).toBe(true);
    view.steps[0].verifications[0].proof_state = "invalid";
    expect(executionObservationMatches(view, owner)).toBe(true);
    expect(executionObservationMatches({ ...view, steps: [{ ...view.steps[0], attempts: null }] }, owner)).toBe(false);
  });

  it("rejects oversized or cross-step journal mixtures instead of silently truncating", () => {
    const { view, owner } = observationFixture();
    const attempt = view.steps[0].attempts[0];
    view.steps[0].attempts = Array.from({ length: 65 }, (_, index) => ({ ...attempt, attempt: index + 1 }));
    expect(executionObservationMatches(view, owner)).toBe(false);
    view.steps[0].attempts = [attempt];
    view.steps.push({ ...view.steps[0], plan_step_id: "36000000-0000-4000-8000-000000000099", sequence: 2 });
    expect(executionObservationMatches(view, owner)).toBe(false);
  });

  it.each(["actor", "key", "plan", "precondition", "receipt requester", "receipt hash"])("binds readonly lookup to the original %s", (field) => {
    const { lookup, owner, selector } = observationFixture();
    if (field === "actor") lookup.actor_subject = "someone-else";
    if (field === "key") lookup.request = { ...selector, idempotency_key: "different-key" };
    if (field === "plan") lookup.request = { ...selector, plan_id: "34000000-0000-4000-8000-000000000099" };
    if (field === "precondition") lookup.request = { ...selector, precondition_hash: `sha256:${"0".repeat(64)}` };
    if (field === "receipt requester") lookup.receipt!.requested_by = "someone-else";
    if (field === "receipt hash") lookup.receipt!.plan_hash = `sha256:${"0".repeat(64)}`;
    expect(executionLookupMatches(lookup, owner, selector)).toBe(false);
  });

  it("allows NotObserved only as an empty exact read snapshot", () => {
    const { lookup, owner, selector } = observationFixture();
    expect(executionLookupMatches(lookup, owner, selector)).toBe(true);
    lookup.status = "not_observed";
    expect(executionLookupMatches(lookup, owner, selector)).toBe(false);
    lookup.receipt = null;
    expect(executionLookupMatches(lookup, owner, selector)).toBe(true);
  });

  it("discovers authorized receipts without borrowing another Plan or hash", () => {
    const { page, view, owner } = observationFixture();
    page.items = [{ ...view.binding, requested_by: "other-authorized-requester" }];
    expect(planExecutionsMatch(page, owner, page.plan_id, page.plan_hash)).toBe(true);
    page.items.push({ ...page.items[0] });
    expect(planExecutionsMatch(page, owner, page.plan_id, page.plan_hash)).toBe(false);
    page.items = [{ ...view.binding, plan_hash: `sha256:${"0".repeat(64)}` }];
    expect(planExecutionsMatch(page, owner, page.plan_id, page.plan_hash)).toBe(false);
  });
});
