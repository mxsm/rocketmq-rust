import { act, render, screen, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { SupervisedSreApi } from "@/api/supervisedClient";
import type { ActionReadinessView } from "@/api/types";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { actionPlanFixture, planReadinessFixture, READINESS_NOW, READINESS_SUBJECT } from "@/features/incidents/test-fixtures/actionReadiness";
import { PlanActionReadiness } from "./PlanActionReadiness";
import { planApprovalChecksPass, planReadinessSatisfied } from "./planReadinessPresentation";

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(READINESS_NOW);
  window.localStorage.clear();
});
afterEach(() => vi.useRealTimers());

describe("PlanActionReadiness", () => {
  it("bounds reads at four and waits for every exact immutable step without granting Unknown dispatch", async () => {
    const { planView } = actionPlanFixture();
    const first = planView.plan.steps[0];
    planView.plan.steps = Array.from({ length: 8 }, (_, index) => ({
      ...first, id: `36000000-0000-4000-8000-${String(index + 1).padStart(12, "0")}`, sequence: index + 1,
    }));
    const completions: Array<(view: ActionReadinessView) => void> = [];
    let inFlight = 0;
    let maximum = 0;
    const read = vi.fn<SupervisedSreApi["readActionReadiness"]>(() => {
      inFlight += 1;
      maximum = Math.max(maximum, inFlight);
      return new Promise<ActionReadinessView>((resolve) => completions.push(resolve))
        .finally(() => { inFlight -= 1; });
    });
    const onReadiness = vi.fn();
    render(
      <OperatorPreferencesProvider>
        <PlanActionReadiness view={planView} api={{ readActionReadiness: read } as unknown as SupervisedSreApi}
          subject={READINESS_SUBJECT} onReadiness={onReadiness} />
      </OperatorPreferencesProvider>,
    );
    await waitFor(() => expect(read).toHaveBeenCalledTimes(4));
    expect(maximum).toBe(4);
    await act(async () => {
      for (let index = 0; index < 4; index += 1) completions[index](planReadinessFixture(planView, index));
    });
    await waitFor(() => expect(read).toHaveBeenCalledTimes(8));
    expect(maximum).toBe(4);
    await act(async () => {
      for (let index = 4; index < 8; index += 1) completions[index](planReadinessFixture(planView, index));
    });
    expect(await screen.findByRole("heading", { name: "目标资格" })).toBeVisible();
    const snapshots = onReadiness.mock.calls.at(-1)?.[1] as ActionReadinessView[];
    expect(snapshots).toHaveLength(8);
    expect(planReadinessSatisfied(planView.plan, snapshots, Date.parse(READINESS_NOW))).toBe(false);
    expect(read.mock.calls.every(([, request, signal]) =>
      typeof request === "object" && request !== null && "plan_id" in request &&
      request.plan_id === planView.plan.id && signal instanceof AbortSignal,
    )).toBe(true);
  });

  it("hides a wrong-step response in full and aborts the batch", async () => {
    const { planView } = actionPlanFixture();
    const wrong = planReadinessFixture(planView);
    wrong.binding.plan_step_id = "foreign-step";
    wrong.binding.policy_version = "foreign-policy-marker";
    let signal: AbortSignal | undefined;
    const api = { readActionReadiness: vi.fn(async (_id: string, _request: unknown, value: AbortSignal) => {
      signal = value;
      return wrong;
    }) } as unknown as SupervisedSreApi;
    render(
      <OperatorPreferencesProvider>
        <PlanActionReadiness view={planView} api={api} subject={READINESS_SUBJECT} onReadiness={vi.fn()} />
      </OperatorPreferencesProvider>,
    );
    expect(await screen.findByRole("alert")).toHaveTextContent("返回结果与当前身份、范围或动作候选不一致");
    expect(screen.queryByText("foreign-policy-marker")).not.toBeInTheDocument();
    expect(signal?.aborted).toBe(true);
  });

  it("does not promote an unknown stage even if a malformed summary says satisfied", () => {
    const { planView } = actionPlanFixture();
    const snapshot = planReadinessFixture(planView);
    snapshot.execution_readiness = "satisfied";
    expect(planReadinessSatisfied(planView.plan, [snapshot], Date.parse(READINESS_NOW))).toBe(false);
  });

  it("keeps human review separate from execution qualification and blocks a newer diagnosis or expiry", () => {
    const { planView } = actionPlanFixture();
    const subject = "independent-approver";
    const snapshot = planReadinessFixture(planView, 0, subject);
    const now = Date.parse(READINESS_NOW);
    expect(snapshot.stages.qualification.state).toBe("unknown");
    expect(planApprovalChecksPass(planView.plan, [snapshot], subject, now)).toBe(true);
    expect(planReadinessSatisfied(planView.plan, [snapshot], now)).toBe(false);
    expect(planApprovalChecksPass(planView.plan, [snapshot], planView.plan.created_by, now)).toBe(false);
    snapshot.diagnosis.is_current = false;
    expect(planApprovalChecksPass(planView.plan, [snapshot], subject, now)).toBe(false);
    snapshot.diagnosis.is_current = true;
    expect(planApprovalChecksPass(planView.plan, [snapshot], subject, Date.parse(snapshot.valid_until!))).toBe(false);
  });

  it("rejects over-limit historical step collections before starting any new reads", async () => {
    const { planView } = actionPlanFixture();
    planView.plan.steps = Array.from({ length: 17 }, (_, index) => ({ ...planView.plan.steps[0], id: `step-${index}` }));
    const read = vi.fn();
    render(
      <OperatorPreferencesProvider>
        <PlanActionReadiness view={planView} api={{ readActionReadiness: read } as unknown as SupervisedSreApi}
          subject={READINESS_SUBJECT} onReadiness={vi.fn()} />
      </OperatorPreferencesProvider>,
    );
    expect(await screen.findByRole("alert")).toHaveTextContent("返回结果与当前身份、范围或动作候选不一致");
    expect(read).not.toHaveBeenCalled();
  });
});
