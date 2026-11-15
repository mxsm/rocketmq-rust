import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { actionPlanFixture, READINESS_NOW } from "@/features/incidents/test-fixtures/actionReadiness";
import { ApprovalPanel } from "./ApprovalPanel";

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(READINESS_NOW);
});
afterEach(() => vi.useRealTimers());

describe("approval versus dispatch readiness", () => {
  it("keeps rejection available when the current diagnosis or required Critic cannot authorize approval", async () => {
    const { planView } = actionPlanFixture();
    planView.plan.status = "needs_critic";
    planView.risk = "r2";
    planView.critic_state = "pending_required";
    const decide = vi.fn(async () => undefined);
    render(<ApprovalPanel view={planView} busy={false} canApprove canReject approvalReady={false}
      canExecute={false} onDecision={decide} onExecute={vi.fn(async () => undefined)}
      onCriticReview={vi.fn(async () => undefined)} />);
    const user = userEvent.setup();
    await user.type(screen.getByPlaceholderText("说明风险判断、观察窗口和回滚依据"), "Reject after independent review");
    expect(screen.getByRole("button", { name: "Reject" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "Approve" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "提交 Executor" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "Reject" }));
    expect(decide).toHaveBeenCalledWith("reject", expect.objectContaining({ plan_hash: planView.plan.plan_hash }));
  });

  it("allows an independent review request while Unknown dispatch qualification keeps execution closed", async () => {
    const { planView } = actionPlanFixture();
    render(<ApprovalPanel view={planView} busy={false} canApprove canReject approvalReady canExecute={false}
      onDecision={vi.fn(async () => undefined)} onExecute={vi.fn(async () => undefined)}
      onCriticReview={vi.fn(async () => undefined)} dispatchBlockedMessage="Qualification remains unknown" />);
    const user = userEvent.setup();
    await user.type(screen.getByPlaceholderText("说明风险判断、观察窗口和回滚依据"), "Reviewed immutable parameters and evidence");
    expect(screen.getByRole("button", { name: "Approve" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "提交 Executor" })).toBeDisabled();
    expect(screen.getByText("Qualification remains unknown")).toBeVisible();
  });
});
