import { act, fireEvent, render as renderComponent, screen } from "@testing-library/react";
import type { ReactElement } from "react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { actionPlanFixture, READINESS_NOW } from "@/features/incidents/test-fixtures/actionReadiness";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { ApprovalPanel } from "./ApprovalPanel";

beforeEach(() => {
  window.localStorage.clear();
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(READINESS_NOW);
});
afterEach(() => vi.useRealTimers());

function render(ui: ReactElement) {
  const result = renderComponent(ui, { wrapper: OperatorPreferencesProvider });
  return result;
}

function approvedFixture(planSeconds = 60, approvalSeconds = 10) {
  const { planView } = actionPlanFixture();
  const at = (seconds: number) => new Date(Date.parse(READINESS_NOW) + seconds * 1000).toISOString();
  planView.plan.status = "approved";
  planView.plan.expires_at = at(planSeconds);
  planView.latest_approval = {
    id: "38000000-0000-4000-8000-000000000001", tenant_id: planView.plan.tenant_id,
    cluster_id: planView.plan.cluster_id, plan_id: planView.plan.id, plan_hash: planView.plan.plan_hash,
    requester_subject: planView.plan.created_by, approver_subject: "independent-reviewer",
    approver_role: "approver", decided_at: READINESS_NOW, expires_at: at(approvalSeconds),
    decision: "approved", reason: "Synthetic independent review, no target authority",
  };
  return planView;
}

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

  it("expires the human approval record at its actual deadline without a parent rerender or request", () => {
    vi.useFakeTimers();
    const execute = vi.fn(async () => undefined);
    const fetch = vi.spyOn(globalThis, "fetch");
    render(<ApprovalPanel view={approvedFixture()} busy={false} canApprove canExecute onDecision={vi.fn()}
      onExecute={execute} onCriticReview={vi.fn()} />);
    expect(screen.getByText("人工批准记录（未到期）")).toBeVisible();
    expect(screen.getByRole("button", { name: "提交 Executor" })).toBeEnabled();
    act(() => vi.advanceTimersByTime(10_000));
    expect(screen.queryByText("人工批准记录（未到期）")).not.toBeInTheDocument();
    expect(screen.getByText("历史人工批准记录（已失效）")).toBeVisible();
    const submit = screen.getByRole("button", { name: "提交 Executor" });
    expect(submit).toBeDisabled();
    fireEvent.click(submit);
    expect(execute).not.toHaveBeenCalled();
    expect(fetch).not.toHaveBeenCalled();
    fetch.mockRestore();
  });

  it("also expires on the Plan deadline when a stored approval has a later timestamp", () => {
    vi.useFakeTimers();
    render(<ApprovalPanel view={approvedFixture(5, 20)} busy={false} canApprove canExecute
      onDecision={vi.fn()} onExecute={vi.fn()} onCriticReview={vi.fn()} />);
    act(() => vi.advanceTimersByTime(5000));
    expect(screen.getByText("历史人工批准记录（已失效）")).toBeVisible();
    expect(screen.getByRole("button", { name: "提交 Executor" })).toBeDisabled();
  });

  it("replaces the actual deadline timer for a newly loaded approval and clears it on unmount", () => {
    vi.useFakeTimers();
    const props = { busy: false, canApprove: true, canExecute: true, onDecision: vi.fn(), onExecute: vi.fn(), onCriticReview: vi.fn() };
    const result = render(<ApprovalPanel {...props} view={approvedFixture(60, 10)} />);
    act(() => vi.advanceTimersByTime(5000));
    result.rerender(<ApprovalPanel {...props} view={approvedFixture(60, 20)} />);
    act(() => vi.advanceTimersByTime(10_000));
    expect(screen.getByText("人工批准记录（未到期）")).toBeVisible();
    act(() => vi.advanceTimersByTime(5000));
    expect(screen.getByText("历史人工批准记录（已失效）")).toBeVisible();
    result.unmount();
    expect(vi.getTimerCount()).toBe(0);
  });

  it("does not label a previous Plan hash or a future-dated record as current approval", () => {
    const view = approvedFixture();
    view.plan.plan_hash = "sha256:" + "9".repeat(64);
    const props = { busy: false, canApprove: true, canExecute: true, onDecision: vi.fn(), onExecute: vi.fn(), onCriticReview: vi.fn() };
    const result = render(<ApprovalPanel {...props} view={view} />);
    expect(screen.getByRole("button", { name: "提交 Executor" })).toBeDisabled();
    expect(screen.getByText("历史人工批准记录（已失效）")).toBeVisible();
    const future = approvedFixture();
    future.latest_approval!.decided_at = "2026-11-17T10:00:01Z";
    result.rerender(<ApprovalPanel {...props} view={future} />);
    expect(screen.getByRole("button", { name: "提交 Executor" })).toBeDisabled();
  });

  it("keeps an unsupported historic Plan read-only even if local role props are true", () => {
    const decide = vi.fn();
    const critic = vi.fn();
    render(<ApprovalPanel view={actionPlanFixture().planView} busy={false} readOnly canApprove canReject canExecute
      onDecision={decide} onExecute={vi.fn()} onCriticReview={critic} />);
    expect(screen.getByRole("button", { name: "Approve" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "Reject" })).toBeDisabled();
    expect(screen.getByRole("button", { name: "提交 Executor" })).toBeDisabled();
    expect(screen.getByPlaceholderText("说明风险判断、观察窗口和回滚依据")).toHaveAttribute("readonly");
    expect(screen.getByText("approver")).toBeVisible();
    expect(screen.queryByText("当前身份没有审批角色，操作保持禁用。")).not.toBeInTheDocument();
    expect(decide).not.toHaveBeenCalled();
    expect(critic).not.toHaveBeenCalled();
  });

  it("gates Critic independently from dispatch and never exposes it to a readonly reviewer", () => {
    const { planView } = actionPlanFixture();
    planView.plan.status = "needs_critic";
    planView.risk = "r2";
    planView.critic_state = "pending_required";
    const critic = vi.fn(async () => undefined);
    const props = { view: planView, busy: false, canApprove: true, canExecute: false,
      onDecision: vi.fn(), onExecute: vi.fn(), onCriticReview: critic };
    const result = render(<ApprovalPanel {...props} />);
    expect(screen.getByRole("button", { name: "运行 Critic" })).toBeDisabled();
    result.rerender(<ApprovalPanel {...props} canCriticReview />);
    expect(screen.getByRole("button", { name: "运行 Critic" })).toBeEnabled();
    expect(screen.getByRole("button", { name: "提交 Executor" })).toBeDisabled();
    fireEvent.click(screen.getByRole("button", { name: "运行 Critic" }));
    expect(critic).toHaveBeenCalledOnce();
  });
});
