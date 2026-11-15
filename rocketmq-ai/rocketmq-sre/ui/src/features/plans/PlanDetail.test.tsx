import { render, screen } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { actionPlanFixture } from "@/features/incidents/test-fixtures/actionReadiness";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { PlanDetail } from "./PlanDetail";
import { supportedPlanReview } from "./planActionReview";

beforeEach(() => window.localStorage.clear());
describe("immutable typed Plan review", () => {
  it("reads saved intent, risk and verification without fabricating before values or making a preparation", () => {
    const { planView } = actionPlanFixture("broker/127.0.0.1:10911");
    const fetch = vi.spyOn(globalThis, "fetch");
    render(<OperatorPreferencesProvider><PlanDetail view={planView} /></OperatorPreferencesProvider>);
    expect(screen.getByText("当前目标值：未提供；须依赖服务端只读预检。")).toBeVisible();
    expect(screen.getByText("DEBUG")).toBeVisible();
    expect(screen.getByText("日志级别已应用")).toBeVisible();
    expect(screen.getByText("声明自动补偿")).toBeVisible();
    expect(screen.getByText("影响范围 · 单一资源")).toBeVisible();
    expect(screen.getByText(/计划声明不证明目标支持/)).toBeVisible();
    expect(screen.queryByText("诊断允许执行")).not.toBeInTheDocument();
    expect(fetch).not.toHaveBeenCalled();
    fetch.mockRestore();
  });

  it("masks the managed reference and never echoes extra unreviewed parameters or verification text", () => {
    const { planView } = actionPlanFixture();
    const step = planView.plan.steps[0];
    step.action = "security.credential_rotate_overlap.v1";
    step.parameters = { credential_set: "broker-api", active_version: "v1", candidate_version: "v2",
      candidate_secret_ref: "vault://private-sensitive-canary/v2", overlap_seconds: 300, validation_probe_topic: "SRE_PROBE_ROTATION" };
    step.verification.resource_conditions = ["private-condition-canary", "constructor", "__proto__", "toString"];
    expect(supportedPlanReview(planView.plan)).toBe(true);
    const result = render(<OperatorPreferencesProvider><PlanDetail view={planView} /></OperatorPreferencesProvider>);
    expect(screen.getByText("[引用已隐藏]")).toBeVisible();
    expect(result.container.innerHTML).not.toContain("private-sensitive-canary");
    expect(result.container.innerHTML).not.toContain("private-condition-canary");
    expect(screen.getAllByText("未识别的条件（未显示原值）")).toHaveLength(4);
    step.parameters = { ...step.parameters as object, arbitrary: "raw-secret-canary" };
    expect(supportedPlanReview(planView.plan)).toBe(false);
    result.rerender(<OperatorPreferencesProvider><PlanDetail view={planView} /></OperatorPreferencesProvider>);
    expect(result.container.innerHTML).not.toContain("raw-secret-canary");
  });

  it("preserves an old-version Plan's metadata and target as readonly without a raw JSON fallback", () => {
    const { planView } = actionPlanFixture();
    planView.plan.steps[0].descriptor_version = "0.9.0";
    planView.plan.steps[0].parameters = { raw: "old-parameter-private-canary" };
    expect(supportedPlanReview(planView.plan)).toBe(false);
    const result = render(<OperatorPreferencesProvider><PlanDetail view={planView} /></OperatorPreferencesProvider>);
    expect(screen.getByText(/observability.logger_level_ttl.v1 \/ 0.9.0/)).toBeVisible();
    expect(screen.getByText("broker/broker-a")).toBeVisible();
    expect(result.container.innerHTML).not.toContain("old-parameter-private-canary");
    expect(result.container.querySelector("pre")).toBeNull();
  });

  it("reads the saved one-canary impact and verification declaration in English without claiming a qualified target", () => {
    window.localStorage.setItem("rocketmq-sre-operator-preferences.v1", JSON.stringify({ locale: "en-US" }));
    const { planView } = actionPlanFixture();
    const step = planView.plan.steps[0];
    step.action = "proxy.rollout_image_canary.v1";
    step.max_impact = "one_replica";
    step.parameters = { namespace: "messaging", workload: "proxy", container: "proxy", expected_generation: 4,
      image_digest: "sha256:" + "a".repeat(64), canary_replicas: 1 };
    step.verification.resource_conditions = ["canary_ready", "old_replicas_unchanged"];
    render(<OperatorPreferencesProvider><PlanDetail view={planView} /></OperatorPreferencesProvider>);
    expect(screen.getByText("Impact scope · One replica")).toBeVisible();
    expect(screen.getByText("Canary ready")).toBeVisible();
    expect(screen.getByText("Old replicas unchanged")).toBeVisible();
    expect(screen.getByText(/A declaration does not prove target support/)).toBeVisible();
  });

  it("does not present an unsafe u64 duration as an exact observed number", () => {
    const { planView } = actionPlanFixture();
    planView.plan.steps[0].verification.stable_window_seconds = Number.MAX_SAFE_INTEGER + 1;
    expect(supportedPlanReview(planView.plan)).toBe(false);
    const result = render(<OperatorPreferencesProvider><PlanDetail view={planView} /></OperatorPreferencesProvider>);
    expect(screen.getByText(/未提供可精确读取的数值/)).toBeVisible();
    expect(result.container.textContent).not.toContain(String(Number.MAX_SAFE_INTEGER + 1));
  });
});
