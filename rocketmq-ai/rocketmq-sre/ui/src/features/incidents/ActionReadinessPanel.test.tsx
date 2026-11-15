import { act, render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { ActionReadinessPanel } from "./ActionReadinessPanel";
import { actionReadinessFixture, READINESS_NOW } from "./test-fixtures/actionReadiness";

beforeEach(() => window.localStorage.clear());
afterEach(() => vi.useRealTimers());

describe("ActionReadinessPanel", () => {
  it.each([
    ["zh-CN", "目标资格", "未知", "尚无此动作版本与目标范围的独立资格记录。"],
    ["en-US", "Target qualification", "Unknown", "No independent qualification record covers this action version and target scope."],
  ])("renders actual stage blockers in %s without treating preparation as qualification", (locale, heading, state, reason) => {
    vi.useFakeTimers({ toFake: ["Date"] });
    vi.setSystemTime(READINESS_NOW);
    window.localStorage.setItem("rocketmq-sre-operator-preferences.v1", JSON.stringify({ locale, timeZone: "UTC", theme: "dark" }));
    const fixture = actionReadinessFixture({ prepared: true });
    render(
      <OperatorPreferencesProvider>
        <ActionReadinessPanel {...fixture} />
      </OperatorPreferencesProvider>,
    );
    const qualification = screen.getByRole("heading", { name: heading }).closest("li")!;
    expect(within(qualification).getByText(state)).toBeVisible();
    expect(within(qualification).getByText(reason)).toBeVisible();
    expect(screen.queryByText("action_target_qualification_unavailable")).not.toBeInTheDocument();
    expect(screen.getAllByRole("heading", { level: 4 })).toHaveLength(9);
  });

  it("hides every field of a foreign projection rather than exposing its source or date", () => {
    const fixture = actionReadinessFixture();
    fixture.view.binding.tenant_id = "foreign-tenant";
    fixture.view.binding.policy_version = "foreign-policy-marker";
    render(
      <OperatorPreferencesProvider>
        <ActionReadinessPanel {...fixture} />
      </OperatorPreferencesProvider>,
    );
    expect(screen.getByRole("alert")).toHaveTextContent("返回结果与当前身份、范围或动作候选不一致");
    expect(screen.queryByText("foreign-policy-marker")).not.toBeInTheDocument();
    expect(screen.queryByRole("heading", { level: 4 })).not.toBeInTheDocument();
  });

  it("removes current fact badges when server validity expires without requesting a source", () => {
    vi.useFakeTimers();
    vi.setSystemTime(READINESS_NOW);
    const fixture = actionReadinessFixture({ prepared: true });
    const refresh = vi.fn();
    render(
      <OperatorPreferencesProvider>
        <ActionReadinessPanel {...fixture} onRefresh={refresh} />
      </OperatorPreferencesProvider>,
    );
    expect(screen.getByRole("heading", { name: "目标资格" })).toBeVisible();
    act(() => vi.advanceTimersByTime(60_002));
    expect(screen.getByRole("alert")).toHaveTextContent("此准备状态已过期");
    expect(screen.queryByRole("heading", { level: 4 })).not.toBeInTheDocument();
    expect(refresh).not.toHaveBeenCalled();
  });

  it("provides a keyboard reachable explicit refresh when no projection exists", async () => {
    const fixture = actionReadinessFixture();
    const refresh = vi.fn();
    const user = userEvent.setup();
    render(
      <OperatorPreferencesProvider>
        <ActionReadinessPanel {...fixture} view={undefined} onRefresh={refresh} />
      </OperatorPreferencesProvider>,
    );
    expect(screen.getByText("尚未读取此候选的准备状态。")).toBeVisible();
    await user.tab();
    expect(screen.getByRole("button", { name: "读取准备状态" })).toHaveFocus();
    await user.keyboard("{Enter}");
    expect(refresh).toHaveBeenCalledOnce();
  });
});
