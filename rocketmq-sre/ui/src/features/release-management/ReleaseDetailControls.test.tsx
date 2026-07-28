import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";

import type { ReleaseStatus, ReleaseWorkflow } from "@/api/types";

import {
  type ReleaseDetailActions,
  ReleaseDetailControls,
} from "./ReleaseDetailControls";

describe("release escort controls", () => {
  it("creates a typed readiness request without a raw payload editor", async () => {
    const user = userEvent.setup();
    const actions = createActions();

    render(
      <ReleaseDetailControls
        actions={actions}
        workflow={workflow("planned")}
      />,
    );

    await user.type(
      screen.getByLabelText("Evidence UUID（逗号分隔）"),
      "51000000-0000-4000-8000-000000000001",
    );
    await user.type(
      screen.getByLabelText("受影响资源键"),
      "broker:broker-a, proxy:proxy-0",
    );
    await user.click(
      screen.getByRole("button", {
        name: "评估 readiness 与 what-if",
      }),
    );

    expect(actions.prepare).toHaveBeenCalledWith({
      pdb_ready: true,
      synthetic_probe_ready: true,
      evidence_ids: ["51000000-0000-4000-8000-000000000001"],
      affected_resource_keys: [
        "broker:broker-a",
        "proxy:proxy-0",
      ],
      configuration_changes: [],
    });
    expect(screen.queryByText(/raw request/i)).not.toBeInTheDocument();
  });

  it("exposes bounded canary transitions and a typed rollback only", async () => {
    const user = userEvent.setup();
    const actions = createActions();

    render(
      <ReleaseDetailControls
        actions={actions}
        workflow={workflow("canary_running")}
      />,
    );

    expect(
      screen.getByRole("button", { name: "记录观察" }),
    ).toBeVisible();
    expect(
      screen.getByRole("button", {
        name: "暂停并保留执行上下文",
      }),
    ).toBeVisible();
    expect(
      screen.getByRole("button", { name: "启动类型化回滚" }),
    ).toBeVisible();
    expect(screen.queryByText(/shell/i)).not.toBeInTheDocument();

    await user.click(
      screen.getByRole("button", { name: "进入发布后验证" }),
    );
    expect(actions.beginVerification).toHaveBeenCalledOnce();
  });

  it("removes mutation controls after a terminal result", () => {
    render(
      <ReleaseDetailControls
        actions={createActions()}
        workflow={workflow("completed")}
      />,
    );

    expect(screen.getByText("自动护航已结束")).toBeVisible();
    expect(
      screen.queryByRole("button", { name: "启动类型化回滚" }),
    ).not.toBeInTheDocument();
    expect(
      screen.queryByRole("button", { name: "进入人工接管" }),
    ).not.toBeInTheDocument();
  });
});

function createActions(): ReleaseDetailActions {
  return {
    prepare: vi.fn().mockResolvedValue(undefined),
    start: vi.fn().mockResolvedValue(undefined),
    observe: vi.fn().mockResolvedValue(undefined),
    pause: vi.fn().mockResolvedValue(undefined),
    resume: vi.fn().mockResolvedValue(undefined),
    beginVerification: vi.fn().mockResolvedValue(undefined),
    complete: vi.fn().mockResolvedValue(undefined),
    startRollback: vi.fn().mockResolvedValue(undefined),
    completeRollback: vi.fn().mockResolvedValue(undefined),
    manualTakeover: vi.fn().mockResolvedValue(undefined),
  };
}

function workflow(status: ReleaseStatus): ReleaseWorkflow {
  return {
    schema_version: "rocketmq-sre.release.v1",
    id: "45000000-0000-4000-8000-000000000001",
    tenant_id: "00000000-0000-4000-8000-000000000001",
    cluster_id: "10000000-0000-4000-8000-000000000001",
    incident_id: "20000000-0000-4000-8000-000000000001",
    change_id: "CHG-20260728-018",
    release_ref: "REL-2026.07.28-PROXY",
    target_version: "5.3.0",
    runbook_id: "47000000-0000-4000-8000-000000000001",
    runbook_version: "1.0.0",
    plan_id: "44000000-0000-4000-8000-000000000001",
    plan_hash: `sha256:${"a".repeat(64)}`,
    rollback_plan_id: "44000000-0000-4000-8000-000000000002",
    rollback_plan_hash: `sha256:${"b".repeat(64)}`,
    status,
    readiness: null,
    active_execution_id: null,
    regression_detected: false,
    pause_reason: null,
    correlation_id: "30000000-0000-4000-8000-000000000001",
    created_by: "release-operator",
    created_at: "2026-07-28T00:00:00Z",
    updated_at: "2026-07-28T00:00:00Z",
  };
}
