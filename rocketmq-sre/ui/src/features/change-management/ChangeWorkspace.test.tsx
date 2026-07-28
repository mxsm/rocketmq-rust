import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { describe, expect, it } from "vitest";

import type {
  ChangeConflict,
  RunbookDefinition,
} from "@/api/types";

import {
  ChangeWorkspaceNav,
  ConflictPanel,
  RunbookDiff,
  ScheduleStatusBadge,
} from "./ChangeWorkspace";

describe("change management workspace", () => {
  it("exposes the desktop change workflow as first-class navigation", () => {
    render(
      <MemoryRouter initialEntries={["/changes/calendar"]}>
        <ChangeWorkspaceNav />
      </MemoryRouter>,
    );

    expect(screen.getByRole("link", { name: "变更中心" })).toHaveAttribute(
      "href",
      "/changes",
    );
    expect(screen.getByRole("link", { name: "Runbook" })).toHaveAttribute(
      "href",
      "/changes/runbooks",
    );
    expect(screen.getByRole("link", { name: "变更日历" })).toHaveClass(
      "active",
    );
    expect(screen.getByRole("link", { name: "排程" })).toHaveAttribute(
      "href",
      "/changes/schedules",
    );
    expect(screen.getByRole("link", { name: "发布护航" })).toHaveAttribute(
      "href",
      "/changes/releases",
    );
    expect(screen.getByRole("link", { name: "外部集成" })).toHaveAttribute(
      "href",
      "/changes/integrations",
    );
  });

  it("renders typed runbook differences and blocking conflicts as text", () => {
    const before = runbook("1.0.0", [
      actionStep(1, "扩容 Proxy", "proxy.scale_out_one.v1"),
    ]);
    const after = runbook("1.1.0", [
      actionStep(1, "扩容 Proxy", "proxy.scale_out_one.v1"),
      actionStep(
        2,
        "轮换凭据",
        "security.credential_rotate_overlap.v1",
      ),
    ]);
    const conflict: ChangeConflict = {
      blocking: true,
      code: "parallelism_exceeded",
      message: "资源并发上限为 1",
      starts_at: "2026-07-28T02:00:00Z",
      ends_at: "2026-07-28T03:00:00Z",
      resource_key: "broker/broker-a",
    };

    render(
      <>
        <ScheduleStatusBadge status="awaiting_manual_gate" />
        <RunbookDiff before={before} after={after} />
        <ConflictPanel conflicts={[conflict]} schedulable={false} />
      </>,
    );

    expect(screen.getByText("等待人工门")).toBeVisible();
    expect(screen.getByText("1 项变化")).toBeVisible();
    expect(screen.getByText("新增")).toBeVisible();
    expect(screen.getByText("排程存在阻断冲突")).toBeVisible();
    expect(screen.getByText("并发上限冲突")).toBeVisible();
    expect(screen.getByText("资源并发上限为 1")).toBeVisible();
  });
});

type ActionBody = Extract<
  RunbookDefinition["steps"][number]["body"],
  { kind: "action" }
>;

function runbook(
  version: string,
  steps: RunbookDefinition["steps"],
): RunbookDefinition {
  return {
    schema_version: "rocketmq-sre.runbook.v1",
    id: "00000000-0000-4000-8000-000000000001",
    name: "Proxy rollout",
    version,
    owner: "sre",
    description: "test",
    risk: "r2",
    max_parallelism: 1,
    steps,
    compensation_edges: [],
    created_at: "2026-07-28T00:00:00Z",
  };
}

function actionStep(
  sequence: number,
  name: string,
  action: ActionBody["action"],
): RunbookDefinition["steps"][number] {
  return {
    id: `00000000-0000-4000-8000-${String(sequence).padStart(12, "0")}`,
    sequence,
    name,
    depends_on: [],
    body: {
      kind: "action",
      action,
      descriptor_version: "1.0.0",
      resource: "deployment/rocketmq/proxy",
      parameters: {},
    },
  };
}
