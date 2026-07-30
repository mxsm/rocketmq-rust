import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter } from "react-router-dom";
import { afterEach, describe, expect, it, vi } from "vitest";

import type { DiagnosisRevision, Incident } from "@/api/types";
import { AuthProvider } from "@/auth/AuthContext";

import { SupervisedDiagnosisPanel } from "./SupervisedDiagnosisPanel";

afterEach(() => {
  vi.restoreAllMocks();
  window.sessionStorage.clear();
});

describe("SupervisedDiagnosisPanel", () => {
  it("binds confirmation, live precondition evidence, and plan creation", async () => {
    const user = userEvent.setup();
    const fetchMock = vi
      .spyOn(globalThis, "fetch")
      .mockResolvedValueOnce(
        jsonResponse({
          schema_version:
            "rocketmq-sre.diagnosis-execution-confirmation.v1",
          incident_id: incident.id,
          source_revision_id: revision.id,
          confirmed_revision_id:
            "31000000-0000-4000-8000-000000000002",
          revision: 2,
          cluster_id: incident.cluster_id,
          primary_model_invocation_id:
            revision.primary_model_invocation_id,
          evidence_ids: revision.evidence_ids,
          execution_eligible: true,
          confirmed_by: "rocketmq-sre-development",
          reason: "reviewed",
          correlation_id:
            "33000000-0000-4000-8000-000000000001",
          confirmed_at: "2026-07-30T00:00:00Z",
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          schema_version:
            "rocketmq-sre.execution-precondition-evidence.v1",
          incident_id: incident.id,
          diagnosis_revision_id:
            "31000000-0000-4000-8000-000000000002",
          evidence: {
            evidence_id:
              "32000000-0000-4000-8000-000000000002",
          },
          precondition_hash: `sha256:${"b".repeat(64)}`,
        }),
      )
      .mockResolvedValueOnce(
        jsonResponse({
          kind: "action_plan",
          plan: {
            id: "34000000-0000-4000-8000-000000000001",
          },
          risk: "r1",
          policy_decision: {},
          precondition_hash: `sha256:${"b".repeat(64)}`,
        }),
      );
    const onChanged = vi.fn();

    render(
      <AuthProvider>
        <MemoryRouter>
          <SupervisedDiagnosisPanel
            incident={incident}
            onChanged={onChanged}
            revisions={[revision]}
          />
        </MemoryRouter>
      </AuthProvider>,
    );

    const confirm = await screen.findByRole("button", {
      name: "确认执行资格",
    });
    expect(confirm).toBeEnabled();
    expect(screen.queryByText(/shell/i)).not.toBeInTheDocument();
    expect(screen.queryByText(/raw request/i)).not.toBeInTheDocument();

    await user.click(confirm);
    expect(
      await screen.findByText("已生成不可变的人工作业确认 revision。"),
    ).toBeVisible();
    expect(onChanged).toHaveBeenCalledOnce();

    await user.click(
      screen.getByRole("button", {
        name: "运行只读前置检查",
      }),
    );
    expect(
      await screen.findByText(
        "Execution Agent 只读前置检查已封装为 Evidence。",
      ),
    ).toBeVisible();

    await user.click(
      screen.getByRole("button", {
        name: "创建受监督计划",
      }),
    );
    expect(
      await screen.findByText(
        "受监督计划已创建，下一步进入 Critic 与人工审批。",
      ),
    ).toBeVisible();

    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(3));
    const confirmationBody = requestBody(fetchMock, 0);
    expect(confirmationBody).toEqual({
      human_confirmed: true,
      reason: "已人工核对模型诊断、Evidence 引用与影响范围",
    });
    const preconditionBody = requestBody(fetchMock, 1);
    expect(preconditionBody).toMatchObject({
      action_id: "observability.logger_level_ttl.v1",
      descriptor_version: "1.0.0",
      resource: incident.resource,
      parameters: {
        component: "broker",
        level: "DEBUG",
        logger: "rocketmq_broker::processor",
        ttl_seconds: 60,
      },
    });
    const planBody = requestBody(fetchMock, 2);
    expect(planBody.steps[0]).toMatchObject({
      action_id: "observability.logger_level_ttl.v1",
      evidence_ids: [
        "32000000-0000-4000-8000-000000000001",
        "32000000-0000-4000-8000-000000000002",
      ],
    });
  });
});

const incident: Incident = {
  id: "30000000-0000-4000-8000-000000000001",
  tenant_id: "00000000-0000-4000-8000-000000000001",
  cluster_id: "10000000-0000-4000-8000-000000000001",
  title: "Broker consumer lag",
  resource: "broker/broker-a",
  status: "monitoring",
  occurrence_count: 1,
  created_at: "2026-07-30T00:00:00Z",
  updated_at: "2026-07-30T00:00:00Z",
};

const revision: DiagnosisRevision = {
  id: "31000000-0000-4000-8000-000000000001",
  incident_id: incident.id,
  revision: 1,
  status: "monitoring",
  rule_result: {
    diagnosis_mode: "model_assisted",
    provider_id: "deepseek",
  },
  hypotheses: [],
  evidence_ids: [
    "32000000-0000-4000-8000-000000000001",
  ],
  primary_model_invocation_id:
    "35000000-0000-4000-8000-000000000001",
  execution_eligible: false,
  partial: false,
  created_at: "2026-07-30T00:00:00Z",
};

function jsonResponse(body: unknown) {
  return new Response(JSON.stringify(body), {
    headers: { "Content-Type": "application/json" },
    status: 200,
  });
}

function requestBody(
  fetchMock: ReturnType<typeof vi.spyOn>,
  index: number,
) {
  return JSON.parse(
    String(fetchMock.mock.calls[index]?.[1]?.body),
  ) as {
    steps: Array<Record<string, unknown>>;
    [key: string]: unknown;
  };
}
