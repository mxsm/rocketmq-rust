import { act, render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { useState } from "react";
import { MemoryRouter, useLocation, useNavigate } from "react-router-dom";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

import type { ActionReadinessRequest, DiagnosisRevision } from "@/api/types";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import { SupervisedDiagnosisPanel } from "./SupervisedDiagnosisPanel";
import {
  actionReadinessFixture, READINESS_NOW, READINESS_SUBJECT,
} from "./test-fixtures/actionReadiness";

const auth = vi.hoisted(() => ({
  tenantId: "", subject: "", roles: [] as string[], clusterIds: [] as string[], token: "",
}));
vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({
    status: "authenticated", session: { ...auth, accessToken: auth.token },
    requestContext: { ...auth },
    hasClusterScope: (id: string) => auth.clusterIds.includes(id),
  }),
}));

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(READINESS_NOW);
  window.localStorage.clear();
  const fixture = actionReadinessFixture();
  Object.assign(auth, {
    tenantId: fixture.incident.tenant_id, subject: READINESS_SUBJECT,
    roles: ["operator"], clusterIds: [fixture.incident.cluster_id], token: "initial-token",
  });
});
afterEach(() => {
  vi.restoreAllMocks();
  vi.useRealTimers();
});

function Fixture({ revision = actionReadinessFixture().revision, onChanged = () => undefined }: {
  revision?: DiagnosisRevision;
  onChanged?: () => void | Promise<void>;
}) {
  const { incident } = actionReadinessFixture();
  return (
    <OperatorPreferencesProvider>
      <MemoryRouter initialEntries={[
        `/incidents/${incident.id}?cluster_id=${incident.cluster_id}&resource_kind=broker&resource_key=broker-a`,
      ]}>
        <SupervisedDiagnosisPanel incident={incident} revisions={[revision]} onChanged={onChanged} />
        <ScopeNavigation />
      </MemoryRouter>
    </OperatorPreferencesProvider>
  );
}

function ScopeNavigation() {
  const navigate = useNavigate();
  const location = useLocation();
  return <>
    <button onClick={() => navigate({ search: "?window_start=2026-11-17T09:00:00Z&window_end=2026-11-17T09:30:00Z" })}>
      change window
    </button>
    <output aria-label="current location">{location.pathname}{location.search}</output>
  </>;
}

describe("SupervisedDiagnosisPanel", () => {
  it("separates authoritative diagnosis confirmation, readonly preparation and immutable draft consent", async () => {
    const user = userEvent.setup();
    const unconfirmed = actionReadinessFixture({ confirmed: false });
    const confirmed = actionReadinessFixture();
    const prepared = actionReadinessFixture({ prepared: true });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (url, init) => {
      if (String(url).endsWith("/action-readiness")) {
        const body = JSON.parse(String(init?.body)) as ActionReadinessRequest;
        return response(body.diagnosis_revision_id === unconfirmed.revision.id ? unconfirmed.view : confirmed.view);
      }
      if (String(url).endsWith("/confirm-execution")) return response(unconfirmed.confirmation);
      if (String(url).endsWith("/execution-preconditions")) return response({ ...prepared.preparation, readiness: prepared.view });
      if (String(url) === "/v1/plans") return response(prepared.plan);
      throw new Error("unexpected endpoint");
    });
    const changed = vi.fn();
    function Flow() {
      const [current, setCurrent] = useState(unconfirmed.revision);
      return <Fixture revision={current} onChanged={() => { changed(); setCurrent(confirmed.revision); }} />;
    }
    render(<Flow />);
    expect(screen.getByRole("button", { name: "确认当前诊断" })).toBeDisabled();
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "确认当前诊断" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "确认当前诊断" }));
    await waitFor(() => expect(changed).toHaveBeenCalledOnce());
    // The local receipt does not keep an old selection across the new saved revision.
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "运行只读前置检查" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "运行只读前置检查" }));
    expect(await screen.findByText("只读前置证据已保存，正在复核当前候选。")).toBeVisible();
    expect(screen.getByText("尚无此动作版本与目标范围的独立资格记录。")).toBeVisible();
    expect(screen.getByRole("button", { name: "创建受监督计划" })).toBeEnabled();
    await user.click(screen.getByRole("button", { name: "创建受监督计划" }));
    const open = await screen.findByRole("button", { name: "打开计划与审批" });
    await user.click(open);
    expect(screen.getByLabelText("current location")).toHaveTextContent("/changes/plans/34000000-0000-4000-8000-000000000001");
    expect(screen.queryByRole("button", { name: "确认执行资格" })).not.toBeInTheDocument();
    expect(fetchMock.mock.calls.map(([url]) => String(url))).toEqual([
      `/v1/incidents/${unconfirmed.incident.id}/action-readiness`,
      `/v1/incidents/${unconfirmed.incident.id}/diagnosis-revisions/${unconfirmed.revision.id}/confirm-execution`,
      `/v1/incidents/${confirmed.incident.id}/action-readiness`,
      `/v1/incidents/${confirmed.incident.id}/execution-preconditions`,
      "/v1/plans",
    ]);
    const planBody = JSON.parse(String(fetchMock.mock.calls[4][1]?.body));
    expect(planBody.diagnosis_revision_id).toBe(confirmed.revision.id);
    expect(planBody.steps[0].evidence_ids).toEqual(prepared.plan.kind === "action_plan" ? prepared.plan.plan.steps[0].evidence_ids : []);
    expect(fetchMock.mock.calls.every(([, init]) => init?.signal instanceof AbortSignal)).toBe(true);
  });

  it("ignores a late readiness read after parameter changes, including changing back to the original value", async () => {
    const pending = deferred<Response>();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockReturnValue(pending.promise);
    const user = userEvent.setup();
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    const signal = fetchMock.mock.calls[0][1]?.signal;
    await user.clear(screen.getByLabelText("Logger"));
    await user.type(screen.getByLabelText("Logger"), "new.logger");
    expect(signal?.aborted).toBe(true);
    await act(async () => pending.resolve(response(actionReadinessFixture().view)));
    expect(screen.queryByRole("heading", { name: "目标资格" })).not.toBeInTheDocument();
    await user.clear(screen.getByLabelText("Logger"));
    await user.type(screen.getByLabelText("Logger"), "rocketmq_broker::processor");
    expect(screen.getByText("尚未读取此候选的准备状态。")).toBeVisible();
    expect(screen.getByRole("button", { name: "运行只读前置检查" })).toBeDisabled();
  });

  it("explicitly opens an authorized logical Incident candidate in the Plan's exact target scope", async () => {
    const target = "broker/127.0.0.1:10911";
    const initial = actionReadinessFixture({ target });
    const prepared = actionReadinessFixture({ target, prepared: true });
    vi.spyOn(globalThis, "fetch").mockImplementation(async (url) => {
      if (String(url).endsWith("/action-readiness")) return response(initial.view);
      if (String(url).endsWith("/execution-preconditions")) return response({ ...prepared.preparation, readiness: prepared.view });
      return response(prepared.plan);
    });
    const user = userEvent.setup();
    render(<Fixture />);
    await user.clear(screen.getByLabelText("目标 Broker 资源"));
    await user.type(screen.getByLabelText("目标 Broker 资源"), target);
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "运行只读前置检查" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "运行只读前置检查" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "创建受监督计划" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "创建受监督计划" }));
    await user.click(await screen.findByRole("button", { name: "打开计划与审批" }));
    const location = new URL(screen.getByLabelText("current location").textContent!, "http://localhost");
    expect(location.searchParams.get("cluster_id")).toBe(initial.incident.cluster_id);
    expect(location.searchParams.get("resource_kind")).toBe("broker");
    expect(location.searchParams.get("resource_key")).toBe("127.0.0.1:10911");
  });

  it("does not attach a late precondition proof to edited TTL parameters", async () => {
    const pending = deferred<Response>();
    const initial = actionReadinessFixture();
    const prepared = actionReadinessFixture({ prepared: true });
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((url) =>
      String(url).endsWith("/action-readiness") ? Promise.resolve(response(initial.view)) : pending.promise,
    );
    const user = userEvent.setup();
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "运行只读前置检查" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "运行只读前置检查" }));
    const signal = fetchMock.mock.calls[1][1]?.signal;
    await user.clear(screen.getByLabelText("有效秒数"));
    await user.type(screen.getByLabelText("有效秒数"), "120");
    await act(async () => pending.resolve(response({ ...prepared.preparation, readiness: prepared.view })));
    expect(signal?.aborted).toBe(true);
    expect(screen.getByRole("button", { name: "创建受监督计划" })).toBeDisabled();
    expect(screen.getByText("尚无适用于此候选的前置证据。")).toBeVisible();
  });

  it("clears the selected immutable Plan when a new explicit preparation replaces its proof", async () => {
    const first = actionReadinessFixture({ prepared: true });
    const second = structuredClone(first);
    const secondId = "32000000-0000-4000-8000-000000000003";
    second.preparation.evidence.evidence_id = secondId;
    second.view.binding.precondition_evidence!.evidence_id = secondId;
    for (const fact of Object.values(second.view.stages)) {
      fact.evidence_ids = fact.evidence_ids.map((id) =>
        id === first.preparation.evidence.evidence_id ? secondId : id,
      );
    }
    const pending = deferred<Response>();
    let preparations = 0;
    vi.spyOn(globalThis, "fetch").mockImplementation((url) => {
      if (String(url).endsWith("/action-readiness")) return Promise.resolve(response(actionReadinessFixture().view));
      if (String(url).endsWith("/execution-preconditions")) {
        preparations += 1;
        return preparations === 1
          ? Promise.resolve(response({ ...first.preparation, readiness: first.view }))
          : pending.promise;
      }
      return Promise.resolve(response(first.plan));
    });
    const user = userEvent.setup();
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "运行只读前置检查" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "运行只读前置检查" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "创建受监督计划" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "创建受监督计划" }));
    expect(await screen.findByRole("button", { name: "打开计划与审批" })).toBeVisible();

    await user.click(screen.getByRole("button", { name: "运行只读前置检查" }));
    expect(screen.queryByRole("button", { name: "打开计划与审批" })).not.toBeInTheDocument();
    expect(screen.getByText("尚无适用于此候选的前置证据。")).toBeVisible();
    expect(screen.getByRole("button", { name: "创建受监督计划" })).toBeDisabled();
    await act(async () => pending.resolve(response({ ...second.preparation, readiness: second.view })));
    await waitFor(() => expect(screen.getByRole("button", { name: "创建受监督计划" })).toBeEnabled());
    expect(screen.queryByRole("button", { name: "打开计划与审批" })).not.toBeInTheDocument();
  });

  it("does not expose an old plan response or navigate after the target changes", async () => {
    const pending = deferred<Response>();
    const prepared = actionReadinessFixture({ prepared: true });
    vi.spyOn(globalThis, "fetch").mockImplementation((url) => {
      if (String(url).endsWith("/action-readiness")) return Promise.resolve(response(actionReadinessFixture().view));
      if (String(url).endsWith("/execution-preconditions")) return Promise.resolve(response({ ...prepared.preparation, readiness: prepared.view }));
      return pending.promise;
    });
    const user = userEvent.setup();
    render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "运行只读前置检查" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "运行只读前置检查" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "创建受监督计划" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "创建受监督计划" }));
    await user.clear(screen.getByLabelText("目标 Broker 资源"));
    await user.type(screen.getByLabelText("目标 Broker 资源"), "broker/broker-b");
    await act(async () => pending.resolve(response(prepared.plan)));
    expect(screen.queryByRole("button", { name: "打开计划与审批" })).not.toBeInTheDocument();
    expect(screen.getByLabelText("current location")).toHaveTextContent("/incidents/");
  });

  it("ignores a confirmation receipt after a newer revision arrives for the same Incident", async () => {
    const initial = actionReadinessFixture({ confirmed: false });
    const pending = deferred<Response>();
    const changed = vi.fn();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation((url) =>
      String(url).endsWith("/action-readiness") ? Promise.resolve(response(initial.view)) : pending.promise,
    );
    const user = userEvent.setup();
    const rendered = render(<Fixture revision={initial.revision} onChanged={changed} />);
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "确认当前诊断" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "确认当前诊断" }));
    rendered.rerender(<Fixture revision={{ ...initial.revision, id: "31000000-0000-4000-8000-000000000003", revision: 3 }} onChanged={changed} />);
    expect(fetchMock.mock.calls[1][1]?.signal?.aborted).toBe(true);
    await act(async () => pending.resolve(response(initial.confirmation)));
    expect(changed).not.toHaveBeenCalled();
    expect(screen.queryByText("当前诊断已人工确认")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "确认当前诊断" })).toBeDisabled();
  });

  it.each(["subject", "tenant", "claims", "cluster", "window"])("discards a late read across a %s boundary", async (kind) => {
    const pending = deferred<Response>();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockReturnValue(pending.promise);
    const user = userEvent.setup();
    const rendered = render(<Fixture />);
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    if (kind === "window") await user.click(screen.getByRole("button", { name: "change window" }));
    else {
      if (kind === "subject") auth.subject = "another-operator";
      if (kind === "tenant") auth.tenantId = "another-tenant";
      if (kind === "claims") auth.roles = [];
      if (kind === "cluster") auth.clusterIds = ["another-cluster"];
      rendered.rerender(<Fixture />);
    }
    expect(fetchMock.mock.calls[0][1]?.signal?.aborted).toBe(true);
    await act(async () => pending.resolve(response(actionReadinessFixture().view)));
    expect(screen.queryByRole("heading", { name: "目标资格" })).not.toBeInTheDocument();
    expect(screen.queryByText("已满足")).not.toBeInTheDocument();
  });

  it("keeps the form and validity on token renewal while using the new HTTP credential", async () => {
    const fixture = actionReadinessFixture();
    const fetchMock = vi.spyOn(globalThis, "fetch").mockImplementation(async (_url, init) => {
      const request = JSON.parse(String(init?.body)) as ActionReadinessRequest;
      return response({ ...fixture.view, binding: { ...fixture.view.binding, parameters: request.parameters } });
    });
    const user = userEvent.setup();
    const rendered = render(<Fixture />);
    await user.clear(screen.getByLabelText("Logger"));
    await user.type(screen.getByLabelText("Logger"), "reviewed.logger");
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    expect(await screen.findByRole("heading", { name: "目标资格" })).toBeVisible();
    auth.token = "renewed-token";
    rendered.rerender(<Fixture />);
    expect(screen.getByLabelText("Logger")).toHaveValue("reviewed.logger");
    expect(screen.getByRole("heading", { name: "目标资格" })).toBeVisible();
    expect(fetchMock).toHaveBeenCalledOnce();
    await user.click(screen.getByRole("button", { name: "刷新状态" }));
    await waitFor(() => expect(fetchMock).toHaveBeenCalledTimes(2));
    expect(new Headers(fetchMock.mock.calls[1][1]?.headers).get("Authorization")).toBe("Bearer renewed-token");
    await waitFor(() => expect(screen.getByRole("button", { name: "运行只读前置检查" })).toBeEnabled());
  });

  it("can recover a server-reported newer diagnosis through an explicit authoritative refresh", async () => {
    const fixture = actionReadinessFixture();
    fixture.view.binding.latest_diagnosis_revision_id = "31000000-0000-4000-8000-000000000003";
    fixture.view.diagnosis.is_current = false;
    fixture.view.diagnosis.reason_codes = ["diagnosis_not_current"];
    fixture.view.operations.prepare_precondition = { allowed: false, reason_codes: ["diagnosis_not_current"] };
    const fetchMock = vi.spyOn(globalThis, "fetch").mockResolvedValue(response(fixture.view));
    const changed = vi.fn();
    const user = userEvent.setup();
    render(<Fixture onChanged={changed} />);
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    await user.click(await screen.findByRole("button", { name: "刷新诊断修订" }));
    expect(changed).toHaveBeenCalledOnce();
    expect(fetchMock).toHaveBeenCalledOnce();
    expect(screen.getByRole("button", { name: "确认当前诊断" })).toBeDisabled();
  });

  it("does not query readiness for a supplied revision from a different Incident", () => {
    const fixture = actionReadinessFixture();
    const fetchMock = vi.spyOn(globalThis, "fetch");
    render(<Fixture revision={{ ...fixture.revision, incident_id: "different-incident" }} />);
    expect(screen.getByRole("alert")).toHaveTextContent("返回结果与当前身份、范围或动作候选不一致");
    expect(screen.queryByRole("button", { name: "读取准备状态" })).not.toBeInTheDocument();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it.each([403, 409])("clears readiness on HTTP %s without showing private error text", async (status) => {
    const changed = vi.fn();
    vi.spyOn(globalThis, "fetch").mockImplementation(async (url) =>
      String(url).endsWith("/action-readiness") ? response(actionReadinessFixture().view) : response({ code: "request_denied", message: "private-backend-detail" }, status),
    );
    const user = userEvent.setup();
    render(<Fixture onChanged={changed} />);
    await user.click(screen.getByRole("button", { name: "读取准备状态" }));
    await waitFor(() => expect(screen.getByRole("button", { name: "运行只读前置检查" })).toBeEnabled());
    await user.click(screen.getByRole("button", { name: "运行只读前置检查" }));
    expect(await screen.findByRole("alert")).toHaveTextContent(status === 403 ? "当前身份无权继续此操作" : "诊断、证据或目标状态已变化");
    expect(screen.queryByText("private-backend-detail")).not.toBeInTheDocument();
    expect(screen.getByRole("button", { name: "创建受监督计划" })).toBeDisabled();
  });
});

function response(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { "Content-Type": "application/json" } });
}

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((finish) => { resolve = finish; });
  return { promise, resolve };
}
