import { act, render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { App } from "@/App";
import { ApiError, type SreApi } from "@/api/client";
import type {
  InvestigationCommandRequest,
  InvestigationRunControls,
} from "@/api/types";
import { demoClusters, demoCoverage } from "@/data/demo";
import { phase1Evidence, phase1Investigations } from "@/data/phase1Demo";
import { OperatorPreferencesProvider } from "@/preferences/OperatorPreferences";
import {
  CLUSTER,
  controlsFixture,
  deferred,
  eventPage,
  EVIDENCE,
  INVESTIGATION,
  listFixture,
  OTHER_RUN,
  RUN,
  runFixture,
  revisionFixture,
  resultFixture,
  REVISION,
  TENANT,
  TIME,
} from "./runTestFixtures";

const state = vi.hoisted(() => ({
  api: undefined as unknown as SreApi,
  auth: {
    mode: "development",
    session: {
      tenantId: "00000000-0000-4000-8000-000000000001",
      subject: "operator",
      clusterIds: ["10000000-0000-4000-8000-000000000001"],
      roles: ["reader"],
      displayName: "SRE",
    },
    hasClusterScope: (id: string) =>
      id === "10000000-0000-4000-8000-000000000001",
  },
}));
vi.mock("@/auth/AuthContext", () => ({ useAuth: () => state.auth }));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({
    api: state.api,
    clusters: demoClusters.slice(0, 1),
    loading: false,
    demoMode: false,
  }),
}));

function open(extra = "") {
  window.history.replaceState(
    null,
    "",
    `/investigations/${INVESTIGATION}?cluster_id=${CLUSTER}&window_start=${TIME.start}&window_end=${TIME.end}${extra}`,
  );
  return render(
    <OperatorPreferencesProvider>
      <App />
    </OperatorPreferencesProvider>,
  );
}

beforeEach(() => {
  window.localStorage.clear();
  window.sessionStorage.clear();
  state.api = {
    getInvestigation: vi.fn().mockResolvedValue({
      investigation: {
        ...phase1Investigations.items[0].investigation,
        id: INVESTIGATION,
        tenant_id: TENANT,
        cluster_id: CLUSTER,
        resource: undefined,
      },
      diagnosis_revisions: [],
      timeline: [],
    }),
    getCoverage: vi.fn().mockResolvedValue(demoCoverage),
    subscribeWorkflowEvents: vi.fn(() => new Promise<void>(() => {})),
    listAssets: vi.fn().mockResolvedValue({
      items: [],
      partial: false,
      warnings: [],
      observed_at: TIME.end,
    }),
    listInvestigationRuns: vi.fn().mockResolvedValue(listFixture()),
    getInvestigationRunControls: vi.fn().mockResolvedValue(controlsFixture()),
    getInvestigationRunEvents: vi.fn().mockResolvedValue(eventPage()),
  } as unknown as SreApi;
});

it("binds saved revisions to the selected run and cancels late drawer content on URL revision changes", async () => {
  const user = userEvent.setup();
  const first = revisionFixture();
  const secondId = "52000000-0000-4000-8000-000000000002";
  const secondEvidence = "40000000-0000-4000-8000-000000000002";
  const second = {
    ...first,
    id: secondId,
    evidence_ids: [secondEvidence],
    findings: [
      {
        ...first.findings[0],
        root_cause: "Second saved rule finding",
        supporting_evidence_ids: [secondEvidence],
      },
    ],
  };
  const lateRevision = deferred<typeof first>();
  const content = deferred<unknown>();
  const run = runFixture({
    pack_results: [resultFixture(first), resultFixture(second)],
  });
  vi.mocked(state.api.getInvestigationRunControls).mockResolvedValue(
    controlsFixture(run),
  );
  state.api.getInvestigationRunRevision = vi
    .fn()
    .mockImplementationOnce(() => lateRevision.promise)
    .mockImplementation(async (_run, id) => (id === secondId ? second : first));
  state.api.getEvidence = vi
    .fn()
    .mockResolvedValue({
      ...phase1Evidence.items[0],
      evidence_id: secondEvidence,
      cluster_id: CLUSTER,
      tenant_id: TENANT,
      observed_at: TIME.end,
      time_range: TIME,
    });
  state.api.getEvidenceContent = vi
    .fn()
    .mockImplementation(() => content.promise);
  open(`&run_id=${RUN}&run_revision_id=${REVISION}`);
  await waitFor(() =>
    expect(state.api.getInvestigationRunRevision).toHaveBeenCalledTimes(1),
  );
  await user.selectOptions(screen.getByLabelText("打开保存修订"), secondId);
  expect(
    await screen.findByRole("heading", { name: "Second saved rule finding" }),
  ).toBeVisible();
  expect(
    vi.mocked(state.api.getInvestigationRunRevision).mock.calls[0][2]?.aborted,
  ).toBe(true);
  await act(async () => lateRevision.resolve(first));
  expect(
    screen.queryByRole("heading", { name: "Saved deterministic fault" }),
  ).not.toBeInTheDocument();
  await user.click(
    screen.getByRole("button", {
      name: `查看证据 · ${secondEvidence.slice(0, 8)}`,
    }),
  );
  const drawer = await screen.findByRole("dialog", { name: "证据详情" });
  await user.click(
    await within(drawer).findByRole("button", { name: "读取脱敏内容" }),
  );
  await waitFor(() =>
    expect(state.api.getEvidenceContent).toHaveBeenCalledTimes(1),
  );
  act(() => {
    window.history.replaceState(
      null,
      "",
      `${window.location.pathname}${window.location.search.replace(secondId, REVISION)}`,
    );
    window.dispatchEvent(new PopStateEvent("popstate"));
  });
  expect(
    await screen.findByRole("heading", { name: "Saved deterministic fault" }),
  ).toBeVisible();
  expect(screen.queryByRole("dialog")).not.toBeInTheDocument();
  expect(
    vi.mocked(state.api.getEvidenceContent).mock.calls[0][1]?.aborted,
  ).toBe(true);
  await act(async () =>
    content.resolve({ content: "late-foreign-marker", evidence_id: EVIDENCE }),
  );
  expect(screen.queryByText("late-foreign-marker")).not.toBeInTheDocument();
});

it("keeps manual deep links readable, discovers runs with exact scope filters and restores URL selection", async () => {
  const user = userEvent.setup();
  const view = open();
  expect(await screen.findByText("手动只读调查")).toBeVisible();
  await waitFor(() =>
    expect(state.api.listInvestigationRuns).toHaveBeenCalled(),
  );
  expect(
    vi.mocked(state.api.listInvestigationRuns).mock.calls[0][1],
  ).toMatchObject({ start: TIME.start, end: TIME.end, limit: 20 });
  const manualUrl = window.location.href;
  await user.selectOptions(screen.getByLabelText("选择调查运行"), RUN);
  expect(
    await screen.findByRole("heading", { name: "共享预算" }),
  ).toBeVisible();
  expect(new URLSearchParams(window.location.search).get("run_id")).toBe(RUN);
  const runUrl = window.location.href;
  act(() => {
    window.history.replaceState(null, "", manualUrl);
    window.dispatchEvent(new PopStateEvent("popstate"));
  });
  expect(await screen.findByText("手动只读调查")).toBeVisible();
  act(() => {
    window.history.replaceState(null, "", runUrl);
    window.dispatchEvent(new PopStateEvent("popstate"));
  });
  expect(
    await screen.findByRole("heading", { name: "共享预算" }),
  ).toBeVisible();
  view.unmount();
});

it("pages server run summaries in both directions and resets the filter-bound cursor for all-history discovery", async () => {
  const user = userEvent.setup();
  const list = vi
    .fn()
    .mockImplementation(async (_id, options) =>
      options?.cursor
        ? listFixture([runFixture({ id: OTHER_RUN })])
        : { ...listFixture(), next_cursor: "opaque-filter-bound-page-2" },
    );
  state.api.listInvestigationRuns = list;
  open();
  await screen.findByRole("option", { name: /51000000/ });
  const discovery = screen.getByRole("region", { name: "有界只读调查" });
  await user.click(within(discovery).getByRole("button", { name: "下一页" }));
  await waitFor(() =>
    expect(list).toHaveBeenLastCalledWith(
      INVESTIGATION,
      expect.objectContaining({
        cursor: "opaque-filter-bound-page-2",
        start: TIME.start,
        end: TIME.end,
      }),
      expect.any(AbortSignal),
    ),
  );
  await waitFor(() =>
    expect(
      within(discovery).getByRole("button", { name: "上一页" }),
    ).toBeEnabled(),
  );
  await user.click(within(discovery).getByRole("button", { name: "上一页" }));
  await waitFor(() =>
    expect(
      within(discovery).getByRole("button", { name: "下一页" }),
    ).toBeEnabled(),
  );
  await user.click(within(discovery).getByRole("button", { name: "下一页" }));
  await waitFor(() =>
    expect(
      list.mock.calls.filter(([, options]) => options?.cursor).length,
    ).toBe(2),
  );
  await user.click(within(discovery).getByLabelText("此调查全部历史"));
  await waitFor(() =>
    expect(list).toHaveBeenLastCalledWith(
      INVESTIGATION,
      { cursor: undefined, limit: 20 },
      expect.any(AbortSignal),
    ),
  );
});

it("recovers a cursor invalidated by another operator's refinement with one bounded first-page reload", async () => {
  const user = userEvent.setup();
  const list = vi.fn().mockImplementation(async (_id, options) => {
    if (options?.cursor)
      throw new ApiError(400, "invalid_input", "stale-anchor-private-detail");
    return { ...listFixture(), next_cursor: "anchor-before-refinement" };
  });
  state.api.listInvestigationRuns = list;
  open();
  const discovery = await screen.findByRole("region", { name: "有界只读调查" });
  await waitFor(() =>
    expect(
      within(discovery).getByRole("button", { name: "下一页" }),
    ).toBeEnabled(),
  );
  await user.click(within(discovery).getByRole("button", { name: "下一页" }));
  await waitFor(() => expect(list).toHaveBeenCalledTimes(3));
  expect(list.mock.calls.map(([, options]) => options?.cursor)).toEqual([
    undefined,
    "anchor-before-refinement",
    undefined,
  ]);
  expect(
    within(discovery).getByRole("button", { name: "上一页" }),
  ).toBeDisabled();
  expect(
    screen.queryByText("stale-anchor-private-detail"),
  ).not.toBeInTheDocument();
});

it("uses only server affordances, retains exact command identity on uncertain retry and ignores old receipt snapshots", async () => {
  const user = userEvent.setup();
  const controls = controlsFixture();
  controls.commands = controls.commands.map((command) => ({
    ...command,
    allowed: command.kind === "pause",
    blocked_reason:
      command.kind === "pause" ? undefined : "control_permission_required",
  }));
  vi.mocked(state.api.getInvestigationRunControls).mockResolvedValue(controls);
  const command = vi
    .fn()
    .mockRejectedValueOnce(new TypeError("network failed"))
    .mockImplementation(
      async (_id: string, input: InvestigationCommandRequest) => {
        vi.mocked(state.api.getInvestigationRunControls).mockResolvedValue(
          controlsFixture(runFixture({ revision: "9", status: "paused" })),
        );
        return {
          run_id: RUN,
          tenant_id: TENANT,
          actor_subject: "operator",
          command_id: input.command_id,
          idempotency_key: input.idempotency_key,
          applied_revision: "1",
        };
      },
    );
  state.api.commandInvestigationRun = command;
  open(`&run_id=${RUN}`);
  await screen.findByRole("button", { name: "暂停调查" });
  expect(
    screen.queryByRole("button", { name: "取消调查" }),
  ).not.toBeInTheDocument();
  await user.type(screen.getByLabelText("控制原因"), "Review source limits");
  await user.click(screen.getByRole("button", { name: "暂停调查" }));
  await user.click(await screen.findByRole("button", { name: "重试相同请求" }));
  await waitFor(() => expect(command).toHaveBeenCalledTimes(2));
  expect(command.mock.calls[1][1]).toEqual(command.mock.calls[0][1]);
  expect(await screen.findByText("已暂停", { selector: "span" })).toBeVisible();
  expect(screen.getByText("9", { selector: "dd" })).toBeInTheDocument();
});

it("creates a bounded server-pack request, restores focus after Escape and reads current state after a replay response", async () => {
  const user = userEvent.setup();
  state.api.createInvestigationRun = vi
    .fn()
    .mockImplementation(async (_id, request) => {
      vi.mocked(state.api.getInvestigationRunControls).mockResolvedValue(
        controlsFixture(
          runFixture({ id: request.run_id, revision: "8", status: "paused" }),
        ),
      );
      vi.mocked(state.api.getInvestigationRunEvents).mockResolvedValue(
        eventPage(runFixture({ id: request.run_id })),
      );
      return {
        run: runFixture({
          id: request.run_id,
          revision: "0",
          status: "queued",
        }),
        actor_subject: "operator",
        replayed: true,
        idempotency_key: request.idempotency_key,
      };
    });
  open();
  const trigger = await screen.findByRole("button", { name: "创建有界调查" });
  await user.click(trigger);
  expect(
    await screen.findByRole("dialog", { name: "创建有界调查" }),
  ).toBeVisible();
  await user.keyboard("{Escape}");
  expect(trigger).toHaveFocus();
  await user.click(trigger);
  const dialog = await screen.findByRole("dialog", { name: "创建有界调查" });
  await user.click(
    within(dialog).getByRole("button", { name: "提交只读调查" }),
  );
  await waitFor(() =>
    expect(state.api.createInvestigationRun).toHaveBeenCalledTimes(1),
  );
  const request = vi.mocked(state.api.createInvestigationRun).mock.calls[0][1];
  expect(request.scope).toEqual(runFixture().initial_scope);
  expect(request.packs).toEqual(runFixture().effective_packs);
  expect(request.budget.max_query_retries).toBe(2);
  await waitFor(() =>
    expect(new URLSearchParams(window.location.search).get("run_id")).toBe(
      request.run_id,
    ),
  );
  expect(await screen.findByText("已暂停", { selector: "span" })).toBeVisible();
});

it("aligns a refined typed run independently of the old parent's canonical resource and restores controls after refresh", async () => {
  const user = userEvent.setup();
  vi.mocked(state.api.getInvestigation).mockResolvedValue({
    investigation: {
      ...phase1Investigations.items[0].investigation,
      id: INVESTIGATION,
      tenant_id: TENANT,
      cluster_id: CLUSTER,
      resource: "topics/orders",
    },
    diagnosis_revisions: [],
    timeline: [],
  });
  const actual = runFixture({ status: "paused" });
  actual.effective_scope = {
    ...actual.effective_scope,
    resource: { kind: "topic", key: "orders" },
    time_range: {
      start: "2026-11-13T09:00:00.123456789Z",
      end: "2026-11-13T10:00:00.123456789Z",
    },
  };
  const policy = controlsFixture(actual);
  policy.commands = policy.commands.map((command) => ({
    ...command,
    allowed: command.kind !== "pause",
    blocked_reason: command.kind === "pause" ? "state_not_eligible" : undefined,
  }));
  vi.mocked(state.api.getInvestigationRunControls).mockResolvedValue(policy);
  const view = open(`&run_id=${RUN}`);
  const align = await screen.findByRole("button", { name: "对齐到运行范围" });
  expect(screen.queryByLabelText("控制原因")).not.toBeInTheDocument();
  await user.click(align);
  expect(await screen.findByLabelText("控制原因")).toBeVisible();
  expect(new URLSearchParams(window.location.search).get("window_start")).toBe(
    actual.effective_scope.time_range.start,
  );
  expect(new URLSearchParams(window.location.search).get("run_id")).toBe(RUN);
  expect(new URLSearchParams(window.location.search).get("resource_kind")).toBe(
    "topic",
  );
  expect(
    vi.mocked(state.api.getInvestigationRunControls).mock.calls[0][1]?.aborted,
  ).toBe(true);
  view.unmount();
  render(
    <OperatorPreferencesProvider>
      <App />
    </OperatorPreferencesProvider>,
  );
  expect(await screen.findByLabelText("控制原因")).toBeVisible();
  expect(screen.getByRole("button", { name: "继续调查" })).toBeVisible();
  expect(screen.getByRole("button", { name: "取消调查" })).toBeVisible();
  expect(
    screen.queryByText("此对象与当前选择的范围不一致"),
  ).not.toBeInTheDocument();
});

it("hides stale claims after authorization is revoked without rendering server error details", async () => {
  const late = deferred<InvestigationRunControls>();
  vi.mocked(state.api.getInvestigationRunControls)
    .mockImplementationOnce(() => late.promise)
    .mockRejectedValue(new ApiError(403, "forbidden", "foreign-marker"));
  open(`&run_id=${RUN}`);
  await waitFor(() =>
    expect(state.api.getInvestigationRunControls).toHaveBeenCalledTimes(1),
  );
  act(() => {
    window.history.pushState(
      null,
      "",
      `${window.location.pathname}${window.location.search.replace(`run_id=${RUN}`, `run_id=${OTHER_RUN}`)}`,
    );
    window.dispatchEvent(new PopStateEvent("popstate"));
  });
  expect(
    await screen.findByText("当前身份无权读取或控制此运行。"),
  ).toBeVisible();
  await act(async () => late.resolve(controlsFixture()));
  expect(
    screen.queryByRole("heading", { name: "共享预算" }),
  ).not.toBeInTheDocument();
  expect(screen.queryByText("foreign-marker")).not.toBeInTheDocument();
});
