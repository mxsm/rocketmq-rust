import { act, renderHook, waitFor } from "@testing-library/react";

import type { SreApi } from "@/api/client";
import { ApiError } from "@/api/client";
import type { InvestigationRunControls } from "@/api/types";
import { useInvestigationRun } from "./useInvestigationRun";
import { RunScopeError } from "./runPresentation";
import {
  CLUSTER,
  controlsFixture,
  deferred,
  eventPage,
  INVESTIGATION,
  OTHER_RUN,
  RUN,
  runFixture,
  TENANT,
  TIME,
} from "./runTestFixtures";

const state = vi.hoisted(() => ({
  api: undefined as unknown as SreApi,
  tenant: "00000000-0000-4000-8000-000000000001",
  scopeKey: "principal-a",
  clusterId: "10000000-0000-4000-8000-000000000001",
}));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({ api: state.api }),
}));
vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({ session: { tenantId: state.tenant, subject: "operator" } }),
}));
vi.mock("@/context/WorkspaceScopeContext", () => ({
  useWorkspaceScope: () => ({
    scopeKey: state.scopeKey,
    clusterId: state.clusterId,
    window: TIME,
  }),
}));

beforeEach(() => {
  state.tenant = TENANT;
  state.scopeKey = "principal-a";
  state.clusterId = CLUSTER;
});

it.each(["tenant_id", "cluster_id", "investigation_id"] as const)(
  "does not expose controls or request evidence for a Run with a foreign %s",
  async (field) => {
    const run = runFixture();
    run.effective_scope = { ...run.effective_scope, [field]: OTHER_RUN };
    const events = vi.fn();
    state.api = {
      getInvestigationRunControls: vi
        .fn()
        .mockResolvedValue(controlsFixture(run)),
      getInvestigationRunEvents: events,
    } as unknown as SreApi;
    const view = renderHook(() => useInvestigationRun(INVESTIGATION, RUN));
    await waitFor(() =>
      expect(view.result.current.error).toBeInstanceOf(RunScopeError),
    );
    expect(view.result.current.run).toBeUndefined();
    expect(view.result.current.controls).toBeUndefined();
    expect(events).not.toHaveBeenCalled();
    view.unmount();
  },
);

it("refuses a silent durable-event gap rather than advancing past a missing event", async () => {
  const event = {
    schema_version: "rocketmq-sre.investigation-run-event.v1",
    tenant_id: TENANT,
    cluster_id: CLUSTER,
    investigation_id: INVESTIGATION,
    run_id: RUN,
    sequence: "2",
    revision: "2",
    control_epoch: "1",
    status: "running",
    actor_subject: "operator",
    occurred_at: TIME.end,
    event: { kind: "started" },
  };
  state.api = {
    getInvestigationRunControls: vi.fn().mockResolvedValue(controlsFixture()),
    getInvestigationRunEvents: vi
      .fn()
      .mockResolvedValue({
        ...eventPage(),
        events: [event],
        latest_sequence: "2",
        next_sequence: "2",
      }),
  } as unknown as SreApi;
  const view = renderHook(() => useInvestigationRun(INVESTIGATION, RUN));
  await waitFor(() =>
    expect(view.result.current.error).toBeInstanceOf(RunScopeError),
  );
  expect(view.result.current.controls).toBeUndefined();
  expect(view.result.current.events).toEqual([]);
  view.unmount();
});

it("keeps u64 revisions monotonic across event resync, older controls and immutable replay receipts", async () => {
  const initial = runFixture({ revision: "9007199254740993" });
  const newer = runFixture({ revision: "9007199254740994", status: "paused" });
  const latest = runFixture({
    revision: "9007199254740995",
    status: "running",
  });
  const controls = vi
    .fn()
    .mockResolvedValueOnce(controlsFixture(initial))
    .mockResolvedValueOnce(controlsFixture(initial))
    .mockResolvedValue(controlsFixture(latest));
  const events = vi
    .fn()
    .mockResolvedValueOnce({
      ...eventPage(),
      snapshot_required: true,
      recovery_reason: "cursor_expired",
      latest_sequence: "5",
      next_sequence: "5",
      current_snapshot: newer,
    })
    .mockResolvedValue({
      ...eventPage(),
      latest_sequence: "5",
      next_sequence: "5",
    });
  const command = vi.fn().mockResolvedValue({
    run_id: RUN,
    tenant_id: TENANT,
    actor_subject: "operator",
    command_id: OTHER_RUN,
    idempotency_key: "same-request",
    applied_revision: "1",
  });
  state.api = {
    getInvestigationRunControls: controls,
    getInvestigationRunEvents: events,
    commandInvestigationRun: command,
  } as unknown as SreApi;
  const view = renderHook(() => useInvestigationRun(INVESTIGATION, RUN));
  await waitFor(() =>
    expect(view.result.current.run?.revision).toBe(newer.revision),
  );
  expect(view.result.current.controls).toBeUndefined();
  await act(() =>
    view.result.current.submit({
      schema_version: initial.schema_version,
      command_id: OTHER_RUN,
      run_id: RUN,
      expected_revision: newer.revision,
      idempotency_key: "same-request",
      reason: "Resume saved work",
      command: { kind: "resume" },
    }),
  );
  expect(view.result.current.run?.revision).toBe(latest.revision);
  expect(view.result.current.run?.status).toBe("running");
  expect(events.mock.calls[1][1]).toBe("5");
  view.unmount();
});

it("hides old scope immediately and ignores a late response even when transport ignores abort", async () => {
  const old = deferred<InvestigationRunControls>();
  const controls = vi
    .fn()
    .mockImplementationOnce(() => old.promise)
    .mockResolvedValue(controlsFixture(runFixture({ id: OTHER_RUN })));
  state.api = {
    getInvestigationRunControls: controls,
    getInvestigationRunEvents: vi
      .fn()
      .mockResolvedValue(eventPage(runFixture({ id: OTHER_RUN }))),
  } as unknown as SreApi;
  const view = renderHook(({ id }) => useInvestigationRun(INVESTIGATION, id), {
    initialProps: { id: RUN },
  });
  await waitFor(() => expect(controls).toHaveBeenCalledTimes(1));
  state.scopeKey = "principal-new-claims";
  view.rerender({ id: OTHER_RUN });
  expect(view.result.current.run).toBeUndefined();
  expect(controls.mock.calls[0][1].aborted).toBe(true);
  await waitFor(() => expect(view.result.current.run?.id).toBe(OTHER_RUN));
  await act(async () => old.resolve(controlsFixture()));
  expect(view.result.current.run?.id).toBe(OTHER_RUN);
  view.unmount();
});

it("serializes refresh bursts and aborts the sole in-flight poll on unmount", async () => {
  const first = deferred<InvestigationRunControls>();
  const second = deferred<InvestigationRunControls>();
  const controls = vi
    .fn()
    .mockImplementationOnce(() => first.promise)
    .mockImplementation(() => second.promise);
  state.api = {
    getInvestigationRunControls: controls,
    getInvestigationRunEvents: vi.fn().mockResolvedValue(eventPage()),
  } as unknown as SreApi;
  const view = renderHook(() => useInvestigationRun(INVESTIGATION, RUN));
  await waitFor(() => expect(controls).toHaveBeenCalledTimes(1));
  void view.result.current.refresh();
  void view.result.current.refresh();
  void view.result.current.refresh();
  expect(controls).toHaveBeenCalledTimes(1);
  await act(async () => first.resolve(controlsFixture()));
  await waitFor(() => expect(controls).toHaveBeenCalledTimes(2));
  view.unmount();
  expect(controls.mock.calls[1][1].aborted).toBe(true);
  await act(async () => second.resolve(controlsFixture()));
  expect(controls).toHaveBeenCalledTimes(2);
});

it("removes authorized snapshots and controls when the server revokes access", async () => {
  const controls = vi
    .fn()
    .mockResolvedValueOnce(controlsFixture())
    .mockRejectedValue(
      new ApiError(403, "forbidden", "foreign-sensitive-message"),
    );
  state.api = {
    getInvestigationRunControls: controls,
    getInvestigationRunEvents: vi.fn().mockResolvedValue(eventPage()),
  } as unknown as SreApi;
  const view = renderHook(() => useInvestigationRun(INVESTIGATION, RUN));
  await waitFor(() => expect(view.result.current.run?.id).toBe(RUN));
  await act(() => view.result.current.refresh());
  expect(view.result.current.run).toBeUndefined();
  expect(view.result.current.controls).toBeUndefined();
  expect(view.result.current.events).toEqual([]);
  view.unmount();
});
