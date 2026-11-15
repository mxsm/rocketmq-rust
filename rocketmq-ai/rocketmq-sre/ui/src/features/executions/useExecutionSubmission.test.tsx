import { act, renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";
import { createSupervisedSreApi } from "@/api/supervisedClient";
import { ApiError } from "@/api/client";
import { actionPlanFixture, READINESS_NOW } from "@/features/incidents/test-fixtures/actionReadiness";
import { useExecutionSubmission } from "./useExecutionSubmission";
import { observationFixture } from "./test-fixtures/observation";

function setup() {
  const fixture = observationFixture();
  const { planView } = actionPlanFixture(fixture.receipt.execution.plan.steps[0].resource);
  planView.plan.status = "approved";
  const owner = {
    tenantId: fixture.owner.tenantId, subject: fixture.owner.subject,
    clusterId: fixture.owner.clusterIds[0], principalKey: "synthetic-operator-owner", scopeKey: "synthetic-absolute-window",
  };
  const api = createSupervisedSreApi({ token: "synthetic-token", tenantId: owner.tenantId,
    subject: owner.subject, clusterIds: fixture.owner.clusterIds, roles: ["operator"] });
  const list = vi.spyOn(api, "listPlanExecutions").mockResolvedValue(fixture.page);
  const submit = vi.spyOn(api, "submitExecution").mockImplementation(async (request) => ({
    ...fixture.receipt, execution: { ...fixture.receipt.execution, idempotency_key: request.idempotency_key },
  }));
  const lookup = vi.spyOn(api, "lookupExecution").mockImplementation(async (request) => ({ ...fixture.lookup, request }));
  const accepted = vi.fn<(id: string) => void>();
  const props = { api, view: planView, owner, enabled: true, onAccepted: accepted };
  return { ...fixture, props, list, submit, lookup, accepted };
}

beforeEach(() => {
  vi.useFakeTimers({ toFake: ["Date"] });
  vi.setSystemTime(READINESS_NOW);
  window.sessionStorage.clear();
});
afterEach(() => { vi.restoreAllMocks(); vi.useRealTimers(); window.sessionStorage.clear(); });

describe("original execution-intent coordination", () => {
  it("retains one request across lost response and remount; NotObserved never enables another POST", async () => {
    const state = setup();
    state.submit.mockRejectedValue(new TypeError("private transport diagnostic"));
    // Both reads below are real contract-shaped snapshots, independent of the
    // local submission error. The first is deliberately not proof of absence.
    state.lookup.mockImplementationOnce(async (request) => ({
      ...observationFixture().lookup, request, status: "not_observed", receipt: null,
    })).mockImplementation(async (request) => ({ ...observationFixture().lookup, request }));
    const first = renderHook(() => useExecutionSubmission(state.props));
    await waitFor(() => expect(first.result.current.canSubmit).toBe(true));
    await act(async () => first.result.current.submit(state.props.view.precondition_hash));
    const original = state.submit.mock.calls[0][0];
    expect(first.result.current.intent.status).toBe("uncertain");
    expect(first.result.current.current?.error).toBe("read");
    first.unmount();

    const restored = renderHook(() => useExecutionSubmission(state.props));
    await waitFor(() => expect(restored.result.current.page).toBeDefined());
    expect(restored.result.current.canSubmit).toBe(false);
    await act(async () => restored.result.current.lookup());
    expect(restored.result.current.current?.notObserved).toBe(true);
    expect(restored.result.current.canSubmit).toBe(false);
    await act(async () => restored.result.current.submit(state.props.view.precondition_hash));
    expect(state.submit).toHaveBeenCalledTimes(1);
    await act(async () => restored.result.current.lookup());
    expect(state.lookup.mock.calls.map(([request]) => request)).toEqual([original, original]);
    expect(state.accepted).toHaveBeenCalledWith(state.receipt.execution.id);
    expect(restored.result.current.canSubmit).toBe(false);
  });

  it("keeps known acceptance closed when navigation fails", async () => {
    const state = setup();
    state.accepted.mockImplementation(() => { throw new Error("synthetic navigation failure"); });
    const hook = renderHook(() => useExecutionSubmission(state.props));
    await waitFor(() => expect(hook.result.current.canSubmit).toBe(true));
    await act(async () => hook.result.current.submit(state.props.view.precondition_hash));
    expect(hook.result.current.current?.acceptedId).toBe(state.receipt.execution.id);
    expect(hook.result.current.current?.navigationFailed).toBe(true);
    await act(async () => hook.result.current.submit(state.props.view.precondition_hash));
    expect(state.submit).toHaveBeenCalledTimes(1);
  });

  it("uses accepted-reference discovery after refresh even when another operator submitted", async () => {
    const state = setup();
    state.list.mockResolvedValue({ ...state.page, items: [{ ...state.view.binding, requested_by: "other-authorized-operator" }] });
    const hook = renderHook(() => useExecutionSubmission(state.props));
    await waitFor(() => expect(hook.result.current.page?.items).toHaveLength(1));
    expect(hook.result.current.canSubmit).toBe(false);
    await act(async () => hook.result.current.submit(state.props.view.precondition_hash));
    expect(state.submit).not.toHaveBeenCalled();
  });

  it("does not POST when session storage cannot preserve the original selector", async () => {
    const state = setup();
    const hook = renderHook(() => useExecutionSubmission(state.props));
    await waitFor(() => expect(hook.result.current.canSubmit).toBe(true));
    vi.spyOn(Storage.prototype, "setItem").mockImplementation(() => { throw new DOMException("quota", "QuotaExceededError"); });
    await act(async () => hook.result.current.submit(state.props.view.precondition_hash));
    expect(hook.result.current.current?.error).toBe("storage");
    expect(state.submit).not.toHaveBeenCalled();
  });

  it("aborts ignored late submission responses when the window owner changes", async () => {
    const state = setup();
    let finish!: (value: typeof state.receipt) => void;
    state.submit.mockImplementation(() => new Promise((resolve) => { finish = resolve; }));
    const hook = renderHook((props) => useExecutionSubmission(props), { initialProps: state.props });
    await waitFor(() => expect(hook.result.current.canSubmit).toBe(true));
    let writing!: Promise<void>;
    act(() => { writing = hook.result.current.submit(state.props.view.precondition_hash); });
    const original = state.submit.mock.calls[0][0];
    const signal = state.submit.mock.calls[0][1];
    hook.rerender({ ...state.props, owner: { ...state.props.owner, scopeKey: "different-window" } });
    expect(signal?.aborted).toBe(true);
    expect(hook.result.current.intent.status).toBe("unavailable");
    await act(async () => {
      finish({ ...state.receipt, execution: { ...state.receipt.execution, idempotency_key: original.idempotency_key } });
      await writing;
    });
    expect(state.accepted).not.toHaveBeenCalled();
    expect(hook.result.current.canSubmit).toBe(false);
  });

  it("hides reference data after authoritative denial and rejects foreign lookup receipts", async () => {
    const state = setup();
    state.submit.mockRejectedValue(new TypeError("response lost"));
    const hook = renderHook(() => useExecutionSubmission(state.props));
    await waitFor(() => expect(hook.result.current.canSubmit).toBe(true));
    await act(async () => hook.result.current.submit(state.props.view.precondition_hash));
    state.lookup.mockImplementation(async (request) => ({ ...observationFixture().lookup, request, actor_subject: "different-actor" }));
    await act(async () => hook.result.current.lookup());
    expect(hook.result.current.current?.error).toBe("scope");
    expect(hook.result.current.page).toBeUndefined();
    expect(state.accepted).not.toHaveBeenCalled();
    state.lookup.mockRejectedValue(new ApiError(403, "forbidden", "private diagnostic"));
    await act(async () => hook.result.current.lookup());
    expect(hook.result.current.current?.error).toBe("forbidden");
    expect(hook.result.current.page).toBeUndefined();
    expect(hook.result.current.intent.status).toBe("uncertain");
  });
});
