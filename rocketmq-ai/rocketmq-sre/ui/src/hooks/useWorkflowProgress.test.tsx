import { act, renderHook } from "@testing-library/react";

import type { WorkflowStreamEvent } from "@/api/types";
import { useWorkflowProgress } from "./useWorkflowProgress";

const A = "cluster-a";
const B = "cluster-b";
const state = vi.hoisted(() => ({
  api: { subscribeWorkflowEvents: vi.fn() },
  emit: undefined as ((event: WorkflowStreamEvent) => void) | undefined,
  end: undefined as (() => void) | undefined,
  signal: undefined as AbortSignal | undefined,
}));
vi.mock("@/data/SreDataContext", () => ({
  useSreData: () => ({ api: state.api }),
}));
vi.mock("@/auth/AuthContext", () => ({
  useAuth: () => ({
    session: { tenantId: "tenant" },
    hasClusterScope: (id: string) => [A, B].includes(id),
  }),
}));

function event(
  overrides: Partial<WorkflowStreamEvent> = {},
): WorkflowStreamEvent {
  return {
    tenant_id: "tenant",
    cluster_id: A,
    aggregate_type: "investigation",
    aggregate_id: "object",
    event_type: "diagnosis.ready",
    payload: {},
    correlation_id: "correlation",
    occurred_at: "2026-11-11T10:00:00Z",
    ...overrides,
  };
}
async function flush() {
  await act(async () => {
    await Promise.resolve();
    await Promise.resolve();
  });
}

describe("shared workflow progress", () => {
  beforeEach(() => {
    state.api = {
      subscribeWorkflowEvents: vi.fn(
        (emit: (event: WorkflowStreamEvent) => void, signal: AbortSignal) => {
          state.emit = emit;
          state.signal = signal;
          return new Promise<void>((resolve) => {
            state.end = resolve;
          });
        },
      ),
    };
  });
  afterEach(() => vi.useRealTimers());

  it("shares a stream, filters identity/object scope, and cancels after the last consumer", async () => {
    const a = vi.fn();
    const b = vi.fn();
    const first = renderHook(() =>
      useWorkflowProgress(A, a, {
        aggregateType: "investigation",
        aggregateId: "object",
      }),
    );
    const second = renderHook(() => useWorkflowProgress(B, b));
    expect(state.api.subscribeWorkflowEvents).toHaveBeenCalledTimes(1);
    act(() => {
      state.emit?.(event({ tenant_id: "foreign" }));
      state.emit?.(event({ aggregate_id: "other-object" }));
      state.emit?.(event());
      state.emit?.(event());
      state.emit?.(event({ cluster_id: B }));
    });
    await flush();
    expect(a).toHaveBeenCalledTimes(1);
    expect(b).toHaveBeenCalledTimes(1);
    expect(first.result.current.events).toHaveLength(1);
    first.unmount();
    expect(state.signal?.aborted).toBe(false);
    second.unmount();
    expect(state.signal?.aborted).toBe(true);
  });

  it("serializes slow polling and discards queued refreshes after unmount", async () => {
    vi.useFakeTimers();
    let finish!: () => void;
    const poll = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          finish = resolve;
        }),
    );
    const resource = renderHook(() => useWorkflowProgress(A, poll));
    act(() => state.end?.());
    await flush();
    expect(poll).toHaveBeenCalledTimes(1);
    await act(async () => {
      vi.advanceTimersByTime(30_000);
    });
    expect(poll).toHaveBeenCalledTimes(1);
    await act(async () => finish());
    await flush();
    expect(poll).toHaveBeenCalledTimes(2);
    await act(async () => {
      vi.advanceTimersByTime(20_000);
    });
    resource.unmount();
    await act(async () => finish());
    await flush();
    expect(poll).toHaveBeenCalledTimes(2);
    expect(state.signal?.aborted).toBe(true);
  });
});
