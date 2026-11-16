import { act, renderHook, waitFor } from "@testing-library/react";

import type { SreApi } from "@/api/client";
import type { CapabilitySnapshot } from "@/api/types";
import { ClusterCapabilityCache } from "@/data/clusterCapabilityCache";
import { useClusterCapabilitySnapshots } from "./useClusterCapabilitySnapshots";

const state = vi.hoisted(() => ({ capabilityCache: undefined as unknown as ClusterCapabilityCache }));
vi.mock("@/data/SreDataContext", () => ({ useSreData: () => state }));

function install(get: SreApi["getClusterCapabilities"], ids: string[]) {
  state.capabilityCache = new ClusterCapabilityCache({ getClusterCapabilities: get } as SreApi, ids);
}

function snapshot(id: string, digest = "current"): CapabilitySnapshot {
  return {
    cluster_id: id,
    digest,
    protocol_version: "fixture",
    schema_version: "fixture",
    mutation_supported: false,
    observed_at: "2026-11-12T10:00:00Z",
    data_sources: [],
  };
}

describe("Fleet capability reads", () => {
  it("bounds 50 reads to four workers and reuses successful snapshots across sorting and filters", async () => {
    const ids = Array.from({ length: 50 }, (_, index) => `cluster-${String(index).padStart(2, "0")}`);
    const pending: Array<{ id: string; finish: (value: CapabilitySnapshot) => void }> = [];
    let active = 0;
    let peak = 0;
    const get = vi.fn((id: string) => {
      peak = Math.max(peak, ++active);
      return new Promise<CapabilitySnapshot>((finish) => pending.push({ id, finish }))
        .finally(() => { active -= 1; });
    });
    install(get, [...ids, "new-cluster"]);
    const resource = renderHook(({ selected }) => useClusterCapabilitySnapshots(selected), {
      initialProps: { selected: ids },
    });
    expect(get).toHaveBeenCalledTimes(4);
    for (let index = 0; index < ids.length; index += 4) {
      await act(async () => {
        pending.slice(index, index + 4).forEach(({ id, finish }) => finish(snapshot(id)));
      });
    }
    expect(peak).toBe(4);
    expect(get).toHaveBeenCalledTimes(50);
    expect(Object.keys(resource.result.current.data?.snapshots ?? {})).toHaveLength(50);
    resource.rerender({ selected: [...ids].reverse() });
    resource.rerender({ selected: ids.slice(0, 10) });
    await waitFor(() => expect(Object.keys(resource.result.current.data?.snapshots ?? {})).toHaveLength(10));
    resource.rerender({ selected: ids });
    await waitFor(() => expect(Object.keys(resource.result.current.data?.snapshots ?? {})).toHaveLength(50));
    expect(get).toHaveBeenCalledTimes(50);
    resource.rerender({ selected: [...ids, "new-cluster"] });
    expect(get).toHaveBeenCalledTimes(51);
    await act(async () => pending[50].finish(snapshot("new-cluster")));
    expect(resource.result.current.data?.unavailable).toBe(false);
  });

  it("isolates API identities and admits no queued reads after cancellation", async () => {
    const pending: Array<{ signal: AbortSignal; finish: (value: CapabilitySnapshot) => void }> = [];
    const old = vi.fn((_id: string, signal?: AbortSignal) =>
      new Promise<CapabilitySnapshot>((finish) => pending.push({ signal: signal!, finish })),
    );
    const ids = ["a", "b", "c", "d", "e", "f"];
    install(old, ids);
    const resource = renderHook(() => useClusterCapabilitySnapshots(ids));
    expect(old).toHaveBeenCalledTimes(4);
    const next = vi.fn((id: string) => Promise.resolve(snapshot(id, "new-owner")));
    install(next, ids);
    resource.rerender();
    expect(resource.result.current.data).toBeUndefined();
    expect(pending.every((request) => request.signal.aborted)).toBe(true);
    await waitFor(() => expect(resource.result.current.data?.snapshots.a.digest).toBe("new-owner"));
    await act(async () => pending.forEach(({ finish }) => finish(snapshot("a", "old-owner"))));
    expect(old).toHaveBeenCalledTimes(4);
    expect(next).toHaveBeenCalledTimes(6);
    expect(resource.result.current.data?.snapshots.a.digest).toBe("new-owner");
  });

  it("keeps failures and wrong-cluster responses unavailable and retries them explicitly", async () => {
    const get = vi.fn()
      .mockRejectedValueOnce(new Error("source unavailable"))
      .mockResolvedValueOnce(snapshot("foreign-cluster", "must-not-display"))
      .mockResolvedValueOnce(snapshot("a", "recovered"));
    install(get, ["a"]);
    const resource = renderHook(() => useClusterCapabilitySnapshots(["a"]));
    await waitFor(() => expect(resource.result.current.data?.unavailable).toBe(true));
    let completion!: Promise<void>;
    act(() => { completion = resource.result.current.reload(); });
    await act(async () => { await completion; });
    expect(resource.result.current.data?.snapshots).toEqual({});
    expect(resource.result.current.data?.unavailable).toBe(true);
    act(() => { completion = resource.result.current.reload(); });
    await act(async () => { await completion; });
    expect(resource.result.current.data?.snapshots.a.digest).toBe("recovered");
    expect(resource.result.current.data?.unavailable).toBe(false);
  });

  it("reuses the provider cache across unmounts, but never extends its lifetime on a hit", async () => {
    vi.useFakeTimers({ toFake: ["performance", "setTimeout", "clearTimeout"] });
    try {
      const get = vi.fn((id: string) => Promise.resolve({
        ...snapshot(id),
        data_sources: [
          { id: "source", availability: "queryable" as const, freshness_ms: 60_000 },
          { id: "missing", availability: "missing_instrumentation" as const, detail: "not collected" },
        ],
      }));
      install(get, ["a"]);
      const first = renderHook(() => useClusterCapabilitySnapshots(["a"]));
      await act(async () => {});
      first.unmount();
      await act(async () => { await vi.advanceTimersByTimeAsync(20_000); });
      const warm = renderHook(() => useClusterCapabilitySnapshots(["a"]));
      await act(async () => {});
      expect(get).toHaveBeenCalledTimes(1);
      expect(warm.result.current.data?.snapshots.a.data_sources[0].freshness_ms).toBe(80_000);
      expect(warm.result.current.data?.snapshots.a.observed_at).toBe("2026-11-12T10:00:00Z");
      expect(warm.result.current.data?.snapshots.a.data_sources[1]).toEqual({
        id: "missing", availability: "missing_instrumentation", detail: "not collected",
      });
      warm.unmount();
      await act(async () => { await vi.advanceTimersByTimeAsync(10_000); });
      const expired = renderHook(() => useClusterCapabilitySnapshots(["a"]));
      await act(async () => {});
      expect(get).toHaveBeenCalledTimes(2);
      expired.unmount();
    } finally {
      vi.useRealTimers();
    }
  });

  it("expires mounted results and hides old evidence while an unavailable source is retried", async () => {
    vi.useFakeTimers({ toFake: ["performance", "setTimeout", "clearTimeout"] });
    try {
      let fail!: (error: Error) => void;
      const get = vi.fn().mockResolvedValueOnce(snapshot("a", "old"))
        .mockImplementationOnce(() => new Promise((_resolve, reject) => { fail = reject; }));
      install(get, ["a"]);
      const resource = renderHook(() => useClusterCapabilitySnapshots(["a"]));
      await act(async () => {});
      expect(resource.result.current.data?.snapshots.a.digest).toBe("old");
      await act(async () => { await vi.advanceTimersByTimeAsync(30_000); });
      expect(resource.result.current.data).toBeUndefined();
      expect(resource.result.current.loading).toBe(true);
      await act(async () => fail(new Error("offline")));
      expect(resource.result.current.data?.snapshots).toEqual({});
      expect(resource.result.current.data?.unavailable).toBe(true);
      resource.unmount();
    } finally {
      vi.useRealTimers();
    }
  });

  it("does not let an invalidated in-flight read repopulate the cache", async () => {
    let finish!: (value: CapabilitySnapshot) => void;
    const get = vi.fn().mockImplementationOnce(() => new Promise((resolve) => { finish = resolve; }))
      .mockResolvedValue(snapshot("a", "refreshed"));
    install(get, ["a"]);
    const resource = renderHook(() => useClusterCapabilitySnapshots(["a"]));
    let completion!: Promise<void>;
    act(() => { completion = resource.result.current.reload(); });
    await act(async () => { await completion; });
    expect(resource.result.current.data?.snapshots.a.digest).toBe("refreshed");
    await act(async () => finish(snapshot("a", "obsolete")));
    resource.unmount();
    const returned = renderHook(() => useClusterCapabilitySnapshots(["a"]));
    await waitFor(() => expect(returned.result.current.data?.snapshots.a.digest).toBe("refreshed"));
    expect(get).toHaveBeenCalledTimes(2);
  });

  it("never reads clusters outside the provider roster", async () => {
    const get = vi.fn((id: string) => Promise.resolve(snapshot(id)));
    install(get, ["a"]);
    const resource = renderHook(() => useClusterCapabilitySnapshots(["a", "forbidden"]));
    await waitFor(() => expect(resource.result.current.loading).toBe(false));
    expect(get).toHaveBeenCalledTimes(1);
    expect(resource.result.current.data?.snapshots).not.toHaveProperty("forbidden");
    expect(resource.result.current.data?.unavailable).toBe(true);
  });

  it("cancels queued work on navigation and ignores late replies from the departed page", async () => {
    const pending: Array<{ id: string; signal?: AbortSignal; finish: (value: CapabilitySnapshot) => void }> = [];
    const get = vi.fn((id: string, signal?: AbortSignal) =>
      new Promise<CapabilitySnapshot>((finish) => pending.push({ id, signal, finish })),
    );
    install(get, ["a", "b", "c", "d", "e"]);
    const first = renderHook(() => useClusterCapabilitySnapshots(["a", "b", "c", "d", "e"]));
    first.unmount();
    expect(pending.every((item) => item.signal?.aborted)).toBe(true);
    get.mockImplementation((id) => Promise.resolve(snapshot(id, "returned")));
    const returned = renderHook(() => useClusterCapabilitySnapshots(["a", "e"]));
    await waitFor(() => expect(returned.result.current.data?.snapshots.a.digest).toBe("returned"));
    await act(async () => pending.forEach(({ id, finish }) => finish(snapshot(id, "departed"))));
    expect(get).toHaveBeenCalledTimes(6);
    returned.unmount();
    const again = renderHook(() => useClusterCapabilitySnapshots(["a", "e"]));
    await waitFor(() => expect(again.result.current.data?.snapshots.a.digest).toBe("returned"));
    expect(get).toHaveBeenCalledTimes(6);
  });
});
