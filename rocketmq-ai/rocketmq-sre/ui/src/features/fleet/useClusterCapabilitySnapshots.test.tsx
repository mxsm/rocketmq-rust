import { act, renderHook, waitFor } from "@testing-library/react";

import type { SreApi } from "@/api/client";
import type { CapabilitySnapshot } from "@/api/types";
import { useClusterCapabilitySnapshots } from "./useClusterCapabilitySnapshots";

const state = vi.hoisted(() => ({ api: undefined as unknown as SreApi }));
vi.mock("@/data/SreDataContext", () => ({ useSreData: () => ({ api: state.api }) }));

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
    state.api = { getClusterCapabilities: get } as unknown as SreApi;
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
    const old = vi.fn((_id: string, signal: AbortSignal) =>
      new Promise<CapabilitySnapshot>((finish) => pending.push({ signal, finish })),
    );
    state.api = { getClusterCapabilities: old } as unknown as SreApi;
    const ids = ["a", "b", "c", "d", "e", "f"];
    const resource = renderHook(() => useClusterCapabilitySnapshots(ids));
    expect(old).toHaveBeenCalledTimes(4);
    const next = vi.fn((id: string) => Promise.resolve(snapshot(id, "new-owner")));
    state.api = { getClusterCapabilities: next } as unknown as SreApi;
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
    state.api = { getClusterCapabilities: get } as unknown as SreApi;
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
});
