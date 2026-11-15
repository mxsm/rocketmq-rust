import { act, renderHook, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import { useAsyncResource } from "./useAsyncResource";
import { useSerialResourceRefresh } from "./useSerialResourceRefresh";

describe("owned observation refresh", () => {
  it("merges timer/stream/manual invalidations into one active and one queued read", async () => {
    let finish!: (value: string) => void;
    const load = vi.fn<(signal: AbortSignal) => Promise<string>>()
      .mockResolvedValueOnce("initial").mockImplementationOnce(() => new Promise((resolve) => { finish = resolve; }))
      .mockResolvedValue("latest");
    const hook = renderHook(() => { const resource = useAsyncResource(load); return { ...resource, refresh: useSerialResourceRefresh(resource.reload) }; });
    await waitFor(() => expect(hook.result.current.data).toBe("initial"));
    act(() => { void hook.result.current.refresh(); });
    await waitFor(() => expect(load).toHaveBeenCalledTimes(2));
    act(() => { void hook.result.current.refresh(); void hook.result.current.refresh(); void hook.result.current.refresh(); });
    expect(load).toHaveBeenCalledTimes(2);
    await act(async () => finish("intermediate"));
    await waitFor(() => expect(hook.result.current.data).toBe("latest"));
    expect(load).toHaveBeenCalledTimes(3);
    expect(load.mock.calls[1][0].aborted).toBe(true);
  });

  it("does not launch its queued read after unmount, even if the source ignores abort", async () => {
    let finish!: (value: string) => void;
    const load = vi.fn<(signal: AbortSignal) => Promise<string>>()
      .mockResolvedValueOnce("initial").mockImplementation(() => new Promise((resolve) => { finish = resolve; }));
    const hook = renderHook(() => { const resource = useAsyncResource(load); return { ...resource, refresh: useSerialResourceRefresh(resource.reload) }; });
    await waitFor(() => expect(hook.result.current.data).toBe("initial"));
    act(() => { void hook.result.current.refresh(); });
    await waitFor(() => expect(load).toHaveBeenCalledTimes(2));
    act(() => { void hook.result.current.refresh(); });
    hook.unmount();
    expect(load.mock.calls[1][0].aborted).toBe(true);
    await act(async () => finish("ignored late result"));
    expect(load).toHaveBeenCalledTimes(2);
  });
});
