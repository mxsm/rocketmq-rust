import { act, renderHook, waitFor } from "@testing-library/react";

import { useAsyncResource } from "./useAsyncResource";

function deferred<T>() {
  let resolve!: (value: T) => void;
  let reject!: (reason: Error) => void;
  const promise = new Promise<T>((done, fail) => {
    resolve = done;
    reject = fail;
  });
  return { promise, resolve, reject };
}

describe("useAsyncResource request ownership", () => {
  it("hides the previous request immediately and ignores its late result", async () => {
    const first = deferred<string>();
    const second = deferred<string>();
    const a = vi.fn<(signal: AbortSignal) => Promise<string>>(
      () => first.promise,
    );
    const b = vi.fn<(signal: AbortSignal) => Promise<string>>(
      () => second.promise,
    );
    const { result, rerender } = renderHook(
      ({ load }) => useAsyncResource(load),
      { initialProps: { load: a } },
    );
    rerender({ load: b });
    expect(a.mock.calls[0]?.[0].aborted).toBe(true);
    expect(result.current.data).toBeUndefined();
    await act(async () => {
      second.resolve("cluster-b");
    });
    expect(result.current.data).toBe("cluster-b");
    await act(async () => {
      first.resolve("late-cluster-a");
    });
    expect(result.current.data).toBe("cluster-b");
  });

  it("preserves same-request data during a background reload", async () => {
    const next = deferred<string>();
    const load = vi
      .fn<(signal: AbortSignal) => Promise<string>>()
      .mockResolvedValueOnce("saved result")
      .mockReturnValueOnce(next.promise);
    const { result } = renderHook(() => useAsyncResource(load));
    await waitFor(() => expect(result.current.data).toBe("saved result"));
    act(() => { void result.current.reload(); });
    expect(result.current.data).toBe("saved result");
    expect(result.current.loading).toBe(true);
    await act(async () => {
      next.resolve("updated result");
    });
    expect(result.current.data).toBe("updated result");
  });

  it("cancels the old signal and hides old data when the request changes", async () => {
    let signal: AbortSignal | undefined;
    const a = vi.fn((nextSignal: AbortSignal) => {
      signal = nextSignal;
      return Promise.resolve("a");
    });
    const b = vi.fn(() => new Promise<string>(() => undefined));
    const { result, rerender } = renderHook(
      ({ load }) => useAsyncResource(load),
      { initialProps: { load: a as (signal: AbortSignal) => Promise<string> } },
    );
    await waitFor(() => expect(result.current.data).toBe("a"));
    rerender({ load: b });
    expect(signal?.aborted).toBe(true);
    expect(result.current.data).toBeUndefined();
    expect(result.current.loading).toBe(true);
  });

  it.each(["success", "failure", "scope change", "unmount"] as const)(
    "settles reload completion after %s while retaining request ownership",
    async (outcome) => {
      const next = deferred<string>();
      const load = vi.fn<(signal: AbortSignal) => Promise<string>>()
        .mockResolvedValueOnce("saved")
        .mockReturnValueOnce(next.promise);
      const resource = renderHook(({ request }) => useAsyncResource(request), {
        initialProps: { request: load as (signal: AbortSignal) => Promise<string> },
      });
      await waitFor(() => expect(resource.result.current.data).toBe("saved"));
      let completion!: Promise<void>;
      let settled = false;
      act(() => {
        completion = resource.result.current.reload();
        void completion.then(() => { settled = true; });
      });
      expect(settled).toBe(false);
      if (outcome === "success") await act(async () => next.resolve("updated"));
      if (outcome === "failure") await act(async () => next.reject(new Error("unavailable")));
      if (outcome === "scope change") resource.rerender({ request: () => Promise.resolve("new scope") });
      if (outcome === "unmount") resource.unmount();
      await act(async () => { await completion; });
      expect(settled).toBe(true);
      if (outcome === "failure") {
        expect(resource.result.current.data).toBe("saved");
        expect(resource.result.current.error).toBeInstanceOf(Error);
      }
      if (outcome === "scope change") expect(resource.result.current.data).toBe("new scope");
      if (outcome === "unmount" || outcome === "scope change") {
        expect(load.mock.calls[1][0].aborted).toBe(true);
      }
    },
  );

  it("settles batched reload callers when their latest request completes", async () => {
    const next = deferred<string>();
    const load = vi.fn().mockResolvedValueOnce("saved").mockReturnValueOnce(next.promise);
    const resource = renderHook(() => useAsyncResource(load));
    await waitFor(() => expect(resource.result.current.data).toBe("saved"));
    let completions!: Promise<void>[];
    act(() => { completions = [resource.result.current.reload(), resource.result.current.reload()]; });
    expect(load).toHaveBeenCalledTimes(2);
    await act(async () => next.resolve("latest"));
    await act(async () => { await Promise.all(completions); });
    expect(resource.result.current.data).toBe("latest");
  });

  it("settles a reload even if the owner unmounts before its new effect runs", async () => {
    const load = vi.fn(() => Promise.resolve("saved"));
    const resource = renderHook(() => useAsyncResource(load));
    await waitFor(() => expect(resource.result.current.data).toBe("saved"));
    let completion!: Promise<void>;
    act(() => {
      completion = resource.result.current.reload();
      resource.unmount();
    });
    await completion;
    expect(load).toHaveBeenCalledTimes(1);
  });
});
