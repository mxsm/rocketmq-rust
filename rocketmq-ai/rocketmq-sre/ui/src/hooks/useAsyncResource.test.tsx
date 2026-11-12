import { act, renderHook, waitFor } from "@testing-library/react";

import { useAsyncResource } from "./useAsyncResource";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>((done) => {
    resolve = done;
  });
  return { promise, resolve };
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
    act(() => result.current.reload());
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
});
