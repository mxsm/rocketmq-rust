import { act, renderHook } from '@testing-library/react';
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import type { TopologySnapshot } from '../../types/topology';
import { consumerGroup, fixtureConsumers, fixtureSnapshot } from './topology-fixtures';
import { failedTopologySnapshot } from './topology-loader';
import { MAX_CHANGE_ENTRIES, useTopologySnapshot } from './useTopologySnapshot';

let visibility: DocumentVisibilityState = 'visible';

function setVisibility(next: DocumentVisibilityState) {
  visibility = next;
  document.dispatchEvent(new Event('visibilitychange'));
}

function deferred() {
  let resolve!: (snapshot: TopologySnapshot) => void;
  const promise = new Promise<TopologySnapshot>((done) => {
    resolve = done;
  });
  return { promise, resolve };
}

/** Lets the pending load promise settle and React apply the result. */
const flush = () => act(async () => {
  await Promise.resolve();
});

describe('useTopologySnapshot', () => {
  beforeEach(() => {
    vi.useFakeTimers({ toFake: ['setTimeout', 'clearTimeout', 'Date'] });
    vi.setSystemTime(100_000);
    visibility = 'visible';
    vi.spyOn(document, 'visibilityState', 'get').mockImplementation(() => visibility);
  });

  afterEach(() => {
    vi.useRealTimers();
    vi.restoreAllMocks();
  });

  it('loads at once and then on every interval', async () => {
    const load = vi.fn(() => Promise.resolve(fixtureSnapshot()));
    const { result } = renderHook(() => useTopologySnapshot({ intervalMs: 10_000, load }));

    await flush();
    expect(load).toHaveBeenCalledTimes(1);
    expect(result.current.snapshot?.clusters).toHaveLength(2);
    expect(result.current.querying).toBe(false);
    expect(result.current.nextRefreshAt).toBe(110_000);

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(load).toHaveBeenCalledTimes(2);
    expect(result.current.refreshCount).toBe(2);
  });

  it('never starts a query while one is still running', async () => {
    const pending = deferred();
    const load = vi.fn(() => pending.promise);
    const { result } = renderHook(() => useTopologySnapshot({ intervalMs: 5_000, load }));

    act(() => result.current.refresh());
    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_000);
    });
    expect(load).toHaveBeenCalledTimes(1);
    expect(result.current.querying).toBe(true);
    expect(result.current.nextRefreshAt).toBeNull();

    pending.resolve(fixtureSnapshot());
    await flush();
    expect(result.current.querying).toBe(false);
    expect(result.current.nextRefreshAt).toBe(Date.now() + 5_000);
  });

  it('pauses while the page is hidden and refreshes as soon as it is visible again', async () => {
    const load = vi.fn(() => Promise.resolve(fixtureSnapshot()));
    const { result } = renderHook(() => useTopologySnapshot({ intervalMs: 10_000, load }));
    await flush();

    act(() => setVisibility('hidden'));
    expect(result.current.nextRefreshAt).toBeNull();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(60_000);
    });
    expect(load).toHaveBeenCalledTimes(1);

    act(() => setVisibility('visible'));
    await flush();
    expect(load).toHaveBeenCalledTimes(2);
    expect(result.current.nextRefreshAt).toBe(Date.now() + 10_000);
  });

  it('refreshes only on demand without an interval', async () => {
    const load = vi.fn(() => Promise.resolve(fixtureSnapshot()));
    const { result } = renderHook(() => useTopologySnapshot({ intervalMs: null, load }));
    await flush();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(120_000);
    });
    expect(load).toHaveBeenCalledTimes(1);
    expect(result.current.nextRefreshAt).toBeNull();

    act(() => result.current.refresh());
    await flush();
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('starts a new countdown when the interval changes', async () => {
    const load = vi.fn(() => Promise.resolve(fixtureSnapshot()));
    const { result, rerender } = renderHook(({ intervalMs }) => useTopologySnapshot({ intervalMs, load }), {
      initialProps: { intervalMs: 30_000 as number | null }
    });
    await flush();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(4_000);
    });

    rerender({ intervalMs: 5_000 });
    expect(result.current.nextRefreshAt).toBe(Date.now() + 5_000);
    await act(async () => {
      await vi.advanceTimersByTimeAsync(5_000);
    });
    expect(load).toHaveBeenCalledTimes(2);
  });

  it('keeps the last good data of a failing layer and says when it last loaded', async () => {
    const load = vi.fn<() => Promise<TopologySnapshot>>()
      .mockResolvedValueOnce(fixtureSnapshot({}, [], 100_000))
      .mockResolvedValueOnce(fixtureSnapshot({}, ['brokers'], 110_000));
    const { result } = renderHook(() => useTopologySnapshot({ intervalMs: 10_000, load }));
    await flush();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });

    expect(result.current.snapshot?.clusters).toHaveLength(2);
    expect(result.current.snapshot?.sources.find((source) => source.source === 'brokers')?.status).toBe('failed');
    expect(result.current.loadedAt.brokers).toBe(100_000);
    expect(result.current.loadedAt.consumers).toBe(110_000);
    expect(result.current.changes).toEqual([]);
  });

  it('shows an error only when nothing could be loaded yet', async () => {
    const load = vi.fn<() => Promise<TopologySnapshot>>()
      .mockResolvedValueOnce(failedTopologySnapshot(new Error('offline'), 3, 100_000))
      .mockResolvedValueOnce(fixtureSnapshot())
      .mockResolvedValueOnce(failedTopologySnapshot(new Error('offline'), 3, 120_000));
    const { result } = renderHook(() => useTopologySnapshot({ intervalMs: 10_000, load }));
    await flush();
    expect(result.current.error).toBe('The query failed.');
    expect(result.current.snapshot).toBeNull();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(result.current.error).toBeNull();

    await act(async () => {
      await vi.advanceTimersByTimeAsync(10_000);
    });
    expect(result.current.error).toBeNull();
    expect(result.current.snapshot?.clusters).toHaveLength(2);
    expect(result.current.snapshot?.sources.every((source) => source.status === 'failed')).toBe(true);
  });

  it('lists the changes of each refresh newest first and keeps the latest twenty', async () => {
    let backlog = 2_000;
    const load = vi.fn(() => {
      backlog *= 2;
      return Promise.resolve(fixtureSnapshot({
        consumers: fixtureConsumers.map((item) => (item.rawGroupName === 'order-consumer' ? consumerGroup('order-consumer', 12, 1012, backlog, ['broker-a']) : item))
      }, [], Date.now()));
    });
    const { result } = renderHook(() => useTopologySnapshot({ intervalMs: 5_000, load }));
    await flush();
    expect(result.current.latestChangeCount).toBeNull();

    for (let refresh = 0; refresh < 24; refresh += 1) {
      await act(async () => {
        await vi.advanceTimersByTimeAsync(5_000);
      });
    }

    expect(result.current.latestChangeCount).toBe(1);
    expect(result.current.changes).toHaveLength(MAX_CHANGE_ENTRIES);
    expect(result.current.changes[0].id).toBe('25-0');
    expect(result.current.changes[0].detail).toMatch(/^lag /);
    expect(result.current.changes[1].at).toBeLessThan(result.current.changes[0].at);
  });

  it('drops a result that arrives after unmount', async () => {
    const pending = deferred();
    const load = vi.fn(() => pending.promise);
    const { result, unmount } = renderHook(() => useTopologySnapshot({ intervalMs: 5_000, load }));

    unmount();
    pending.resolve(fixtureSnapshot());
    await flush();
    await act(async () => {
      await vi.advanceTimersByTimeAsync(30_000);
    });

    expect(result.current.snapshot).toBeNull();
    expect(load).toHaveBeenCalledTimes(1);
  });
});
