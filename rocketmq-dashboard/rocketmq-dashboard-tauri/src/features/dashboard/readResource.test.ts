import { afterEach, expect, it, vi } from 'vitest';
import { createDashboardRead } from './readResource';

function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}

afterEach(() => vi.restoreAllMocks());

it('coalesces repeated refreshes into the same request', async () => {
    const pending = deferred<number>();
    const loader = vi.fn(() => pending.promise);
    const resource = createDashboardRead(loader, 'Unavailable');
    const first = resource.read();
    expect(resource.read()).toBe(first);
    await Promise.resolve();
    expect(loader).toHaveBeenCalledTimes(1);
    pending.resolve(42);
    expect(await first).toBe(true);
    expect(resource.getSnapshot()).toMatchObject({ data: 42, pending: false, error: '' });
});

it('keeps the previous observation and success time after a failed refresh', async () => {
    const now = vi.spyOn(Date, 'now').mockReturnValue(100);
    const loader = vi.fn<() => Promise<number>>().mockResolvedValueOnce(4).mockRejectedValueOnce(new Error('offline'));
    const resource = createDashboardRead(loader, 'Unavailable');
    await resource.read();
    now.mockReturnValue(200);
    expect(await resource.read()).toBe(false);
    expect(resource.getSnapshot()).toMatchObject({ data: 4, receivedAt: 100, pending: false });
    expect(resource.getSnapshot().error).not.toBe('');
});

it('ignores late completion after disposal, including a late failure', async () => {
    const pending = deferred<number>();
    const resource = createDashboardRead(() => pending.promise, 'Unavailable');
    const result = resource.read();
    await Promise.resolve();
    resource.invalidate();
    const state = resource.getSnapshot();
    pending.reject(new Error('old request'));
    expect(await result).toBe(false);
    expect(resource.getSnapshot()).toBe(state);
});

it('allows the StrictMode mount-cleanup-mount sequence without old responses winning', async () => {
    const old = deferred<string>();
    const next = deferred<string>();
    const loader = vi.fn().mockReturnValueOnce(old.promise).mockReturnValueOnce(next.promise);
    const resource = createDashboardRead<string>(loader, 'Unavailable');
    const before = resource.read();
    await Promise.resolve();
    resource.invalidate();
    const after = resource.read();
    old.resolve('old environment');
    expect(await before).toBe(false);
    expect(resource.getSnapshot().pending).toBe(true);
    next.resolve('current environment');
    expect(await after).toBe(true);
    expect(resource.getSnapshot().data).toBe('current environment');
});

it('preserves successful pagination if a later page fails and permits retry', async () => {
    const resource = createDashboardRead(async () => [3, 4], 'Unavailable');
    await resource.read();
    expect(await resource.read(async previous => [...previous!, 1, 2])).toBe(true);
    expect(await resource.read(() => Promise.reject(new Error('offline')))).toBe(false);
    expect(resource.getSnapshot().data).toEqual([3, 4, 1, 2]);
    expect(await resource.read()).toBe(true);
    expect(resource.getSnapshot().data).toEqual([3, 4]);
});

it('does not fetch or show pending for an unselected optional query', async () => {
    const resource = createDashboardRead(null, 'Unavailable');
    expect(await resource.read()).toBe(true);
    expect(resource.getSnapshot()).toEqual({ data: null, error: '', receivedAt: null, pending: false });
});

it('does not dispatch a read invalidated before it starts', async () => {
    const loader = vi.fn(async () => 1);
    const resource = createDashboardRead(loader, 'Unavailable');
    const request = resource.read();
    resource.invalidate();
    expect(await request).toBe(false);
    expect(loader).not.toHaveBeenCalled();
});
