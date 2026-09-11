import { afterEach, expect, it, vi } from 'vitest';
import { createProxyController } from './proxyController';
import type { ConnectionSettingsView } from '../../services/connection.store';
import type { ProxyHomePageInfo, ProxyMutationResult } from './types/proxy.types';
import { DashboardClientError } from '../../services/invoke';
import { resolveConsumerScope } from '../consumer/scope';

function settings(revision = 1, current: string | null = 'a:8080'): ConnectionSettingsView {
    const addresses = current === null ? [] : ['a:8080', 'b:8080'];
    return { revision, credentialsConfigured: false, currentNameserverId: null, environmentId: 'env-a',
        nameserver: { currentNamesrv: null, namesrvAddrList: [], useTLS: false, useVIPChannel: false },
        currentProxyId: current, proxy: { currentProxyAddr: current, proxyAddrList: addresses },
        endpoints: addresses.map(address => ({ endpointId: address, kind: 'proxy', address, environmentId: null })) };
}
function home(revision = 1): ProxyHomePageInfo {
    const config = settings(revision);
    return { ...config.proxy, settings: config };
}
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
function setup() {
    const service = {
        getHomePageInfo: vi.fn<() => Promise<ProxyHomePageInfo>>().mockResolvedValue(home()),
        addProxyAddr: vi.fn<(address: string, revision: number) => Promise<ProxyMutationResult>>(),
        switchProxyAddr: vi.fn<(address: string, revision: number) => Promise<ProxyMutationResult>>(),
        deleteProxyAddr: vi.fn<(address: string, revision: number) => Promise<ProxyMutationResult>>(),
    };
    const controller = createProxyController(service);
    controller.start();
    return { service, controller };
}
const conflict = () => new DashboardClientError({ code: 'dashboard.configuration_conflict', message: 'Settings changed.', category: 'validation', retryable: false });
afterEach(() => vi.restoreAllMocks());

it('coalesces overlapping configuration refreshes', async () => {
    const { controller, service } = setup();
    const pending = deferred<ProxyHomePageInfo>();
    service.getHomePageInfo.mockReturnValue(pending.promise);
    const first = controller.refresh();
    expect(controller.refresh(true)).toBe(first);
    await Promise.resolve();
    expect(service.getHomePageInfo).toHaveBeenCalledTimes(1);
    pending.resolve(home());
    expect(await first).toBe(true);
    expect(controller.getSnapshot()).toMatchObject({ settings: { revision: 1 }, refreshing: false });
});

it('preserves configuration and its observation time after refresh failure without exposing raw errors', async () => {
    const { controller, service } = setup();
    vi.spyOn(Date, 'now').mockReturnValue(100);
    await controller.refresh();
    service.getHomePageInfo.mockRejectedValue(new Error('secret details'));
    expect(await controller.refresh()).toBe(false);
    expect(controller.getSnapshot()).toMatchObject({ refreshedAt: 100, settings: { revision: 1 }, loadError: 'Could not refresh Proxy settings.' });
});

it('does not let an old read undo a newer shared revision requiring review', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    const pending = deferred<ProxyHomePageInfo>();
    service.getHomePageInfo.mockReturnValueOnce(pending.promise).mockResolvedValue(home(2));
    const old = controller.refresh();
    await Promise.resolve();
    controller.observeRevision(2);
    expect(await controller.submit({ kind: 'delete', address: 'a:8080' })).toBe(false);
    pending.resolve(home());
    expect(await old).toBe(false);
    expect(controller.getSnapshot()).toMatchObject({ settings: { revision: 1 }, needsReview: true, refreshedAt: null });
    expect(await controller.refresh(true)).toBe(true);
    expect(controller.getSnapshot()).toMatchObject({ settings: { revision: 2 }, needsReview: false });
    expect(service.deleteProxyAddr).not.toHaveBeenCalled();
});

it('keeps a conflicted intention until explicit retry at the reviewed revision', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    const change = { kind: 'add', address: 'new:8080' } as const;
    service.addProxyAddr.mockRejectedValueOnce(conflict()).mockResolvedValueOnce({ message: 'Saved', settings: settings(3) });
    expect(await controller.submit(change)).toBe(false);
    expect(controller.getSnapshot()).toMatchObject({ failedChange: change, needsReview: true, settings: { revision: 1 } });
    service.getHomePageInfo.mockResolvedValue(home(2));
    await controller.refresh(true);
    expect(controller.getSnapshot().failedChange).toEqual(change);
    expect(service.addProxyAddr).toHaveBeenCalledTimes(1);
    expect(await controller.submit(change)).toBe(true);
    expect(service.addProxyAddr.mock.calls).toEqual([['new:8080', 1], ['new:8080', 2]]);
});

it('retains a successful selection and its scoped endpoint when readback fails', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    service.switchProxyAddr.mockResolvedValue({ message: 'Selected', settings: settings(2, 'b:8080') });
    service.getHomePageInfo.mockRejectedValue(new Error('unavailable'));
    expect(await controller.submit({ kind: 'switch', address: 'b:8080' })).toBe(true);
    await controller.refresh();
    expect(controller.getSnapshot()).toMatchObject({ settings: { revision: 2 }, receipt: { revision: 2, message: 'Selected' }, changeError: '', refreshedAt: null });
    expect(resolveConsumerScope(controller.getSnapshot().settings, 'proxy')).toEqual({ mode: 'proxy', endpointId: 'b:8080' });
});

it('serializes writes and invalidates a read that started before a mutation', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    const oldRead = deferred<ProxyHomePageInfo>();
    service.getHomePageInfo.mockReturnValueOnce(oldRead.promise).mockResolvedValue(home(2));
    const reading = controller.refresh();
    await Promise.resolve();
    const pending = deferred<ProxyMutationResult>();
    service.deleteProxyAddr.mockReturnValue(pending.promise);
    const writing = controller.submit({ kind: 'delete', address: 'b:8080' });
    expect(await controller.submit({ kind: 'delete', address: 'b:8080' })).toBe(false);
    expect(await controller.refresh()).toBe(false);
    oldRead.resolve(home());
    expect(await reading).toBe(false);
    pending.resolve({ message: 'Deleted', settings: settings(2) });
    expect(await writing).toBe(true);
    expect(service.deleteProxyAddr).toHaveBeenCalledExactlyOnceWith('b:8080', 1);
});

it('preserves the accepted receipt if another revision arrives during the write', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    const pending = deferred<ProxyMutationResult>();
    service.switchProxyAddr.mockReturnValue(pending.promise);
    const writing = controller.submit({ kind: 'switch', address: 'b:8080' });
    controller.observeRevision(3);
    pending.resolve({ message: 'Selected', settings: settings(2, 'b:8080') });
    expect(await writing).toBe(true);
    expect(controller.getSnapshot()).toMatchObject({ receipt: { revision: 2 }, needsReview: true, settings: { revision: 2 } });
    expect(service.getHomePageInfo).toHaveBeenCalledTimes(1);
});

it('uses the backend fallback after deleting a selected Proxy and allows an empty catalog', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    const fallback = settings(2, 'b:8080');
    fallback.proxy.proxyAddrList = ['b:8080'];
    service.deleteProxyAddr.mockResolvedValueOnce({ message: 'Deleted', settings: fallback }).mockResolvedValueOnce({ message: 'Deleted', settings: settings(3, null) });
    service.getHomePageInfo.mockResolvedValueOnce({ settings: fallback, ...fallback.proxy });
    await controller.submit({ kind: 'delete', address: 'a:8080' });
    await controller.refresh();
    expect(resolveConsumerScope(controller.getSnapshot().settings, 'proxy')).toEqual({ mode: 'proxy', endpointId: 'b:8080' });
    await controller.submit({ kind: 'delete', address: 'b:8080' });
    expect(resolveConsumerScope(controller.getSnapshot().settings, 'proxy')).toBeNull();
    expect(resolveConsumerScope(controller.getSnapshot().settings, 'name_server')).toEqual({ mode: 'name_server' });
});

it('ignores late callbacks after disposal and supports Strict Mode restart', async () => {
    const { controller, service } = setup();
    const oldRead = deferred<ProxyHomePageInfo>();
    service.getHomePageInfo.mockReturnValueOnce(oldRead.promise).mockResolvedValue(home(2));
    const old = controller.refresh();
    await Promise.resolve();
    controller.stop();
    controller.start();
    const current = controller.refresh();
    oldRead.reject(new Error('old'));
    expect(await old).toBe(false);
    expect(await current).toBe(true);
    expect(controller.getSnapshot()).toMatchObject({ settings: { revision: 2 }, loadError: '' });
});

it('does not dispatch a read stopped before the first microtask', async () => {
    const { controller, service } = setup();
    const pending = controller.refresh();
    controller.stop();
    expect(await pending).toBe(false);
    expect(service.getHomePageInfo).not.toHaveBeenCalled();
});

it('does not apply a late mutation receipt to an unmounted page', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    const pending = deferred<ProxyMutationResult>();
    service.addProxyAddr.mockReturnValue(pending.promise);
    const writing = controller.submit({ kind: 'add', address: 'b:8080' });
    controller.stop();
    const stopped = controller.getSnapshot();
    pending.resolve({ message: 'Added', settings: settings(2) });
    expect(await writing).toBe(false);
    expect(controller.getSnapshot()).toBe(stopped);
    expect(service.getHomePageInfo).toHaveBeenCalledTimes(1);
});
