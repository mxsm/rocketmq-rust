import { afterEach, expect, it, vi } from 'vitest';
import { createNameServerController } from './nameServerController';
import { DashboardClientError } from '../../services/invoke';
import type { ConnectionSettingsView } from '../../services/connection.store';
import type { NameServerHomePageInfo, NameServerMutationResult } from './types/nameserver.types';

function settings(revision = 1): ConnectionSettingsView {
    return { revision, credentialsConfigured: false, endpoints: [], currentNameserverId: 'a', currentProxyId: null, environmentId: 'env',
        nameserver: { currentNamesrv: 'a:9876', namesrvAddrList: ['a:9876', 'b:9876'], useVIPChannel: false, useTLS: false },
        proxy: { currentProxyAddr: null, proxyAddrList: [] } };
}
function home(revision = 1): NameServerHomePageInfo {
    const config = settings(revision);
    return { settings: config, ...config.nameserver,
        servers: [{ address: 'a:9876', isCurrent: true, isAlive: true }, { address: 'b:9876', isCurrent: false, isAlive: false }] };
}
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (error: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
function setup() {
    const service = {
        getHomePageInfo: vi.fn<() => Promise<NameServerHomePageInfo>>().mockResolvedValue(home()),
        addNameServer: vi.fn<(address: string, revision: number) => Promise<NameServerMutationResult>>(),
        switchNameServer: vi.fn<(address: string, revision: number) => Promise<NameServerMutationResult>>(),
        deleteNameServer: vi.fn<(address: string, revision: number) => Promise<NameServerMutationResult>>(),
        updateVipChannel: vi.fn<(enabled: boolean, revision: number) => Promise<NameServerMutationResult>>(),
        updateUseTls: vi.fn<(enabled: boolean, revision: number) => Promise<NameServerMutationResult>>(),
    };
    const controller = createNameServerController(service);
    controller.start();
    return { controller, service };
}
const conflict = () => new DashboardClientError({ code: 'dashboard.configuration_conflict', message: 'Configuration changed.', category: 'validation', retryable: false });
afterEach(() => vi.restoreAllMocks());

it('coalesces polling and manual refresh into one observation', async () => {
    const { controller, service } = setup();
    const pending = deferred<NameServerHomePageInfo>();
    service.getHomePageInfo.mockReturnValue(pending.promise);
    const first = controller.refresh();
    expect(controller.refresh(true)).toBe(first);
    await Promise.resolve();
    expect(service.getHomePageInfo).toHaveBeenCalledTimes(1);
    expect(controller.getSnapshot().observation).toBeNull();
    pending.resolve(home());
    expect(await first).toBe(true);
    expect(controller.getSnapshot().observation?.servers[1].isAlive).toBe(false);
});

it('preserves the observation time and safe error after a failed refresh', async () => {
    const { controller, service } = setup();
    vi.spyOn(Date, 'now').mockReturnValue(100);
    await controller.refresh();
    service.getHomePageInfo.mockRejectedValue(new Error('secret transport details'));
    expect(await controller.refresh()).toBe(false);
    expect(controller.getSnapshot().observation?.receivedAt).toBe(100);
    expect(controller.getSnapshot().loadError).toBe('Could not refresh NameServer settings.');
});

it('requires review of a changed revision without replaying a pending intent', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    service.getHomePageInfo.mockResolvedValue(home(2));
    expect(await controller.refresh()).toBe(false);
    expect(controller.getSnapshot()).toMatchObject({ settings: { revision: 1 }, observation: null, needsReview: true });
    expect(await controller.submit({ kind: 'vip', enabled: true })).toBe(false);
    expect(service.updateVipChannel).not.toHaveBeenCalled();
    expect(await controller.refresh(true)).toBe(true);
    expect(controller.getSnapshot()).toMatchObject({ settings: { revision: 2 }, needsReview: false });
    expect(service.updateVipChannel).not.toHaveBeenCalled();
});

it('keeps the rejected change for explicit review and retries at the reviewed revision', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    service.updateUseTls.mockRejectedValueOnce(conflict()).mockResolvedValueOnce({ message: 'TLS enabled', settings: settings(3) });
    const change = { kind: 'tls', enabled: true } as const;
    expect(await controller.submit(change)).toBe(false);
    expect(controller.getSnapshot()).toMatchObject({ failedChange: change, needsReview: true, settings: { nameserver: { useTLS: false } } });
    service.getHomePageInfo.mockResolvedValue(home(2));
    await controller.refresh(true);
    expect(controller.getSnapshot().failedChange).toEqual(change);
    expect(await controller.submit(change)).toBe(true);
    expect(service.updateUseTls.mock.calls).toEqual([[true, 1], [true, 2]]);
});

it('retains an accepted write and clears old probes when readback fails', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    service.updateVipChannel.mockResolvedValue({ message: 'VIP enabled', settings: { ...settings(2), nameserver: { ...settings(2).nameserver, useVIPChannel: true } } });
    service.getHomePageInfo.mockRejectedValue(new Error('probe service unavailable'));
    expect(await controller.submit({ kind: 'vip', enabled: true })).toBe(true);
    await controller.refresh();
    expect(controller.getSnapshot()).toMatchObject({
        settings: { revision: 2, nameserver: { useVIPChannel: true } }, observation: null,
        receipt: { message: 'VIP enabled', revision: 2 }, changeError: '', loadError: 'Could not refresh NameServer settings.',
    });
});

it('invalidates in-flight reads before a write and rejects duplicate submissions synchronously', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    const oldRead = deferred<NameServerHomePageInfo>();
    service.getHomePageInfo.mockReturnValueOnce(oldRead.promise).mockResolvedValue(home(2));
    const reading = controller.refresh();
    await Promise.resolve();
    const pendingWrite = deferred<NameServerMutationResult>();
    service.switchNameServer.mockReturnValue(pendingWrite.promise);
    const writing = controller.submit({ kind: 'switch', address: 'b:9876' });
    expect(await controller.submit({ kind: 'switch', address: 'b:9876' })).toBe(false);
    expect(await controller.refresh()).toBe(false);
    oldRead.resolve(home());
    expect(await reading).toBe(false);
    expect(controller.getSnapshot().pendingChange?.kind).toBe('switch');
    pendingWrite.resolve({ message: 'Selected', settings: settings(2) });
    expect(await writing).toBe(true);
    expect(service.switchNameServer).toHaveBeenCalledExactlyOnceWith('b:9876', 1);
    expect(controller.getSnapshot().settings?.revision).toBe(2);
});

it('ignores late reads after stop and supports Strict Mode restart', async () => {
    const { controller, service } = setup();
    const oldRead = deferred<NameServerHomePageInfo>();
    service.getHomePageInfo.mockReturnValueOnce(oldRead.promise).mockResolvedValue(home(2));
    const old = controller.refresh();
    await Promise.resolve();
    controller.stop();
    controller.start();
    const current = controller.refresh();
    oldRead.reject(new Error('retired read'));
    expect(await old).toBe(false);
    expect(await current).toBe(true);
    expect(controller.getSnapshot()).toMatchObject({ settings: { revision: 2 }, loadError: '' });
});

it('does not dispatch a read stopped before its first microtask', async () => {
    const { controller, service } = setup();
    const reading = controller.refresh();
    controller.stop();
    expect(await reading).toBe(false);
    expect(service.getHomePageInfo).not.toHaveBeenCalled();
});

it('does not write local state or read back an accepted mutation after unmount', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    const mutation = deferred<NameServerMutationResult>();
    service.deleteNameServer.mockReturnValue(mutation.promise);
    const writing = controller.submit({ kind: 'delete', address: 'b:9876' });
    controller.stop();
    const stopped = controller.getSnapshot();
    mutation.resolve({ message: 'Deleted', settings: settings(2) });
    expect(await writing).toBe(false);
    expect(controller.getSnapshot()).toBe(stopped);
    expect(service.getHomePageInfo).toHaveBeenCalledTimes(1);
});

it('accepts an empty configuration and passes a complete address group to the service', async () => {
    const { controller, service } = setup();
    const empty = home();
    empty.settings.nameserver = { ...empty.settings.nameserver, currentNamesrv: null, namesrvAddrList: [] };
    empty.servers = [];
    service.getHomePageInfo.mockResolvedValue(empty);
    await controller.refresh();
    service.addNameServer.mockResolvedValue({ message: 'Added', settings: settings(2) });
    await controller.submit({ kind: 'add', address: 'one:9876;two:9876' });
    expect(service.addNameServer).toHaveBeenCalledExactlyOnceWith('one:9876;two:9876', 1);
});

it('ignores an older revision returned after a confirmed mutation', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    service.addNameServer.mockResolvedValue({ message: 'Added', settings: settings(2) });
    await controller.submit({ kind: 'add', address: 'b:9876' });
    await controller.refresh();
    expect(controller.getSnapshot()).toMatchObject({ settings: { revision: 2 }, observation: null });
});
