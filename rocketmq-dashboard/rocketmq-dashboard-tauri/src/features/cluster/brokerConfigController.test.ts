import { expect, it, vi } from 'vitest';
import { createBrokerConfigController } from './brokerConfigController';
import type { BrokerConfigUpdateResult, ClusterBrokerConfigView } from './types/cluster.types';

const broker = { clusterName: 'cluster-a', brokerName: 'broker-a', brokerId: 0, address: 'a:10911' };
const entries = { brokerPermission: '6', listenPort: '10911' };
const view = (): ClusterBrokerConfigView => ({ brokerAddr: broker.address, entries: { ...entries } });
const receipt = (readBack: BrokerConfigUpdateResult['readBack'] = 'confirmed'): BrokerConfigUpdateResult =>
    ({ brokerAddr: broker.address, written: true, changedKeys: ['brokerPermission'], readBack,
        entries: readBack === 'unavailable' ? null : { ...entries, brokerPermission: readBack === 'different' ? '2' : '4' } });
function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
function setup() {
    let current = true;
    const service = {
        getClusterBrokerConfig: vi.fn<() => Promise<ClusterBrokerConfigView>>().mockResolvedValue(view()),
        updateBrokerConfig: vi.fn<(request: unknown) => Promise<BrokerConfigUpdateResult>>().mockResolvedValue(receipt()),
    };
    const controller = createBrokerConfigController({ broker, isCurrent: () => current, service });
    controller.start();
    return { controller, service, switchContext: () => { current = false; controller.observeContext(); } };
}
async function reviewed() {
    const fixture = setup();
    await fixture.controller.refresh();
    fixture.controller.setText(JSON.stringify({ ...entries, brokerPermission: '4' }));
    fixture.controller.review();
    return fixture;
}

it('submits only the reviewed difference with the complete Broker identity', async () => {
    const { controller, service } = await reviewed();
    expect(await controller.submit()).toBe(true);
    expect(service.updateBrokerConfig).toHaveBeenCalledExactlyOnceWith({
        clusterName: 'cluster-a', brokerName: 'broker-a', brokerId: 0, brokerAddr: 'a:10911', entries: { brokerPermission: '4' },
    });
    expect(controller.getSnapshot()).toMatchObject({ pending: null, operation: 'idle', receipt: { written: true, readBack: 'confirmed' } });
});

it('serializes pending writes and prevents refresh, edits and duplicate submission', async () => {
    const { controller, service } = await reviewed();
    const pending = deferred<BrokerConfigUpdateResult>();
    service.updateBrokerConfig.mockReturnValue(pending.promise);
    const writing = controller.submit();
    controller.setText('{}');
    expect(await controller.submit()).toBe(false);
    expect(await controller.refresh()).toBe(false);
    expect(controller.getSnapshot().text).toContain('brokerPermission');
    pending.resolve(receipt());
    await writing;
    expect(service.updateBrokerConfig).toHaveBeenCalledTimes(1);
    expect(service.getClusterBrokerConfig).toHaveBeenCalledTimes(1);
});

it('retains an acknowledged write after readback failure and requires a fresh read before another edit', async () => {
    const { controller, service } = await reviewed();
    service.updateBrokerConfig.mockResolvedValue(receipt('unavailable'));
    await controller.submit();
    controller.review();
    expect(await controller.submit()).toBe(false);
    expect(controller.getSnapshot()).toMatchObject({ receipt: { written: true, readBack: 'unavailable' }, requiresRead: true, pending: null });
    service.getClusterBrokerConfig.mockRejectedValueOnce(new Error('private detail'));
    await controller.refresh();
    expect(controller.getSnapshot()).toMatchObject({ receipt: { written: true }, requiresRead: true, error: 'Unable to read the selected Broker configuration.' });
    await controller.refresh();
    expect(controller.getSnapshot()).toMatchObject({ receipt: { written: true }, requiresRead: false });
    expect(service.updateBrokerConfig).toHaveBeenCalledTimes(1);
});

it('shows actual different readback without automatically reapplying the proposed patch', async () => {
    const { controller, service } = await reviewed();
    service.updateBrokerConfig.mockResolvedValue(receipt('different'));
    await controller.submit();
    expect(JSON.parse(controller.getSnapshot().text).brokerPermission).toBe('2');
    expect(controller.getSnapshot()).toMatchObject({ receipt: { readBack: 'different' }, pending: null, requiresRead: false });
    controller.review();
    expect(controller.getSnapshot().error).toBe('No configuration values have changed.');
    expect(service.updateBrokerConfig).toHaveBeenCalledTimes(1);
});

it('retains a late acknowledgement across a connection change without enabling another write', async () => {
    const { controller, service, switchContext } = await reviewed();
    const pending = deferred<BrokerConfigUpdateResult>();
    service.updateBrokerConfig.mockReturnValue(pending.promise);
    const writing = controller.submit();
    switchContext();
    expect(controller.getSnapshot().operation).toBe('writing');
    pending.resolve(receipt('unavailable'));
    expect(await writing).toBe(true);
    expect(controller.getSnapshot()).toMatchObject({ contextChanged: true, receipt: { written: true, brokerAddr: 'a:10911' }, operation: 'idle' });
    expect(await controller.refresh()).toBe(false);
    expect(await controller.submit()).toBe(false);
    expect(service.updateBrokerConfig).toHaveBeenCalledTimes(1);
});

it('preserves the draft on connection change and refuses to dispatch its reviewed target', async () => {
    const { controller, service, switchContext } = await reviewed();
    const text = controller.getSnapshot().text;
    switchContext();
    expect(await controller.submit()).toBe(false);
    expect(controller.getSnapshot()).toMatchObject({ text, contextChanged: true });
    expect(service.updateBrokerConfig).not.toHaveBeenCalled();
});

it('does not accept an old configuration read after a connection switch', async () => {
    const { controller, service, switchContext } = setup();
    const pending = deferred<ClusterBrokerConfigView>();
    service.getClusterBrokerConfig.mockReturnValue(pending.promise);
    const loading = controller.refresh();
    await Promise.resolve();
    switchContext();
    pending.resolve(view());
    expect(await loading).toBe(false);
    expect(controller.getSnapshot()).toMatchObject({ original: null, contextChanged: true, operation: 'idle' });
});

it('allows Strict Mode cleanup/restart and rejects a late response from the first read', async () => {
    const { controller, service } = setup();
    const pending = deferred<ClusterBrokerConfigView>();
    service.getClusterBrokerConfig.mockReturnValueOnce(pending.promise);
    const first = controller.refresh();
    await Promise.resolve();
    controller.stop();
    controller.start();
    const second = controller.refresh();
    pending.reject(new Error('old failure'));
    expect(await first).toBe(false);
    expect(await second).toBe(true);
    expect(controller.getSnapshot()).toMatchObject({ original: entries, error: '', operation: 'idle' });
});

it('does not dispatch an initial read disposed before its first microtask', async () => {
    const { controller, service } = setup();
    const reading = controller.refresh();
    controller.stop();
    expect(await reading).toBe(false);
    expect(service.getClusterBrokerConfig).not.toHaveBeenCalled();
});

it('checks the captured connection again immediately before dispatching the initial read', async () => {
    let current = true;
    const service = { getClusterBrokerConfig: vi.fn().mockResolvedValue(view()), updateBrokerConfig: vi.fn() };
    const controller = createBrokerConfigController({ broker, service, isCurrent: () => current });
    controller.start();
    const reading = controller.refresh();
    current = false;
    expect(await reading).toBe(false);
    expect(service.getClusterBrokerConfig).not.toHaveBeenCalled();
    expect(controller.getSnapshot()).toMatchObject({ contextChanged: true, operation: 'idle', original: null });
});

it('does not resurrect a receipt after the dialog owner is disposed', async () => {
    const { controller, service } = await reviewed();
    const pending = deferred<BrokerConfigUpdateResult>();
    service.updateBrokerConfig.mockReturnValue(pending.promise);
    const writing = controller.submit();
    controller.stop();
    pending.resolve(receipt());
    expect(await writing).toBe(false);
    expect(controller.getSnapshot().receipt).toBeNull();
});

it('requires read and review after an unconfirmed write and redacts unknown error details', async () => {
    const { controller, service } = await reviewed();
    service.updateBrokerConfig.mockRejectedValue(new Error('private backend details'));
    expect(await controller.submit()).toBe(false);
    expect(controller.getSnapshot()).toMatchObject({ requiresRead: true, pending: null, receipt: null,
        error: 'Write result was not confirmed. Read the current configuration before another change.' });
    controller.review();
    expect(await controller.submit()).toBe(false);
    expect(service.updateBrokerConfig).toHaveBeenCalledTimes(1);
});

it('rejects mismatched Broker responses without displaying a successful result', async () => {
    const { controller, service } = await reviewed();
    service.updateBrokerConfig.mockResolvedValue({ ...receipt(), brokerAddr: 'other:10911' });
    expect(await controller.submit()).toBe(false);
    expect(controller.getSnapshot()).toMatchObject({ receipt: null, requiresRead: true });
});

it('keeps invalid JSON or removed keys out of a submitted patch', async () => {
    const { controller, service } = setup();
    await controller.refresh();
    controller.setText('{');
    controller.review();
    expect(controller.getSnapshot().error).toBe('Enter valid configuration JSON.');
    controller.setText('{}');
    controller.review();
    expect(controller.getSnapshot().error).toContain('Removing configuration keys');
    expect(await controller.submit()).toBe(false);
    expect(service.updateBrokerConfig).not.toHaveBeenCalled();
});
