import { describe, expect, it, vi } from 'vitest';
import { DashboardClientError } from '../../services/invoke';
import { createMonitorActions } from './monitorActions';
import type { MonitorTarget } from './monitorModel';

const context = { environmentId: 'env-a', revision: 7 };
const target: MonitorTarget = { id: 'attempt', draftId: 'draft', context, kind: 'save', request: { consumerGroup: 'orders', minCount: 2, maxDiffTotal: 500, expectedRevision: 3 } };
const deferred = () => {
    let resolve!: (value: unknown) => void;
    const promise = new Promise<unknown>(done => { resolve = done; });
    return { promise, resolve };
};
describe('session-owned monitor writes', () => {
    it('retains an accepted receipt in the original environment and prevents overlapping submissions', async () => {
        let current = { ...context };
        const write = deferred();
        const dispatch = vi.fn(() => write.promise);
        const actions = createMonitorActions(() => current, dispatch);
        actions.start();
        const pending = actions.submit(target);
        expect(await actions.submit(target)).toBeNull();
        current = { environmentId: 'env-b', revision: 8 };
        write.resolve({ message: 'Monitor rule saved.' });
        expect(await pending).toMatchObject({ outcome: 'success', target: { context } });
        expect(actions.getSnapshot().receipt?.target.context.environmentId).toBe('env-a');
        expect(dispatch).toHaveBeenCalledTimes(1);
    });
    it('rejects a stale target before dispatch', async () => {
        const dispatch = vi.fn();
        const actions = createMonitorActions(() => ({ ...context, revision: 8 }), dispatch);
        actions.start();
        await expect(actions.submit(target)).rejects.toThrow('environment changed');
        expect(dispatch).not.toHaveBeenCalled();
    });
    it('recognizes a compare-and-swap conflict without retrying or changing the expected revision', async () => {
        const dispatch = vi.fn().mockRejectedValue(new DashboardClientError({ code: 'dashboard.monitor_conflict', category: 'validation', retryable: false, message: 'conflict' }));
        const actions = createMonitorActions(() => context, dispatch);
        actions.start();
        expect(await actions.submit(target)).toMatchObject({ outcome: 'conflict', target: { request: { expectedRevision: 3 } } });
        expect(dispatch).toHaveBeenCalledTimes(1);
    });
    it('requires a commit receipt and does not expose arbitrary thrown details', async () => {
        const dispatch = vi.fn().mockResolvedValueOnce({ message: 'unexpected' }).mockRejectedValueOnce(new Error('private connection details'));
        const actions = createMonitorActions(() => context, dispatch);
        actions.start();
        expect(await actions.submit(target)).toMatchObject({ outcome: 'unconfirmed' });
        const receipt = await actions.submit({ ...target, id: 'explicit-next-attempt' });
        expect(receipt?.outcome).toBe('unconfirmed');
        expect(receipt?.message).not.toContain('private');
    });
    it('clears session results on disposal and ignores an old completion after a new session starts', async () => {
        const old = deferred();
        const actions = createMonitorActions(() => context, () => old.promise);
        expect(await actions.submit(target)).toBeNull();
        actions.start();
        const pending = actions.submit(target);
        actions.stop();
        actions.start();
        old.resolve({ message: 'Monitor rule saved.' });
        expect(await pending).toBeNull();
        expect(actions.getSnapshot()).toEqual({ pending: null, receipt: null });
    });
});
