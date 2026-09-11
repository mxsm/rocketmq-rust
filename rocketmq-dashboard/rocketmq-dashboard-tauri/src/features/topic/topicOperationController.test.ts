import { describe, expect, it, vi } from 'vitest';
import { createTopicOperationController } from './topicOperationController';

function deferred<T>() {
    let resolve!: (value: T) => void;
    let reject!: (reason: unknown) => void;
    const promise = new Promise<T>((yes, no) => { resolve = yes; reject = no; });
    return { promise, resolve, reject };
}
function setup() {
    let current = true;
    const controller = createTopicOperationController(() => current);
    controller.start();
    return { controller, change: () => { current = false; }, restore: () => { current = true; } };
}

describe('Topic dialog request ownership', () => {
    it('drops a read whose connection changed before dispatch', async () => {
        const { controller, change } = setup();
        const task = vi.fn(async () => 'old');
        const result = controller.read(task, 'Read unavailable');
        change();
        expect(await result).toBeNull();
        expect(task).not.toHaveBeenCalled();
        expect(controller.getSnapshot()).toMatchObject({ contextChanged: true, operation: null });
    });
    it('drops an old read response and its error after a context change', async () => {
        for (const failure of [false, true]) {
            const { controller, change } = setup();
            const response = deferred<string>();
            const result = controller.read(() => response.promise, 'Read unavailable');
            await Promise.resolve();
            change(); controller.observeContext();
            if (failure) response.reject(new Error('private upstream value')); else response.resolve('old');
            expect(await result).toBeNull();
            expect(controller.getSnapshot()).toMatchObject({ operation: null, error: '', contextChanged: true });
        }
    });
    it('retains an accepted write acknowledgement when the context changes', async () => {
        const { controller, change } = setup();
        const response = deferred<{ success: boolean }>();
        const result = controller.write(() => response.promise, 'Write unavailable');
        change(); controller.observeContext();
        expect(controller.getSnapshot().operation).toBe('write');
        response.resolve({ success: true });
        expect(await result).toEqual({ success: true });
        expect(controller.getSnapshot()).toMatchObject({ operation: null, contextChanged: true });
        const next = vi.fn(async () => true);
        expect(await controller.read(next, 'Read unavailable')).toBeNull();
        expect(await controller.write(next, 'Write unavailable')).toBeNull();
        expect(next).not.toHaveBeenCalled();
    });
    it('reports a safe failed write without automatically retrying', async () => {
        const { controller, change } = setup();
        const response = deferred<boolean>();
        const task = vi.fn(() => response.promise);
        const result = controller.write(task, 'Inspect the original target before retrying.');
        change();
        response.reject(new Error('credential=private'));
        expect(await result).toBeNull();
        expect(task).toHaveBeenCalledTimes(1);
        expect(controller.getSnapshot()).toMatchObject({ operation: null, contextChanged: true, error: 'Inspect the original target before retrying.' });
    });
    it('blocks overlapping reads and writes synchronously', async () => {
        const { controller } = setup();
        const response = deferred<boolean>();
        const first = controller.write(() => response.promise, 'Write unavailable');
        const duplicate = vi.fn(async () => true);
        expect(await controller.write(duplicate, 'Write unavailable')).toBeNull();
        expect(await controller.read(duplicate, 'Read unavailable')).toBeNull();
        expect(duplicate).not.toHaveBeenCalled();
        response.resolve(true); expect(await first).toBe(true);
    });
    it('does not deliver an old session write result after disposal and restart', async () => {
        const { controller } = setup();
        const response = deferred<boolean>();
        const result = controller.write(() => response.promise, 'Write unavailable');
        controller.stop(); controller.start();
        response.resolve(true);
        expect(await result).toBeNull();
        expect(await controller.read(async () => 'current', 'Read unavailable')).toBe('current');
    });
    it('supports effect replay without duplicate reads', async () => {
        const { controller } = setup();
        const task = vi.fn(async () => 'fresh');
        const first = controller.read(task, 'Read unavailable');
        controller.stop(); controller.start();
        const second = controller.read(task, 'Read unavailable');
        expect(await first).toBeNull();
        expect(await second).toBe('fresh');
        expect(task).toHaveBeenCalledTimes(1);
    });
    it('does not reuse a frozen draft after the previous connection is selected again', async () => {
        const { controller, change, restore } = setup();
        change(); controller.observeContext(); restore();
        expect(controller.isCurrent()).toBe(false);
        expect(await controller.write(async () => true, 'Write unavailable')).toBeNull();
    });
    it('drops pending work on disposal before dispatch', async () => {
        const { controller } = setup();
        const task = vi.fn(async () => true);
        const result = controller.read(task, 'Read unavailable');
        controller.stop();
        expect(await result).toBeNull();
        expect(task).not.toHaveBeenCalled();
    });
});
