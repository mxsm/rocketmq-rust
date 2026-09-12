import { describe, expect, it, vi } from 'vitest';
import { createTraceQueryController } from './traceQuery';
import type { TraceQuery } from './traceModel';
import type { MessageKeyQueryRequest, MessageSummaryListResponse } from '../message/types/message.types';
import type { MessageTraceQueryRequest } from './types/message-trace.types';

const response: MessageSummaryListResponse = { items: [{ topic: 'orders', msgId: 'unique-1', queryMsgId: 'physical-1', storeTimestamp: 1000 }], total: 1 };
const id: TraceQuery = { mode: 'id', traceTopic: 'custom.trace', messageId: 'unique-1' };
const key: TraceQuery = { mode: 'key', traceTopic: 'custom.trace', topic: 'orders', key: 'order-1' };
function service() {
    return {
        queryMessageByTopicKey: vi.fn(async (_request: MessageKeyQueryRequest) => response),
        queryMessageTraceById: vi.fn(async (_request: MessageTraceQueryRequest) => response),
    };
}
function deferred<T>() { let resolve!: (value: T) => void; const promise = new Promise<T>(complete => { resolve = complete; }); return { promise, resolve }; }

describe('Trace request ownership', () => {
    it('routes Key and ID queries to their existing APIs with only relevant fields', async () => {
        const api = service(), controller = createTraceQueryController(api, () => true);
        controller.start();
        await controller.read(id); await controller.read(key);
        expect(api.queryMessageTraceById).toHaveBeenCalledExactlyOnceWith({ traceTopic: 'custom.trace', messageId: 'unique-1' });
        expect(api.queryMessageByTopicKey).toHaveBeenCalledExactlyOnceWith({ topic: 'orders', key: 'order-1' });
        expect(controller.getSnapshot().result?.items[0].msgId).toBe('unique-1');
    });
    it('coalesces duplicate requests and discards a late response after changing the input scope', async () => {
        const api = service(), delayed = deferred<MessageSummaryListResponse>();
        api.queryMessageTraceById.mockReturnValueOnce(delayed.promise);
        const controller = createTraceQueryController(api, () => true); controller.start();
        const pending = controller.read(id);
        expect(controller.read(id)).toBe(pending);
        await Promise.resolve(); controller.reset();
        await controller.read({ ...key, traceTopic: 'another.trace' });
        delayed.resolve({ items: [], total: 0 });
        expect(await pending).toBe(false);
        expect(controller.getSnapshot().result?.query).toEqual({ ...key, traceTopic: 'another.trace' });
        expect(api.queryMessageTraceById).toHaveBeenCalledTimes(1);
    });
    it('retains same-query observations on failure, but clears them for a different query', async () => {
        const api = service(), controller = createTraceQueryController(api, () => true); controller.start();
        await controller.read(id); const before = controller.getSnapshot();
        api.queryMessageTraceById.mockRejectedValue(new Error('unavailable'));
        expect(await controller.read(id)).toBe(false);
        expect(controller.getSnapshot()).toMatchObject({ result: before.result, receivedAt: before.receivedAt, pending: false });
        expect(controller.getSnapshot().error).not.toBe('');
        await controller.read({ ...id, traceTopic: 'another.trace' });
        expect(controller.getSnapshot()).toMatchObject({ result: null, receivedAt: null });
    });
    it('rejects mismatched candidates and distinguishes successful empty results from failure', async () => {
        const api = service(), controller = createTraceQueryController(api, () => true); controller.start();
        expect(await controller.read({ ...id, messageId: 'another-id' })).toBe(false);
        expect(controller.getSnapshot().result).toBeNull();
        api.queryMessageTraceById.mockResolvedValueOnce({ items: [], total: 0 });
        expect(await controller.read(id)).toBe(true);
        expect(controller.getSnapshot()).toMatchObject({ error: '', result: { items: [] } });
    });
    it('prevents queued dispatch after disposal and ignores responses from an old environment', async () => {
        const api = service(); let current = true;
        const controller = createTraceQueryController(api, () => current); controller.start();
        const queued = controller.read(id); controller.stop();
        expect(await queued).toBe(false); expect(api.queryMessageTraceById).not.toHaveBeenCalled();
        controller.start(); const delayed = deferred<MessageSummaryListResponse>();
        api.queryMessageTraceById.mockReturnValueOnce(delayed.promise);
        const pending = controller.read(id); await Promise.resolve(); current = false;
        delayed.resolve(response); expect(await pending).toBe(false);
        expect(controller.getSnapshot().result).toBeNull();
        controller.stop();
        expect(controller.getSnapshot().pending).toBe(false);
        expect(await controller.read(key)).toBe(false);
    });
});
