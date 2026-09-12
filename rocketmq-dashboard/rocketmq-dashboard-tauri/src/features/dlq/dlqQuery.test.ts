import { expect, it, vi } from 'vitest';
import { buildDlqQuery, createDlqQueryController, dlqGroup } from './dlqQuery';
import type { DlqMessageDetail, DlqMessagePageResponse } from './types/dlq.types';

const page = (number = 0, topic = '%DLQ%g'): DlqMessagePageResponse => ({ taskId: 'scan', page: { content: [{ topic, msgId: 'unique', queryMsgId: 'physical', storeTimestamp: 0 }],
    number, size: 12, totalPages: 2, totalElements: 13, numberOfElements: 1, first: number === 0, last: number === 1, empty: false } });
const detail: DlqMessageDetail = { topic: '%DLQ%g', msgId: 'physical', properties: { UNIQ_KEY: 'unique' }, storeTimestamp: 0 };
const service = () => ({ queryDlqMessageByConsumerGroup: vi.fn(async () => page()), viewDlqMessageDetail: vi.fn(async () => detail) });

it('normalizes prefixed groups and validates only fields required for the active mode', () => {
    expect(dlqGroup(' %DLQ% g ')).toBe('g');
    expect(() => dlqGroup('%DLQ% ')).toThrow('Consumer group');
    expect(buildDlqQuery({ mode: 'key', consumerGroup: '%DLQ%g', key: ' order ', messageId: '', begin: '', end: '' })).toEqual({ mode: 'key', topic: '%DLQ%g', key: 'order' });
    expect(() => buildDlqQuery({ mode: 'time', consumerGroup: 'g', key: '', messageId: '', begin: '2026-02-30T10:00', end: '2026-03-01T10:00' })).toThrow();
});

it('uses time cursors only within the same scan and starts explicit refresh from page one', async () => {
    const api = service();
    const controller = createDlqQueryController(() => true, api); controller.start();
    const query = { mode: 'time' as const, topic: '%DLQ%g', begin: 0, end: 100 };
    await controller.read(query);
    await controller.read(query, 2);
    expect(api.queryDlqMessageByConsumerGroup).toHaveBeenLastCalledWith({ consumerGroup: 'g', begin: 0, end: 100, pageNum: 2, pageSize: 12, taskId: 'scan' });
    await controller.read(query, 1, true);
    expect(api.queryDlqMessageByConsumerGroup).toHaveBeenLastCalledWith(expect.objectContaining({ pageNum: 1, taskId: undefined }));
    await controller.read({ mode: 'key', topic: '%DLQ%g', key: 'order' });
    expect(api.queryDlqMessageByConsumerGroup).toHaveBeenLastCalledWith({ consumerGroup: 'g', key: 'order', begin: 0, end: expect.any(Number), pageNum: 1, pageSize: 64 });
});

it('resolves ID lookup to a physical DLQ target and rejects wrong identity', async () => {
    const api = service(); const controller = createDlqQueryController(() => true, api); controller.start();
    await controller.read({ mode: 'id', topic: '%DLQ%g', messageId: 'unique' });
    expect(controller.getSnapshot().result?.items[0]).toMatchObject({ queryMsgId: 'physical', msgId: 'unique', topic: '%DLQ%g' });
    await controller.read({ mode: 'id', topic: '%DLQ%g', messageId: 'unrelated' });
    expect(controller.getSnapshot().result).toBeNull();
    expect(controller.getSnapshot().error).not.toBe('');
});

it('rejects another group and retains the last successful observation on refresh failure', async () => {
    const api = service(); const controller = createDlqQueryController(() => true, api); controller.start();
    const query = { mode: 'key' as const, topic: '%DLQ%g', key: 'order' };
    await controller.read(query); const before = controller.getSnapshot();
    api.queryDlqMessageByConsumerGroup.mockResolvedValueOnce(page(0, '%DLQ%other'));
    expect(await controller.read(query, 1, true)).toBe(false);
    expect(controller.getSnapshot().result).toBe(before.result);
    expect(controller.getSnapshot().receivedAt).toBe(before.receivedAt);
});

it('coalesces duplicate queries and drops a late response after reset or context change', async () => {
    let resolve!: (value: DlqMessagePageResponse) => void;
    let current = true;
    const api = service(); api.queryDlqMessageByConsumerGroup.mockImplementation(() => new Promise(done => { resolve = done; }));
    const controller = createDlqQueryController(() => current, api); controller.start();
    const query = { mode: 'key' as const, topic: '%DLQ%g', key: 'order' };
    const first = controller.read(query); const duplicate = controller.read(query); await Promise.resolve();
    expect(duplicate).toBe(first); expect(api.queryDlqMessageByConsumerGroup).toHaveBeenCalledTimes(1);
    controller.reset(); resolve(page()); await first; expect(controller.getSnapshot().result).toBeNull();
    const second = controller.read(query); await Promise.resolve(); current = false; resolve(page()); await second;
    expect(controller.getSnapshot().result).toBeNull();
});
