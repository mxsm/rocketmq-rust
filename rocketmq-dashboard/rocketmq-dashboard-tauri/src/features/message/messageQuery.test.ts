import { describe, expect, it, vi } from 'vitest';
import { buildMessageQuery, createMessageQueryController, parseMessageTime, type MessageQueryDraft } from './messageQuery';
import type { MessageIdQueryRequest, MessageKeyQueryRequest, MessagePageQueryRequest, MessagePageResponse, MessageSummary } from './types/message.types';

const draft: MessageQueryDraft = { mode: 'key', topic: ' orders.events ', key: ' order-1 ', messageId: 'id-1', begin: '2026-09-11T08:00', end: '2026-09-11T09:00' };
const summary: MessageSummary = { topic: 'orders.events', msgId: 'physical-id', queryMsgId: 'query-id', storeTimestamp: 1000 };
function service() { return { queryMessageByTopicKey: vi.fn(async (_request: MessageKeyQueryRequest) => ({ items: [summary], total: 1 })), queryMessageById: vi.fn(async (_request: MessageIdQueryRequest) => ({ items: [summary], total: 1 })),
    queryMessagePageByTopic: vi.fn(async (_request: MessagePageQueryRequest): Promise<MessagePageResponse> => ({ taskId: 'cursor-1', page: { content: [summary], number: 0, size: 12, totalElements: 20, totalPages: 2, numberOfElements: 1, first: true, last: false, empty: false } })) }; }

describe('Message query model', () => {
    it('constructs only the fields required by the chosen mode', () => {
        expect(buildMessageQuery(draft)).toEqual({ mode: 'key', topic: 'orders.events', key: 'order-1' });
        expect(buildMessageQuery({ ...draft, mode: 'id' })).toEqual({ mode: 'id', topic: 'orders.events', messageId: 'id-1' });
        expect(buildMessageQuery({ ...draft, mode: 'time' })).toEqual({ mode: 'time', topic: 'orders.events', begin: parseMessageTime(draft.begin), end: parseMessageTime(draft.end) });
    });
    it('rejects empty identity, invalid calendar dates and reversed windows', () => {
        expect(() => buildMessageQuery({ ...draft, topic: ' ' })).toThrow();
        expect(() => buildMessageQuery({ ...draft, key: '' })).toThrow();
        expect(() => buildMessageQuery({ ...draft, mode: 'id', messageId: '' })).toThrow();
        for (const value of ['', '2026-02-31T10:00', '2026-09-11T25:00', '2026-09-11T10:00:61', '1960-01-01T00:00']) expect(() => parseMessageTime(value)).toThrow();
        expect(() => buildMessageQuery({ ...draft, mode: 'time', begin: draft.end, end: draft.begin })).toThrow();
    });
});

describe('Message query request ownership', () => {
    it('uses the correct API and never carries parameters between modes', async () => {
        const api = service(), controller = createMessageQueryController(api, () => true);
        controller.start();
        await controller.read(buildMessageQuery(draft));
        await controller.read(buildMessageQuery({ ...draft, mode: 'id' }));
        expect(api.queryMessageByTopicKey).toHaveBeenCalledWith({ topic: 'orders.events', key: 'order-1' });
        expect(api.queryMessageById).toHaveBeenCalledWith({ topic: 'orders.events', messageId: 'id-1' });
        expect(api.queryMessagePageByTopic).not.toHaveBeenCalled();
    });
    it('reuses the time cursor only for the same window and explicitly restarts refresh', async () => {
        const api = service(), controller = createMessageQueryController(api, () => true);
        controller.start();
        const query = buildMessageQuery({ ...draft, mode: 'time' });
        await controller.read(query);
        await controller.read(query, 2);
        expect(api.queryMessagePageByTopic.mock.calls[1][0]).toMatchObject({ pageNum: 2, taskId: 'cursor-1' });
        await controller.read(query, 1, true);
        expect(api.queryMessagePageByTopic.mock.calls[2][0]).toHaveProperty('taskId', undefined);
        await controller.read(buildMessageQuery({ ...draft, mode: 'time', end: '2026-09-11T10:00' }));
        expect(api.queryMessagePageByTopic.mock.calls[3][0]).toHaveProperty('taskId', undefined);
    });
    it('coalesces duplicate requests and discards a late response after switching mode', async () => {
        const api = service();
        let resolve!: (value: { items: MessageSummary[]; total: number }) => void;
        api.queryMessageByTopicKey.mockReturnValue(new Promise(complete => { resolve = complete; }));
        const controller = createMessageQueryController(api, () => true);
        controller.start();
        const query = buildMessageQuery(draft), pending = controller.read(query);
        expect(controller.read(query)).toBe(pending);
        await Promise.resolve();
        controller.reset();
        await controller.read(buildMessageQuery({ ...draft, mode: 'id' }));
        resolve({ items: [], total: 0 });
        expect(await pending).toBe(false);
        expect(controller.getSnapshot().result?.query.mode).toBe('id');
        expect(api.queryMessageByTopicKey).toHaveBeenCalledTimes(1);
    });
    it('retains same-query data on failure and rejects a cross-Topic response', async () => {
        const api = service(), controller = createMessageQueryController(api, () => true);
        controller.start();
        const query = buildMessageQuery(draft);
        await controller.read(query);
        const before = controller.getSnapshot();
        api.queryMessageByTopicKey.mockRejectedValueOnce(new Error('unavailable'));
        expect(await controller.read(query)).toBe(false);
        expect(controller.getSnapshot()).toMatchObject({ result: before.result, receivedAt: before.receivedAt });
        api.queryMessageByTopicKey.mockResolvedValueOnce({ items: [{ ...summary, topic: 'another-topic' }], total: 1 });
        expect(await controller.read(query)).toBe(false);
        expect(controller.getSnapshot().result?.items).toEqual([summary]);
    });
    it('prevents dispatch after disposal and ignores an environment change', async () => {
        const api = service();
        let current = true;
        const controller = createMessageQueryController(api, () => current);
        controller.start();
        const pending = controller.read(buildMessageQuery(draft));
        controller.stop();
        expect(await pending).toBe(false);
        expect(api.queryMessageByTopicKey).not.toHaveBeenCalled();
        controller.start();
        current = false;
        expect(await controller.read(buildMessageQuery(draft))).toBe(false);
    });
});
