import { dashboardErrorMessage } from '../../services/invoke';
import type { MessageService } from '../../services/message.service';
import type { MessageSummary } from './types/message.types';

export type MessageQuery =
    | { mode: 'key'; topic: string; key: string }
    | { mode: 'id'; topic: string; messageId: string }
    | { mode: 'time'; topic: string; begin: number; end: number };

export interface MessageQueryDraft {
    mode: MessageQuery['mode'];
    topic: string;
    key: string;
    messageId: string;
    begin: string;
    end: string;
}

export function parseMessageTime(value: string): number {
    const parts = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(value);
    if (!parts) throw new Error('Enter a valid local date and time.');
    const [year, month, day, hour, minute] = parts.slice(1, 6).map(Number);
    const seconds = Number(parts[6] ?? 0);
    const date = new Date(value);
    const timestamp = date.getTime();
    if (!Number.isSafeInteger(timestamp) || timestamp < 0 || date.getFullYear() !== year || date.getMonth() + 1 !== month ||
        date.getDate() !== day || date.getHours() !== hour || date.getMinutes() !== minute || date.getSeconds() !== seconds) {
        throw new Error('The local date and time is invalid or does not exist in this time zone.');
    }
    return timestamp;
}

export function buildMessageQuery(draft: MessageQueryDraft): MessageQuery {
    const topic = draft.topic.trim();
    if (!topic) throw new Error('Enter a Topic.');
    switch (draft.mode) {
        case 'key': {
            const key = draft.key.trim();
            if (!key) throw new Error('Enter a message Key.');
            return { mode: 'key', topic, key };
        }
        case 'id': {
            const messageId = draft.messageId.trim();
            if (!messageId) throw new Error('Enter a message ID.');
            return { mode: 'id', topic, messageId };
        }
        case 'time': {
            const begin = parseMessageTime(draft.begin);
            const end = parseMessageTime(draft.end);
            if (end < begin) throw new Error('End time must be at or after begin time.');
            return { mode: 'time', topic, begin, end };
        }
    }
}

export interface MessageQueryResult {
    query: MessageQuery;
    items: MessageSummary[];
    total: number;
    page: number;
    totalPages: number;
    taskId: string | null;
}

type QueryService = Pick<typeof MessageService, 'queryMessageByTopicKey' | 'queryMessageById' | 'queryMessagePageByTopic'>;
interface QueryState { result: MessageQueryResult | null; pending: boolean; error: string; receivedAt: number | null; }

/** Cursor reuse is confined to one normalized Topic/time window and one mounted connection context. */
export function createMessageQueryController(service: QueryService, contextIsCurrent: () => boolean) {
    const empty = (): QueryState => ({ result: null, pending: false, error: '', receivedAt: null });
    let state = empty();
    let active = false;
    let generation = 0;
    let flight: { key: string; promise: Promise<boolean> } | null = null;
    const listeners = new Set<() => void>();
    const publish = (next: QueryState) => { state = next; listeners.forEach(listener => listener()); };
    const reset = () => { generation++; flight = null; publish(empty()); };
    const read = (query: MessageQuery, page = 1, restart = false): Promise<boolean> => {
        if (!active || !contextIsCurrent()) return Promise.resolve(false);
        const key = JSON.stringify([query, page, restart]);
        if (flight?.key === key) return flight.promise;
        const sameQuery = JSON.stringify(state.result?.query) === JSON.stringify(query);
        const previous = sameQuery ? state.result : null;
        const request = ++generation;
        const isCurrent = () => active && request === generation && contextIsCurrent();
        publish({ result: previous, pending: true, error: '', receivedAt: sameQuery ? state.receivedAt : null });
        const promise = (async () => {
            await Promise.resolve();
            if (!isCurrent()) return false;
            try {
                let result: MessageQueryResult;
                switch (query.mode) {
                    case 'key': {
                        const response = await service.queryMessageByTopicKey({ topic: query.topic, key: query.key });
                        result = { query, items: response.items, total: response.total, page: 1, totalPages: 1, taskId: null };
                        break;
                    }
                    case 'id': {
                        const response = await service.queryMessageById({ topic: query.topic, messageId: query.messageId });
                        result = { query, items: response.items, total: response.total, page: 1, totalPages: 1, taskId: null };
                        break;
                    }
                    case 'time': {
                        const response = await service.queryMessagePageByTopic({ topic: query.topic, begin: query.begin, end: query.end,
                            pageNum: page, pageSize: 12, taskId: restart ? undefined : previous?.taskId ?? undefined });
                        result = { query, items: response.page.content, total: response.page.totalElements,
                            page: response.page.number + 1, totalPages: response.page.totalPages, taskId: response.taskId };
                        break;
                    }
                }
                if (!isCurrent()) return false;
                if (result.items.some(item => item.topic !== query.topic)) throw new Error('The message response does not match the queried Topic.');
                publish({ result, pending: false, error: '', receivedAt: Date.now() });
                return true;
            } catch (error) {
                if (isCurrent()) publish({ ...state, pending: false, error: dashboardErrorMessage(error, 'Message query failed.') });
                return false;
            } finally {
                if (request === generation) flight = null;
            }
        })();
        flight = { key, promise };
        return promise;
    };
    return {
        read, reset,
        start: () => { active = true; },
        stop: () => { active = false; reset(); },
        getSnapshot: () => state,
        subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    };
}
