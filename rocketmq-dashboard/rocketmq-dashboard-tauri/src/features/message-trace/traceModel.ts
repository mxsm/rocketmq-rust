import type { MessageSummary } from '../message/types/message.types';
import type { MessageTraceDetail, MessageTraceNode } from './types/message-trace.types';

export type TraceQuery =
    | { mode: 'id'; traceTopic: string; messageId: string }
    | { mode: 'key'; traceTopic: string; topic: string; key: string };

export interface TraceQueryDraft {
    mode: TraceQuery['mode'];
    traceTopic: string;
    messageId: string;
    topic: string;
    key: string;
}

export function buildTraceQuery(draft: TraceQueryDraft): TraceQuery {
    const traceTopic = draft.traceTopic.trim();
    if (!traceTopic) throw new Error('Enter a Trace Topic.');
    if (draft.mode === 'id') {
        const messageId = draft.messageId.trim();
        if (!messageId) throw new Error('Enter a producer unique message ID.');
        return { mode: 'id', traceTopic, messageId };
    }
    const topic = draft.topic.trim(), key = draft.key.trim();
    if (!topic || !key) throw new Error('Enter a business Topic and message Key.');
    return { mode: 'key', traceTopic, topic, key };
}

export const traceCandidateIdentity = (message: MessageSummary) => JSON.stringify([message.topic, message.msgId]);

/** Physical copies of one unique ID resolve to the same trace, not separate timelines. */
export function traceCandidates(query: TraceQuery, items: MessageSummary[]): MessageSummary[] {
    const candidates = new Map<string, MessageSummary>();
    for (const item of items) {
        if (!item.msgId.trim() || (query.mode === 'id' ? item.msgId !== query.messageId : item.topic !== query.topic)) {
            throw new Error('The trace candidates do not match the requested message identity.');
        }
        const identity = traceCandidateIdentity(item);
        if (!candidates.has(identity)) candidates.set(identity, item);
    }
    return [...candidates.values()];
}

export function checkTraceDetail(detail: MessageTraceDetail, traceTopic: string, message: MessageSummary): MessageTraceDetail {
    if (detail.traceTopic !== traceTopic || detail.msgId !== message.msgId ||
        (message.topic && detail.topic && detail.topic !== message.topic)) {
        throw new Error('The trace detail does not match the selected message and Trace Topic.');
    }
    return detail;
}

export function traceStatus(status?: string | null): 'success' | 'failed' | 'unknown' {
    if (status === 'success') return 'success';
    if (status === 'failed') return 'failed';
    return 'unknown';
}

export const traceDuration = (value?: number | null) =>
    value != null && Number.isFinite(value) && value >= 0 ? `${value.toLocaleString()} ms` : 'Not reported';

export function traceTimestamp(value?: number | null): string {
    if (value == null || !Number.isFinite(value) || value <= 0) return 'Not reported';
    const date = new Date(value);
    if (!Number.isFinite(date.getTime())) return 'Not reported';
    const pad = (part: number) => String(part).padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}.${String(date.getMilliseconds()).padStart(3, '0')}`;
}

/** There is no event ID in the DTO. Preserve identical events and never transfer selection to a changed event. */
export function traceEvents(nodes: readonly MessageTraceNode[]) {
    const occurrences = new Map<string, number>();
    return nodes.map(node => {
        const signature = JSON.stringify([node.traceType, node.role, node.groupName, node.clientHost, node.storeHost,
            node.timestamp, node.costTime, node.status, node.retryTimes, node.fromTransactionCheck]);
        const occurrence = occurrences.get(signature) ?? 0;
        occurrences.set(signature, occurrence + 1);
        return { id: JSON.stringify([signature, occurrence]), node };
    });
}
