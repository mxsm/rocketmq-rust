import { describe, expect, it } from 'vitest';
import { buildTraceQuery, checkTraceDetail, traceCandidates, traceCandidateIdentity, traceDuration, traceEvents, traceStatus, traceTimestamp, type TraceQueryDraft } from './traceModel';
import type { MessageSummary } from '../message/types/message.types';
import type { MessageTraceDetail, MessageTraceNode } from './types/message-trace.types';

const draft: TraceQueryDraft = { mode: 'id', traceTopic: ' custom.trace ', topic: ' orders.events ', messageId: ' unique-1 ', key: ' order-1 ' };
const message: MessageSummary = { topic: 'orders.events', msgId: 'unique-1', queryMsgId: 'physical-1', storeTimestamp: 1000 };
const detail: MessageTraceDetail = { msgId: 'unique-1', traceTopic: 'custom.trace', topic: 'orders.events', timeline: [], consumerGroups: [], transactionChecks: [] };
const node: MessageTraceNode = { traceType: 'SubAfter', role: 'CONSUMER', groupName: 'workers', clientHost: 'client-a', storeHost: 'broker-a', timestamp: 1000, costTime: 0, status: 'failed', retryTimes: 0, fromTransactionCheck: false };

describe('Trace query and identity', () => {
    it('requires only the selected mode fields and supports explicit Trace Topics', () => {
        expect(buildTraceQuery({ ...draft, topic: '', key: '' })).toEqual({ mode: 'id', traceTopic: 'custom.trace', messageId: 'unique-1' });
        expect(buildTraceQuery({ ...draft, mode: 'key', messageId: '' })).toEqual({ mode: 'key', traceTopic: 'custom.trace', topic: 'orders.events', key: 'order-1' });
        for (const invalid of [{ ...draft, traceTopic: ' ' }, { ...draft, messageId: '' }, { ...draft, mode: 'key' as const, topic: '' }, { ...draft, mode: 'key' as const, key: ' ' }]) {
            expect(() => buildTraceQuery(invalid)).toThrow();
        }
    });
    it('deduplicates physical copies by unique ID without confusing the Trace Topic with the business Topic', () => {
        const copies = [message, { ...message, queryMsgId: 'physical-2' }, { ...message, msgId: 'unique-2' }];
        expect(traceCandidates(buildTraceQuery({ ...draft, mode: 'key' }), copies)).toEqual([copies[0], copies[2]]);
        expect(traceCandidates(buildTraceQuery(draft), [message])).toEqual([message]);
        expect(traceCandidateIdentity(copies[0])).toBe(traceCandidateIdentity(copies[1]));
        expect(() => traceCandidates(buildTraceQuery(draft), [copies[2]])).toThrow('identity');
        expect(() => traceCandidates(buildTraceQuery({ ...draft, mode: 'key' }), [{ ...message, topic: 'other' }])).toThrow('identity');
        expect(() => traceCandidates(buildTraceQuery({ ...draft, mode: 'key' }), [{ ...message, msgId: '' }])).toThrow('identity');
    });
    it('rejects wrong detail identities and allows an unreported business Topic', () => {
        expect(checkTraceDetail(detail, 'custom.trace', message)).toBe(detail);
        expect(() => checkTraceDetail({ ...detail, msgId: message.queryMsgId }, 'custom.trace', message)).toThrow();
        expect(() => checkTraceDetail(detail, 'other.trace', message)).toThrow();
        expect(() => checkTraceDetail({ ...detail, topic: 'other' }, 'custom.trace', message)).toThrow();
        expect(checkTraceDetail({ ...detail, topic: null }, 'custom.trace', { ...message, topic: '' }).topic).toBeNull();
    });
});

describe('Trace event presentation', () => {
    it('preserves millisecond differences and rejects missing or invalid event timestamps', () => {
        const start = new Date(2026, 8, 11, 6, 38, 12, 102).getTime();
        expect(traceTimestamp(start)).not.toBe(traceTimestamp(start + 6));
        expect(traceTimestamp(start)).toContain('102');
        expect(traceTimestamp(start + 6)).toContain('108');
        for (const timestamp of [null, undefined, 0, -1, NaN, Infinity, 1e20]) expect(traceTimestamp(timestamp)).toBe('Not reported');
    });
    it('does not turn unrecognized states or missing durations into failure or zero', () => {
        expect(traceStatus('success')).toBe('success');
        expect(traceStatus('failed')).toBe('failed');
        for (const status of ['', null, undefined, 'pending', 'future-status']) expect(traceStatus(status)).toBe('unknown');
        expect(traceDuration(0)).toBe('0 ms');
        for (const duration of [null, undefined, NaN, Infinity, -1]) expect(traceDuration(duration)).toBe('Not reported');
    });
    it('keeps distinct duplicate events and preserves selection identity when unrelated events are inserted', () => {
        const original = traceEvents([node, { ...node }]);
        expect(original[0].id).not.toBe(original[1].id);
        const refreshed = traceEvents([{ ...node, groupName: 'another-group' }, node, { ...node }]);
        expect(refreshed.slice(1).map(event => event.id)).toEqual(original.map(event => event.id));
        expect(traceEvents([{ ...node, status: 'success' }])[0].id).not.toBe(original[0].id);
        expect(traceEvents([]).find(event => event.id === original[0].id)).toBeUndefined();
    });
});
