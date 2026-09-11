import { describe, expect, it } from 'vitest';
import { checkMessageDetail, messageIdentity, messageLookup, messageNumber, messageTimestamp, traceMessageIdentity, visibleMessageText } from './messageModel';
import type { MessageDetail, MessageSummary } from './types/message.types';

const summary: MessageSummary = { topic: 'orders.events', msgId: 'unique-1', queryMsgId: 'physical-1', storeTimestamp: 1000 };
const detail: MessageDetail = { topic: summary.topic, msgId: 'physical-1', properties: { UNIQ_KEY: 'unique-1' } };

describe('Message identity and presentation', () => {
    it('keeps physical copies with the same unique ID separately selectable', () => {
        expect(messageIdentity(summary)).not.toBe(messageIdentity({ ...summary, queryMsgId: 'physical-2' }));
        expect(messageLookup(summary)).toEqual({ topic: summary.topic, messageId: 'physical-1' });
        expect(checkMessageDetail(detail, summary)).toBe(detail);
    });
    it('accepts a unique-ID resolution only when the returned record carries that identity', () => {
        expect(checkMessageDetail(detail, { ...summary, queryMsgId: 'unique-1' })).toBe(detail);
        expect(() => checkMessageDetail({ ...detail, msgId: 'physical-2' }, summary)).toThrow('selected message ID');
        expect(() => checkMessageDetail({ ...detail, properties: {} }, { ...summary, queryMsgId: 'unique-1' })).toThrow('selected message ID');
        expect(() => checkMessageDetail({ ...detail, topic: 'another-topic' }, summary)).toThrow('different Topic');
    });
    it('uses the producer unique identity for Trace and ignores blank unique properties', () => {
        expect(traceMessageIdentity(detail, summary)).toBe('unique-1');
        expect(traceMessageIdentity({ ...detail, properties: { UNIQ_KEY: '  ' } }, summary)).toBe('physical-1');
    });
    it('escapes control and directional characters without changing normal text or its source', () => {
        const raw = '<script>example</script>\n\ttext\u0000\u001b\u202e';
        expect(visibleMessageText(raw)).toBe('<script>example</script>\n\ttext\\u0000\\u001b\\u202e');
        expect(raw).toContain('\u0000');
        expect(visibleMessageText('')).toBe('');
    });
    it('keeps reported numeric zero distinct from missing and invalid values', () => {
        expect(messageNumber(0)).toBe('0');
        for (const value of [undefined, null, NaN, Infinity]) expect(messageNumber(value)).toBe('Not reported');
        for (const value of [undefined, null, 0, NaN, Infinity, 1e18]) expect(messageTimestamp(value)).toBe('Not reported');
        expect(messageTimestamp(1000)).toBe(new Date(1000).toLocaleString());
    });
});
