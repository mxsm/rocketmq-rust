import { expect, it } from 'vitest';
import { dlqReceiptCsv, dlqReceiptRows, failedDlqSelection, type DlqReceipt } from './receipts';
import type { DlqResendMessageResult } from './types/dlq.types';

const item = (id: string, success = false): DlqResendMessageResult => ({ requestMessageId: id, msgId: 'original-' + id,
    consumerGroup: 'g', success, consumeResult: success ? 'CR_SUCCESS' : 'CR_LATER', topic: 'orders', message: 'Returned outcome' });
const receipt = (items: DlqResendMessageResult[]): DlqReceipt => ({ requests: ['a', 'b'].map(messageId => ({ consumerGroup: 'g', messageId })),
    environmentId: 'local', revision: 1, startedAt: 1, completedAt: 2, error: '', response: { items, total: items.length, successCount: 1, failureCount: 1 } });
const messages = ['a', 'b'].map(id => ({ queryMsgId: id, msgId: 'unique-' + id, topic: '%DLQ%g', storeTimestamp: 0 }));

it('joins by requested DLQ identity, preserving a different original identity', () => {
    const result = receipt([item('a', true), item('b')]);
    expect(dlqReceiptRows(result).map(row => [row.request.messageId, row.result?.msgId, row.outcome])).toEqual([
        ['a', 'original-a', 'success'], ['b', 'original-b', 'failed'],
    ]);
    expect([...failedDlqSelection(result, 'local', 'g', messages)]).toEqual(['b']);
});

it('does not select results from another environment, group or nonvisible page', () => {
    const result = receipt([item('a', true), item('b')]);
    expect(failedDlqSelection(result, 'other', 'g', messages).size).toBe(0);
    expect(failedDlqSelection(result, 'local', 'other', messages).size).toBe(0);
    expect(failedDlqSelection(result, 'local', 'g', [messages[0]]).size).toBe(0);
});

it('excludes duplicate and conflicting acknowledgements, including a success and failure for one ID', () => {
    const result = receipt([item('a', true), item('a'), item('b')]);
    expect(dlqReceiptRows(result)[0].outcome).toBe('unknown');
    expect([...failedDlqSelection(result, 'local', 'g', messages)]).toEqual(['b']);
});

it('cannot mistake an original ID or another group for the requested DLQ identity', () => {
    const result = receipt([{ ...item('a'), requestMessageId: null, msgId: 'a' }, { ...item('b'), consumerGroup: 'other' }]);
    expect(dlqReceiptRows(result).every(row => row.outcome === 'unknown')).toBe(true);
    expect(failedDlqSelection(result, 'local', 'g', messages).size).toBe(0);
});

it('keeps transport errors and unrecognized consume states unknown even when success is reported', () => {
    const result = receipt([{ ...item('a'), consumeResult: null, topic: '' }, { ...item('b', true), consumeResult: 'UNKNOWN' }]);
    expect(dlqReceiptRows(result).every(row => row.outcome === 'unknown')).toBe(true);
    expect(dlqReceiptRows({ ...result, response: null, error: 'Unavailable' })).toHaveLength(2);
    expect(failedDlqSelection(result, 'local', 'g', messages).size).toBe(0);
});

it('does not confirm success for a DLQ Topic or contradictory consume outcome', () => {
    const result = receipt([{ ...item('a', true), topic: '%DLQ%g' }, { ...item('b', true), consumeResult: 'CR_LATER' }]);
    expect(dlqReceiptRows(result).every(row => row.outcome === 'unknown')).toBe(true);
});

it('exports original targets and safe quoted cells without spreadsheet formula execution', () => {
    const result = receipt([{ ...item('a'), message: '=SUM(1,2)', remark: 'a "quote"\nsecond line' }]);
    const csv = dlqReceiptCsv(result);
    expect(csv).toContain('"\'=SUM(1,2)"');
    expect(csv).toContain('"a ""quote""\nsecond line"');
    expect(csv).toContain('"local","g","a"');
});
