import { expect, it } from 'vitest';
import { dlqQueryTaskId, failedDlqSelection } from './receipts';

it('reviews only visible failed DLQ request identities without successful or other-group messages', () => {
    const row = (id: string, success: boolean, group = 'g') => ({ requestMessageId: id, msgId: `original-${id}`, consumerGroup: group, success, topic: 'orders', message: '' });
    const receipt = { items: [row('a', true), row('a', false), row('b', false), row('c', false, 'other'), row('missing', false)], total: 5, successCount: 1, failureCount: 4 };
    const messages = ['a', 'b', 'c'].map(id => ({ queryMsgId: id, msgId: id, topic: '%DLQ%g', storeTimestamp: 0 }));
    expect([...failedDlqSelection(receipt, 'g', messages)]).toEqual(['b']);
});

it('starts fresh searches and never reuses time pagination in exact modes', () => {
    expect(dlqQueryTaskId('Consumer', 2, 'task')).toBe('task');
    expect(dlqQueryTaskId('Consumer', 1, 'task')).toBeUndefined();
    expect(dlqQueryTaskId('Key', 2, 'task')).toBeUndefined();
    expect(dlqQueryTaskId('Message ID', 2, 'task')).toBeUndefined();
});
