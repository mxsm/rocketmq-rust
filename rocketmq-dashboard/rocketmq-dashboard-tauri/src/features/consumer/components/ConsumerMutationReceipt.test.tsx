import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { ConsumerMutationReceipt } from './ConsumerMutationReceipt';
import { failedConsumerBrokers, isReadOnlyConsumer } from '../mutation';
import type { ConsumerMutationResult, ConsumerGroupListItem } from '../types/consumer.types';

const partial: ConsumerMutationResult = {
    operation: 'delete', consumerGroup: 'orders', success: false, targetCount: 3,
    targets: [
        { kind: 'BROKER', target: 'broker-a', success: true, errorCode: null, message: 'Completed' },
        { kind: 'BROKER', target: 'broker-b', success: false, errorCode: 'target.failed', message: 'Refresh current state' },
        { kind: 'INTERNAL_TOPIC_CLEANUP', target: '%DLQ%orders', success: false, errorCode: 'target.failed', message: 'Cleanup failed' },
    ],
};

describe('Consumer mutation results', () => {
    it('shows successful, failed, and cleanup results independently', () => {
        const html = renderToStaticMarkup(createElement(ConsumerMutationReceipt, { result: partial, onReviewFailed: () => {}, disabled: false }));
        for (const text of ['broker-a', 'broker-b', '%DLQ%orders', 'INTERNAL_TOPIC_CLEANUP', 'Completed', 'Not confirmed']) expect(html).toContain(text);
        expect(failedConsumerBrokers(partial)).toEqual(['broker-b']);
    });
    it('never offers cleanup failures as a Broker retry', () => {
        const result = { ...partial, targets: [partial.targets[0], partial.targets[2]] };
        expect(failedConsumerBrokers(result)).toEqual([]);
        expect(renderToStaticMarkup(createElement(ConsumerMutationReceipt, { result, onReviewFailed: () => {}, disabled: false }))).not.toContain('Refresh and review failed Brokers only');
    });
    it('keeps system groups read-only through either display or raw names', () => {
        const group = { category: 'NORMAL', rawGroupName: 'orders', displayGroupName: 'orders' } as ConsumerGroupListItem;
        expect(isReadOnlyConsumer(group)).toBe(false);
        expect(isReadOnlyConsumer({ ...group, category: 'SYSTEM' })).toBe(true);
        expect(isReadOnlyConsumer({ ...group, rawGroupName: ' %SYS%orders ' })).toBe(true);
        expect(isReadOnlyConsumer({ ...group, displayGroupName: '%SYS%orders' })).toBe(true);
    });
});
