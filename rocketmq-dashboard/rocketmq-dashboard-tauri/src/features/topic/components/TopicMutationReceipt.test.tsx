import { createElement } from 'react';
import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it } from 'vitest';
import { failedBrokerNames, TopicMutationReceipt } from './TopicMutationReceipt';
import type { TopicBatchResult } from '../types/topic.types';

const partial: TopicBatchResult = {
    operation: 'update', topic: 'orders', targetCount: 2, success: false, message: 'Review results',
    targets: [
        { kind: 'broker', name: 'broker-a', success: true, errorCode: null, message: 'Completed' },
        { kind: 'broker', name: 'broker-b', success: false, errorCode: 'target.failed', message: 'Verify current state' },
    ],
    orderConfig: { success: false, errorCode: 'order.failed', message: 'Order configuration failed' },
};

describe('Topic mutation receipts', () => {
    it('renders both targets and the independent order failure', () => {
        const html = renderToStaticMarkup(createElement(TopicMutationReceipt, { result: partial, onReviewFailed: () => {} }));
        expect(html).toContain('broker-a');
        expect(html).toContain('broker-b');
        expect(html).toContain('ORDER_TOPIC_CONFIG');
        expect(html).toContain('order.failed');
        expect(html).toContain('Review failed Brokers only');
        expect(failedBrokerNames(partial)).toEqual(['broker-b']);
    });

    it('does not offer successful Brokers or Cluster-level results as failed Broker retries', () => {
        const result = { ...partial, targets: partial.targets.map((target) => ({ ...target, success: true })), success: true, orderConfig: { success: true, errorCode: null, message: 'Completed' } };
        expect(failedBrokerNames(result)).toEqual([]);
        expect(renderToStaticMarkup(createElement(TopicMutationReceipt, { result, onReviewFailed: () => {} }))).not.toContain('Review failed Brokers only');
        expect(failedBrokerNames({ ...partial, targets: [{ ...partial.targets[1], kind: 'cluster' }] })).toEqual([]);
    });
});
