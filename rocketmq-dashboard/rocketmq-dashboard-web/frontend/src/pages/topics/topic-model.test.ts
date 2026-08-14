import { describe, expect, it } from 'vitest';
import type { TopicInfo } from '../../types/topic';
import { filterTopics, getTopicCategory, getTopicMetrics, getTopicPermissionLabel } from './topic-model';

const topics: TopicInfo[] = [
  { topic: 'orders', brokerName: 'broker-a', readQueueCount: 8, writeQueueCount: 8, perm: 6, category: 'NORMAL' },
  { topic: 'payments-fifo', brokerName: 'broker-b', readQueueCount: 4, writeQueueCount: 4, perm: 4, category: 'FIFO' },
  { topic: '%RETRY%order-service', brokerName: 'broker-a', readQueueCount: 1, writeQueueCount: 1, perm: 2, category: 'RETRY' },
  { topic: '%DLQ%payment-service', brokerName: 'broker-b', readQueueCount: 1, writeQueueCount: 1, perm: 0, category: 'DLQ' },
  { topic: 'RMQ_SYS_TRACE_TOPIC', brokerName: null, readQueueCount: 1, writeQueueCount: 1, perm: 7, category: 'SYSTEM' }
];

describe('topic model', () => {
  it('classifies special topics before ordinary API categories', () => {
    expect(topics.map(getTopicCategory)).toEqual(['application', 'application', 'retry', 'dlq', 'system']);
  });

  it('filters by topic name, broker, and operational category together', () => {
    expect(filterTopics(topics, { query: 'service', brokerName: 'broker-a', category: 'retry' }).map((topic) => topic.topic)).toEqual([
      '%RETRY%order-service'
    ]);
    expect(filterTopics(topics, { query: 'PAY', brokerName: 'all', category: 'application' }).map((topic) => topic.topic)).toEqual([
      'payments-fifo'
    ]);
  });

  it('maps RocketMQ permission bits to readable labels', () => {
    expect([0, 2, 4, 6, 7].map(getTopicPermissionLabel)).toEqual(['None', 'W', 'R', 'RW', 'RW']);
  });

  it('derives inventory totals without mutating the API rows', () => {
    const snapshot = structuredClone(topics);

    expect(getTopicMetrics(topics)).toEqual({ total: 5, application: 2, retry: 1, dlq: 1, system: 1 });
    expect(topics).toEqual(snapshot);
  });
});
