import { describe, expect, it } from 'vitest';
import type { TopicInfo } from '../../types/topic';
import {
  filterTopics,
  getTopicActionAvailability,
  getTopicCategory,
  getTopicMetrics,
  getTopicPermissionLabel,
  type TopicCategory,
  type TopicMessageType
} from './topic-model';

const topic = (overrides: Partial<TopicInfo>): TopicInfo => ({
  topic: 'orders',
  brokerName: 'broker-a',
  brokers: ['broker-a'],
  clusters: ['DefaultCluster'],
  readQueueCount: 8,
  writeQueueCount: 8,
  perm: 6,
  category: 'NORMAL',
  messageType: 'NORMAL',
  order: false,
  systemTopic: false,
  ...overrides
});

const normalTopic = topic({ topic: 'orders' });
const delayTopic = topic({ topic: 'delay-orders', messageType: 'DELAY' });
const fifoTopic = topic({ topic: 'fifo-orders', messageType: 'FIFO' });
const transactionTopic = topic({ topic: 'transaction-orders', messageType: 'TRANSACTION' });
const unspecifiedTopic = topic({ topic: 'legacy-orders', messageType: 'UNSPECIFIED' });
const retryTopic = topic({ topic: '%RETRY%orders', category: 'RETRY', messageType: 'RETRY' });
const dlqTopic = topic({ topic: '%DLQ%orders', category: 'DLQ', messageType: 'DLQ' });
const systemTopic = topic({
  topic: 'RMQ_SYS_TRACE_TOPIC',
  category: 'SYSTEM',
  messageType: 'SYSTEM',
  systemTopic: true
});
const fixtures = [normalTopic, delayTopic, fifoTopic, transactionTopic, unspecifiedTopic, retryTopic, dlqTopic, systemTopic];

describe('topic model', () => {
  it('filters authoritative message types and operational categories together', () => {
    const result = filterTopics([fifoTopic, unspecifiedTopic, retryTopic, dlqTopic, systemTopic], {
      query: '',
      brokerName: 'broker-a',
      clusterName: 'DefaultCluster',
      messageTypes: ['FIFO', 'UNSPECIFIED'],
      categories: ['APPLICATION', 'RETRY']
    });

    expect(result.map((item) => item.topic)).toEqual(['fifo-orders', 'legacy-orders', '%RETRY%orders']);
  });

  it.each<[TopicMessageType, string]>([
    ['NORMAL', 'orders'],
    ['DELAY', 'delay-orders'],
    ['FIFO', 'fifo-orders'],
    ['TRANSACTION', 'transaction-orders'],
    ['UNSPECIFIED', 'legacy-orders']
  ])('matches the %s message type from catalog metadata', (messageType, expectedTopic) => {
    const result = filterTopics(fixtures, {
      query: '',
      brokerName: 'all',
      clusterName: 'all',
      messageTypes: [messageType],
      categories: []
    });

    expect(result.map((item) => item.topic)).toEqual([expectedTopic]);
  });

  it.each<[TopicCategory, string[]]>([
    ['APPLICATION', ['orders', 'delay-orders', 'fifo-orders', 'transaction-orders', 'legacy-orders']],
    ['RETRY', ['%RETRY%orders']],
    ['DLQ', ['%DLQ%orders']],
    ['SYSTEM', ['RMQ_SYS_TRACE_TOPIC']]
  ])('matches the %s operational category from catalog metadata', (category, expectedTopics) => {
    const result = filterTopics(fixtures, {
      query: '',
      brokerName: 'all',
      clusterName: 'all',
      messageTypes: [],
      categories: [category]
    });

    expect(result.map((item) => item.topic)).toEqual(expectedTopics);
  });

  it('combines text, broker, and cluster filters with the classification union', () => {
    const crossClusterTopic = topic({
      topic: 'orders-archive',
      brokerName: 'broker-b',
      brokers: ['broker-b'],
      clusters: ['ArchiveCluster'],
      messageType: 'FIFO'
    });
    const result = filterTopics([...fixtures, crossClusterTopic], {
      query: 'orders',
      brokerName: 'broker-a',
      clusterName: 'DefaultCluster',
      messageTypes: ['FIFO'],
      categories: ['RETRY']
    });

    expect(result.map((item) => item.topic)).toEqual(['fifo-orders', '%RETRY%orders']);
  });

  it('leaves classification unrestricted when no type or category is selected', () => {
    expect(filterTopics(fixtures, {
      query: '',
      brokerName: 'all',
      clusterName: 'all',
      messageTypes: [],
      categories: []
    })).toEqual(fixtures);
  });

  it('uses authoritative category metadata before the legacy topic-name fallback', () => {
    expect(getTopicCategory(topic({ topic: '%RETRY%ordinary', category: 'NORMAL' }))).toBe('application');
    expect(getTopicCategory(topic({ topic: 'ordinary', category: 'SYSTEM', systemTopic: false }))).toBe('system');
    expect(getTopicCategory({
      ...topic({ topic: '%DLQ%legacy' }),
      category: undefined,
      systemTopic: undefined
    } as unknown as TopicInfo)).toBe('dlq');
  });

  it('allows Java operations on retry and dlq but none on system topics', () => {
    expect(getTopicActionAvailability(retryTopic).send).toBe(true);
    expect(getTopicActionAvailability(dlqTopic).skip).toBe(true);
    expect(getTopicActionAvailability(systemTopic)).toEqual({
      edit: false,
      send: false,
      reset: false,
      skip: false,
      deleteBroker: false,
      deleteTopic: false
    });
  });

  it('maps RocketMQ permission bits to readable labels', () => {
    expect([0, 2, 4, 6, 7].map(getTopicPermissionLabel)).toEqual(['None', 'W', 'R', 'RW', 'RW']);
  });

  it('derives inventory totals without mutating the API rows', () => {
    const snapshot = structuredClone(fixtures);

    expect(getTopicMetrics(fixtures)).toEqual({ total: 8, application: 5, retry: 1, dlq: 1, system: 1 });
    expect(fixtures).toEqual(snapshot);
  });
});
