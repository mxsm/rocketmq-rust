import type { ConsumerConfigTarget, ConsumerGroupListItem } from '../../types/consumer';
import {
  clampConsumerPage,
  consumerOperationSucceeded,
  consumerQueryIdentity,
  deriveInconsistentConsumerFields,
  getConsumerActionAvailability,
  getConsumerMetrics,
  normalizeConsumerValue,
  selectConsumerRows,
  summarizeConsumerTargets
} from './consumer-model';

function consumerFixture(overrides: Partial<ConsumerGroupListItem> = {}): ConsumerGroupListItem {
  return {
    displayGroupName: 'orders-consumer',
    rawGroupName: 'orders-consumer',
    category: 'NORMAL',
    connectionCount: 3,
    consumeTps: 120,
    diffTotal: 0,
    messageModel: 'MESSAGE_MODEL_CLUSTERING',
    consumeType: 'CONSUME_PASSIVELY',
    version: 530,
    versionDesc: 'V5_3_0',
    brokerNames: ['broker-a'],
    brokerAddresses: ['10.0.0.1:10911'],
    updateTimestamp: 1_700_000_000_000,
    ...overrides
  };
}

describe('consumer-model', () => {
  it('filters and sorts by canonical consumer metadata', () => {
    const rows = [
      consumerFixture({ rawGroupName: 'orders-standard', diffTotal: 10, category: 'NORMAL' }),
      consumerFixture({ rawGroupName: 'orders-priority', diffTotal: 50, category: 'NORMAL' }),
      consumerFixture({ rawGroupName: 'payment', diffTotal: 0, category: 'FIFO', versionDesc: 'V5_2_0' })
    ];

    const result = selectConsumerRows(rows, {
      query: 'orders',
      categories: ['NORMAL'],
      consumeTypes: [],
      messageModels: [],
      lag: 'lagging',
      brokers: ['broker-a'],
      versions: ['V5_3_0'],
      sort: { key: 'diffTotal', direction: 'desc' }
    } as never);

    expect(result.map((item) => item.rawGroupName)).toEqual(['orders-priority', 'orders-standard']);
  });

  it('protects system groups and keeps normal actions complete', () => {
    const system = consumerFixture({ rawGroupName: '%SYS%internal', category: 'SYSTEM' });
    const normal = consumerFixture({ rawGroupName: 'orders-consumer', category: 'NORMAL' });

    expect(getConsumerActionAvailability(system)).toEqual({
      inspect: true, clients: true, progress: true, config: true, edit: false, reset: false, delete: false
    });
    expect(getConsumerActionAvailability(normal)).toEqual({
      inspect: true, clients: true, progress: true, config: true, edit: true, reset: true, delete: true
    });
  });

  it('aggregates API-backed metrics', () => {
    const metrics = getConsumerMetrics([
      consumerFixture({ connectionCount: 6, diffTotal: 8_700 }),
      consumerFixture({ rawGroupName: 'payment', connectionCount: 2, diffTotal: 0 })
    ]);
    expect(metrics).toEqual({ groups: 2, connectedClients: 8, totalLag: 8_700, laggingGroups: 1 });
  });

  it('normalizes enum prefixes for display', () => {
    expect(normalizeConsumerValue('CONSUME_PASSIVELY')).toBe('PASSIVELY');
    expect(normalizeConsumerValue('MESSAGE_MODEL_CLUSTERING')).toBe('CLUSTERING');
    expect(normalizeConsumerValue('')).toBe('UNKNOWN');
  });

  it('clamps page numbers after filters shrink the result', () => {
    expect(clampConsumerPage(9, 10, 25)).toBe(3);
    expect(clampConsumerPage(0, 10, 25)).toBe(1);
  });

  it('summarizes broker targets compactly', () => {
    expect(summarizeConsumerTargets(consumerFixture({ brokerNames: ['broker-a', 'broker-b', 'broker-c'] })))
      .toBe('broker-a +2');
  });

  it('derives inconsistent configuration fields', () => {
    const targets: ConsumerConfigTarget[] = [
      {
        brokerName: 'broker-a',
        brokerAddress: 'a',
        config: { consumeEnable: true, consumeFromMinEnable: false, consumeBroadcastEnable: false, consumeMessageOrderly: false, retryQueueNums: 2, retryMaxTimes: 16, brokerId: 0, whichBrokerWhenConsumeSlowly: 1, notifyConsumerIdsChangedEnable: true, groupSysFlag: 0, consumeTimeoutMinute: 15, groupRetryPolicyJson: '{}' },
        subscriptionTopics: [],
        attributes: []
      },
      {
        brokerName: 'broker-b',
        brokerAddress: 'b',
        config: { consumeEnable: true, consumeFromMinEnable: false, consumeBroadcastEnable: false, consumeMessageOrderly: false, retryQueueNums: 2, retryMaxTimes: 20, brokerId: 0, whichBrokerWhenConsumeSlowly: 1, notifyConsumerIdsChangedEnable: true, groupSysFlag: 0, consumeTimeoutMinute: 15, groupRetryPolicyJson: '{}' },
        subscriptionTopics: [],
        attributes: []
      }
    ];

    expect(deriveInconsistentConsumerFields(targets)).toEqual(['retryMaxTimes']);
  });

  it('treats mutation success as every target succeeding', () => {
    expect(consumerOperationSucceeded({
      operation: 'UPDATE', consumerGroup: 'g', success: true, targetCount: 2, message: '', targets: [
        { target: 'a', kind: 'BROKER', success: true, message: '' },
        { target: 'b', kind: 'BROKER', success: false, message: '' }
      ]
    })).toBe(false);
  });

  it('builds a stable query identity', () => {
    expect(consumerQueryIdentity({ mode: 'nameServer' })).toBe('nameServer:');
    expect(consumerQueryIdentity({ mode: 'proxy', proxyAddress: 'proxy-a:8081' })).toBe('proxy:proxy-a:8081');
  });
});
