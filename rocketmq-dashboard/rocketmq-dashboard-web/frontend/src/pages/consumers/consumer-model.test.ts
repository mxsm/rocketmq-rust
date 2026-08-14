import type { ConsumerGroupInfo } from '../../types/consumer';
import { filterConsumers, getConsumerMetrics, normalizeConsumerValue } from './consumer-model';

const consumers: ConsumerGroupInfo[] = [
  { group: 'order-service', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 6, diffTotal: 8_700 },
  { group: 'payment-worker', consumeType: 'CONSUME_PASSIVELY', messageModel: 'MESSAGE_MODEL_BROADCASTING', clientCount: 2, diffTotal: 0 },
  { group: 'audit-puller', consumeType: 'CONSUME_ACTIVELY', messageModel: 'MESSAGE_MODEL_CLUSTERING', clientCount: 0, diffTotal: 25 }
];

describe('consumer-model', () => {
  it('aggregates only API-backed consumer metrics', () => {
    expect(getConsumerMetrics(consumers)).toEqual({
      groups: 3,
      connectedClients: 8,
      totalLag: 8_725,
      laggingGroups: 2
    });
  });

  it('combines group, consume type, message model, and lag filters', () => {
    expect(filterConsumers(consumers, {
      query: 'audit',
      consumeType: 'ACTIVELY',
      messageModel: 'CLUSTERING',
      lag: 'lagging'
    })).toEqual([consumers[2]]);

    expect(filterConsumers(consumers, {
      query: '',
      consumeType: 'all',
      messageModel: 'BROADCASTING',
      lag: 'clear'
    })).toEqual([consumers[1]]);
  });

  it('normalizes API enum prefixes for display and filtering', () => {
    expect(normalizeConsumerValue('CONSUME_PASSIVELY')).toBe('PASSIVELY');
    expect(normalizeConsumerValue('MESSAGE_MODEL_CLUSTERING')).toBe('CLUSTERING');
    expect(normalizeConsumerValue('')).toBe('UNKNOWN');
  });
});
