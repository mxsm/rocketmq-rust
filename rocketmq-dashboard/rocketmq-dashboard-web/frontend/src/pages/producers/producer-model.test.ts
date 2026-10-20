import type { ProducerInfo } from '../../types/producer';
import { filterProducers, getProducerMetrics, summarizeProducerBrokers } from './producer-model';

const producers: ProducerInfo[] = [
  { topic: '', producerGroup: 'order-producer', connectionCount: 3 },
  { topic: '', producerGroup: 'payment-producer', connectionCount: 4 },
  { topic: '', producerGroup: 'audit-producer', connectionCount: 0 }
];

describe('producer-model', () => {
  it('counts group discovery without depending on the backend topic placeholder', () => {
    expect(getProducerMetrics(producers)).toEqual({
      producerGroups: 3,
      discoveredConnections: 7,
      connectedGroups: 2
    });
  });

  it('filters producer rows by group', () => {
    expect(filterProducers(producers, 'PAYMENT')).toEqual([producers[1]]);
    expect(filterProducers(producers, 'audit-producer')).toEqual([producers[2]]);
    expect(filterProducers(producers, '')).toEqual(producers);
  });

  it('also matches the brokers a group is registered with', () => {
    const registered = [
      { ...producers[0], brokerNames: ['broker-a'] },
      { ...producers[1], brokerNames: ['broker-a', 'broker-b'] }
    ];

    expect(filterProducers(registered, 'BROKER-B')).toEqual([registered[1]]);
  });

  it('summarizes the brokers a group is registered with', () => {
    expect(summarizeProducerBrokers({ ...producers[0], brokerNames: ['broker-a', 'broker-b', 'broker-c'] })).toBe('broker-a +2');
    expect(summarizeProducerBrokers({ ...producers[0], brokerNames: ['broker-b'] })).toBe('broker-b');
    expect(summarizeProducerBrokers({ ...producers[2], brokerNames: [] })).toBe('None');
    expect(summarizeProducerBrokers(producers[0])).toBe('Unknown');
  });
});
