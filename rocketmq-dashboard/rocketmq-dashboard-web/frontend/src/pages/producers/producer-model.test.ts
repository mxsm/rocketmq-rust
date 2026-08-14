import type { ProducerInfo } from '../../types/producer';
import { filterProducers, getProducerMetrics } from './producer-model';

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
});
