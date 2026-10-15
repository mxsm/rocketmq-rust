import type { ProducerInfo } from '../../types/producer';

export function getProducerMetrics(producers: ProducerInfo[]) {
  const groups = new Set<string>();
  const connectedGroups = new Set<string>();
  let discoveredConnections = 0;

  for (const producer of producers) {
    if (producer.producerGroup) groups.add(producer.producerGroup);
    discoveredConnections += producer.connectionCount;
    if (producer.connectionCount > 0 && producer.producerGroup) connectedGroups.add(producer.producerGroup);
  }

  return {
    producerGroups: groups.size,
    discoveredConnections,
    connectedGroups: connectedGroups.size
  };
}

/** The first broker a group is registered with and how many more; `Unknown` when the backend does not say. */
export function summarizeProducerBrokers(producer: ProducerInfo): string {
  const brokers = producer.brokerNames;
  if (brokers === undefined) return 'Unknown';
  if (brokers.length === 0) return 'None';
  return brokers.length === 1 ? brokers[0] : `${brokers[0]} +${brokers.length - 1}`;
}

export function filterProducers(producers: ProducerInfo[], query: string) {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) return producers;
  return producers.filter((producer) => (
    [producer.producerGroup, producer.topic, ...(producer.brokerNames ?? [])].join(' ').toLowerCase().includes(normalizedQuery)
  ));
}
