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

export function filterProducers(producers: ProducerInfo[], query: string) {
  const normalizedQuery = query.trim().toLowerCase();
  if (!normalizedQuery) return producers;
  return producers.filter((producer) => (
    `${producer.producerGroup} ${producer.topic}`.toLowerCase().includes(normalizedQuery)
  ));
}
