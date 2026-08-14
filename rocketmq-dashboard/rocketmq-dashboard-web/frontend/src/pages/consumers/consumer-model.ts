import type { ConsumerGroupInfo } from '../../types/consumer';

export type ConsumerLagFilter = 'all' | 'lagging' | 'clear';

export interface ConsumerFilters {
  query: string;
  consumeType: string;
  messageModel: string;
  lag: ConsumerLagFilter;
}

export function getConsumerMetrics(consumers: ConsumerGroupInfo[]) {
  return consumers.reduce(
    (metrics, consumer) => ({
      groups: metrics.groups + 1,
      connectedClients: metrics.connectedClients + consumer.clientCount,
      totalLag: metrics.totalLag + consumer.diffTotal,
      laggingGroups: metrics.laggingGroups + (consumer.diffTotal > 0 ? 1 : 0)
    }),
    { groups: 0, connectedClients: 0, totalLag: 0, laggingGroups: 0 }
  );
}

export function filterConsumers(consumers: ConsumerGroupInfo[], filters: ConsumerFilters) {
  const query = filters.query.trim().toLowerCase();
  return consumers.filter((consumer) => {
    const consumeType = normalizeConsumerValue(consumer.consumeType);
    const messageModel = normalizeConsumerValue(consumer.messageModel);
    const matchesQuery = !query || consumer.group.toLowerCase().includes(query);
    const matchesType = filters.consumeType === 'all' || consumeType === filters.consumeType;
    const matchesModel = filters.messageModel === 'all' || messageModel === filters.messageModel;
    const matchesLag = filters.lag === 'all'
      || (filters.lag === 'lagging' && consumer.diffTotal > 0)
      || (filters.lag === 'clear' && consumer.diffTotal === 0);
    return matchesQuery && matchesType && matchesModel && matchesLag;
  });
}

export function normalizeConsumerValue(value: string) {
  return value
    .replace(/^CONSUME_/, '')
    .replace(/^MESSAGE_MODEL_/, '') || 'UNKNOWN';
}
