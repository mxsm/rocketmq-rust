import { apiClient } from './client';
import type { ProducerConnectionView, ProducerInfo } from '../types/producer';

export const producerApi = {
  list: () => apiClient.get<ProducerInfo[]>('/api/producers'),
  connections: (topic: string, producerGroup: string) =>
    apiClient.get<ProducerConnectionView>(
      `/api/producers/connections?topic=${encodeURIComponent(topic)}&producerGroup=${encodeURIComponent(producerGroup)}`
    )
};
