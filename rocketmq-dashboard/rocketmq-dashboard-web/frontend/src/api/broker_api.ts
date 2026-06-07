import { apiClient } from './client';
import type { BrokerConfigUpdateRequest, BrokerConfigView, BrokerListView, BrokerRuntimeStats } from '../types/broker';
import type { MutationResult } from '../types/topic';

export const brokerApi = {
  list: () => apiClient.get<BrokerListView>('/api/brokers'),
  runtime: (brokerName: string) =>
    apiClient.get<BrokerRuntimeStats>(`/api/brokers/${encodeURIComponent(brokerName)}/runtime`),
  config: (brokerName: string) =>
    apiClient.get<BrokerConfigView>(`/api/brokers/${encodeURIComponent(brokerName)}/config`),
  updateConfig: (brokerName: string, request: BrokerConfigUpdateRequest) =>
    apiClient.put<MutationResult>(`/api/brokers/${encodeURIComponent(brokerName)}/config`, request)
};
