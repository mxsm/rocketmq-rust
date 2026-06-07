import { apiClient } from './client';
import type {
  ConsumerMonitorMutationResult,
  ConsumerMonitorUpsertRequest,
  ConsumerMonitorView
} from '../types/monitor';

export const monitorApi = {
  listConsumerMonitors: () => apiClient.get<ConsumerMonitorView[]>('/api/monitors/consumers'),
  saveConsumerMonitor: (payload: ConsumerMonitorUpsertRequest) =>
    apiClient.post<ConsumerMonitorMutationResult>('/api/monitors/consumers', payload),
  deleteConsumerMonitor: (consumerGroup: string) =>
    apiClient.delete<ConsumerMonitorMutationResult>(`/api/monitors/consumers/${encodeURIComponent(consumerGroup)}`)
};
