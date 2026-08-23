import { apiClient } from './client';
import type {
  ConsumerMonitorMutationResult,
  ConsumerMonitorUpsertRequest,
  ConsumerMonitorView
} from '../types/monitor';

export const monitorApi = {
  listConsumerMonitors: (environmentId: string) =>
    apiClient.get<ConsumerMonitorView[]>(`/api/monitors/consumers?environmentId=${encodeURIComponent(environmentId)}`),
  saveConsumerMonitor: (payload: ConsumerMonitorUpsertRequest) =>
    apiClient.post<ConsumerMonitorMutationResult>('/api/monitors/consumers', payload),
  deleteConsumerMonitor: (environmentId: string, consumerGroup: string, expectedRevision: number) =>
    apiClient.delete<ConsumerMonitorMutationResult>(
      `/api/monitors/consumers/${encodeURIComponent(consumerGroup)}?environmentId=${encodeURIComponent(environmentId)}&expectedRevision=${expectedRevision}`
    )
};
