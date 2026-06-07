import { apiClient } from './client';
import type { ConsumerListView, ConsumerProgress, ConsumerResetOffsetRequest } from '../types/consumer';
import type { MutationResult } from '../types/topic';

export const consumerApi = {
  list: () => apiClient.get<ConsumerListView>('/api/consumers'),
  progress: (group: string) => apiClient.get<ConsumerProgress>(`/api/consumers/${encodeURIComponent(group)}/progress`),
  resetOffset: (group: string, request: ConsumerResetOffsetRequest) =>
    apiClient.post<MutationResult>(`/api/consumers/${encodeURIComponent(group)}/reset-offset`, request)
};
