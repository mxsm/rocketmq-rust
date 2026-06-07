import { apiClient } from './client';
import type {
  MutationResult,
  TopicInfo,
  TopicListView,
  TopicMutationRequest,
  TopicRouteInfo,
  TopicStatsInfo
} from '../types/topic';

export const topicApi = {
  list: () => apiClient.get<TopicListView>('/api/topics'),
  get: (topic: string) => apiClient.get<TopicInfo>(`/api/topics/${encodeURIComponent(topic)}`),
  create: (request: TopicMutationRequest) => apiClient.post<MutationResult>('/api/topics', request),
  update: (topic: string, request: TopicMutationRequest) =>
    apiClient.put<MutationResult>(`/api/topics/${encodeURIComponent(topic)}`, request),
  delete: (topic: string) => apiClient.delete<MutationResult>(`/api/topics/${encodeURIComponent(topic)}`),
  route: (topic: string) => apiClient.get<TopicRouteInfo>(`/api/topics/${encodeURIComponent(topic)}/route`),
  stats: (topic: string) => apiClient.get<TopicStatsInfo>(`/api/topics/${encodeURIComponent(topic)}/stats`)
};
