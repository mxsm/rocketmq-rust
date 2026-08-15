import { apiClient } from './client';
import type {
  TopicConfigView,
  TopicConsumersView,
  TopicInfo,
  TopicListView,
  TopicMutationRequest,
  TopicOffsetResult,
  TopicOperationResult,
  TopicResetOffsetRequest,
  TopicRouteInfo,
  TopicSendResultView,
  TopicSkipOffsetRequest,
  TopicStatsInfo,
  TopicTestMessageRequest
} from '../types/topic';

const topicPath = (topic: string) => `/api/topics/${encodeURIComponent(topic)}`;

export const topicApi = {
  list: () => apiClient.get<TopicListView>('/api/topics'),
  get: (topic: string) => apiClient.get<TopicInfo>(topicPath(topic)),
  create: (request: TopicMutationRequest) => apiClient.post<TopicOperationResult>('/api/topics', request),
  update: (topic: string, request: TopicMutationRequest) =>
    apiClient.put<TopicOperationResult>(topicPath(topic), request),
  delete: (topic: string) => apiClient.delete<TopicOperationResult>(topicPath(topic)),
  route: (topic: string) => apiClient.get<TopicRouteInfo>(`${topicPath(topic)}/route`),
  stats: (topic: string) => apiClient.get<TopicStatsInfo>(`${topicPath(topic)}/stats`),
  config: (topic: string, brokerName?: string) => apiClient.get<TopicConfigView>(
    `${topicPath(topic)}/config${brokerName ? `?brokerName=${encodeURIComponent(brokerName)}` : ''}`
  ),
  consumers: (topic: string) => apiClient.get<TopicConsumersView>(`${topicPath(topic)}/consumers`),
  sendTestMessage: (topic: string, request: TopicTestMessageRequest) =>
    apiClient.post<TopicSendResultView>(`${topicPath(topic)}/test-message`, request),
  resetOffset: (topic: string, request: TopicResetOffsetRequest) =>
    apiClient.post<TopicOffsetResult>(`${topicPath(topic)}/consumer-offset/reset`, request),
  skipBacklog: (topic: string, request: TopicSkipOffsetRequest) =>
    apiClient.post<TopicOffsetResult>(`${topicPath(topic)}/consumer-offset/skip`, request),
  deleteFromBroker: (topic: string, broker: string) =>
    apiClient.delete<TopicOperationResult>(`${topicPath(topic)}/brokers/${encodeURIComponent(broker)}`)
};
