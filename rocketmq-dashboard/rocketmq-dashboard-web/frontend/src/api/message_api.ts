import { apiClient } from './client';
import type { MessageListView, MessageQueryParams, MessageResendRequest, MessageTraceView } from '../types/message';
import type { MutationResult } from '../types/topic';

function toQueryString(params: Record<string, string | number | undefined | null>) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      search.set(key, String(value));
    }
  });
  const query = search.toString();
  return query ? `?${query}` : '';
}

export const messageApi = {
  list: (params: MessageQueryParams = {}) => apiClient.get<MessageListView>(`/api/messages${toQueryString({ ...params })}`),
  byKey: (topic: string, key: string) =>
    apiClient.get<MessageListView>(`/api/messages/by-key?topic=${encodeURIComponent(topic)}&key=${encodeURIComponent(key)}`),
  byId: (topic: string, messageId: string) =>
    apiClient.get<MessageListView>(`/api/messages${toQueryString({ topic, messageId })}`),
  trace: (messageId: string, topic: string, traceTopic = 'RMQ_SYS_TRACE_TOPIC') =>
    apiClient.get<MessageTraceView>(
      `/api/messages/${encodeURIComponent(messageId)}/trace${toQueryString({ topic, traceTopic })}`
    ),
  resend: (messageId: string, request: MessageResendRequest) =>
    apiClient.post<MutationResult>(`/api/messages/${encodeURIComponent(messageId)}/resend`, request)
};
