import { apiClient } from './client';
import type { DlqBatchResendRequest, DlqExportView, DlqMessageQueryParams, DlqMessageResendResult, MessageListView } from '../types/message';

function toQueryString(params: Partial<DlqMessageQueryParams> = {}) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      search.set(key, String(value));
    }
  });
  const query = search.toString();
  return query ? `?${query}` : '';
}

export const dlqApi = {
  list: (params: DlqMessageQueryParams) => apiClient.get<MessageListView>(`/api/messages/dlq${toQueryString(params)}`),
  resend: (payload: DlqBatchResendRequest) => apiClient.post<DlqMessageResendResult[]>('/api/messages/dlq/resend', payload),
  export: (params: DlqMessageQueryParams) => apiClient.get<DlqExportView>(`/api/messages/dlq/export${toQueryString(params)}`)
};
