import { apiClient } from './client';
import type { DashboardHistoryQuery, DashboardHistorySeries, DashboardOverview, DashboardTopicCurrent } from '../types/dashboard';

function toQueryString(params: DashboardHistoryQuery) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      search.set(key, String(value));
    }
  });
  return `?${search.toString()}`;
}

export const dashboardApi = {
  overview: () => apiClient.get<DashboardOverview>('/api/dashboard/overview'),
  topicCurrent: () => apiClient.get<DashboardTopicCurrent>('/api/dashboard/topic-current'),
  brokerHistory: (query: DashboardHistoryQuery) =>
    apiClient.get<DashboardHistorySeries>(`/api/dashboard/brokers/history${toQueryString(query)}`),
  topicHistory: (query: DashboardHistoryQuery) =>
    apiClient.get<DashboardHistorySeries>(`/api/dashboard/topics/history${toQueryString(query)}`)
};
