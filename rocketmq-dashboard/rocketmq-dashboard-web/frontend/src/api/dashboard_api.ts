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

const MAX_HISTORY_PAGES = 64;

export async function readHistoryPages(
  request: (query: DashboardHistoryQuery) => Promise<DashboardHistorySeries>,
  query: DashboardHistoryQuery
): Promise<DashboardHistorySeries> {
  const points = [] as DashboardHistorySeries['points'];
  let cursor = query.cursor;
  for (let page = 0; page < MAX_HISTORY_PAGES; page += 1) {
    const response = await request({ ...query, cursor });
    points.push(...response.points);
    if (!response.nextCursor) return { ...response, points, nextCursor: null };
    cursor = response.nextCursor;
  }
  throw new Error('History response exceeded the safe page limit');
}

const brokerHistoryPage = (query: DashboardHistoryQuery) =>
  apiClient.get<DashboardHistorySeries>(`/api/dashboard/brokers/history${toQueryString(query)}`);
const topicHistoryPage = (query: DashboardHistoryQuery) =>
  apiClient.get<DashboardHistorySeries>(`/api/dashboard/topics/history${toQueryString(query)}`);

export const dashboardApi = {
  overview: () => apiClient.get<DashboardOverview>('/api/dashboard/overview'),
  topicCurrent: () => apiClient.get<DashboardTopicCurrent>('/api/dashboard/topic-current'),
  brokerHistory: (query: DashboardHistoryQuery) => readHistoryPages(brokerHistoryPage, query),
  topicHistory: (query: DashboardHistoryQuery) => readHistoryPages(topicHistoryPage, query)
};
