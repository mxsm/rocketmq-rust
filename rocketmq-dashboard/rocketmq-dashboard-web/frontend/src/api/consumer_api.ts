import { apiClient } from './client';
import type { MutationResult } from '../types/topic';
import type {
  ConsumerBrokerListView,
  ConsumerConfigView,
  ConsumerConnectionView,
  ConsumerDeleteRequest,
  ConsumerGroupListView,
  ConsumerJStackView,
  ConsumerOperationResult,
  ConsumerProgressView,
  ConsumerQueryScope,
  ConsumerResetOffsetRequest,
  ConsumerRunningInfoView,
  ConsumerSummaryView,
  ConsumerUpsertRequest
} from '../types/consumer';

export function consumerQueryParams(scope: ConsumerQueryScope): string {
  const params = new URLSearchParams({ mode: scope.mode });
  if (scope.mode === 'proxy' && scope.proxyAddress) {
    params.set('proxyAddress', scope.proxyAddress);
  }
  return params.toString();
}

function withScope(path: string, scope: ConsumerQueryScope): string {
  const params = consumerQueryParams(scope);
  return `${path}${path.includes('?') ? '&' : '?'}${params}`;
}

export const consumerApi = {
  list: (scope: ConsumerQueryScope = { mode: 'nameServer' }) =>
    apiClient.get<ConsumerGroupListView>(withScope('/api/consumers', scope)),

  summary: (group: string, scope: ConsumerQueryScope) =>
    apiClient.get<ConsumerSummaryView>(
      withScope(`/api/consumers/${encodeURIComponent(group)}`, scope)
    ),

  connections: (group: string, scope: ConsumerQueryScope) =>
    apiClient.get<ConsumerConnectionView>(
      withScope(`/api/consumers/${encodeURIComponent(group)}/connections`, scope)
    ),

  progress: (group: string, scope: ConsumerQueryScope) =>
    apiClient.get<ConsumerProgressView>(
      withScope(`/api/consumers/${encodeURIComponent(group)}/progress`, scope)
    ),

  config: (group: string, scope: ConsumerQueryScope) =>
    apiClient.get<ConsumerConfigView>(
      withScope(`/api/consumers/${encodeURIComponent(group)}/config`, scope)
    ),

  brokers: (group: string) =>
    apiClient.get<ConsumerBrokerListView>(`/api/consumers/${encodeURIComponent(group)}/brokers`),

  runningInfo: (group: string, clientId: string, scope: ConsumerQueryScope) =>
    apiClient.get<ConsumerRunningInfoView>(
      withScope(
        `/api/consumers/${encodeURIComponent(group)}/clients/${encodeURIComponent(clientId)}/running-info`,
        scope
      )
    ),

  jstack: (group: string, clientId: string, scope: ConsumerQueryScope) =>
    apiClient.get<ConsumerJStackView>(
      withScope(
        `/api/consumers/${encodeURIComponent(group)}/clients/${encodeURIComponent(clientId)}/jstack`,
        scope
      )
    ),

  create: (request: ConsumerUpsertRequest) =>
    apiClient.post<ConsumerOperationResult>('/api/consumers', request),

  update: (group: string, request: ConsumerUpsertRequest) =>
    apiClient.put<ConsumerOperationResult>(`/api/consumers/${encodeURIComponent(group)}`, request),

  delete: (group: string, request: ConsumerDeleteRequest) =>
    apiClient.delete<ConsumerOperationResult>(
      `/api/consumers/${encodeURIComponent(group)}`,
      request
    ),

  resetOffset: (group: string, request: ConsumerResetOffsetRequest) =>
    apiClient.post<MutationResult>(
      `/api/consumers/${encodeURIComponent(group)}/reset-offset`,
      request
    )
};
