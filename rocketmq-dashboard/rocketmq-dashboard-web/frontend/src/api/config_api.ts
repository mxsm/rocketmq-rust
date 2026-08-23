import { apiClient } from './client';
import type {
  AddressRequest,
  BoolSettingRequest,
  ConfigMutationResult,
  DashboardConfigView,
  EndpointRequest,
  NameserverAvailabilityView,
  NameserverListRequest
} from '../types/config';

export const configApi = {
  getConfig: () => apiClient.get<DashboardConfigView>('/api/config'),
  getNameserverAvailability: () =>
    apiClient.get<NameserverAvailabilityView>('/api/config/nameservers'),
  replaceNameservers: (request: NameserverListRequest) =>
    apiClient.put<ConfigMutationResult>('/api/config/nameservers', request),
  addNameserver: (request: AddressRequest) =>
    apiClient.post<ConfigMutationResult>('/api/config/nameservers', request),
  switchNameserver: (request: EndpointRequest) =>
    apiClient.put<ConfigMutationResult>('/api/config/nameservers/current', request),
  deleteNameserver: (endpointId: string, expectedRevision: number) =>
    apiClient.delete<ConfigMutationResult>(`/api/config/nameservers/${encodeURIComponent(endpointId)}?expectedRevision=${expectedRevision}`),
  setVipChannel: (request: BoolSettingRequest) =>
    apiClient.put<ConfigMutationResult>('/api/config/vip-channel', request),
  setTls: (request: BoolSettingRequest) => apiClient.put<ConfigMutationResult>('/api/config/tls', request),
  addProxy: (request: AddressRequest) => apiClient.post<ConfigMutationResult>('/api/config/proxies', request),
  switchProxy: (request: EndpointRequest) =>
    apiClient.put<ConfigMutationResult>('/api/config/proxies/current', request),
  deleteProxy: (endpointId: string, expectedRevision: number) =>
    apiClient.delete<ConfigMutationResult>(`/api/config/proxies/${encodeURIComponent(endpointId)}?expectedRevision=${expectedRevision}`)
};
