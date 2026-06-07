import { apiClient } from './client';
import type {
  AddressRequest,
  BoolSettingRequest,
  ConfigMutationResult,
  DashboardConfigView,
  NameserverListRequest
} from '../types/config';

export const configApi = {
  getConfig: () => apiClient.get<DashboardConfigView>('/api/config'),
  replaceNameservers: (request: NameserverListRequest) =>
    apiClient.put<ConfigMutationResult>('/api/config/nameservers', request),
  addNameserver: (request: AddressRequest) =>
    apiClient.post<ConfigMutationResult>('/api/config/nameservers', request),
  setVipChannel: (request: BoolSettingRequest) =>
    apiClient.put<ConfigMutationResult>('/api/config/vip-channel', request),
  setTls: (request: BoolSettingRequest) => apiClient.put<ConfigMutationResult>('/api/config/tls', request),
  addProxy: (request: AddressRequest) => apiClient.post<ConfigMutationResult>('/api/config/proxies', request),
  switchProxy: (request: AddressRequest) =>
    apiClient.put<ConfigMutationResult>('/api/config/proxies/current', request),
  deleteProxy: (address: string) =>
    apiClient.delete<ConfigMutationResult>(`/api/config/proxies/${encodeURIComponent(address)}`)
};
