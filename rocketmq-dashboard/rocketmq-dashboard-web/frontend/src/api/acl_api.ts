import { apiClient } from './client';
import type {
  AclMutationResult,
  AclPolicyRequest,
  AclPolicyView,
  AclQueryParams,
  AclUserUpsertRequest,
  AclUserView
} from '../types/acl';

function toQueryString(params: AclQueryParams = {}) {
  const search = new URLSearchParams();
  Object.entries(params).forEach(([key, value]) => {
    if (value !== undefined && value !== null && String(value).trim() !== '') {
      search.set(key, String(value));
    }
  });
  const query = search.toString();
  return query ? `?${query}` : '';
}

export const aclApi = {
  listUsers: (params: AclQueryParams = {}) => apiClient.get<AclUserView[]>(`/api/acl/users${toQueryString(params)}`),
  createUser: (payload: AclUserUpsertRequest) => apiClient.post<AclMutationResult>('/api/acl/users', payload),
  updateUser: (username: string, payload: AclUserUpsertRequest) =>
    apiClient.put<AclMutationResult>(`/api/acl/users/${encodeURIComponent(username)}`, payload),
  deleteUser: (username: string, params: AclQueryParams = {}) =>
    apiClient.delete<AclMutationResult>(`/api/acl/users/${encodeURIComponent(username)}${toQueryString(params)}`),
  listPolicies: (params: AclQueryParams = {}) =>
    apiClient.get<AclPolicyView[]>(`/api/acl/policies${toQueryString(params)}`),
  createPolicy: (payload: AclPolicyRequest) => apiClient.post<AclMutationResult>('/api/acl/policies', payload),
  updatePolicy: (subject: string, payload: AclPolicyRequest) =>
    apiClient.put<AclMutationResult>(`/api/acl/policies/${encodeURIComponent(subject)}`, payload),
  deletePolicy: (subject: string, params: AclQueryParams = {}) =>
    apiClient.delete<AclMutationResult>(`/api/acl/policies/${encodeURIComponent(subject)}${toQueryString(params)}`)
};
