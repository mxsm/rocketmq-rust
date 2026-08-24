import { apiClient } from './client';
import type { AuditEventPage, AuditEventQuery, SessionListPage } from '../types/audit';

function queryString(query: object) {
  const parameters = new URLSearchParams();
  for (const [key, value] of Object.entries(query)) {
    if ((typeof value === 'string' || typeof value === 'number') && value !== '') parameters.set(key, String(value));
  }
  const encoded = parameters.toString();
  return encoded ? `?${encoded}` : '';
}

export const auditApi = {
  listSessions: (query: { username?: string; cursor?: string; limit?: number } = {}) =>
    apiClient.get<SessionListPage>(`/api/auth/sessions${queryString(query)}`),
  revokeAllSessions: (username: string) =>
    apiClient.post<{ revoked: number }>('/api/auth/sessions/revoke-all', { username }),
  listEvents: (query: AuditEventQuery = {}) =>
    apiClient.get<AuditEventPage>(`/api/audit/events${queryString(query)}`)
};
