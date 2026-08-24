import { apiClient, authSessionStore } from './client';
import type { LoginRequest, SessionView } from '../types/auth';

export const authApi = {
  session: () => apiClient.get<SessionView>('/api/auth/session'),
  login: async (request: LoginRequest) => {
    const session = await apiClient.post<SessionView>('/api/auth/login', request);
    // New sessions are carried by the HttpOnly cookie. Clear a stale legacy
    // header credential so it cannot conflict with that cookie on the next
    // protected request.
    authSessionStore.clear();
    return session;
  },
  logout: async () => {
    const session = await apiClient.post<SessionView>('/api/auth/logout');
    authSessionStore.clear();
    return session;
  }
};
