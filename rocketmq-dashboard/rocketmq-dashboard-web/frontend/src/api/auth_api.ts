import { apiClient, authSessionStore } from './client';
import type { LoginRequest, SessionView } from '../types/auth';

export const authApi = {
  session: () => apiClient.get<SessionView>('/api/auth/session'),
  login: async (request: LoginRequest) => {
    const session = await apiClient.post<SessionView>('/api/auth/login', request);
    if (session.sessionId) {
      authSessionStore.set(session.sessionId);
    }
    return session;
  },
  logout: async () => {
    const session = await apiClient.post<SessionView>('/api/auth/logout');
    authSessionStore.clear();
    return session;
  }
};
