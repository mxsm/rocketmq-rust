import { apiClient } from './client';
import type { LoginRequest, SessionView } from '../types/auth';

export const authApi = {
  session: () => apiClient.get<SessionView>('/api/auth/session'),
  login: (request: LoginRequest) => apiClient.post<SessionView>('/api/auth/login', request),
  logout: () => apiClient.post<SessionView>('/api/auth/logout')
};
