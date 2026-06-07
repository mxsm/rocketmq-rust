export interface LoginRequest {
  username: string;
  password: string;
}

export interface SessionView {
  loginRequired: boolean;
  authenticated: boolean;
  username?: string | null;
  sessionId?: string | null;
  loginTime?: number | null;
}
