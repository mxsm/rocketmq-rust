import { SessionStorageService } from './session.storage';
import { invokeAuthenticatedCommand, invokePublicCommand, invokeSessionCommand } from './invoke';
import type {
    AuthSessionResponse,
    BootstrapStatus,
    ChangePasswordPayload,
    CommonResponse,
    LoginCredentials,
    UserProfile,
} from '../features/auth/types/auth.types';

export class AuthService {
    static async login(credentials: LoginCredentials): Promise<AuthSessionResponse> {
        return invokePublicCommand<AuthSessionResponse>('login', {
            username: credentials.username,
            password: credentials.password,
        });
    }

    static async logout(sessionId: string): Promise<CommonResponse> {
        return invokeSessionCommand<CommonResponse>('logout', sessionId);
    }

    static async restoreSession(sessionId: string): Promise<AuthSessionResponse> {
        return invokeSessionCommand<AuthSessionResponse>('restore_session', sessionId);
    }

    static async changePassword(payload: ChangePasswordPayload): Promise<CommonResponse> {
        const sessionId = SessionStorageService.getSessionId();
        const result = await invokeAuthenticatedCommand<CommonResponse>('change_password', {
            oldPassword: payload.oldPassword,
            newPassword: payload.newPassword,
        });
        SessionStorageService.reportAuthenticationFailure(sessionId, 'invalid');
        return result;
    }

    static async getCurrentUserProfile(): Promise<UserProfile> {
        return invokeAuthenticatedCommand<UserProfile>('get_current_user_profile');
    }

    static async listSessions(username: string, cursor?: string): Promise<SessionPage> {
        return invokeAuthenticatedCommand<SessionPage>('list_sessions', { username, cursor, limit: 25 });
    }

    static async revokeUserSessions(username: string): Promise<RevokeSessionsResponse> {
        const sessionId = SessionStorageService.getSessionId();
        const result = await invokeAuthenticatedCommand<RevokeSessionsResponse>('revoke_user_sessions', { username });
        if (result.currentSessionRevoked) SessionStorageService.reportAuthenticationFailure(sessionId, 'invalid');
        return result;
    }

    static async getBootstrapStatus(): Promise<BootstrapStatus> {
        return invokePublicCommand<BootstrapStatus>('get_auth_bootstrap_status');
    }
}

export interface SessionView {
    id: string;
    username: string;
    createdAtMs: number;
    expiresAtMs: number;
    lastSeenAtMs: number;
    revokedAtMs: number | null;
    current: boolean;
}

export interface SessionPage {
    items: SessionView[];
    nextCursor: string | null;
}

export interface RevokeSessionsResponse {
    revokedCount: number;
    currentSessionRevoked: boolean;
}
