import { invoke } from '@tauri-apps/api/core';
import type {
    AuthSessionResponse,
    BootstrapStatus,
    ChangePasswordPayload,
    CommonResponse,
    LoginCredentials,
    UserProfileResponse,
} from '../features/auth/types/auth.types';

export class AuthService {
    static async login(credentials: LoginCredentials): Promise<AuthSessionResponse> {
        return invoke<AuthSessionResponse>('login', {
            username: credentials.username,
            password: credentials.password,
        });
    }

    static async logout(sessionId: string): Promise<CommonResponse> {
        return invoke<CommonResponse>('logout', { sessionId });
    }

    static async restoreSession(sessionId: string): Promise<AuthSessionResponse> {
        return invoke<AuthSessionResponse>('restore_session', { sessionId });
    }

    static async changePassword(payload: ChangePasswordPayload): Promise<CommonResponse> {
        return invoke<CommonResponse>('change_password', {
            sessionId: payload.sessionId,
            oldPassword: payload.oldPassword,
            newPassword: payload.newPassword,
        });
    }

    static async getCurrentUserProfile(sessionId: string): Promise<UserProfileResponse> {
        return invoke<UserProfileResponse>('get_current_user_profile', { sessionId });
    }

    static async getBootstrapStatus(): Promise<BootstrapStatus> {
        return invoke<BootstrapStatus>('get_auth_bootstrap_status');
    }
}
