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
        return invokeAuthenticatedCommand<CommonResponse>('change_password', {
            oldPassword: payload.oldPassword,
            newPassword: payload.newPassword,
        });
    }

    static async getCurrentUserProfile(): Promise<UserProfile> {
        return invokeAuthenticatedCommand<UserProfile>('get_current_user_profile');
    }

    static async getBootstrapStatus(): Promise<BootstrapStatus> {
        return invokePublicCommand<BootstrapStatus>('get_auth_bootstrap_status');
    }
}
