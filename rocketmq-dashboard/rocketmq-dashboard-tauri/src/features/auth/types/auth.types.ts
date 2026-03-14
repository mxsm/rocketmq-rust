export interface LoginCredentials {
    username: string;
    password: string;
}

export interface SessionUser {
    sessionId: string;
    userId: number;
    username: string;
    mustChangePassword: boolean;
    createdAt: string;
}

export interface AuthSessionResponse {
    success: boolean;
    message: string;
    sessionId: string | null;
    currentUser: SessionUser | null;
    mustChangePassword: boolean;
}

export interface CommonResponse {
    success: boolean;
    message: string;
}

export interface ChangePasswordPayload {
    sessionId: string;
    oldPassword: string;
    newPassword: string;
}

export interface BootstrapStatus {
    username: string;
    created: boolean;
    hasDefaultAdmin: boolean;
    mustChangePassword: boolean;
}

export interface AuthError {
    message: string;
    code?: string;
}
