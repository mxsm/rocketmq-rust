export interface LoginCredentials {
    username: string;
    password: string;
}

export interface SessionUser {
    userId: number;
    username: string;
    mustChangePassword: boolean;
    createdAt: string;
}

export interface AuthSessionResponse {
    sessionId: string;
    currentUser: SessionUser;
}

export interface UserProfile {
    userId: number;
    username: string;
    isActive: boolean;
    mustChangePassword: boolean;
    createdAt: string;
    updatedAt: string;
    lastLoginAt: string | null;
}

export interface CommonResponse {
    message: string;
}

export interface ChangePasswordPayload {
    oldPassword: string;
    newPassword: string;
}

export interface BootstrapStatus {
    username: string;
    created: boolean;
    hasDefaultAdmin: boolean;
    mustChangePassword: boolean;
}
