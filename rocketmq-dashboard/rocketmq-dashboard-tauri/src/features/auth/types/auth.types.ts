export interface LoginCredentials {
    username: string;
    password: string;
}

export interface LoginResponse {
    success: boolean;
    message: string;
}

export interface AuthError {
    message: string;
    code?: string;
}
