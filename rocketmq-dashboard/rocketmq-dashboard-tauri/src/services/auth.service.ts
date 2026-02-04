import { invoke } from '@tauri-apps/api/core';
import type { LoginCredentials, LoginResponse } from '../features/auth/types/auth.types';

/**
 * Authentication service for Tauri backend
 */
export class AuthService {
    /**
     * Verify user credentials via Tauri backend
     */
    static async login(credentials: LoginCredentials): Promise<LoginResponse> {
        try {
            const result = await invoke<LoginResponse>('verify_login', {
                username: credentials.username,
                password: credentials.password,
            });
            return result;
        } catch (error) {
            console.error('Authentication error:', error);
            throw new Error('Failed to connect to authentication service');
        }
    }
}
