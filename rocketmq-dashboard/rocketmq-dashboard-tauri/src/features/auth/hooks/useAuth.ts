import { useState } from 'react';
import { AuthService } from '../../../services/auth.service';
import { SessionStorageService } from '../../../services/session.storage';
import { useAppStore } from '../../../stores/app.store';
import type { ChangePasswordPayload, LoginCredentials } from '../types/auth.types';

export const useAuth = () => {
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');
    const [shake, setShake] = useState(false);
    const { clearAuthSession, markPasswordChanged, sessionId, setAuthSession } = useAppStore();

    const login = async (credentials: LoginCredentials) => {
        setIsLoading(true);
        setError('');
        setShake(false);

        try {
            const result = await AuthService.login(credentials);

            if (result.success && result.sessionId && result.currentUser) {
                SessionStorageService.setSessionId(result.sessionId);
                setAuthSession(result.sessionId, result.currentUser);
                return { success: true, mustChangePassword: result.mustChangePassword };
            }

            const errorMessage = result.message || 'Invalid username or password';
            setError(errorMessage);
            triggerShake();
            return { success: false, error: errorMessage };
        } catch (err) {
            const errorMessage = err instanceof Error ? err.message : 'Failed to connect to authentication service';
            setError(errorMessage);
            triggerShake();
            return { success: false, error: errorMessage };
        } finally {
            setIsLoading(false);
        }
    };

    const changePassword = async (payload: Omit<ChangePasswordPayload, 'sessionId'>) => {
        if (!sessionId) {
            const errorMessage = 'Session not found';
            setError(errorMessage);
            return { success: false, error: errorMessage };
        }

        setIsLoading(true);
        setError('');

        try {
            const result = await AuthService.changePassword({
                sessionId,
                ...payload,
            });

            if (result.success) {
                markPasswordChanged();
                return { success: true };
            }

            setError(result.message);
            return { success: false, error: result.message };
        } catch (err) {
            const errorMessage = err instanceof Error ? err.message : 'Failed to update password';
            setError(errorMessage);
            return { success: false, error: errorMessage };
        } finally {
            setIsLoading(false);
        }
    };

    const logout = async () => {
        try {
            if (sessionId) {
                await AuthService.logout(sessionId);
            }
        } catch (error) {
            console.error('Logout failed', error);
        } finally {
            SessionStorageService.clearSessionId();
            clearAuthSession();
        }
    };

    const triggerShake = () => {
        setShake(true);
        setTimeout(() => setShake(false), 650);
    };

    const clearError = () => {
        setError('');
    };

    return {
        isLoading,
        error,
        shake,
        login,
        changePassword,
        logout,
        clearError,
    };
};
