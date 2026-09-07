import { useState } from 'react';
import { AuthService } from '../../../services/auth.service';
import { dashboardErrorMessage } from '../../../services/invoke';
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

            SessionStorageService.setSessionId(result.sessionId);
            setAuthSession(result.sessionId, result.currentUser);
            return { success: true, mustChangePassword: result.currentUser.mustChangePassword };
        } catch (err) {
            const errorMessage = dashboardErrorMessage(err, 'Failed to connect to authentication service');
            setError(errorMessage);
            triggerShake();
            return { success: false, error: errorMessage };
        } finally {
            setIsLoading(false);
        }
    };

    const changePassword = async (payload: ChangePasswordPayload) => {
        if (!sessionId) {
            const errorMessage = 'Session not found';
            setError(errorMessage);
            return { success: false, error: errorMessage };
        }

        setIsLoading(true);
        setError('');

        try {
            await AuthService.changePassword(payload);
            markPasswordChanged();
            return { success: true };
        } catch (err) {
            const errorMessage = dashboardErrorMessage(err, 'Failed to update password');
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
        } catch {
            // Local session state must still be cleared when the backend session is unavailable.
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
