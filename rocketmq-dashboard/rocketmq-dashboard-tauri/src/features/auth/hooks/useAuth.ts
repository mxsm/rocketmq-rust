import { useState } from 'react';
import { AuthService } from '../../../services/auth.service';
import { useAppStore } from '../../../stores/app.store';
import type { LoginCredentials } from '../types/auth.types';

export const useAuth = () => {
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');
    const [shake, setShake] = useState(false);
    const { setIsLoggedIn } = useAppStore();

    const login = async (credentials: LoginCredentials) => {
        setIsLoading(true);
        setError('');
        setShake(false);

        try {
            const result = await AuthService.login(credentials);

            if (result.success) {
                setIsLoggedIn(true);
                return { success: true };
            } else {
                const errorMessage = result.message || 'Invalid username or password';
                setError(errorMessage);
                triggerShake();
                return { success: false, error: errorMessage };
            }
        } catch (err) {
            const errorMessage = err instanceof Error ? err.message : 'Failed to connect to authentication service';
            setError(errorMessage);
            triggerShake();
            return { success: false, error: errorMessage };
        } finally {
            setIsLoading(false);
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
        clearError,
    };
};
