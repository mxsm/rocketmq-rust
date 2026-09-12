import { useEffect, useRef, useState } from 'react';
import { AuthService } from '../../../services/auth.service';
import { dashboardErrorMessage } from '../../../services/invoke';
import { SessionStorageService } from '../../../services/session.storage';
import { useAppStore } from '../../../stores/app.store';
import type { ChangePasswordPayload, LoginCredentials } from '../types/auth.types';

export const useAuth = () => {
    const [isLoading, setIsLoading] = useState(false);
    const [error, setError] = useState('');
    const { sessionId, setAuthSession } = useAppStore();
    const active = useRef(true);
    const pending = useRef(false);
    useEffect(() => { active.current = true; return () => { active.current = false; }; }, []);
    const begin = () => {
        if (!active.current || pending.current) return false;
        pending.current = true; setIsLoading(true); setError('');
        return true;
    };
    const finish = () => { pending.current = false; if (active.current) setIsLoading(false); };
    const login = async (credentials: LoginCredentials) => {
        if (!begin()) return { success: false };
        const previousSession = SessionStorageService.getSessionId();
        try {
            const result = await AuthService.login(credentials);
            if (!active.current || SessionStorageService.getSessionId() !== previousSession) return { success: false };
            SessionStorageService.setSessionId(result.sessionId);
            setAuthSession(result.sessionId, result.currentUser);
            return { success: true, mustChangePassword: result.currentUser.mustChangePassword };
        } catch (failure) {
            const message = dashboardErrorMessage(failure, 'Failed to connect to authentication service.');
            if (active.current) setError(message);
            return { success: false, error: message };
        } finally { finish(); }
    };
    const changePassword = async (payload: ChangePasswordPayload) => {
        if (!sessionId || SessionStorageService.getSessionId() !== sessionId) {
            const message = 'The sign-in session changed. Reopen the password dialog.';
            if (active.current) setError(message);
            return { success: false, error: message };
        }
        if (!begin()) return { success: false };
        try {
            await AuthService.changePassword(payload);
            return { success: true };
        } catch (failure) {
            const message = dashboardErrorMessage(failure, 'Failed to update password.');
            if (active.current) setError(message);
            return { success: false, error: message };
        } finally { finish(); }
    };
    const logout = async () => {
        if (!begin()) return;
        try {
            if (sessionId) await AuthService.logout(sessionId);
        } catch {
            // Signing out locally remains possible when the service cannot acknowledge logout.
        } finally {
            SessionStorageService.reportAuthenticationFailure(sessionId, 'invalid');
            finish();
        }
    };
    return { isLoading, error, login, changePassword, logout, clearError: () => setError('') };
};
