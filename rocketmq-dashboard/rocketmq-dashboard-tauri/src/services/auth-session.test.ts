import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { invoke } from '@tauri-apps/api/core';
import { AuthService } from './auth.service';
import { invokeAuthenticatedCommand } from './invoke';
import { SessionStorageService } from './session.storage';

vi.mock('@tauri-apps/api/core', () => ({ invoke: vi.fn() }));
const failure = (code: string) => ({ code, message: 'Authentication failed.', category: 'authentication', retryable: false });

beforeEach(() => {
    const entries = new Map<string, string>();
    vi.stubGlobal('window', { localStorage: {
        getItem: (key: string) => entries.get(key) ?? null,
        setItem: (key: string, value: string) => entries.set(key, value),
        removeItem: (key: string) => entries.delete(key),
    } });
    SessionStorageService.setSessionId('current-token');
});
afterEach(() => { vi.resetAllMocks(); vi.unstubAllGlobals(); });

describe('authoritative session lifecycle', () => {
    it('clears authentication when an ordinary business command rejects the current session', async () => {
        const onFailure = vi.fn();
        const unsubscribe = SessionStorageService.subscribeAuthenticationFailure(onFailure);
        try {
            vi.mocked(invoke).mockRejectedValue(failure('auth.session.invalid'));
            await expect(invokeAuthenticatedCommand('get_topic_list')).rejects.toMatchObject({ code: 'auth.session.invalid' });
            expect(SessionStorageService.getSessionId()).toBeNull();
            expect(onFailure).toHaveBeenCalledWith('invalid');
        } finally { unsubscribe(); }
    });

    it('a delayed old request cannot invalidate a newer login', async () => {
        let reject!: (reason: unknown) => void;
        vi.mocked(invoke).mockImplementation(() => new Promise((_resolve, rejectRequest) => { reject = rejectRequest; }));
        const request = invokeAuthenticatedCommand('get_topic_list');
        SessionStorageService.setSessionId('new-token');
        reject(failure('auth.session.invalid'));
        await expect(request).rejects.toMatchObject({ code: 'auth.session.invalid' });
        expect(SessionStorageService.getSessionId()).toBe('new-token');
    });

    it('wrong credentials do not log out an otherwise valid session', async () => {
        vi.mocked(invoke).mockRejectedValue(failure('auth.credentials.invalid'));
        await expect(AuthService.changePassword({ oldPassword: 'wrong', newPassword: 'new-secret' })).rejects.toMatchObject({ code: 'auth.credentials.invalid' });
        expect(SessionStorageService.getSessionId()).toBe('current-token');
    });

    it('password-required errors keep the token and request the forced password flow', async () => {
        const onFailure = vi.fn();
        const unsubscribe = SessionStorageService.subscribeAuthenticationFailure(onFailure);
        try {
            vi.mocked(invoke).mockRejectedValue(failure('auth.password_change_required'));
            await expect(invokeAuthenticatedCommand('get_topic_list')).rejects.toBeDefined();
            expect(onFailure).toHaveBeenCalledWith('password-change-required');
            expect(SessionStorageService.getSessionId()).toBe('current-token');
        } finally { unsubscribe(); }
    });

    it('revoking all current-account sessions clears the token and notifies the login gate', async () => {
        const onFailure = vi.fn();
        const unsubscribe = SessionStorageService.subscribeAuthenticationFailure(onFailure);
        try {
            vi.mocked(invoke).mockResolvedValue({ revokedCount: 2, currentSessionRevoked: true });
            await AuthService.revokeUserSessions('admin');
            expect(SessionStorageService.getSessionId()).toBeNull();
            expect(onFailure).toHaveBeenCalledWith('invalid');
            expect(invoke).toHaveBeenCalledWith('revoke_user_sessions', { username: 'admin', sessionId: 'current-token' });
        } finally { unsubscribe(); }
    });

    it('successful password changes require a new login', async () => {
        vi.mocked(invoke).mockResolvedValue({ message: 'Password changed' });
        await AuthService.changePassword({ oldPassword: 'old-secret', newPassword: 'new-secret' });
        expect(SessionStorageService.getSessionId()).toBeNull();
    });
});
