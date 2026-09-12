import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { invoke } from '@tauri-apps/api/core';
import { ConnectionStore } from './connection.store';
import { AuthService } from './auth.service';
import { invokeAuthenticatedCommand, subscribeAuditWarning } from './invoke';
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
    ConnectionStore.reset();
    ConnectionStore.accept('current-token', { revision: 0, credentialsConfigured: false, endpoints: [], currentNameserverId: null, currentProxyId: null, environmentId: null, nameserver: { currentNamesrv: null, namesrvAddrList: [], useVIPChannel: false, useTLS: false }, proxy: { currentProxyAddr: null, proxyAddrList: [] } });
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
        await Promise.resolve();
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

    it('does not log out on failed or unconfirmed revocation', async () => {
        vi.mocked(invoke).mockRejectedValueOnce({ code: 'dashboard.storage_unavailable', message: 'Storage unavailable.', category: 'unavailable', retryable: false });
        await expect(AuthService.revokeUserSessions('admin')).rejects.toBeDefined();
        expect(SessionStorageService.getSessionId()).toBe('current-token');
        for (const currentSessionRevoked of [false, 'false', undefined]) {
            vi.mocked(invoke).mockResolvedValueOnce({ revokedCount: 0, currentSessionRevoked });
            await AuthService.revokeUserSessions('admin');
            expect(SessionStorageService.getSessionId()).toBe('current-token');
        }
    });

    it('does not invalidate a newer login when an old revocation completes', async () => {
        let resolve!: (value: unknown) => void;
        vi.mocked(invoke).mockImplementation(() => new Promise(complete => { resolve = complete; }));
        const pending = AuthService.revokeUserSessions('admin');
        SessionStorageService.setSessionId('new-token');
        resolve({ revokedCount: 2, currentSessionRevoked: true });
        await pending;
        expect(SessionStorageService.getSessionId()).toBe('new-token');
        expect(invoke).toHaveBeenCalledTimes(1);
    });

    it('successful password changes require a new login', async () => {
        vi.mocked(invoke).mockResolvedValue({ message: 'Password changed' });
        await AuthService.changePassword({ oldPassword: 'old-secret', newPassword: 'new-secret' });
        expect(SessionStorageService.getSessionId()).toBeNull();
    });
});

describe('audit warnings', () => {
    it('preserves the successful receipt and reports a separate warning without retrying', async () => {
        const warning = vi.fn();
        const unsubscribe = subscribeAuditWarning(warning);
        try {
            const receipt = { success: true, messageId: 'sent-once', auditWarning: 'Audit record unavailable.' };
            vi.mocked(invoke).mockResolvedValue(receipt);
            const result = await invokeAuthenticatedCommand('send_topic_message');
            expect(result).toEqual(receipt);
            expect(warning).toHaveBeenCalledWith('Audit record unavailable.');
            expect(invoke).toHaveBeenCalledTimes(1);
        } finally { unsubscribe(); }
    });

    it('keeps the operation error when recording its failure also fails', async () => {
        const warning = vi.fn();
        const unsubscribe = subscribeAuditWarning(warning);
        try {
            vi.mocked(invoke).mockRejectedValue({ code: 'client.component.unavailable', category: 'unavailable', message: 'Client unavailable.', retryable: false, auditWarning: 'Audit record unavailable.' });
            await expect(invokeAuthenticatedCommand('delete_topic')).rejects.toMatchObject({ code: 'client.component.unavailable' });
            expect(warning).toHaveBeenCalledOnce();
            expect(invoke).toHaveBeenCalledTimes(1);
        } finally { unsubscribe(); }
    });
});

describe('connection revisions', () => {
    it('rejects a late read response after a configuration switch', async () => {
        let resolve!: (value: unknown) => void;
        vi.mocked(invoke).mockImplementation(() => new Promise((complete) => { resolve = complete; }));
        const request = invokeAuthenticatedCommand('get_topic_list');
        await Promise.resolve();
        const previous = ConnectionStore.getSnapshot()!;
        ConnectionStore.accept('current-token', { ...previous, revision: 1, environmentId: 'new-environment' });
        resolve({ topics: ['old-environment-topic'] });
        await expect(request).rejects.toMatchObject({ code: 'dashboard.configuration_conflict' });
        expect(invoke).toHaveBeenCalledWith('get_topic_list', { sessionId: 'current-token', expectedRevision: 0 });
    });

    it.each([
        'send_topic_message',
        'update_cluster_broker_config',
        'create_acl_user', 'update_acl_user', 'delete_acl_user',
        'create_acl_policy', 'update_acl_policy', 'delete_acl_policy',
        'save_consumer_monitor_rule', 'delete_consumer_monitor_rule',
    ])('preserves a completed %s receipt after the view changes', async (command) => {
        let resolve!: (value: unknown) => void;
        vi.mocked(invoke).mockImplementation(() => new Promise((complete) => { resolve = complete; }));
        const request = invokeAuthenticatedCommand(command);
        await Promise.resolve();
        ConnectionStore.accept('current-token', { ...ConnectionStore.getSnapshot()!, revision: 1 });
        resolve({ success: true, messageId: 'already-sent' });
        await expect(request).resolves.toMatchObject({ success: true, messageId: 'already-sent' });
        expect(invoke).toHaveBeenCalledTimes(1);
        expect(invoke).toHaveBeenCalledWith(command, { sessionId: 'current-token', expectedRevision: 0 });
    });

    it('does not automatically retry a conflicting configuration draft', async () => {
        vi.mocked(invoke).mockRejectedValue({ code: 'dashboard.configuration_conflict', message: 'Review configuration.', category: 'validation', retryable: false });
        await expect(invokeAuthenticatedCommand('add_name_server', { address: '127.0.0.2:9876', expectedRevision: 0 })).rejects.toMatchObject({ code: 'dashboard.configuration_conflict' });
        expect(invoke).toHaveBeenCalledTimes(1);
    });

    it('a stale settings response cannot roll back the shared identity', () => {
        const previous = ConnectionStore.getSnapshot()!;
        ConnectionStore.accept('current-token', { ...previous, revision: 2, environmentId: 'current-environment' });
        ConnectionStore.accept('current-token', { ...previous, revision: 1, environmentId: 'old-environment' });
        expect(ConnectionStore.getSnapshot()?.environmentId).toBe('current-environment');
    });
});
