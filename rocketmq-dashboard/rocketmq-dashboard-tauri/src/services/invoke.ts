import { ConnectionStore, type ConnectionSettingsView } from './connection.store';
import { invoke } from '@tauri-apps/api/core';
import { SessionStorageService } from './session.storage';

export type CommandErrorCategory =
    | 'authentication'
    | 'validation'
    | 'not_found'
    | 'configuration'
    | 'unavailable'
    | 'internal';

export interface CommandErrorPayload {
    code: string;
    message: string;
    category: CommandErrorCategory;
    retryable: boolean;
    field?: string;
    auditWarning?: string;
}

export class DashboardClientError extends Error {
    readonly code: string;
    readonly category: CommandErrorCategory;
    readonly retryable: boolean;
    readonly field?: string;

    constructor(payload: CommandErrorPayload) {
        super(payload.message);
        this.name = 'DashboardClientError';
        this.code = payload.code;
        this.category = payload.category;
        this.retryable = payload.retryable;
        this.field = payload.field;
    }
}

const isCommandError = (value: unknown): value is CommandErrorPayload => {
    if (!value || typeof value !== 'object') {
        return false;
    }

    const candidate = value as Partial<CommandErrorPayload>;
    return (
        typeof candidate.code === 'string' &&
        typeof candidate.message === 'string' &&
        typeof candidate.category === 'string' &&
        typeof candidate.retryable === 'boolean'
    );
};

const normalizeCommandError = (error: unknown): DashboardClientError => {
    if (isCommandError(error)) {
        return new DashboardClientError(error);
    }

    return new DashboardClientError({
        code: 'dashboard.protocol.invalid_error',
        message: 'The dashboard could not complete the operation.',
        category: 'internal',
        retryable: false,
    });
};

export const isDashboardClientError = (error: unknown): error is DashboardClientError =>
    error instanceof DashboardClientError;

export const dashboardErrorMessage = (error: unknown, fallback: string): string =>
    isDashboardClientError(error) && error.message.trim().length > 0 ? error.message : fallback;

const auditWarningListeners = new Set<(message: string) => void>();
export const subscribeAuditWarning = (listener: (message: string) => void): (() => void) => {
    auditWarningListeners.add(listener);
    return () => { auditWarningListeners.delete(listener); };
};
const reportAuditWarning = (value: unknown): void => {
    if (value && typeof value === 'object' && 'auditWarning' in value && typeof value.auditWarning === 'string') {
        for (const listener of auditWarningListeners) listener(value.auditWarning);
    }
};

const invokeDecoded = async <T>(command: string, args?: Record<string, unknown>): Promise<T> => {
    try {
        const result = await invoke<T>(command, args);
        reportAuditWarning(result);
        return result;
    } catch (error) {
        if (isCommandError(error)) reportAuditWarning(error);
        throw normalizeCommandError(error);
    }
};

export const invokePublicCommand = <T>(command: string, args?: Record<string, unknown>): Promise<T> =>
    invokeDecoded<T>(command, args);

export const invokeSessionCommand = <T>(
    command: string,
    sessionId: string,
    args?: Record<string, unknown>,
): Promise<T> => invokeDecoded<T>(command, { ...args, sessionId }).catch((error: unknown) => {
    if (isDashboardClientError(error)) {
        if (error.code === 'auth.session.invalid') {
            SessionStorageService.reportAuthenticationFailure(sessionId, 'invalid');
        } else if (error.code === 'auth.password_change_required') {
            SessionStorageService.reportAuthenticationFailure(sessionId, 'password-change-required');
        }
    }
    throw error;
});

const localCommands = new Set(['get_storage_status', 'get_history_status', 'change_password', 'get_current_user_profile', 'get_auth_bootstrap_status', 'list_sessions', 'revoke_user_sessions', 'query_audit_events']);
const connectionWrites = new Set(['add_name_server', 'switch_name_server', 'delete_name_server', 'update_vip_channel', 'update_use_tls', 'add_proxy_addr', 'switch_proxy_addr', 'delete_proxy_addr', 'replace_name_servers']);
const mutationCommands = new Set([
    'create_or_update_topic', 'delete_topic', 'delete_topic_by_broker',
    'reset_consumer_offset', 'skip_message_accumulate', 'send_topic_message',
    'create_or_update_consumer_group', 'delete_consumer_group',
    'consume_message_directly', 'resend_dlq_message', 'batch_resend_dlq_message',
    'update_cluster_broker_config',
    'create_acl_user', 'update_acl_user', 'delete_acl_user',
    'create_acl_policy', 'update_acl_policy', 'delete_acl_policy',
    'save_consumer_monitor_rule', 'delete_consumer_monitor_rule',
]);
const pendingSettings = new Map<string, Promise<ConnectionSettingsView>>();
const ensureSettings = (token: string): Promise<ConnectionSettingsView> => {
    const current = ConnectionStore.getSnapshot();
    if (current) return Promise.resolve(current);
    const pending = pendingSettings.get(token);
    if (pending) return pending;
    const loading = invokeSessionCommand<ConnectionSettingsView>('get_connection_settings', token).then((settings) => {
        ConnectionStore.accept(token, settings);
        return ConnectionStore.getSnapshot() ?? settings;
    }).finally(() => pendingSettings.delete(token));
    pendingSettings.set(token, loading);
    return loading;
};
const configurationChanged = () => new DashboardClientError({ code: 'dashboard.configuration_conflict', category: 'validation', retryable: false, message: 'Connection settings changed. Refresh and review the current configuration.' });

export const invokeAuthenticatedCommand = async <T>(command: string, args?: Record<string, unknown>): Promise<T> => {
    const sessionId = SessionStorageService.getSessionId();
    if (!sessionId) {
        SessionStorageService.reportAuthenticationFailure(null, 'invalid');
        return Promise.reject(
            new DashboardClientError({
                code: 'auth.session.invalid',
                message: 'Your session is no longer valid. Sign in again.',
                category: 'authentication',
                retryable: false,
            }),
        );
    }

    if (localCommands.has(command)) return invokeSessionCommand<T>(command, sessionId, args);
    if (command === 'get_connection_settings') {
        const result = await invokeSessionCommand<ConnectionSettingsView>(command, sessionId, args);
        ConnectionStore.accept(sessionId, result);
        return result as T;
    }
    if (connectionWrites.has(command)) {
        const result = await invokeSessionCommand<T>(command, sessionId, args);
        if (result && typeof result === 'object' && 'settings' in result) ConnectionStore.accept(sessionId, result.settings as ConnectionSettingsView);
        return result;
    }
    const settings = await ensureSettings(sessionId);
    if (SessionStorageService.getSessionId() !== sessionId) throw configurationChanged();
    let result: T;
    try {
        result = await invokeSessionCommand<T>(command, sessionId, { ...args, expectedRevision: settings.revision });
    } catch (error) {
        if (isDashboardClientError(error) && error.code === 'dashboard.configuration_conflict') {
            // Refresh the view identity only. Never replay the rejected operation.
            try { ConnectionStore.accept(sessionId, await invokeSessionCommand<ConnectionSettingsView>('get_connection_settings', sessionId)); } catch {}
        }
        throw error;
    }
    // Accepted writes retain their receipt after a switch, including committed local rules.
    // The backend rejects stale writes before execution; page generations guard old views.
    if (!mutationCommands.has(command) && ConnectionStore.getSnapshot()?.revision !== settings.revision) throw configurationChanged();
    if (result && typeof result === 'object' && 'settings' in result) ConnectionStore.accept(sessionId, result.settings as ConnectionSettingsView);
    return result;
};
