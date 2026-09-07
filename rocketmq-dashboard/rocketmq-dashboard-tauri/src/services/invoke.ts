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

const invokeDecoded = async <T>(command: string, args?: Record<string, unknown>): Promise<T> => {
    try {
        return await invoke<T>(command, args);
    } catch (error) {
        throw normalizeCommandError(error);
    }
};

export const invokePublicCommand = <T>(command: string, args?: Record<string, unknown>): Promise<T> =>
    invokeDecoded<T>(command, args);

export const invokeSessionCommand = <T>(
    command: string,
    sessionId: string,
    args?: Record<string, unknown>,
): Promise<T> => invokeDecoded<T>(command, { ...args, sessionId });

export const invokeAuthenticatedCommand = <T>(command: string, args?: Record<string, unknown>): Promise<T> => {
    const sessionId = SessionStorageService.getSessionId();
    if (!sessionId) {
        return Promise.reject(
            new DashboardClientError({
                code: 'auth.session.invalid',
                message: 'Your session is no longer valid. Sign in again.',
                category: 'authentication',
                retryable: false,
            }),
        );
    }

    return invokeSessionCommand<T>(command, sessionId, args);
};
