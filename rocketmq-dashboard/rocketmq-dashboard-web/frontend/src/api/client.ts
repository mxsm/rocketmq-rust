import type { ApiResponse } from '../types/api';

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? '';
const auditWarningMessage = 'The operation completed, but its audit record could not be stored.';

export class ApiClientError extends Error {
  readonly code: string;
  readonly status: number | null;
  readonly details: Readonly<Record<string, unknown>>;

  constructor(
    code: string,
    message: string,
    options: { status?: number; details?: Record<string, unknown> } = {}
  ) {
    super(message);
    this.name = 'ApiClientError';
    this.code = code;
    this.status = options.status ?? null;
    this.details = options.details ?? {};
  }
}

export function userErrorMessage(error: unknown, fallback: string): string {
  return error instanceof ApiClientError ? error.message : fallback;
}

function notifyAuthenticationExpired() {
  window.dispatchEvent(new Event('rocketmq-auth-expired'));
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function decodeResponse<T>(value: unknown): ApiResponse<T> | null {
  if (!isRecord(value)
    || typeof value.success !== 'boolean'
    || typeof value.code !== 'string'
    || typeof value.message !== 'string') {
    return null;
  }
  if (value.details !== undefined && !isRecord(value.details)) return null;
  return value as unknown as ApiResponse<T>;
}

function safeServerErrorMessage(code: string, status: number): string {
  switch (code) {
    case 'AUTH_ERROR':
    case 'AUTH_TOKEN_AMBIGUOUS':
      return 'Authentication failed.';
    case 'FORBIDDEN':
      return 'Permission was denied.';
    case 'VALIDATION_ERROR':
    case 'core.argument.invalid':
    case 'INVALID_JSON':
    case 'INVALID_JSON_DATA':
    case 'INVALID_QUERY':
    case 'INVALID_PATH':
      return 'The request is invalid.';
    case 'UNSUPPORTED_MEDIA_TYPE':
      return 'The request must use JSON.';
    case 'PAYLOAD_TOO_LARGE':
      return 'The request is too large.';
    case 'NOT_FOUND':
    case 'API_ROUTE_NOT_FOUND':
      return 'The requested resource was not found.';
    case 'METHOD_NOT_ALLOWED':
      return 'The requested operation is not allowed.';
    case 'client.lifecycle.not_started':
    case 'client.component.unavailable':
      return 'The admin service is unavailable.';
    case 'client.lifecycle.invalid_state':
      return 'The admin operation cannot continue because its target state changed.';
    case 'tools.operation.failed':
      return 'The RocketMQ administration operation failed.';
    case 'CONFIG_ERROR':
      return 'Dashboard configuration is invalid.';
    case 'NOT_IMPLEMENTED':
      return 'The requested operation is not implemented.';
    case 'INTERNAL_ERROR':
    case 'MISSING_REQUEST_CONTEXT':
    case 'REQUEST_BODY_ERROR':
      return 'The dashboard request could not be completed.';
    default:
      if (status === 401) return 'Authentication failed.';
      if (status === 403) return 'Permission was denied.';
      if (status === 404) return 'The requested resource was not found.';
      return 'The dashboard request failed.';
  }
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  let response: Response;
  try {
    response = await fetch(`${apiBaseUrl}${path}`, {
      ...init,
      credentials: 'include',
      headers: {
        'Content-Type': 'application/json',
        ...init?.headers
      }
    });
  } catch {
    throw new ApiClientError('NETWORK_ERROR', 'Unable to reach the dashboard backend.');
  }

  if (response.status === 401) notifyAuthenticationExpired();
  if (response.headers.get('x-dashboard-audit') === 'failed') {
    window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: auditWarningMessage }));
  }

  const responseText = await response.text();
  if (responseText.trim() === '') {
    throw new ApiClientError(
      'EMPTY_RESPONSE',
      response.ok ? 'The dashboard backend returned no data.' : 'The dashboard request failed.',
      { status: response.status }
    );
  }

  let decoded: unknown;
  try {
    decoded = JSON.parse(responseText);
  } catch {
    throw new ApiClientError('INVALID_RESPONSE', 'The dashboard backend returned an invalid response.', {
      status: response.status
    });
  }
  const payload = decodeResponse<T>(decoded);
  if (!payload) {
    throw new ApiClientError('INVALID_RESPONSE', 'The dashboard backend returned an invalid response.', {
      status: response.status
    });
  }

  if (!response.ok || !payload.success) {
    if (response.status === 401 || payload.code === 'AUTH_ERROR' || payload.code === 'AUTH_TOKEN_AMBIGUOUS') {
      notifyAuthenticationExpired();
    }
    throw new ApiClientError(payload.code, safeServerErrorMessage(payload.code, response.status), {
      status: response.status,
      details: payload.details
    });
  }
  if (payload.data === undefined || payload.data === null) {
    throw new ApiClientError('EMPTY_RESPONSE', 'The server returned no data.', { status: response.status });
  }
  return payload.data;
}

export const apiClient = {
  get: <T>(path: string) => request<T>(path),
  post: <T>(path: string, body?: unknown) =>
    request<T>(path, {
      method: 'POST',
      body: body === undefined ? undefined : JSON.stringify(body)
    }),
  put: <T>(path: string, body?: unknown) =>
    request<T>(path, {
      method: 'PUT',
      body: body === undefined ? undefined : JSON.stringify(body)
    }),
  delete: <T>(path: string, body?: unknown) =>
    request<T>(path, {
      method: 'DELETE',
      body: body === undefined ? undefined : JSON.stringify(body)
    })
};
