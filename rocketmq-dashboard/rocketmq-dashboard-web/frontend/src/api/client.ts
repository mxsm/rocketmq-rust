import type { ApiResponse } from '../types/api';

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? '';
const sessionStorageKey = 'rocketmq-dashboard-web-session';
export const appliedAuditFailedCode = 'APPLIED_AUDIT_FAILED';

export const authSessionStore = {
  get: () => window.localStorage.getItem(sessionStorageKey),
  set: (sessionId: string) => window.localStorage.setItem(sessionStorageKey, sessionId),
  clear: () => window.localStorage.removeItem(sessionStorageKey)
};

export class ApiClientError extends Error {
  readonly code: string;
  readonly mutationApplied: boolean;

  constructor(code: string, message: string, options: { mutationApplied?: boolean } = {}) {
    super(message);
    this.code = code;
    this.mutationApplied = options.mutationApplied ?? false;
  }
}

export function isAppliedAuditFailure(error: unknown): error is ApiClientError {
  // The error code is the protocol contract. `mutationApplied` is an
  // additional local signal for callers that need it, but a UI must never
  // expose a retry merely because an adapter reconstructed this typed error.
  return error instanceof ApiClientError && error.code === appliedAuditFailedCode;
}

/**
 * Settles a mutation that committed remotely but whose audit event could not
 * be recorded. The mutation must remain terminal: close or disable its UI
 * synchronously, then fetch authoritative state exactly once without
 * resubmitting the original request.
 */
export async function handleAppliedAuditFailure(
  error: unknown,
  options: {
    onApplied: () => void;
    refresh?: () => Promise<unknown> | void;
  }
): Promise<boolean> {
  if (!isAppliedAuditFailure(error)) return false;
  options.onApplied();
  try {
    await options.refresh?.();
  } catch {
    // The global audit warning is already retained by the API client. A
    // refresh failure must not turn this terminal mutation into a retry.
  }
  return true;
}

function notifyAuthenticationExpired() {
  authSessionStore.clear();
  window.dispatchEvent(new Event('rocketmq-auth-expired'));
}

function emptyResponseMessage(path: string, response: Response) {
  if (response.ok) {
    return `The dashboard backend returned an empty response for ${path}.`;
  }

  return `The dashboard backend is unavailable or returned an empty response for ${path} (${response.status} ${response.statusText || 'HTTP error'}).`;
}

function invalidJsonMessage(path: string, response: Response) {
  if (response.ok) {
    return `The dashboard backend returned an invalid JSON response for ${path}.`;
  }

  return `The dashboard backend returned a non-JSON error response for ${path} (${response.status} ${response.statusText || 'HTTP error'}).`;
}

async function request<T>(path: string, init?: RequestInit): Promise<T> {
  const sessionId = authSessionStore.get();
  const response = await fetch(`${apiBaseUrl}${path}`, {
    ...init,
    credentials: 'include',
    headers: {
      'Content-Type': 'application/json',
      ...(sessionId ? { 'x-dashboard-session': sessionId } : {}),
      ...init?.headers
    }
  });
  const responseText = await response.text();
  // An HTTP 401 is authoritative even when an intermediary has stripped or
  // replaced the JSON body. Do not retain a legacy header credential that
  // would conflict with a fresh HttpOnly session on the next request.
  if (response.status === 401) notifyAuthenticationExpired();
  if (responseText.trim() === '') {
    throw new ApiClientError(response.ok ? 'EMPTY_RESPONSE' : String(response.status), emptyResponseMessage(path, response));
  }

  let payload: ApiResponse<T>;
  try {
    payload = JSON.parse(responseText) as ApiResponse<T>;
  } catch {
    throw new ApiClientError(response.ok ? 'INVALID_JSON' : String(response.status), invalidJsonMessage(path, response));
  }

  // This terminal result is intentionally handled before generic success and
  // data checks. The backend has applied the mutation, so exposing response
  // data (or an empty-response retry) could cause a second mutation.
  if (payload.code === appliedAuditFailedCode) {
    window.dispatchEvent(new CustomEvent('rocketmq-audit-warning', { detail: payload.message }));
    throw new ApiClientError(appliedAuditFailedCode, payload.message || 'The mutation was applied, but audit persistence failed.', {
      mutationApplied: true
    });
  }

  if (!response.ok || !payload.success) {
    if (response.status === 401 || payload.code === 'AUTH_ERROR' || payload.code === 'AUTH_TOKEN_AMBIGUOUS') {
      notifyAuthenticationExpired();
    }
    throw new ApiClientError(payload.code || String(response.status), payload.message || response.statusText);
  }
  if (payload.data === undefined || payload.data === null) {
    throw new ApiClientError('EMPTY_RESPONSE', 'The server returned no data.');
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
