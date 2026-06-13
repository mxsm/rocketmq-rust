import type { ApiResponse } from '../types/api';

const apiBaseUrl = import.meta.env.VITE_API_BASE_URL ?? '';
const sessionStorageKey = 'rocketmq-dashboard-web-session';

export const authSessionStore = {
  get: () => window.localStorage.getItem(sessionStorageKey),
  set: (sessionId: string) => window.localStorage.setItem(sessionStorageKey, sessionId),
  clear: () => window.localStorage.removeItem(sessionStorageKey)
};

export class ApiClientError extends Error {
  readonly code: string;

  constructor(code: string, message: string) {
    super(message);
    this.code = code;
  }
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
    headers: {
      'Content-Type': 'application/json',
      ...(sessionId ? { 'x-dashboard-session': sessionId } : {}),
      ...init?.headers
    }
  });
  const responseText = await response.text();
  if (responseText.trim() === '') {
    throw new ApiClientError(response.ok ? 'EMPTY_RESPONSE' : String(response.status), emptyResponseMessage(path, response));
  }

  let payload: ApiResponse<T>;
  try {
    payload = JSON.parse(responseText) as ApiResponse<T>;
  } catch {
    throw new ApiClientError(response.ok ? 'INVALID_JSON' : String(response.status), invalidJsonMessage(path, response));
  }

  if (!response.ok || !payload.success) {
    if (payload.code === 'AUTH_ERROR') {
      authSessionStore.clear();
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
  delete: <T>(path: string) =>
    request<T>(path, {
      method: 'DELETE'
    })
};
