import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { authApi } from './auth_api';
import { ApiClientError, apiClient, userErrorMessage } from './client';

function apiResponse(body: unknown, status = 200, headers: Record<string, string> = {}) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json', ...headers }
  });
}

describe('dashboard API client response boundary', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
  });

  afterEach(() => {
    vi.unstubAllGlobals();
  });

  it('preserves successful data when audit persistence failed and emits a fixed warning', async () => {
    const warning = vi.fn();
    window.addEventListener('rocketmq-audit-warning', warning, { once: true });
    vi.mocked(fetch).mockResolvedValueOnce(apiResponse(
      { success: true, code: 'OK', message: 'success', data: { changed: true } },
      200,
      { 'x-dashboard-audit': 'failed' }
    ));

    await expect(apiClient.post('/api/mutation', { changed: true })).resolves.toEqual({ changed: true });
    expect(warning).toHaveBeenCalledOnce();
    const event = warning.mock.calls[0][0] as CustomEvent<string>;
    expect(event.detail).toBe('The operation completed, but its audit record could not be stored.');
  });

  it('does not expose unrecognized server or thrown error messages', async () => {
    vi.mocked(fetch).mockResolvedValueOnce(apiResponse(
      { success: false, code: 'UNKNOWN_BACKEND_CODE', message: 'sensitive-rejection-detail', data: null },
      500
    ));

    await expect(apiClient.get('/api/config')).rejects.toMatchObject({
      code: 'UNKNOWN_BACKEND_CODE',
      message: 'The dashboard request failed.'
    } satisfies Partial<ApiClientError>);
    expect(userErrorMessage(new Error('sensitive-rejection-detail'), 'Unable to load data.')).toBe('Unable to load data.');
    expect(userErrorMessage('sensitive-rejection-detail', 'Unable to load data.')).toBe('Unable to load data.');
  });

  it('uses cookie credentials for login and later protected requests', async () => {
    vi.mocked(fetch)
      .mockResolvedValueOnce(apiResponse({ success: true, code: 'OK', message: 'success', data: { authenticated: true } }))
      .mockResolvedValueOnce(apiResponse({ success: true, code: 'OK', message: 'success', data: { configured: true } }));

    await authApi.login({ username: 'operator', password: 'password' });
    await apiClient.get('/api/config');

    for (const [, init] of vi.mocked(fetch).mock.calls) {
      expect(init?.credentials).toBe('include');
      const headers = new Headers(init?.headers);
      expect([...headers.keys()]).toEqual(['content-type']);
    }
  });

  it('rejects malformed success envelopes with a fixed client error', async () => {
    vi.mocked(fetch).mockResolvedValueOnce(apiResponse({ success: 'yes', data: { secret: true } }));

    await expect(apiClient.get('/api/config')).rejects.toMatchObject({
      code: 'INVALID_RESPONSE',
      message: 'The dashboard backend returned an invalid response.'
    });
  });
});
