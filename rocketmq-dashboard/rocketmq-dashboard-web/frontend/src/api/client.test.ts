import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { authApi } from './auth_api';
import { ApiClientError, apiClient, authSessionStore, handleAppliedAuditFailure } from './client';

function apiResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'Content-Type': 'application/json' }
  });
}

describe('dashboard API client terminal mutation and session handling', () => {
  beforeEach(() => {
    vi.stubGlobal('fetch', vi.fn());
    authSessionStore.clear();
  });

  afterEach(() => {
    vi.unstubAllGlobals();
    authSessionStore.clear();
  });

  it.each([
    { success: true, code: 'APPLIED_AUDIT_FAILED', message: 'Applied without audit.' },
    { success: true, code: 'APPLIED_AUDIT_FAILED', message: 'Applied with audit fallback.', data: { changed: true } }
  ])('treats APPLIED_AUDIT_FAILED as terminal before data validation', async (body) => {
    const warning = vi.fn();
    window.addEventListener('rocketmq-audit-warning', warning, { once: true });
    vi.mocked(fetch).mockResolvedValueOnce(apiResponse(body));

    await expect(apiClient.post('/api/mutation', { changed: true })).rejects.toMatchObject({
      code: 'APPLIED_AUDIT_FAILED',
      mutationApplied: true
    } satisfies Partial<ApiClientError>);
    expect(warning).toHaveBeenCalledOnce();
  });

  it('clears a stale header credential after cookie login so later requests do not conflict', async () => {
    authSessionStore.set('legacy-stale-token');
    vi.mocked(fetch)
      .mockResolvedValueOnce(apiResponse({ success: true, code: 'OK', message: 'success', data: { authenticated: true } }))
      .mockResolvedValueOnce(apiResponse({ success: true, code: 'OK', message: 'success', data: { configured: true } }));

    await authApi.login({ username: 'operator', password: 'password' });
    expect(authSessionStore.get()).toBeNull();
    await apiClient.get('/api/config');

    const protectedRequest = vi.mocked(fetch).mock.calls[1][1];
    expect(new Headers(protectedRequest?.headers).has('x-dashboard-session')).toBe(false);
  });

  it('clears the legacy credential for an unauthenticated response even without JSON', async () => {
    authSessionStore.set('expired-token');
    vi.mocked(fetch).mockResolvedValueOnce(new Response('', { status: 401, statusText: 'Unauthorized' }));

    await expect(apiClient.get('/api/config')).rejects.toMatchObject({ code: '401' });
    expect(authSessionStore.get()).toBeNull();
  });

  it('settles an applied audit failure once without giving mutation UIs a retry path', async () => {
    const onApplied = vi.fn();
    const refresh = vi.fn().mockResolvedValue(undefined);
    const handled = await handleAppliedAuditFailure(
      new ApiClientError('APPLIED_AUDIT_FAILED', 'Mutation applied.', { mutationApplied: true }),
      { onApplied, refresh }
    );

    expect(handled).toBe(true);
    expect(onApplied).toHaveBeenCalledTimes(1);
    expect(refresh).toHaveBeenCalledTimes(1);
    await expect(handleAppliedAuditFailure(new Error('ordinary failure'), { onApplied, refresh })).resolves.toBe(false);
    expect(onApplied).toHaveBeenCalledTimes(1);
    expect(refresh).toHaveBeenCalledTimes(1);
  });
});
