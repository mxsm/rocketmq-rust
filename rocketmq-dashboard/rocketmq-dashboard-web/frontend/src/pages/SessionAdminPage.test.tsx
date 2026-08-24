import { screen, waitFor, within } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { auditApi } from '../api/audit_api';
import { ApiClientError } from '../api/client';
import { renderAtRoute } from '../test/render';
import type { SessionListPage } from '../types/audit';
import SessionAdminPage from './SessionAdminPage';

vi.mock('../api/audit_api', () => ({
  auditApi: { listSessions: vi.fn(), revokeAllSessions: vi.fn() }
}));

const now = Date.now();
const firstPage: SessionListPage = {
  items: [
    { sessionId: 'active', username: 'operator', createdAtMs: now - 3_000, expiresAtMs: now + 60_000, lastSeenAtMs: now, current: true },
    { sessionId: 'revoked', username: 'operator', createdAtMs: now - 4_000, expiresAtMs: now + 60_000, lastSeenAtMs: now, revokedAtMs: now - 1_000, current: false },
    { sessionId: 'expired', username: 'operator', createdAtMs: now - 5_000, expiresAtMs: now - 1_000, lastSeenAtMs: now - 2_000, current: false }
  ],
  nextCursor: 'next-page'
};

describe('SessionAdminPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(auditApi.listSessions).mockResolvedValue(firstPage);
    vi.mocked(auditApi.revokeAllSessions).mockResolvedValue({ revoked: 2 });
  });

  it('applies an exact username filter, renders terminal state, and follows keyset pagination', async () => {
    const user = userEvent.setup();
    renderAtRoute(<SessionAdminPage />, '/sessions');
    await screen.findByText('Active');
    expect(screen.getByText(/^Revoked /)).toBeInTheDocument();
    expect(screen.getByText('Expired')).toBeInTheDocument();

    await user.type(screen.getByLabelText('Exact username'), 'operator');
    await user.click(screen.getByRole('button', { name: 'Filter' }));
    await waitFor(() => expect(auditApi.listSessions).toHaveBeenLastCalledWith({ username: 'operator', cursor: undefined, limit: 50 }));

    await user.click(screen.getByRole('button', { name: 'Next' }));
    await waitFor(() => expect(auditApi.listSessions).toHaveBeenLastCalledWith({ username: 'operator', cursor: 'next-page', limit: 50 }));
  });

  it('requires confirmation before revoke-all and refreshes without a second mutation', async () => {
    const user = userEvent.setup();
    renderAtRoute(<SessionAdminPage />, '/sessions');
    await screen.findByText('Active');
    await user.type(screen.getByLabelText('Exact username'), 'operator');
    await user.click(screen.getByRole('button', { name: 'Revoke all' }));
    const dialog = screen.getByRole('alertdialog', { name: 'Revoke all sessions?' });
    expect(dialog).toHaveTextContent('operator');
    await user.click(screen.getByRole('button', { name: 'Revoke all' }));
    await waitFor(() => expect(auditApi.revokeAllSessions).toHaveBeenCalledWith('operator'));
    expect(auditApi.revokeAllSessions).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(auditApi.listSessions).toHaveBeenCalledTimes(2));
  });

  it('reports storage unavailability and retries only the session list', async () => {
    const user = userEvent.setup();
    vi.mocked(auditApi.listSessions)
      .mockRejectedValueOnce(new ApiClientError('STORAGE_UNAVAILABLE', 'Storage unavailable'))
      .mockResolvedValueOnce({ items: [] });
    renderAtRoute(<SessionAdminPage />, '/sessions');
    expect(await screen.findByRole('alert')).toHaveTextContent('Storage unavailable Retry when storage is available.');
    await user.click(screen.getByRole('button', { name: 'Retry sessions' }));
    await screen.findByText('No sessions match this filter');
    expect(auditApi.revokeAllSessions).not.toHaveBeenCalled();
  });

  it('does not offer a second revoke after the backend applied it without an audit receipt', async () => {
    const user = userEvent.setup();
    vi.mocked(auditApi.revokeAllSessions).mockRejectedValueOnce(
      new ApiClientError('APPLIED_AUDIT_FAILED', 'Sessions were revoked, but audit persistence failed.')
    );
    renderAtRoute(<SessionAdminPage />, '/sessions');
    await screen.findByText('Active');
    await user.type(screen.getByLabelText('Exact username'), 'operator');
    await user.click(screen.getByRole('button', { name: 'Revoke all' }));
    await user.click(within(screen.getByRole('alertdialog', { name: 'Revoke all sessions?' })).getByRole('button', { name: 'Revoke all' }));
    expect(await screen.findByRole('alert')).toHaveTextContent('Sessions were revoked, but audit persistence failed.');
    expect(screen.queryByRole('button', { name: 'Retry revoke' })).not.toBeInTheDocument();
    expect(auditApi.revokeAllSessions).toHaveBeenCalledTimes(1);
    await waitFor(() => expect(auditApi.listSessions).toHaveBeenCalledTimes(2));
  });
});
