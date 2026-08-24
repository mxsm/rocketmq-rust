import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { beforeEach, describe, expect, it, vi } from 'vitest';
import { auditApi } from '../api/audit_api';
import { ApiClientError } from '../api/client';
import { renderAtRoute } from '../test/render';
import type { AuditEventPage } from '../types/audit';
import AuditPage from './AuditPage';

vi.mock('../api/audit_api', () => ({ auditApi: { listEvents: vi.fn() } }));

const page: AuditEventPage = {
  events: [{
    eventId: 'event-1', requestId: 'request-1', actor: 'operator', actorKind: 'admin',
    action: 'config.tls.set', resourceType: 'environment', resourceName: 'default',
    outcome: 'succeeded', detail: { operation: 'config.tls.set' }, createdAtMs: Date.now()
  }],
  nextCursor: 'next-page'
};

describe('AuditPage', () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(auditApi.listEvents).mockResolvedValue(page);
  });

  it('sends filters and navigates keyset pages without exposing unfiltered request state', async () => {
    const user = userEvent.setup();
    renderAtRoute(<AuditPage />, '/audit');
    await screen.findByText('config.tls.set');
    await user.type(screen.getByLabelText('Actor'), 'operator');
    await user.type(screen.getByLabelText('Action code'), 'config.tls.set');
    await user.selectOptions(screen.getByLabelText('Outcome'), 'succeeded');
    await user.click(screen.getByRole('button', { name: 'Filter' }));
    await waitFor(() => expect(auditApi.listEvents).toHaveBeenLastCalledWith(expect.objectContaining({
      actor: 'operator', action: 'config.tls.set', outcome: 'succeeded', cursor: undefined, limit: 50
    })));
    await user.click(screen.getByRole('button', { name: 'Next' }));
    await waitFor(() => expect(auditApi.listEvents).toHaveBeenLastCalledWith(expect.objectContaining({ cursor: 'next-page' })));
  });

  it('labels storage outages and retries the query without any mutation retry path', async () => {
    const user = userEvent.setup();
    vi.mocked(auditApi.listEvents)
      .mockRejectedValueOnce(new ApiClientError('STORAGE_UNAVAILABLE', 'Storage unavailable'))
      .mockResolvedValueOnce({ events: [] });
    renderAtRoute(<AuditPage />, '/audit');
    expect(await screen.findByRole('alert')).toHaveTextContent('Storage unavailable Retry when storage is available.');
    await user.click(screen.getByRole('button', { name: 'Retry audit' }));
    await screen.findByText('No audit events match these filters');
    expect(screen.queryByRole('button', { name: /retry.*mutation/i })).not.toBeInTheDocument();
  });
});
