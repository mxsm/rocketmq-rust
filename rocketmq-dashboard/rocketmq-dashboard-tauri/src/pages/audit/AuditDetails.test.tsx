import { renderToStaticMarkup } from 'react-dom/server';
import { expect, it } from 'vitest';
import { AuditDetails } from './AuditDetails';
import type { AuditEvent } from '../../services/audit.service';

it('renders safe audit metadata without spreading raw details or treating missing counts as zero', () => {
    const event: AuditEvent = {
        eventId: 'event-1', requestId: 'request-1', actor: 'admin', action: 'message.resend_dlq',
        resourceType: 'consumer_group', resourceName: 'orders', environmentId: null, outcome: 'unknown', createdAtMs: 1,
        detail: { resultUnknown: true, rawError: 'SECRET_RAW_ERROR', password: 'SECRET_PASSWORD', body: 'SECRET_BODY', accessToken: 'SECRET_TOKEN' } as AuditEvent['detail'],
    };
    const html = renderToStaticMarkup(<AuditDetails event={event} />);
    expect(html).toContain('request-1');
    expect(html).toContain('Not recorded');
    expect(html).toContain('Unknown');
    expect(html).not.toContain('SECRET_');
    expect(html).not.toContain('Target count');
    expect(html).not.toContain('data-tone="success"');
});
