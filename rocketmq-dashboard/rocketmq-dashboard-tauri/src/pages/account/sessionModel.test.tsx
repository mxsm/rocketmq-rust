import { renderToStaticMarkup } from 'react-dom/server';
import { describe, expect, it, vi } from 'vitest';
import type { SessionView } from '../../services/auth.service';
import { SessionTable } from './SessionTable';
import { sessionCursorHistory, sessionLabel, sessionStatus, sessionTimestamp } from './sessionModel';

const session: SessionView = { id: 'a81f32c0-1234-4234-a234-123456789012', username: 'admin', createdAtMs: 1, expiresAtMs: 200, lastSeenAtMs: 100, revokedAtMs: null, current: true };
describe('safe account session display', () => {
    it('uses absolute expiry, with revocation taking priority over current and active states', () => {
        expect(sessionStatus(session, 199).label).toBe('Active');
        expect(sessionStatus(session, 200).label).toBe('Expired');
        expect(sessionStatus({ ...session, revokedAtMs: 150 }, 175).label).toBe('Revoked');
        expect(sessionStatus({ ...session, expiresAtMs: NaN }, 1).label).toBe('Unknown');
        expect(sessionTimestamp(undefined)).toBe('Not recorded');
        expect(sessionTimestamp(0)).not.toBe('Not recorded');
    });
    it('shows only the safe record label and never renders token/digest extras', () => {
        vi.spyOn(Date, 'now').mockReturnValue(150);
        try {
            const row = { ...session, token: 'SECRET_TOKEN', tokenDigest: 'SECRET_DIGEST', device: 'FAKE_DEVICE', location: 'FAKE_LOCATION' };
            const html = renderToStaticMarkup(<SessionTable username="admin" items={[row]} />);
            expect(html).toContain('a81f32c0');
            expect(html).toContain('Current');
            expect(html).toContain('Active');
            expect(html).not.toMatch(/SECRET_|FAKE_/);
            expect(html).not.toContain(session.id);
            expect(sessionLabel('not-a-public-uuid-token')).toBe('Unrecognized ID');
        } finally { vi.restoreAllMocks(); }
    });
    it('keeps opaque cursor history and ignores missing or repeated cursors', () => {
        const first = [undefined];
        const second = sessionCursorHistory(first, 'opaque-a');
        expect(second).toEqual([undefined, 'opaque-a']);
        expect(first).toEqual([undefined]);
        expect(sessionCursorHistory(second, 'opaque-a')).toBe(second);
        expect(sessionCursorHistory(second, null)).toBe(second);
    });
});
