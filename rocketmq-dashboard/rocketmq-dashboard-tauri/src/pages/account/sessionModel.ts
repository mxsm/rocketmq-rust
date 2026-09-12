import type { SessionView } from '../../services/auth.service';

export function sessionTimestamp(value: unknown): string {
    return typeof value === 'number' && Number.isSafeInteger(value) && !Number.isNaN(new Date(value).getTime()) ? new Date(value).toLocaleString() : 'Not recorded';
}
export function sessionStatus(session: SessionView, now: number): { label: string; tone: 'success' | 'neutral'; note?: string } {
    if (session.revokedAtMs !== null) return { label: sessionTimestamp(session.revokedAtMs) === 'Not recorded' ? 'Unknown' : 'Revoked', tone: 'neutral', note: sessionTimestamp(session.revokedAtMs) };
    if (sessionTimestamp(session.expiresAtMs) === 'Not recorded') return { label: 'Unknown', tone: 'neutral' };
    return session.expiresAtMs <= now ? { label: 'Expired', tone: 'neutral' } : { label: 'Active', tone: 'success' };
}
export function sessionLabel(id: string): string {
    // This UUID is the public record identifier, never the login token or its digest.
    return /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i.test(id) ? id.slice(0, 8) : 'Unrecognized ID';
}
export function sessionCursorHistory(cursors: Array<string | undefined>, next: string | null): Array<string | undefined> {
    return next && !cursors.includes(next) ? [...cursors, next] : cursors;
}
