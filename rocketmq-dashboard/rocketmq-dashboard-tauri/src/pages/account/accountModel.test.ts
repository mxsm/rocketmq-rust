import { describe, expect, it } from 'vitest';
import type { UserProfile } from '../../features/auth/types/auth.types';
import { accountStatus, accountTimestamp } from './accountModel';

const profile: UserProfile = { userId: 1, username: 'admin', isActive: true, mustChangePassword: false, createdAt: '2026-09-11T00:00:00Z', updatedAt: '2026-09-11T00:00:00Z', lastLoginAt: null };
describe('account observations', () => {
    it('does not infer an active account from a missing or failed profile read', () => {
        expect(accountStatus(null, false).tone).toBe('neutral');
        expect(accountStatus(null, true).label).toBe('Status not available');
        expect(accountStatus(profile, true)).toEqual({ label: 'Previous observation', tone: 'neutral' });
        expect(accountStatus(profile, false).label).toBe('Active');
        expect(accountStatus({ ...profile, isActive: false }, false).label).toBe('Disabled');
    });
    it('keeps missing or malformed dates distinct from a recorded timestamp', () => {
        expect(accountTimestamp(null)).toBe('Not recorded');
        expect(accountTimestamp('not a timestamp')).toBe('Not recorded');
        expect(accountTimestamp('2026-09-11T00:00:00Z')).not.toBe('Not recorded');
    });
});
