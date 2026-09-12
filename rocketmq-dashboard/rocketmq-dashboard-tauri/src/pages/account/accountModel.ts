import type { UserProfile } from '../../features/auth/types/auth.types';
export function accountStatus(profile: UserProfile | null, stale: boolean): { label: string; tone: 'neutral' | 'success' | 'warning' } {
    if (!profile) return { label: 'Status not available', tone: 'neutral' };
    if (stale) return { label: 'Previous observation', tone: 'neutral' };
    return profile.isActive ? { label: 'Active', tone: 'success' } : { label: 'Disabled', tone: 'warning' };
}
export function accountTimestamp(value: string | null): string {
    if (!value) return 'Not recorded';
    const time = new Date(value);
    return Number.isNaN(time.getTime()) ? 'Not recorded' : time.toLocaleString();
}
