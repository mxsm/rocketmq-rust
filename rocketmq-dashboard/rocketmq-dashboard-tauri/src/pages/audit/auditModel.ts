import type { AuditEvent, AuditOutcome, AuditQuery } from '../../services/audit.service';

export interface AuditFilters { from: string; to: string; actor: string; action: string; outcome: string; environmentId: string }
export const emptyAuditFilters = (): AuditFilters => ({ from: '', to: '', actor: '', action: '', outcome: '', environmentId: '' });
export const auditOutcomes: AuditOutcome[] = ['success', 'rejected', 'failed', 'partial', 'unknown'];
function localTime(value: string): number | undefined {
    if (!value) return undefined;
    const match = /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2})(?::(\d{2}))?$/.exec(value);
    if (!match) throw new Error('Choose a valid local date and time.');
    const [, year, month, day, hour, minute, second = '00'] = match;
    const date = new Date(value);
    if (!Number.isSafeInteger(date.getTime()) || date.getFullYear() !== Number(year) || date.getMonth() + 1 !== Number(month) ||
        date.getDate() !== Number(day) || date.getHours() !== Number(hour) || date.getMinutes() !== Number(minute) || date.getSeconds() !== Number(second)) {
        throw new Error('Choose a valid local date and time.');
    }
    return date.getTime();
}
export function auditQuery(filters: AuditFilters): AuditQuery {
    const fromMs = localTime(filters.from), toMs = localTime(filters.to);
    if (fromMs !== undefined && toMs !== undefined && fromMs > toMs) throw new Error('The start time must be before or equal to the end time.');
    if (filters.outcome && !auditOutcomes.includes(filters.outcome as AuditOutcome)) throw new Error('Choose one of the supported outcomes.');
    return { fromMs, toMs, actor: filters.actor || undefined, action: filters.action || undefined,
        outcome: filters.outcome as AuditOutcome || undefined, environmentId: filters.environmentId || undefined, limit: 50 };
}
export interface AuditLocation { query: AuditQuery; cursors: Array<string | undefined>; generation: number }
export const initialAuditLocation = (): AuditLocation => ({ query: { limit: 50 }, cursors: [undefined], generation: 0 });
export type AuditNavigation = { kind: 'apply'; query: AuditQuery } | { kind: 'refresh' } | { kind: 'previous' } | { kind: 'next'; cursor: string | null };
export function navigateAudit(location: AuditLocation, action: AuditNavigation): AuditLocation {
    switch (action.kind) {
        case 'apply': {
            // A cursor belongs only to its applied filters, never the editable draft.
            const { cursor: _cursor, ...query } = action.query;
            return { query, cursors: [undefined], generation: location.generation + 1 };
        }
        case 'refresh': return { ...location, cursors: [undefined], generation: location.generation + 1 };
        case 'previous': return location.cursors.length > 1 ? { ...location, cursors: location.cursors.slice(0, -1) } : location;
        case 'next': return action.cursor && !location.cursors.includes(action.cursor) ? { ...location, cursors: [...location.cursors, action.cursor] } : location;
    }
}
export function auditOutcome(value: string, resultUnknown = false): { label: string; tone: 'neutral' | 'success' | 'warning' | 'danger' } {
    if (resultUnknown) return { label: 'Unknown', tone: 'neutral' };
    switch (value) {
        case 'success': return { label: 'Success', tone: 'success' };
        case 'rejected': return { label: 'Rejected', tone: 'warning' };
        case 'failed': return { label: 'Failed', tone: 'danger' };
        case 'partial': return { label: 'Partial', tone: 'warning' };
        default: return { label: 'Unknown', tone: 'neutral' };
    }
}
export function auditTimestamp(value: number): string {
    return Number.isSafeInteger(value) && !Number.isNaN(new Date(value).getTime()) ? new Date(value).toLocaleString() : 'Not recorded';
}
const count = (value: unknown): string => value === undefined || value === null ? 'Not recorded' :
    typeof value === 'number' && Number.isSafeInteger(value) && value >= 0 ? String(value) : 'Unknown';
/** Deliberately project only the safe detail fields; raw request/exception payloads have no display path. */
export function auditDetail(detail: AuditEvent['detail']) {
    return {
        successCount: count(detail?.successCount), failureCount: count(detail?.failureCount),
        resultUnknown: detail?.resultUnknown === true,
        errorCode: typeof detail?.errorCode === 'string' && /^[a-z0-9][a-z0-9._-]{0,127}$/i.test(detail.errorCode) ? detail.errorCode : 'Not recorded',
    };
}
