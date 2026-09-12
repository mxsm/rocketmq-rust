import { describe, expect, it } from 'vitest';
import { auditDetail, auditOutcome, auditQuery, emptyAuditFilters, initialAuditLocation, navigateAudit } from './auditModel';

describe('audit filters and page identity', () => {
    it('preserves exact actor/action/environment strings and rejects unsupported outcomes', () => {
        const query = auditQuery({ ...emptyAuditFilters(), actor: ' admin ', action: 'topic.delete', environmentId: 'env-a', outcome: 'partial' });
        expect(query).toMatchObject({ actor: ' admin ', action: 'topic.delete', environmentId: 'env-a', outcome: 'partial', limit: 50 });
        expect(query.cursor).toBeUndefined();
        expect(() => auditQuery({ ...emptyAuditFilters(), outcome: 'completed' })).toThrow();
    });
    it('rejects impossible local dates, malformed dates and reversed ranges while allowing inclusive equal endpoints', () => {
        for (const from of ['2026-02-30T12:00', '2026-13-01T12:00', '2026-09-11T25:00', 'not-a-date', '2026-09-11']) {
            expect(() => auditQuery({ ...emptyAuditFilters(), from })).toThrow();
        }
        expect(() => auditQuery({ ...emptyAuditFilters(), from: '2026-09-12T12:00', to: '2026-09-11T12:00' })).toThrow();
        const query = auditQuery({ ...emptyAuditFilters(), from: '2024-02-29T12:00:20', to: '2024-02-29T12:00:20' });
        expect(query.fromMs).toBe(query.toMs);
        expect(query.fromMs).toBe(new Date('2024-02-29T12:00:20').getTime());
    });
    it('resets opaque cursor history when filters change and refreshes from the newest page', () => {
        let page = navigateAudit(initialAuditLocation(), { kind: 'next', cursor: 'opaque-a' });
        page = navigateAudit(page, { kind: 'next', cursor: 'opaque-b' });
        expect(navigateAudit(page, { kind: 'previous' }).cursors).toEqual([undefined, 'opaque-a']);
        expect(navigateAudit(page, { kind: 'next', cursor: 'opaque-a' })).toBe(page);
        expect(navigateAudit(page, { kind: 'next', cursor: null })).toBe(page);
        const changed = navigateAudit(page, { kind: 'apply', query: { actor: 'other', cursor: 'old-cursor', limit: 50 } });
        expect(changed.cursors).toEqual([undefined]);
        expect(changed.query).toEqual({ actor: 'other', limit: 50 });
        expect(navigateAudit(page, { kind: 'refresh' })).toMatchObject({ cursors: [undefined], generation: 1 });
    });
});
describe('safe audit details', () => {
    it('retains explicit zero, distinguishes missing/invalid counts and omits non-whitelisted fields', () => {
        const detail = auditDetail({ resultUnknown: false, successCount: 0, password: 'secret', error: 'raw exception', body: 'message text', token: 'token' } as Parameters<typeof auditDetail>[0]);
        expect(detail).toEqual({ resultUnknown: false, successCount: '0', failureCount: 'Not recorded', errorCode: 'Not recorded' });
        expect(JSON.stringify(detail)).not.toMatch(/secret|exception|message text|token/);
        expect(auditDetail({ resultUnknown: true, successCount: -1, failureCount: Number.MAX_SAFE_INTEGER + 1 }).successCount).toBe('Unknown');
        expect(auditDetail({ resultUnknown: false, errorCode: 'password: secret' }).errorCode).toBe('Not recorded');
        expect(auditDetail({ resultUnknown: false, errorCode: 'dashboard.monitor_conflict' }).errorCode).toBe('dashboard.monitor_conflict');
    });
    it('never labels unrecognized or explicitly unknown results as success', () => {
        expect(auditOutcome('success')).toEqual({ label: 'Success', tone: 'success' });
        for (const value of ['unknown', 'unexpected', '', 'SUCCESS']) expect(auditOutcome(value).tone).not.toBe('success');
        expect(auditOutcome('success', true).label).toBe('Unknown');
        expect(['rejected', 'failed', 'partial'].map(value => auditOutcome(value).label)).toEqual(['Rejected', 'Failed', 'Partial']);
    });
});
