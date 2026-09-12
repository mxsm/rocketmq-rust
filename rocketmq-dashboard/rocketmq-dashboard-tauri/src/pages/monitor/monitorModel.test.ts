import { describe, expect, it } from 'vitest';
import type { MonitorRule } from '../../services/monitor.service';
import { applyMonitorReceipt, captureMonitorTarget, monitorDraft, monitorDraftError, monitorSaveRequest, reviewMonitorDraft, type MonitorReceipt, type MonitorTarget } from './monitorModel';

const context = { environmentId: 'env-a', revision: 7 };
const rule: MonitorRule = { consumerGroup: 'orders', minCount: 2, maxDiffTotal: 500, revision: 3, createdAtMs: 1, updatedAtMs: 2 };
describe('monitor rule drafts', () => {
    it('preserves empty and invalid numeric input instead of coercing it to zero', () => {
        const draft = monitorDraft(rule);
        for (const value of ['', '-1', '1.5', '1e3', 'Infinity', 'NaN', '9007199254740992']) {
            expect(monitorDraftError({ ...draft, minCount: value })).not.toBeNull();
            expect(() => monitorSaveRequest({ ...draft, maxDiffTotal: value })).toThrow();
        }
        expect(monitorSaveRequest({ ...draft, minCount: '0', maxDiffTotal: '9007199254740991' })).toMatchObject({ minCount: 0, maxDiffTotal: Number.MAX_SAFE_INTEGER });
    });
    it('matches the backend UTF-8 identity and revision boundaries', () => {
        const draft = monitorDraft(rule);
        expect(monitorDraftError({ ...draft, consumerGroup: '中'.repeat(85) })).toBeNull();
        for (const group of ['', 'a'.repeat(256), '中'.repeat(86), 'has space', 'line\nfeed', 'a\u0085b', 'a\u0000b', '\ud800']) {
            expect(monitorDraftError({ ...draft, consumerGroup: group })).not.toBeNull();
        }
        expect(monitorDraftError({ ...draft, consumerGroup: 'orders🚀' })).toBeNull();
        for (const revision of [-1, 0.5, Number.MAX_SAFE_INTEGER, NaN]) expect(monitorDraftError({ ...draft, expectedRevision: revision })).not.toBeNull();
    });
    it('retains the entire conflicted draft until an explicit revision review', () => {
        const draft = { ...monitorDraft(rule), minCount: '17', maxDiffTotal: '', lastAttemptId: 'attempt' };
        const target: MonitorTarget = { id: 'attempt', draftId: draft.id, context, kind: 'save', request: { consumerGroup: rule.consumerGroup, minCount: 17, maxDiffTotal: 500, expectedRevision: 3 } };
        const receipt: MonitorReceipt = { target, outcome: 'conflict', completedAt: 20, message: 'conflict' };
        const conflicted = applyMonitorReceipt(draft, receipt, context)!;
        expect(conflicted).toMatchObject({ minCount: '17', maxDiffTotal: '', expectedRevision: 3, needsReview: true, lastAttemptId: null });
        const reviewed = reviewMonitorDraft(conflicted, { ...rule, revision: 4, minCount: 9 });
        expect(reviewed).toMatchObject({ minCount: '17', maxDiffTotal: '', expectedRevision: 4, needsReview: false });
        expect(reviewMonitorDraft(conflicted, null)).toMatchObject({ consumerGroup: 'orders', minCount: '17', expectedRevision: 0 });
        expect(() => reviewMonitorDraft(conflicted, { ...rule, consumerGroup: 'another' })).toThrow();
    });
    it('does not clear another environment, connection revision or replacement draft', () => {
        const draft = { ...monitorDraft(rule), lastAttemptId: 'attempt' };
        const receipt: MonitorReceipt = { target: { id: 'attempt', draftId: draft.id, context, kind: 'delete', request: { consumerGroup: 'orders', expectedRevision: 3 } }, outcome: 'success', completedAt: 20, message: 'ok' };
        expect(applyMonitorReceipt(draft, receipt, context)).toBeNull();
        for (const other of [{ ...context, environmentId: 'env-b' }, { ...context, revision: 8 }]) expect(applyMonitorReceipt(draft, receipt, other)).toBe(draft);
        const replacement = { ...draft, id: 'replacement' };
        expect(applyMonitorReceipt(replacement, receipt, context)).toBe(replacement);
    });
    it('captures the exact target and rejects stale contexts or invalid deletions', () => {
        const target: MonitorTarget = { id: 'attempt', draftId: 'draft', context: { ...context }, kind: 'delete', request: { consumerGroup: 'orders', expectedRevision: 3 } };
        const captured = captureMonitorTarget(target, context);
        target.request.consumerGroup = 'replacement';
        target.context.environmentId = 'env-b';
        expect(captured.request.consumerGroup).toBe('orders');
        expect(captured.context.environmentId).toBe('env-a');
        expect(() => captureMonitorTarget(captured, { ...context, revision: 8 })).toThrow();
        expect(() => captureMonitorTarget({ ...captured, kind: 'delete', request: { ...captured.request, expectedRevision: 0 } }, context)).toThrow();
        expect(() => monitorSaveRequest({ ...monitorDraft(rule), needsReview: true })).toThrow();
    });
});
