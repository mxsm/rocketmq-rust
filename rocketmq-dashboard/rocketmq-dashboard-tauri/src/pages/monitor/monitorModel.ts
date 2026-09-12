import type { MonitorRule, SaveMonitorRule } from '../../services/monitor.service';

export interface MonitorContext { environmentId: string; revision: number }
export interface MonitorDraft {
    id: string;
    consumerGroup: string;
    minCount: string;
    maxDiffTotal: string;
    expectedRevision: number;
    lastAttemptId: string | null;
    needsReview: boolean;
}
export type MonitorTarget = {
    id: string;
    draftId: string;
    context: MonitorContext;
} & ({ kind: 'save'; request: SaveMonitorRule } | {
    kind: 'delete'; request: Pick<SaveMonitorRule, 'consumerGroup' | 'expectedRevision'>;
});
export interface MonitorReceipt {
    target: MonitorTarget;
    outcome: 'success' | 'conflict' | 'unconfirmed';
    completedAt: number;
    message: string;
}
export function monitorContextMatches(expected: MonitorContext, current: { environmentId: string | null; revision: number } | null) {
    return current?.environmentId === expected.environmentId && current.revision === expected.revision;
}
export function monitorDraft(rule: MonitorRule | null): MonitorDraft {
    return {
        id: crypto.randomUUID(), consumerGroup: rule?.consumerGroup ?? '',
        minCount: String(rule?.minCount ?? 0), maxDiffTotal: String(rule?.maxDiffTotal ?? 0),
        expectedRevision: rule?.revision ?? 0, lastAttemptId: null, needsReview: false,
    };
}
function identityError(group: string, revision: number): string | null {
    if (!group || new TextEncoder().encode(group).length > 255 || /[\p{White_Space}\p{Cc}\p{Cs}]/u.test(group)) {
        return 'Use a group name of 1–255 UTF-8 bytes without whitespace or control characters.';
    }
    if (!Number.isSafeInteger(revision) || revision < 0 || revision >= Number.MAX_SAFE_INTEGER) {
        return 'This rule revision cannot be changed safely. Reload the current rule.';
    }
    return null;
}
function threshold(value: string): number | null {
    if (!/^\d+$/.test(value)) return null;
    const number = Number(value);
    return Number.isSafeInteger(number) && number >= 0 ? number : null;
}
export function monitorDraftError(draft: MonitorDraft): string | null {
    return identityError(draft.consumerGroup, draft.expectedRevision) ??
        (threshold(draft.minCount) === null || threshold(draft.maxDiffTotal) === null
            ? 'Thresholds must be whole numbers from 0 to 9007199254740991.' : null);
}
export function monitorSaveRequest(draft: MonitorDraft): SaveMonitorRule {
    const error = monitorDraftError(draft);
    if (error) throw new Error(error);
    if (draft.needsReview) throw new Error('Read and review the current rule before saving this draft.');
    return { consumerGroup: draft.consumerGroup, minCount: Number(draft.minCount), maxDiffTotal: Number(draft.maxDiffTotal), expectedRevision: draft.expectedRevision };
}
export function captureMonitorTarget(target: MonitorTarget, current: { environmentId: string | null; revision: number } | null): MonitorTarget {
    if (!target.context.environmentId || !monitorContextMatches(target.context, current)) throw new Error('The environment changed. Reopen the rule in the current environment.');
    const error = identityError(target.request.consumerGroup, target.request.expectedRevision);
    if (error) throw new Error(error);
    if (target.kind === 'delete' && target.request.expectedRevision === 0) throw new Error('Only a stored rule can be deleted.');
    if (target.kind === 'save' && (!Number.isSafeInteger(target.request.minCount) || target.request.minCount < 0 ||
        !Number.isSafeInteger(target.request.maxDiffTotal) || target.request.maxDiffTotal < 0)) throw new Error('Invalid monitor thresholds.');
    return { ...target, context: { ...target.context }, request: { ...target.request } } as MonitorTarget;
}
/** A write receipt may affect only the draft that submitted it, in the same environment and connection. */
export function applyMonitorReceipt(draft: MonitorDraft | null, receipt: MonitorReceipt, context: MonitorContext): MonitorDraft | null {
    if (!draft || !monitorContextMatches(context, receipt.target.context) || draft.id !== receipt.target.draftId || draft.lastAttemptId !== receipt.target.id) return draft;
    return receipt.outcome === 'success' ? null : { ...draft, lastAttemptId: null, needsReview: true };
}
/** Keep user-entered thresholds; adopting a new compare-and-swap version is an explicit action. */
export function reviewMonitorDraft(draft: MonitorDraft, current: MonitorRule | null): MonitorDraft {
    if (current && current.consumerGroup !== draft.consumerGroup) throw new Error('The current rule belongs to a different group.');
    return { ...draft, expectedRevision: current?.revision ?? 0, lastAttemptId: null, needsReview: false };
}
