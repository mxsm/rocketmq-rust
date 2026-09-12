import { invokeAuthenticatedCommand } from './invoke';
export type AuditOutcome = 'success' | 'rejected' | 'failed' | 'partial' | 'unknown';
export interface AuditQuery {
    fromMs?: number; toMs?: number; actor?: string; action?: string;
    outcome?: AuditOutcome; environmentId?: string; cursor?: string; limit?: number;
}
export interface AuditEvent {
    eventId: string; requestId: string; actor: string | null; action: string;
    resourceType: string; resourceName: string | null; environmentId: string | null;
    outcome: string; createdAtMs: number;
    detail: { resultUnknown: boolean; errorCode?: string; successCount?: number; failureCount?: number };
}
export interface AuditPage { items: AuditEvent[]; nextCursor: string | null }
export const queryAuditEvents = (query: AuditQuery) => invokeAuthenticatedCommand<AuditPage>('query_audit_events', { query });
