export type AuditOutcome = 'succeeded' | 'rejected' | 'failed';

export interface SessionListItem {
  sessionId: string;
  username: string;
  createdAtMs: number;
  expiresAtMs: number;
  lastSeenAtMs: number;
  revokedAtMs?: number | null;
  current: boolean;
}

export interface SessionListPage {
  items: SessionListItem[];
  nextCursor?: string | null;
}

export interface AuditEvent {
  eventId: string;
  requestId: string;
  actor: string;
  actorKind: 'admin' | 'local_operator' | 'system';
  action: string;
  resourceType: string;
  resourceName?: string | null;
  environmentId?: string | null;
  outcome: AuditOutcome;
  detail?: unknown;
  createdAtMs: number;
}

export interface AuditEventPage {
  events: AuditEvent[];
  nextCursor?: string | null;
}

export interface AuditEventQuery {
  startMs?: number;
  endMs?: number;
  actor?: string;
  action?: string;
  outcome?: AuditOutcome;
  environmentId?: string;
  cursor?: string;
  limit?: number;
}
