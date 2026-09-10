import { invokeAuthenticatedCommand } from './invoke';

export interface HistorySample { metric: 'broker-count' | 'topic-count' | 'topic-total-messages'; dimension: string; timestampMs: number; value: number }
export interface HistoryQuery { beginMs: number; endMs: number; topicName?: string; limit?: number; beforeMs?: number }
export interface HistoryPage { samples: HistorySample[]; nextBeforeMs: number | null }
export interface CollectorStatus { intervalSeconds: number; retentionDays: number; lastSampleMs: number | null; lastWriteMs: number | null; lastError: string | null }

export const HistoryService = {
    query(kind: 'broker' | 'topic', request: HistoryQuery): Promise<HistoryPage> { return invokeAuthenticatedCommand(kind === 'broker' ? 'query_broker_history' : 'query_topic_history', { request }); },
    status(): Promise<CollectorStatus> { return invokeAuthenticatedCommand('get_history_status'); },
};
