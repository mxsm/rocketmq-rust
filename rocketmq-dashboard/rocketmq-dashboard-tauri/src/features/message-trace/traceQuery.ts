import { dashboardErrorMessage } from '../../services/invoke';
import type { MessageService } from '../../services/message.service';
import type { MessageTraceService } from '../../services/message-trace.service';
import type { MessageSummary } from '../message/types/message.types';
import { traceCandidates, type TraceQuery } from './traceModel';

type QueryService = Pick<typeof MessageService, 'queryMessageByTopicKey'> & Pick<typeof MessageTraceService, 'queryMessageTraceById'>;
export interface TraceResult { query: TraceQuery; items: MessageSummary[]; }
interface QueryState { result: TraceResult | null; pending: boolean; error: string; receivedAt: number | null; }

/** A mounted connection owns candidate reads; input changes invalidate both dispatched and queued requests. */
export function createTraceQueryController(service: QueryService, contextIsCurrent: () => boolean) {
    const empty = (): QueryState => ({ result: null, pending: false, error: '', receivedAt: null });
    let state = empty();
    let active = false;
    let generation = 0;
    let flight: { key: string; promise: Promise<boolean> } | null = null;
    const listeners = new Set<() => void>();
    const publish = (next: QueryState) => { state = next; listeners.forEach(listener => listener()); };
    const reset = () => { generation++; flight = null; publish(empty()); };
    const read = (query: TraceQuery): Promise<boolean> => {
        if (!active || !contextIsCurrent()) return Promise.resolve(false);
        const key = JSON.stringify(query);
        if (flight?.key === key) return flight.promise;
        const previous = JSON.stringify(state.result?.query) === key ? state.result : null;
        const request = ++generation;
        const isCurrent = () => active && request === generation && contextIsCurrent();
        publish({ result: previous, pending: true, error: '', receivedAt: previous ? state.receivedAt : null });
        const promise = (async () => {
            await Promise.resolve();
            if (!isCurrent()) return false;
            try {
                const response = query.mode === 'key'
                    ? await service.queryMessageByTopicKey({ topic: query.topic, key: query.key })
                    : await service.queryMessageTraceById({ traceTopic: query.traceTopic, messageId: query.messageId });
                if (!isCurrent()) return false;
                const items = traceCandidates(query, response.items);
                publish({ result: { query, items }, pending: false, error: '', receivedAt: Date.now() });
                return true;
            } catch (error) {
                if (isCurrent()) publish({ ...state, pending: false, error: dashboardErrorMessage(error, 'Trace query failed.') });
                return false;
            } finally {
                if (request === generation) flight = null;
            }
        })();
        flight = { key, promise };
        return promise;
    };
    return {
        read, reset,
        start: () => { active = true; },
        stop: () => { active = false; reset(); },
        getSnapshot: () => state,
        subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    };
}
