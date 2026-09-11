import { dashboardErrorMessage } from '../../services/invoke';
import { ProducerRequestGuard } from './requestGuard';
import type { ProducerConnectionQueryRequest, ProducerConnectionView } from './types/producer.types';

interface LookupState {
    result: ProducerConnectionView | null;
    pending: boolean;
    hasSearched: boolean;
    error: string;
    receivedAt: number | null;
}

/** A lookup belongs to one page and input pair; editing either field invalidates its callbacks. */
export function createProducerLookup(query: (request: ProducerConnectionQueryRequest) => Promise<ProducerConnectionView>, contextIsCurrent: () => boolean) {
    const empty = (): LookupState => ({ result: null, pending: false, hasSearched: false, error: '', receivedAt: null });
    let state = empty();
    let active = false;
    let flight: Promise<boolean> | null = null;
    const guard = new ProducerRequestGuard();
    const listeners = new Set<() => void>();
    const publish = (next: LookupState) => { state = next; listeners.forEach(listener => listener()); };
    const reset = () => { guard.invalidate(); flight = null; publish(empty()); };
    const search = (input: ProducerConnectionQueryRequest): Promise<boolean> => {
        if (!active || !contextIsCurrent()) return Promise.resolve(false);
        if (flight) return flight;
        const request = { topic: input.topic.trim(), producerGroup: input.producerGroup.trim() };
        if (!request.topic || !request.producerGroup) {
            publish({ ...state, error: 'Enter both a Topic and a Producer group.' });
            return Promise.resolve(false);
        }
        const ownsRequest = guard.begin();
        const isCurrent = () => active && ownsRequest() && contextIsCurrent();
        publish({ ...state, pending: true, hasSearched: true, error: '' });
        flight = (async () => {
            await Promise.resolve();
            if (!isCurrent()) return false;
            try {
                const result = await query(request);
                if (!isCurrent()) return false;
                if (result.topic !== request.topic || result.producerGroup !== request.producerGroup) throw new Error('The returned Producer scope does not match the query.');
                publish({ result, pending: false, hasSearched: true, error: '', receivedAt: Date.now() });
                return true;
            } catch (error) {
                if (isCurrent()) publish({ ...state, pending: false, error: dashboardErrorMessage(error, 'Producer connection lookup failed.') });
                return false;
            } finally {
                if (ownsRequest()) flight = null;
            }
        })();
        return flight;
    };
    return {
        search, reset,
        start: () => { active = true; },
        stop: () => { active = false; reset(); },
        getSnapshot: () => state,
        subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
    };
}
