import { dashboardErrorMessage } from '../../services/invoke';

export interface ReadState<T> {
    data: T | null;
    error: string;
    pending: boolean;
    receivedAt: number | null;
}

/** One dashboard region owns its request and last successful observation. */
export function createDashboardRead<T>(loader: (() => Promise<T>) | null, fallback: string) {
    const initial = (): ReadState<T> => ({ data: null, error: '', pending: loader !== null, receivedAt: null });
    let snapshot = initial();
    let generation = 0;
    let flight: Promise<boolean> | null = null;
    const listeners = new Set<() => void>();
    const publish = (next: ReadState<T>) => {
        snapshot = next;
        listeners.forEach(listener => listener());
    };
    const read = (request: (previous: T | null) => Promise<T> = () => loader!()): Promise<boolean> => {
        if (!loader) return Promise.resolve(true);
        if (flight) return flight;
        const current = generation;
        const previous = snapshot.data;
        publish({ ...snapshot, error: '', pending: true });
        flight = (async () => {
            // Cleanup can happen before dispatch (including a StrictMode effect replay).
            await Promise.resolve();
            if (generation !== current) return false;
            try {
                const data = await request(previous);
                if (generation !== current) return false;
                publish({ data, error: '', pending: false, receivedAt: Date.now() });
                return true;
            } catch (error) {
                if (generation !== current) return false;
                publish({ ...snapshot, error: dashboardErrorMessage(error, fallback), pending: false });
                return false;
            } finally {
                if (generation === current) flight = null;
            }
        })();
        return flight;
    };
    return {
        read,
        getSnapshot: () => snapshot,
        subscribe: (listener: () => void) => {
            listeners.add(listener);
            return () => { listeners.delete(listener); };
        },
        invalidate: () => {
            generation += 1;
            flight = null;
            publish(initial());
        },
    };
}
