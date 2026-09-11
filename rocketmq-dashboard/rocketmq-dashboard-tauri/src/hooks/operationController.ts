import { dashboardErrorMessage } from '../services/invoke';

interface OperationState { operation: 'read' | 'write' | null; contextChanged: boolean; error: string; }

/** A dialog owns its writes until acknowledgement; only current-context reads may update a draft. */
export function createOperationController(isCurrent: () => boolean) {
    let state: OperationState = { operation: null, contextChanged: false, error: '' };
    let active = false;
    let generation = 0;
    const listeners = new Set<() => void>();
    const publish = (patch: Partial<OperationState>) => { state = { ...state, ...patch }; listeners.forEach(listener => listener()); };
    const observeContext = () => {
        if (state.contextChanged || isCurrent()) return;
        if (state.operation === 'read') generation++;
        publish({ contextChanged: true, ...(state.operation === 'read' ? { operation: null } : {}) });
    };
    const run = async <T>(operation: 'read' | 'write', task: () => Promise<T>, fallback: string): Promise<T | null> => {
        observeContext();
        if (!active || state.operation || state.contextChanged) return null;
        const request = ++generation;
        publish({ operation, error: '' });
        if (operation === 'read') {
            await Promise.resolve();
            observeContext();
            if (!active || generation !== request) return null;
        }
        try {
            const result = await task();
            observeContext();
            return active && generation === request && (operation === 'write' || !state.contextChanged) ? result : null;
        } catch (error) {
            observeContext();
            if (active && generation === request) publish({ error: dashboardErrorMessage(error, fallback) });
            return null;
        } finally {
            if (active && generation === request) publish({ operation: null });
        }
    };
    return {
        start: () => { active = true; },
        stop: () => { active = false; generation++; state = { ...state, operation: null }; },
        getSnapshot: () => state,
        subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
        observeContext,
        isCurrent: () => active && !state.contextChanged && isCurrent(),
        validationError: (error: string) => { if (!state.operation) publish({ error }); },
        read: <T>(task: () => Promise<T>, fallback: string) => run('read', task, fallback),
        write: <T>(task: () => Promise<T>, fallback: string) => run('write', task, fallback),
    };
}
