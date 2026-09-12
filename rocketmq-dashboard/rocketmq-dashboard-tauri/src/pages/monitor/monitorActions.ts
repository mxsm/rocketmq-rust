import { dashboardErrorMessage, isDashboardClientError } from '../../services/invoke';
import { captureMonitorTarget, type MonitorContext, type MonitorReceipt, type MonitorTarget } from './monitorModel';

interface Snapshot { pending: MonitorTarget | null; receipt: MonitorReceipt | null }
/** The signed-in session owns accepted writes, even when navigation or the environment changes. */
export function createMonitorActions(current: () => MonitorContext | null, dispatch: (target: MonitorTarget) => Promise<unknown>) {
    let state: Snapshot = { pending: null, receipt: null };
    let active = false;
    let generation = 0;
    const listeners = new Set<() => void>();
    const publish = (next: Snapshot) => { state = next; listeners.forEach(listener => listener()); };
    return {
        start: () => { active = true; },
        stop: () => { active = false; generation++; state = { pending: null, receipt: null }; },
        getSnapshot: () => state,
        subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
        submit: async (input: MonitorTarget): Promise<MonitorReceipt | null> => {
            if (!active || state.pending) return null;
            const target = captureMonitorTarget(input, current());
            const request = ++generation;
            publish({ ...state, pending: target });
            let outcome: MonitorReceipt['outcome'] = 'unconfirmed';
            let message = 'The write was not confirmed. Read the original environment before deciding whether another change is needed.';
            try {
                const result = await dispatch(target);
                // The local command emits this receipt only after committing both the rule and its audit record.
                if (result && typeof result === 'object' && 'message' in result && result.message === 'Monitor rule saved.') {
                    outcome = 'success';
                    message = target.kind === 'delete' ? 'Rule deletion committed.' : 'Rule change committed.';
                }
            } catch (error) {
                if (isDashboardClientError(error) && error.code === 'dashboard.monitor_conflict') {
                    outcome = 'conflict';
                    message = 'The rule changed or was deleted. Your draft is retained for review.';
                } else {
                    message = dashboardErrorMessage(error, message);
                }
            }
            if (!active || generation !== request) return null;
            const receipt = { target, outcome, message, completedAt: Date.now() };
            publish({ pending: null, receipt });
            return receipt;
        },
    };
}
