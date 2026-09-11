import { ClusterService } from '../../services/cluster.service';
import { dashboardErrorMessage } from '../../services/invoke';
import type { BrokerConfigUpdateResult } from './types/cluster.types';
import { changedBrokerConfig } from './config';
import { brokerIdentity, type BrokerIdentity } from './brokerIdentity';

interface EditorState {
    original: Record<string, string> | null;
    text: string;
    operation: 'idle' | 'reading' | 'writing';
    pending: Record<string, string> | null;
    receipt: BrokerConfigUpdateResult | null;
    requiresRead: boolean;
    contextChanged: boolean;
    error: string;
}

interface EditorOptions {
    broker: BrokerIdentity;
    isCurrent: () => boolean;
    service?: Pick<typeof ClusterService, 'getClusterBrokerConfig' | 'updateBrokerConfig'>;
}

/** The dialog owns the write until acknowledgement, independently of route remounts. */
export function createBrokerConfigController({ broker, isCurrent, service = ClusterService }: EditorOptions) {
    const target = brokerIdentity(broker);
    let state: EditorState = { original: null, text: '', operation: 'idle', pending: null,
        receipt: null, requiresRead: false, contextChanged: false, error: '' };
    const listeners = new Set<() => void>();
    let active = false;
    let generation = 0;
    const publish = (patch: Partial<EditorState>) => {
        state = { ...state, ...patch };
        listeners.forEach(listener => listener());
    };
    const observeContext = () => {
        if (state.contextChanged || isCurrent()) return;
        if (state.operation === 'reading') generation++;
        publish({ contextChanged: true, ...(state.operation === 'reading' ? { operation: 'idle' as const } : {}) });
    };
    const available = () => {
        observeContext();
        return active && !state.contextChanged && state.operation === 'idle';
    };
    const refresh = async (): Promise<boolean> => {
        if (!available()) return false;
        const request = ++generation;
        publish({ operation: 'reading', error: '', pending: null });
        await Promise.resolve();
        observeContext();
        if (!active || generation !== request) return false;
        try {
            const result = await service.getClusterBrokerConfig({ brokerAddr: target.address });
            observeContext();
            if (!active || generation !== request || state.contextChanged) return false;
            if (result.brokerAddr !== target.address) throw new Error('Unexpected Broker response');
            publish({ original: result.entries, text: JSON.stringify(result.entries, null, 2), requiresRead: false });
            return true;
        } catch (error) {
            if (active && generation === request) publish({ error: dashboardErrorMessage(error, 'Unable to read the selected Broker configuration.') });
            return false;
        } finally {
            if (active && generation === request) publish({ operation: 'idle' });
        }
    };
    const setText = (text: string) => {
        if (available() && state.original && !state.pending && !state.requiresRead) publish({ text, error: '' });
    };
    const review = () => {
        if (!available() || !state.original || state.requiresRead) return;
        try {
            const pending = changedBrokerConfig(state.text, state.original);
            if (!Object.keys(pending).length) { publish({ error: 'No configuration values have changed.' }); return; }
            publish({ pending, error: '' });
        } catch (error) {
            publish({ error: error instanceof SyntaxError ? 'Enter valid configuration JSON.'
                : error instanceof Error ? error.message : 'Invalid configuration values.' });
        }
    };
    const submit = async (): Promise<boolean> => {
        if (!available() || !state.pending || state.requiresRead) return false;
        const entries = { ...state.pending };
        const request = ++generation;
        publish({ operation: 'writing', error: '', receipt: null });
        try {
            const receipt = await service.updateBrokerConfig({ clusterName: target.clusterName,
                brokerName: target.brokerName, brokerId: target.brokerId, brokerAddr: target.address, entries });
            if (!active || generation !== request) return false;
            if (receipt.brokerAddr !== target.address) throw new Error('Unexpected Broker response');
            // Connection changes freeze editing, but cannot erase a completed write.
            observeContext();
            publish({ receipt, pending: null, requiresRead: !receipt.entries || !receipt.written,
                ...(receipt.entries ? { original: receipt.entries, text: JSON.stringify(receipt.entries, null, 2) } : {}) });
            return true;
        } catch (error) {
            if (active && generation === request) {
                observeContext();
                publish({ pending: null, requiresRead: true,
                    error: dashboardErrorMessage(error, 'Write result was not confirmed. Read the current configuration before another change.') });
            }
            return false;
        } finally {
            if (active && generation === request) publish({ operation: 'idle' });
        }
    };
    return {
        getSnapshot: () => state,
        subscribe: (listener: () => void) => { listeners.add(listener); return () => { listeners.delete(listener); }; },
        start: () => { active = true; },
        stop: () => { active = false; generation++; state = { ...state, operation: 'idle' }; },
        refresh, setText, review, submit, observeContext,
        editAgain: () => { if (available()) publish({ pending: null }); },
    };
}
