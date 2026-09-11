import { useCallback, useState, type ReactNode } from 'react';
import { ConnectionStore } from '../../../services/connection.store';
import { brokerIdentity, type BrokerIdentity } from '../brokerIdentity';
import { BrokerConfigEditorContext, type OpenBrokerConfigEditor } from '../brokerConfigEditorContext';
import { BrokerConfigEditor } from './BrokerConfigEditor';
import '../cluster.css';

interface EditorRequest {
    broker: BrokerIdentity;
    revision: number;
    environmentId: string | null;
    onClosed: () => void;
    trigger: HTMLElement | null;
}

/** Outside the route key so connection/navigation changes cannot discard a pending write receipt. */
export function BrokerConfigEditorProvider({ children }: { children: ReactNode }) {
    const [request, setRequest] = useState<EditorRequest | null>(null);
    const open = useCallback<OpenBrokerConfigEditor>((broker, onClosed) => {
        const settings = ConnectionStore.getSnapshot();
        if (!settings) return;
        const trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
        setRequest(current => current ?? { broker: brokerIdentity(broker), revision: settings.revision,
            environmentId: settings.environmentId, onClosed, trigger });
    }, []);
    const close = () => {
        request?.onClosed();
        setRequest(null);
    };
    return <BrokerConfigEditorContext.Provider value={open}>
        {children}
        {request && <BrokerConfigEditor broker={request.broker} revision={request.revision} environmentId={request.environmentId}
            onClose={close} onReturnFocus={() => {
                if (request.trigger?.isConnected) request.trigger.focus();
                else document.getElementById('main-content')?.focus();
            }} />}
    </BrokerConfigEditorContext.Provider>;
}
