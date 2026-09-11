import { useCallback, useState, useSyncExternalStore, type ReactNode } from 'react';
import { ConnectionStore } from '../../../services/connection.store';
import { useAppStore } from '../../../stores/app.store';
import { useOperationOwner } from '../../../hooks/useOperationOwner';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { ConsumerActionContext, type ConsumerAction, type OpenConsumerAction } from '../consumerActionContext';
import { consumerScopeKey, resolveConsumerScope } from '../scope';
import { isReadOnlyConsumer } from '../mutation';
import { ConsumerEditorForm } from './ConsumerEditorForm';
import { ConsumerDeleteForm } from './ConsumerDeleteForm';
import { ConsumerOffsetForm } from './ConsumerOffsetForm';
import '../consumer.css';

interface Request { action: ConsumerAction; revision: number; environmentId: string | null; trigger: HTMLElement | null; onClosed: () => void; }
const titles: Record<ConsumerAction['kind'], string> = { create: 'Create Consumer group', edit: 'Edit Consumer group', delete: 'Delete Consumer group', reset: 'Reset Consumer offset' };
function ConsumerActionDialog({ request, currentScope, close }: { request: Request; currentScope: string; close: () => void }) {
    const owner = useOperationOwner(request.revision, request.environmentId, consumerScopeKey(request.action.scope), currentScope);
    const { action } = request;
    return <Dialog open onOpenChange={open => { if (!open && !owner.busy) close(); }}><DialogContent className="ops-consumer-dialog" showCloseButton={!owner.busy}
        onEscapeKeyDown={event => { if (owner.busy) event.preventDefault(); }} onInteractOutside={event => { if (owner.busy) event.preventDefault(); }}
        onCloseAutoFocus={event => { event.preventDefault(); if (request.trigger?.isConnected) request.trigger.focus(); else document.getElementById('main-content')?.focus(); }}>
        <DialogHeader><DialogTitle>{titles[action.kind]}</DialogTitle><DialogDescription>Review the exact group and targets before applying this change.</DialogDescription></DialogHeader>
        <div className="ops-consumer-target"><strong tabIndex={action.kind !== 'create' && action.consumer.rawGroupName.length > 200 ? 0 : undefined}>{action.kind === 'create' ? 'New Consumer group' : action.consumer.rawGroupName}</strong>
            <span>Environment {request.environmentId ?? 'Not configured'} · Revision {request.revision} · Writes use NameServer Broker discovery</span></div>
        {(action.kind === 'create' || action.kind === 'edit') && <ConsumerEditorForm action={action} owner={owner} onClose={close} />}
        {action.kind === 'delete' && <ConsumerDeleteForm action={action} owner={owner} onClose={close} />}
        {action.kind === 'reset' && <ConsumerOffsetForm action={action} owner={owner} onClose={close} />}
    </DialogContent></Dialog>;
}
export function ConsumerActionProvider({ children }: { children: ReactNode }) {
    const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
    const { consumerQueryMode, activeTab, navigation, pageStates } = useAppStore();
    const mode = activeTab === 'Consumer' ? (pageStates.current.get(navigation.id)?.queryMode as typeof consumerQueryMode | undefined)
        ?? (navigation.target?.kind === 'consumer' ? navigation.target.scope.mode : consumerQueryMode) : consumerQueryMode;
    const scope = resolveConsumerScope(settings, mode);
    const [request, setRequest] = useState<Request | null>(null);
    const open = useCallback<OpenConsumerAction>((action, onClosed) => {
        const settings = ConnectionStore.getSnapshot();
        if (!settings || (action.kind !== 'create' && isReadOnlyConsumer(action.consumer))) return;
        if (action.kind === 'reset' && action.scope.mode !== 'name_server') return;
        const trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
        setRequest(previous => previous ?? { action: structuredClone(action), revision: settings.revision, environmentId: settings.environmentId, trigger, onClosed });
    }, []);
    return <ConsumerActionContext.Provider value={open}>{children}{request && <ConsumerActionDialog request={request} currentScope={scope ? consumerScopeKey(scope) : 'unavailable'}
        close={() => { request.onClosed(); setRequest(null); }} />}</ConsumerActionContext.Provider>;
}
