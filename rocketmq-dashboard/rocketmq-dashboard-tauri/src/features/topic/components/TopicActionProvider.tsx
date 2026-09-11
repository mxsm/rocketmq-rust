import { useCallback, useState, type ReactNode } from 'react';
import { ConnectionStore } from '../../../services/connection.store';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { TopicActionContext, type TopicAction, type OpenTopicAction } from '../topicActionContext';
import { canSendToTopic, isProtectedTopic } from '../topicModel';
import { useTopicOperation } from '../hooks/useTopicOperation';
import { TopicEditorForm } from './TopicEditorForm';
import { TopicDeleteForm } from './TopicDeleteForm';
import { TopicSendForm } from './TopicSendForm';
import { TopicOffsetForm } from './TopicOffsetForm';
import '../topic.css';

interface ActionRequest { action: TopicAction; revision: number; environmentId: string | null; onClosed: () => void; trigger: HTMLElement | null; }
const titles: Record<TopicAction['kind'], string> = {
    create: 'Create Topic', edit: 'Edit Topic', send: 'Send message', delete: 'Delete Topic',
    delete_broker: 'Delete Topic from Broker', reset: 'Reset Consumer offset', skip: 'Skip accumulated messages',
};

function TopicActionDialog({ request, onClose }: { request: ActionRequest; onClose: () => void }) {
    const owner = useTopicOperation(request.revision, request.environmentId);
    const { action } = request;
    return <Dialog open onOpenChange={open => { if (!open && !owner.busy) onClose(); }}>
        <DialogContent className="ops-topic-action" showCloseButton={!owner.busy}
            onEscapeKeyDown={event => { if (owner.busy) event.preventDefault(); }}
            onInteractOutside={event => { if (owner.busy) event.preventDefault(); }}
            onCloseAutoFocus={event => {
                event.preventDefault();
                if (request.trigger?.isConnected) request.trigger.focus();
                else document.getElementById('main-content')?.focus();
            }}>
            <DialogHeader><DialogTitle>{titles[action.kind]}</DialogTitle>
                <DialogDescription>Review the target and keep the operation result for verification.</DialogDescription></DialogHeader>
            <div className="ops-topic-action-target"><strong tabIndex={action.kind !== 'create' && action.topic.topic.length > 200 ? 0 : undefined}>{action.kind === 'create' ? 'New Topic' : action.topic.topic}</strong>
                <span>Environment {request.environmentId ?? 'Not configured'} · Connection revision {request.revision}</span></div>
            {(action.kind === 'create' || action.kind === 'edit') && <TopicEditorForm action={action} owner={owner} onClose={onClose} />}
            {(action.kind === 'delete' || action.kind === 'delete_broker') && <TopicDeleteForm action={action} owner={owner} onClose={onClose} />}
            {action.kind === 'send' && <TopicSendForm topic={action.topic} owner={owner} onClose={onClose} />}
            {(action.kind === 'reset' || action.kind === 'skip') && <TopicOffsetForm topic={action.topic.topic} skip={action.kind === 'skip'} owner={owner} onClose={onClose} />}
        </DialogContent>
    </Dialog>;
}

/** Session-owned dialogs survive route/environment changes until the accepted write returns. */
export function TopicActionProvider({ children }: { children: ReactNode }) {
    const [request, setRequest] = useState<ActionRequest | null>(null);
    const open = useCallback<OpenTopicAction>((action, onClosed) => {
        const settings = ConnectionStore.getSnapshot();
        if (!settings) return;
        if (action.kind !== 'create' && isProtectedTopic(action.topic)) return;
        if ((action.kind === 'send' || action.kind === 'reset' || action.kind === 'skip') && !canSendToTopic(action.topic)) return;
        const trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
        setRequest(current => current ?? { action: structuredClone(action), revision: settings.revision,
            environmentId: settings.environmentId, onClosed, trigger });
    }, []);
    return <TopicActionContext.Provider value={open}>{children}
        {request && <TopicActionDialog request={request} onClose={() => { request.onClosed(); setRequest(null); }} />}
    </TopicActionContext.Provider>;
}
