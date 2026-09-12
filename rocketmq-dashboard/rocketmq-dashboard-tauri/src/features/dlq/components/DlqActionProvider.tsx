import { useCallback, useRef, useState, type ReactNode } from 'react';
import { Download } from 'lucide-react';
import { toast } from 'sonner';
import { ConnectionStore } from '../../../services/connection.store';
import { DlqService } from '../../../services/dlq.service';
import { useOperationOwner } from '../../../hooks/useOperationOwner';
import { Dialog, DialogContent, DialogDescription, DialogFooter, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { Button } from '../../../components/ui/LegacyButton';
import { PageState } from '../../../components/layout/PageState';
import { visibleMessageText } from '../../message/messageModel';
import { DlqActionContext, type OpenDlqAction } from '../dlqActionContext';
import { captureDlqTargets } from '../dlqTargets';
import { dlqReceiptRows, type DlqReceipt } from '../receipts';
import { downloadDlqCsv } from '../download';
import type { DlqBatchMessageExportPayload } from '../types/dlq.types';
import '../../message/message.css';

interface Request extends ReturnType<typeof captureDlqTargets> {
    trigger: HTMLElement | null;
}

function DlqActionDialog({ request, close, saveReceipt }: { request: Request; close: () => void; saveReceipt: (value: DlqReceipt) => void }) {
    const owner = useOperationOwner(request.revision, request.environmentId);
    const attempted = useRef(false);
    const [submitted, setSubmitted] = useState(false);
    const [receipt, setReceipt] = useState<DlqReceipt | null>(null);
    const [exported, setExported] = useState<DlqBatchMessageExportPayload | null>(null);
    const resend = request.action === 'resend';
    const submit = async () => {
        if (attempted.current || owner.blocked) return;
        attempted.current = true; setSubmitted(true);
        const startedAt = Date.now();
        if (resend) {
            const response = await owner.controller.write(async () => {
                if (request.messages.length !== 1) return DlqService.batchResendDlqMessage({ messages: request.messages });
                const result = await DlqService.resendDlqMessage(request.messages[0]);
                return { items: [result], total: 1, successCount: Number(result.success), failureCount: Number(!result.success) };
            }, 'Resend was not confirmed. Inspect the Consumer before another attempt.');
            const completed: DlqReceipt = { requests: request.messages, environmentId: request.environmentId, revision: request.revision,
                startedAt, completedAt: Date.now(), response, error: owner.controller.getSnapshot().error };
            setReceipt(completed); saveReceipt(completed);
        } else {
            const result = await owner.controller.read(async () => {
                const messages = request.messages.map(({ consumerGroup, messageId }) => ({ consumerGroup, messageId }));
                if (messages.length !== 1) return DlqService.batchExportDlqMessage({ messages });
                return { ...await DlqService.exportDlqMessage(messages[0]), total: 1, successCount: 1, failureCount: 0 };
            }, 'CSV could not be prepared. Close and review the targets before another request.');
            if (result) setExported(result);
        }
    };
    const outcomes = receipt ? dlqReceiptRows(receipt) : [];
    return <Dialog open onOpenChange={open => { if (!open && !owner.busy) close(); }}><DialogContent className="ops-message-operation" showCloseButton={!owner.busy}
        onEscapeKeyDown={event => { if (owner.busy) event.preventDefault(); }} onInteractOutside={event => { if (owner.busy) event.preventDefault(); }}
        onCloseAutoFocus={event => {
            event.preventDefault();
            // Completing a batch clears its selection, which can disable the original trigger.
            if (request.trigger?.isConnected && !request.trigger.matches(':disabled, [aria-disabled="true"]')) request.trigger.focus();
            else document.getElementById('main-content')?.focus();
        }}>
        <DialogHeader><DialogTitle>{resend ? 'Resend dead-letter messages' : 'Export dead-letter messages'}</DialogTitle>
            <DialogDescription>{resend ? 'Review the fixed Consumer and message targets before requesting consumption.' : 'Prepare CSV for these targets, then download the returned file.'}</DialogDescription></DialogHeader>
        <form className="ops-message-operation-form" onSubmit={event => { event.preventDefault(); void submit(); }}>
            <div className="ops-message-operation-body">
                <p className="ops-message-note">Environment {request.environmentId ?? 'Not configured'} · Connection revision {request.revision}</p>
                {owner.contextChanged && <PageState kind="stale" title="Connection context changed" description="Accepted resend results remain attached to the original environment. No new request can be submitted from this dialog." />}
                {owner.state.error && <PageState kind="error" title={resend ? 'Resend result is unconfirmed' : 'Export unavailable'} description={owner.state.error} />}
                <dl className="ops-message-properties"><div><dt>Consumer group</dt><dd>{visibleMessageText(request.messages[0].consumerGroup)}</dd></div>
                    {resend && <div><dt>Client</dt><dd>{visibleMessageText(request.messages[0].clientId || 'Selected by Broker')}</dd></div>}
                    <div><dt>{request.messages.length} DLQ request IDs</dt><dd tabIndex={0}>{request.messages.map(item => <div key={item.messageId}>{visibleMessageText(item.messageId)}</div>)}</dd></div></dl>
                {resend && <p>The Broker resolves each original Topic and message ID from its DLQ record. This can run an already processed message again. The batch is not atomic; no automatic retry is performed.</p>}
                {owner.busy && <PageState kind="loading" title={resend ? 'Requesting consumption' : 'Preparing CSV'} />}
                {receipt && <PageState kind={outcomes.every(row => row.outcome === 'success') ? 'empty' : 'partial'} title="Resend outcome"
                    description={`${outcomes.filter(row => row.outcome === 'success').length} succeeded · ${outcomes.filter(row => row.outcome === 'failed').length} failed · ${outcomes.filter(row => row.outcome === 'unknown').length} unknown. Close to inspect the persistent results table.`} />}
                {exported && <PageState kind={exported.failureCount ? 'partial' : 'empty'} title="CSV prepared" description={`${exported.successCount} exported · ${exported.failureCount} failed. Review errors included in the CSV. Download does not resend any messages.`} />}
            </div>
            <DialogFooter><Button variant="outline" disabled={owner.busy} onClick={close}>Close</Button>
                {exported ? <Button icon={Download} onClick={() => { downloadDlqCsv(exported); toast.success('CSV download requested'); }}>Download CSV</Button>
                    : <Button type="submit" variant={resend ? 'danger' : 'primary'} disabled={owner.blocked || submitted}>{resend ? 'Confirm resend' : 'Prepare CSV'}</Button>}
            </DialogFooter>
        </form>
    </DialogContent></Dialog>;
}

/** Session-owned dialogs and receipts survive route and connection changes, but never sign-out. */
export function DlqActionProvider({ children }: { children: ReactNode }) {
    const [request, setRequest] = useState<Request | null>(null);
    const [receipt, setReceipt] = useState<DlqReceipt | null>(null);
    const open = useCallback<OpenDlqAction>(input => {
        try {
            const captured = captureDlqTargets(input, ConnectionStore.getSnapshot());
            const trigger = document.activeElement instanceof HTMLElement ? document.activeElement : null;
            setRequest(current => current ?? { ...captured, trigger });
        } catch (error) { toast.error(error instanceof Error ? error.message : 'DLQ targets could not be selected.'); }
    }, []);
    return <DlqActionContext.Provider value={{ open, receipt }}>{children}{request && <DlqActionDialog request={request} close={() => setRequest(null)} saveReceipt={setReceipt} />}</DlqActionContext.Provider>;
}
