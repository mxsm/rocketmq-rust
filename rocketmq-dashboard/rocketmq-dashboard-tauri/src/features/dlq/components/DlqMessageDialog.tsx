import { useCallback, useRef } from 'react';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from '../../../components/ui/dialog';
import { MessageInspector } from '../../message/components/MessageInspector';
import { messageLookup, visibleMessageText } from '../../message/messageModel';
import { DlqService } from '../../../services/dlq.service';
import type { DlqMessageSummary } from '../types/dlq.types';

export function DlqMessageDialog({ message, group, close }: { message: DlqMessageSummary; group: string; close: () => void }) {
    const trigger = useRef(document.activeElement instanceof HTMLElement ? document.activeElement : null);
    const load = useCallback(() => DlqService.viewDlqMessageDetail({ consumerGroup: group, messageId: messageLookup(message).messageId }), [group, message]);
    return <Dialog open onOpenChange={open => { if (!open) close(); }}><DialogContent className="ops-message-detail-dialog"
        onCloseAutoFocus={event => { event.preventDefault(); if (trigger.current?.isConnected) trigger.current.focus(); else document.getElementById('main-content')?.focus(); }}>
        <DialogHeader><DialogTitle>Inspect dead-letter message</DialogTitle><DialogDescription>Consumer group: {group}. Use the DLQ results table to select and review resend targets.</DialogDescription></DialogHeader>
        <div className="ops-message-dialog-scroll ops-message-stack">
            <dl className="ops-message-properties" aria-label="Selected query record">{[
                ['Displayed message ID', message.msgId], ['Tags', message.tags || 'Not reported'], ['Keys', message.keys || 'Not reported'],
            ].map(([label, value]) => <div key={label}><dt>{label}</dt><dd tabIndex={value.length > 200 ? 0 : undefined}>{visibleMessageText(value)}</dd></div>)}</dl>
            <MessageInspector message={message} loadDetail={load} actions={null} />
        </div>
    </DialogContent></Dialog>;
}
