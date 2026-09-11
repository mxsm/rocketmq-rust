import { useRef } from 'react';
import { Dialog, DialogContent, DialogDescription, DialogHeader, DialogTitle } from './ui/dialog';
import { MessageInspector } from '../features/message/components/MessageInspector';
import { messageIdentity } from '../features/message/messageModel';
import type { MessageSummary } from '../features/message/types/message.types';

interface Props { isOpen: boolean; onClose: () => void; message: MessageSummary | null; }
export function MessageDetailModal({ isOpen, onClose, message }: Props) {
    if (!isOpen || !message) return null;
    return <MessageDetailDialog key={messageIdentity(message)} message={message} onClose={onClose} />;
}

function MessageDetailDialog({ message, onClose }: { message: MessageSummary; onClose: () => void }) {
    const trigger = useRef(document.activeElement instanceof HTMLElement ? document.activeElement : null);
    return <Dialog open onOpenChange={open => { if (!open) onClose(); }}><DialogContent className="ops-message-detail-dialog"
        onCloseAutoFocus={event => { event.preventDefault(); if (trigger.current?.isConnected) trigger.current.focus(); else document.getElementById('main-content')?.focus(); }}>
        <DialogHeader><DialogTitle>Inspect message</DialogTitle><DialogDescription>Read the selected message body, properties and delivery records.</DialogDescription></DialogHeader>
        <div className="ops-message-dialog-scroll"><MessageInspector message={message} /></div>
    </DialogContent></Dialog>;
}
