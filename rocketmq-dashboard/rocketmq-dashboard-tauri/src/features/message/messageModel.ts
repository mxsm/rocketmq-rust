import type { MessageDetail, MessageSummary } from './types/message.types';

export const messageIdentity = (message: MessageSummary) => JSON.stringify([message.topic, message.msgId, message.queryMsgId || message.msgId]);
export function messageTimestamp(value?: number | null) {
    const date = value != null && value > 0 ? new Date(value) : null;
    return date && Number.isFinite(date.getTime()) ? date.toLocaleString() : 'Not reported';
}
export const messageNumber = (value?: number | null) => value != null && Number.isFinite(value) ? value.toLocaleString() : 'Not reported';
export const messageLookup = (message: MessageSummary) => ({ topic: message.topic, messageId: message.queryMsgId || message.msgId });

// Preserve the exact clipboard value; display control characters so payload text cannot conceal identity.
export function visibleMessageText(value: string) {
    return value.replace(/[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f\u202a-\u202e\u2066-\u2069]/g,
        character => '\\u' + character.charCodeAt(0).toString(16).padStart(4, '0'));
}

export function checkMessageDetail(detail: MessageDetail, message: MessageSummary) {
    if (detail.topic !== message.topic) throw new Error('The returned message belongs to a different Topic.');
    const queryId = messageLookup(message).messageId;
    // Physical lookups must resolve exactly. Unique-ID lookups may return a physical ID,
    // but the returned message must carry the queried unique identity.
    if (detail.msgId !== queryId && detail.properties.UNIQ_KEY !== queryId) {
        throw new Error('The returned message does not match the selected message ID.');
    }
    return detail;
}

export const traceMessageIdentity = (detail: MessageDetail, message: MessageSummary) => detail.properties.UNIQ_KEY?.trim() || message.queryMsgId || detail.msgId;
