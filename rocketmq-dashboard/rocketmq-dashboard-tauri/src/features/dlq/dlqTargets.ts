import { messageLookup } from '../message/messageModel';
import { dlqGroup } from './dlqQuery';
import type { DlqMessageSummary } from './types/dlq.types';

export interface DlqContext { revision: number; environmentId: string | null; }
export interface DlqActionInput {
    action: 'resend' | 'export';
    group: string;
    messages: DlqMessageSummary[];
    clientId: string;
    context: DlqContext;
}

export function captureDlqTargets(input: DlqActionInput, current: DlqContext | null) {
    if (!current || current.revision !== input.context.revision || current.environmentId !== input.context.environmentId) {
        throw new Error('The connection changed. Query the current environment before selecting targets.');
    }
    const group = dlqGroup(input.group);
    const ids = new Set<string>();
    for (const message of input.messages) {
        const id = messageLookup(message).messageId;
        if (message.topic !== '%DLQ%' + group || !id.trim()) throw new Error('A selected message does not identify this group’s DLQ record.');
        ids.add(id);
    }
    if (!ids.size || ids.size > 256) throw new Error('Select between 1 and 256 distinct DLQ messages.');
    return { action: input.action, revision: current.revision, environmentId: current.environmentId,
        messages: [...ids].map(messageId => ({ consumerGroup: group, messageId, clientId: input.clientId.trim() || undefined })) };
}
