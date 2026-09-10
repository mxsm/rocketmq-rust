import type { DlqBatchResendMessageResponse, DlqMessageSummary } from './types/dlq.types';

export function failedDlqSelection(receipt: DlqBatchResendMessageResponse, group: string, messages: DlqMessageSummary[]): Set<string> {
    const visible = new Set(messages.map(message => message.queryMsgId));
    const successful = new Set(receipt.items.filter(item => item.success && item.consumerGroup === group).map(item => item.requestMessageId ?? item.msgId));
    return new Set(receipt.items.filter(item => !item.success && item.consumerGroup === group)
        .map(item => item.requestMessageId ?? item.msgId).filter(id => visible.has(id) && !successful.has(id)));
}

export const dlqQueryTaskId = (mode: string, page: number, taskId: string) => mode === 'Consumer' && page > 1 ? taskId || undefined : undefined;
