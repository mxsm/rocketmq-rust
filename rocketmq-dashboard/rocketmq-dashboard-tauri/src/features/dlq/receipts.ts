import type { DlqBatchResendMessageResponse, DlqMessageSummary, DlqResendMessageRequest, DlqResendMessageResult } from './types/dlq.types';

export interface DlqReceipt {
    requests: DlqResendMessageRequest[];
    environmentId: string | null;
    revision: number;
    startedAt: number;
    completedAt: number;
    response: DlqBatchResendMessageResponse | null;
    error: string;
}
export interface DlqReceiptRow {
    request: DlqResendMessageRequest;
    result?: DlqResendMessageResult;
    outcome: 'success' | 'failed' | 'unknown';
}

const failures = new Set(['CR_LATER', 'CR_ROLLBACK', 'CR_THROW_EXCEPTION', 'CR_RETURN_NULL']);
export function dlqReceiptRows(receipt: DlqReceipt): DlqReceiptRow[] {
    return receipt.requests.map(request => {
        // The returned original ID differs from the requested DLQ ID. Never join on the original ID.
        const matches = receipt.response?.items.filter(item => item.consumerGroup === request.consumerGroup && item.requestMessageId === request.messageId) ?? [];
        const result = matches.length === 1 ? matches[0] : undefined;
        const origin = result?.topic.trim() && !/^%(DLQ|RETRY)%/.test(result.topic) && result.msgId.trim();
        const outcome = origin && result
            ? result.success && ['CR_SUCCESS', 'CR_COMMIT'].includes(result.consumeResult ?? '') ? 'success'
                : !result.success && failures.has(result.consumeResult ?? '') ? 'failed' : 'unknown'
            : 'unknown';
        return { request, result, outcome };
    });
}

export function failedDlqSelection(receipt: DlqReceipt, environmentId: string | null, group: string, messages: DlqMessageSummary[]): Set<string> {
    if (receipt.environmentId !== environmentId) return new Set();
    const visible = new Set(messages.filter(message => message.topic === '%DLQ%' + group).map(message => message.queryMsgId || message.msgId));
    return new Set(dlqReceiptRows(receipt).filter(row => row.outcome === 'failed' && row.request.consumerGroup === group && visible.has(row.request.messageId))
        .map(row => row.request.messageId));
}

export function dlqReceiptCsv(receipt: DlqReceipt) {
    // Quoting alone does not neutralize spreadsheet formulas in user-controlled identities.
    const cell = (value: string) => '"' + (/^[\s]*[=+\-@\t\r\n]/.test(value) ? "'" + value : value).replace(/"/g, '""') + '"';
    const rows = [['Environment', 'Consumer group', 'Request ID', 'Client ID', 'Outcome', 'Original Topic', 'Original ID', 'Consume result', 'Message', 'Remark'],
        ...dlqReceiptRows(receipt).map(({ request, result, outcome }) => [receipt.environmentId ?? '', request.consumerGroup, request.messageId,
            request.clientId ?? '', outcome, result?.topic ?? '', result?.msgId ?? '', result?.consumeResult ?? '', result?.message ?? receipt.error, result?.remark ?? ''])];
    return '\ufeff' + rows.map(row => row.map(cell).join(',')).join('\r\n');
}
