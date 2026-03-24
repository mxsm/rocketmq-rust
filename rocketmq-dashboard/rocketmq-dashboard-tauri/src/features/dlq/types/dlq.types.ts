import type {
    MessageDetail,
    MessagePageResponse,
    MessageSummary,
} from '../../message/types/message.types';

export interface DlqMessagePageQueryRequest {
    consumerGroup: string;
    begin: number;
    end: number;
    pageNum: number;
    pageSize: number;
    taskId?: string | null;
}

export interface DlqMessageDetailRequest {
    consumerGroup: string;
    messageId: string;
}

export interface DlqMessageExportRequest {
    consumerGroup: string;
    messageId: string;
}

export interface DlqResendMessageRequest {
    consumerGroup: string;
    messageId: string;
}

export interface DlqBatchResendMessageRequest {
    messages: DlqResendMessageRequest[];
}

export interface DlqBatchExportMessageRequest {
    messages: DlqMessageExportRequest[];
}

export interface DlqResendMessageResult {
    success: boolean;
    message: string;
    consumerGroup: string;
    topic: string;
    msgId: string;
    consumeResult?: string | null;
    remark?: string | null;
}

export interface DlqBatchResendMessageResponse {
    items: DlqResendMessageResult[];
    total: number;
    successCount: number;
    failureCount: number;
}

export interface DlqMessageExportPayload {
    fileName: string;
    mimeType: string;
    content: string;
}

export interface DlqBatchMessageExportPayload {
    fileName: string;
    mimeType: string;
    content: string;
    total: number;
    successCount: number;
    failureCount: number;
}

export type DlqMessageSummary = MessageSummary;
export type DlqMessagePageResponse = MessagePageResponse;
export type DlqMessageDetail = MessageDetail;
