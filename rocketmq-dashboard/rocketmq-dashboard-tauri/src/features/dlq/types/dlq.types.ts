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

export interface DlqResendMessageRequest {
    consumerGroup: string;
    messageId: string;
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

export type DlqMessageSummary = MessageSummary;
export type DlqMessagePageResponse = MessagePageResponse;
export type DlqMessageDetail = MessageDetail;
