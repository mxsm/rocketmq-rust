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

export type DlqMessageSummary = MessageSummary;
export type DlqMessagePageResponse = MessagePageResponse;
export type DlqMessageDetail = MessageDetail;
