import { invoke } from '@tauri-apps/api/core';
import type {
    MessageDetail,
    MessageIdQueryRequest,
    MessageKeyQueryRequest,
    MessagePageQueryRequest,
    MessagePageResponse,
    MessageSummaryListResponse,
    ViewMessageRequest,
} from '../features/message/types/message.types';

export class MessageService {
    static async queryMessageByTopicKey(request: MessageKeyQueryRequest): Promise<MessageSummaryListResponse> {
        return invoke<MessageSummaryListResponse>('query_message_by_topic_key', { request });
    }

    static async queryMessageById(request: MessageIdQueryRequest): Promise<MessageSummaryListResponse> {
        return invoke<MessageSummaryListResponse>('query_message_by_id', { request });
    }

    static async queryMessagePageByTopic(request: MessagePageQueryRequest): Promise<MessagePageResponse> {
        return invoke<MessagePageResponse>('query_message_page_by_topic', { request });
    }

    static async viewMessageDetail(request: ViewMessageRequest): Promise<MessageDetail> {
        return invoke<MessageDetail>('view_message_detail', { request });
    }
}
