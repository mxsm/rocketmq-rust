import { invoke } from '@tauri-apps/api/core';
import type {
    MessageIdQueryRequest,
    MessageKeyQueryRequest,
    MessageSummaryListResponse,
} from '../features/message/types/message.types';

export class MessageService {
    static async queryMessageByTopicKey(request: MessageKeyQueryRequest): Promise<MessageSummaryListResponse> {
        return invoke<MessageSummaryListResponse>('query_message_by_topic_key', { request });
    }

    static async queryMessageById(request: MessageIdQueryRequest): Promise<MessageSummaryListResponse> {
        return invoke<MessageSummaryListResponse>('query_message_by_id', { request });
    }
}
