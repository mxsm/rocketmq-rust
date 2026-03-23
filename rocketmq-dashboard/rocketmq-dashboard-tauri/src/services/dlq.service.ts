import { invoke } from '@tauri-apps/api/core';
import type {
    DlqMessageDetail,
    DlqMessageDetailRequest,
    DlqMessagePageQueryRequest,
    DlqMessagePageResponse,
} from '../features/dlq/types/dlq.types';

export class DlqService {
    static async queryDlqMessageByConsumerGroup(
        request: DlqMessagePageQueryRequest,
    ): Promise<DlqMessagePageResponse> {
        return invoke<DlqMessagePageResponse>('query_dlq_message_by_consumer_group', { request });
    }

    static async viewDlqMessageDetail(request: DlqMessageDetailRequest): Promise<DlqMessageDetail> {
        return invoke<DlqMessageDetail>('view_dlq_message_detail', { request });
    }
}
