import { invoke } from '@tauri-apps/api/core';
import type {
    DlqBatchExportMessageRequest,
    DlqBatchMessageExportPayload,
    DlqBatchResendMessageRequest,
    DlqBatchResendMessageResponse,
    DlqMessageDetail,
    DlqMessageDetailRequest,
    DlqMessageExportPayload,
    DlqMessageExportRequest,
    DlqMessagePageQueryRequest,
    DlqMessagePageResponse,
    DlqResendMessageRequest,
    DlqResendMessageResult,
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

    static async resendDlqMessage(request: DlqResendMessageRequest): Promise<DlqResendMessageResult> {
        return invoke<DlqResendMessageResult>('resend_dlq_message', { request });
    }

    static async batchResendDlqMessage(
        request: DlqBatchResendMessageRequest,
    ): Promise<DlqBatchResendMessageResponse> {
        return invoke<DlqBatchResendMessageResponse>('batch_resend_dlq_message', { request });
    }

    static async exportDlqMessage(request: DlqMessageExportRequest): Promise<DlqMessageExportPayload> {
        return invoke<DlqMessageExportPayload>('export_dlq_message', { request });
    }

    static async batchExportDlqMessage(
        request: DlqBatchExportMessageRequest,
    ): Promise<DlqBatchMessageExportPayload> {
        return invoke<DlqBatchMessageExportPayload>('batch_export_dlq_message', { request });
    }
}
