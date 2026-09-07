import { invokeAuthenticatedCommand } from './invoke';
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
        return invokeAuthenticatedCommand<DlqMessagePageResponse>('query_dlq_message_by_consumer_group', { request });
    }

    static async viewDlqMessageDetail(request: DlqMessageDetailRequest): Promise<DlqMessageDetail> {
        return invokeAuthenticatedCommand<DlqMessageDetail>('view_dlq_message_detail', { request });
    }

    static async resendDlqMessage(request: DlqResendMessageRequest): Promise<DlqResendMessageResult> {
        return invokeAuthenticatedCommand<DlqResendMessageResult>('resend_dlq_message', { request });
    }

    static async batchResendDlqMessage(
        request: DlqBatchResendMessageRequest,
    ): Promise<DlqBatchResendMessageResponse> {
        return invokeAuthenticatedCommand<DlqBatchResendMessageResponse>('batch_resend_dlq_message', { request });
    }

    static async exportDlqMessage(request: DlqMessageExportRequest): Promise<DlqMessageExportPayload> {
        return invokeAuthenticatedCommand<DlqMessageExportPayload>('export_dlq_message', { request });
    }

    static async batchExportDlqMessage(
        request: DlqBatchExportMessageRequest,
    ): Promise<DlqBatchMessageExportPayload> {
        return invokeAuthenticatedCommand<DlqBatchMessageExportPayload>('batch_export_dlq_message', { request });
    }
}
