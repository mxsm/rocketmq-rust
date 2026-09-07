import { invokeAuthenticatedCommand } from './invoke';
import type { MessageSummaryListResponse } from '../features/message/types/message.types';
import type { MessageTraceDetail, MessageTraceQueryRequest } from '../features/message-trace/types/message-trace.types';

export class MessageTraceService {
    static async queryMessageTraceById(request: MessageTraceQueryRequest): Promise<MessageSummaryListResponse> {
        return invokeAuthenticatedCommand<MessageSummaryListResponse>('query_message_trace_by_id', { request });
    }

    static async viewMessageTraceDetail(request: MessageTraceQueryRequest): Promise<MessageTraceDetail> {
        return invokeAuthenticatedCommand<MessageTraceDetail>('view_message_trace_detail', { request });
    }
}
