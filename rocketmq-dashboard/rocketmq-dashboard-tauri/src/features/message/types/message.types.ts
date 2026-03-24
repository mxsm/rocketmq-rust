export interface MessageSummary {
    topic: string;
    msgId: string;
    tags?: string | null;
    keys?: string | null;
    storeTimestamp: number;
}

export interface MessageSummaryListResponse {
    items: MessageSummary[];
    total: number;
}

export interface MessageKeyQueryRequest {
    topic: string;
    key: string;
}

export interface MessageIdQueryRequest {
    topic: string;
    messageId: string;
}

export interface MessagePageQueryRequest {
    topic: string;
    begin: number;
    end: number;
    pageNum: number;
    pageSize: number;
    taskId?: string | null;
}

export interface MessagePage {
    content: MessageSummary[];
    number: number;
    size: number;
    totalElements: number;
    totalPages: number;
    numberOfElements: number;
    first: boolean;
    last: boolean;
    empty: boolean;
}

export interface MessagePageResponse {
    page: MessagePage;
    taskId: string;
}

export interface ViewMessageRequest {
    topic: string;
    messageId: string;
}

export interface MessageDirectConsumeRequest {
    topic: string;
    consumerGroup: string;
    messageId: string;
    clientId?: string | null;
}

export interface MessageTrack {
    consumerGroup: string;
    trackType: string;
    exceptionDesc?: string | null;
}

export interface MessageDetail {
    topic: string;
    msgId: string;
    bornHost?: string | null;
    storeHost?: string | null;
    bornTimestamp?: number | null;
    storeTimestamp?: number | null;
    queueId?: number | null;
    queueOffset?: number | null;
    storeSize?: number | null;
    reconsumeTimes?: number | null;
    bodyCrc?: number | null;
    sysFlag?: number | null;
    flag?: number | null;
    preparedTransactionOffset?: number | null;
    properties: Record<string, string>;
    bodyText?: string | null;
    bodyBase64?: string | null;
    messageTrackList?: MessageTrack[] | null;
}

export interface MessageDirectConsumeResult {
    success: boolean;
    message: string;
    consumerGroup: string;
    topic: string;
    msgId: string;
    consumeResult?: string | null;
    remark?: string | null;
}
