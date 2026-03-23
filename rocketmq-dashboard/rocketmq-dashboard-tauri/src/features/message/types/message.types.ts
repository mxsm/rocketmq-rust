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
