import { invoke } from '@tauri-apps/api/core';
import type {
    ConsumerGroupListItem,
    ConsumerGroupListRequest,
    ConsumerGroupListResponse,
    ConsumerGroupRefreshRequest,
} from '../features/consumer/types/consumer.types';

export class ConsumerService {
    static async queryConsumerGroups(request: ConsumerGroupListRequest = {}): Promise<ConsumerGroupListResponse> {
        return invoke<ConsumerGroupListResponse>('query_consumer_groups', { request });
    }

    static async refreshConsumerGroup(request: ConsumerGroupRefreshRequest): Promise<ConsumerGroupListItem> {
        return invoke<ConsumerGroupListItem>('refresh_consumer_group', { request });
    }
}
