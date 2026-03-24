import { invoke } from '@tauri-apps/api/core';
import type {
    ConsumerConfigQueryRequest,
    ConsumerConfigView,
    ConsumerCreateOrUpdateRequest,
    ConsumerDeleteRequest,
    ConsumerConnectionQueryRequest,
    ConsumerConnectionView,
    ConsumerGroupListItem,
    ConsumerGroupListRequest,
    ConsumerGroupListResponse,
    ConsumerGroupRefreshRequest,
    ConsumerMutationResult,
    ConsumerTopicDetailQueryRequest,
    ConsumerTopicDetailView,
} from '../features/consumer/types/consumer.types';

export class ConsumerService {
    static async queryConsumerGroups(request: ConsumerGroupListRequest = {}): Promise<ConsumerGroupListResponse> {
        return invoke<ConsumerGroupListResponse>('query_consumer_groups', { request });
    }

    static async refreshConsumerGroup(request: ConsumerGroupRefreshRequest): Promise<ConsumerGroupListItem> {
        return invoke<ConsumerGroupListItem>('refresh_consumer_group', { request });
    }

    static async queryConsumerConnection(request: ConsumerConnectionQueryRequest): Promise<ConsumerConnectionView> {
        return invoke<ConsumerConnectionView>('query_consumer_connection', { request });
    }

    static async queryConsumerTopicDetail(request: ConsumerTopicDetailQueryRequest): Promise<ConsumerTopicDetailView> {
        return invoke<ConsumerTopicDetailView>('query_consumer_topic_detail', { request });
    }

    static async queryConsumerConfig(request: ConsumerConfigQueryRequest): Promise<ConsumerConfigView> {
        return invoke<ConsumerConfigView>('query_consumer_config', { request });
    }

    static async createOrUpdateConsumerGroup(
        request: ConsumerCreateOrUpdateRequest,
    ): Promise<ConsumerMutationResult> {
        return invoke<ConsumerMutationResult>('create_or_update_consumer_group', { request });
    }

    static async deleteConsumerGroup(request: ConsumerDeleteRequest): Promise<ConsumerMutationResult> {
        return invoke<ConsumerMutationResult>('delete_consumer_group', { request });
    }
}
