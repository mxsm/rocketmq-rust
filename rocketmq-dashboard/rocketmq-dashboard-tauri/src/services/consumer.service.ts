import { invokeAuthenticatedCommand } from './invoke';
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
    static async queryConsumerGroups(request: ConsumerGroupListRequest): Promise<ConsumerGroupListResponse> {
        return invokeAuthenticatedCommand<ConsumerGroupListResponse>('query_consumer_groups', { request });
    }

    static async refreshConsumerGroup(request: ConsumerGroupRefreshRequest): Promise<ConsumerGroupListItem> {
        return invokeAuthenticatedCommand<ConsumerGroupListItem>('refresh_consumer_group', { request });
    }

    static async refreshAllConsumerGroups(
        request: ConsumerGroupListRequest,
    ): Promise<ConsumerGroupListResponse> {
        return invokeAuthenticatedCommand<ConsumerGroupListResponse>('refresh_all_consumer_groups', { request });
    }

    static async queryConsumerConnection(request: ConsumerConnectionQueryRequest): Promise<ConsumerConnectionView> {
        return invokeAuthenticatedCommand<ConsumerConnectionView>('query_consumer_connection', { request });
    }

    static async queryConsumerTopicDetail(request: ConsumerTopicDetailQueryRequest): Promise<ConsumerTopicDetailView> {
        return invokeAuthenticatedCommand<ConsumerTopicDetailView>('query_consumer_topic_detail', { request });
    }

    static async queryConsumerConfig(request: ConsumerConfigQueryRequest): Promise<ConsumerConfigView> {
        return invokeAuthenticatedCommand<ConsumerConfigView>('query_consumer_config', { request });
    }

    static async createOrUpdateConsumerGroup(
        request: ConsumerCreateOrUpdateRequest,
    ): Promise<ConsumerMutationResult> {
        return invokeAuthenticatedCommand<ConsumerMutationResult>('create_or_update_consumer_group', { request });
    }

    static async deleteConsumerGroup(request: ConsumerDeleteRequest): Promise<ConsumerMutationResult> {
        return invokeAuthenticatedCommand<ConsumerMutationResult>('delete_consumer_group', { request });
    }
}
