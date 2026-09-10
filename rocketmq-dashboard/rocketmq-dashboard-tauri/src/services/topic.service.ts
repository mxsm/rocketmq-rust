import { invokeAuthenticatedCommand } from './invoke';
import type {
    DeleteTopicByBrokerRequest,
    DeleteTopicRequest,
    ResetOffsetRequest,
    SendTopicMessageRequest,
    TopicConfigQueryRequest,
    TopicConfigRequest,
    TopicConfigView,
    TopicConsumerGroupListResponse,
    TopicConsumerInfoResponse,
    TopicListRequest,
    TopicListResponse,
    TopicMutationResult,
    TopicBatchResult,
    TopicQueryRequest,
    TopicRouteView,
    TopicSendMessageResult,
    TopicStatusView,
} from '../features/topic/types/topic.types';

export class TopicService {
    static async getTopicList(request: TopicListRequest): Promise<TopicListResponse> {
        return invokeAuthenticatedCommand<TopicListResponse>('get_topic_list', { request });
    }

    static async getTopicRoute(request: TopicQueryRequest): Promise<TopicRouteView> {
        return invokeAuthenticatedCommand<TopicRouteView>('get_topic_route', { request });
    }

    static async getTopicStats(request: TopicQueryRequest): Promise<TopicStatusView> {
        return invokeAuthenticatedCommand<TopicStatusView>('get_topic_stats', { request });
    }

    static async getTopicConfig(request: TopicConfigQueryRequest): Promise<TopicConfigView> {
        return invokeAuthenticatedCommand<TopicConfigView>('get_topic_config', { request });
    }

    static async createOrUpdateTopic(request: TopicConfigRequest, mode: 'create' | 'update'): Promise<TopicBatchResult> {
        return invokeAuthenticatedCommand<TopicBatchResult>('create_or_update_topic', { request, mode });
    }

    static async deleteTopic(request: DeleteTopicRequest): Promise<TopicBatchResult> {
        return invokeAuthenticatedCommand<TopicBatchResult>('delete_topic', { request });
    }

    static async deleteTopicByBroker(request: DeleteTopicByBrokerRequest): Promise<TopicBatchResult> {
        return invokeAuthenticatedCommand<TopicBatchResult>('delete_topic_by_broker', { request });
    }

    static async getTopicConsumerGroups(request: TopicQueryRequest): Promise<TopicConsumerGroupListResponse> {
        return invokeAuthenticatedCommand<TopicConsumerGroupListResponse>('get_topic_consumer_groups', { request });
    }

    static async getTopicConsumers(request: TopicQueryRequest): Promise<TopicConsumerInfoResponse> {
        return invokeAuthenticatedCommand<TopicConsumerInfoResponse>('get_topic_consumers', { request });
    }

    static async resetConsumerOffset(request: ResetOffsetRequest): Promise<TopicMutationResult> {
        return invokeAuthenticatedCommand<TopicMutationResult>('reset_consumer_offset', { request });
    }

    static async skipMessageAccumulate(request: ResetOffsetRequest): Promise<TopicMutationResult> {
        return invokeAuthenticatedCommand<TopicMutationResult>('skip_message_accumulate', { request });
    }

    static async sendTopicMessage(request: SendTopicMessageRequest): Promise<TopicSendMessageResult> {
        return invokeAuthenticatedCommand<TopicSendMessageResult>('send_topic_message', { request });
    }
}
