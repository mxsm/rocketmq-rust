import { invoke } from '@tauri-apps/api/core';
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
    TopicQueryRequest,
    TopicRouteView,
    TopicSendMessageResult,
    TopicStatusView,
} from '../features/topic/types/topic.types';

export class TopicService {
    static async getTopicList(request: TopicListRequest): Promise<TopicListResponse> {
        return invoke<TopicListResponse>('get_topic_list', { request });
    }

    static async getTopicRoute(request: TopicQueryRequest): Promise<TopicRouteView> {
        return invoke<TopicRouteView>('get_topic_route', { request });
    }

    static async getTopicStats(request: TopicQueryRequest): Promise<TopicStatusView> {
        return invoke<TopicStatusView>('get_topic_stats', { request });
    }

    static async getTopicConfig(request: TopicConfigQueryRequest): Promise<TopicConfigView> {
        return invoke<TopicConfigView>('get_topic_config', { request });
    }

    static async createOrUpdateTopic(request: TopicConfigRequest): Promise<TopicMutationResult> {
        return invoke<TopicMutationResult>('create_or_update_topic', { request });
    }

    static async deleteTopic(request: DeleteTopicRequest): Promise<TopicMutationResult> {
        return invoke<TopicMutationResult>('delete_topic', { request });
    }

    static async deleteTopicByBroker(request: DeleteTopicByBrokerRequest): Promise<TopicMutationResult> {
        return invoke<TopicMutationResult>('delete_topic_by_broker', { request });
    }

    static async getTopicConsumerGroups(request: TopicQueryRequest): Promise<TopicConsumerGroupListResponse> {
        return invoke<TopicConsumerGroupListResponse>('get_topic_consumer_groups', { request });
    }

    static async getTopicConsumers(request: TopicQueryRequest): Promise<TopicConsumerInfoResponse> {
        return invoke<TopicConsumerInfoResponse>('get_topic_consumers', { request });
    }

    static async resetConsumerOffset(request: ResetOffsetRequest): Promise<TopicMutationResult> {
        return invoke<TopicMutationResult>('reset_consumer_offset', { request });
    }

    static async skipMessageAccumulate(request: ResetOffsetRequest): Promise<TopicMutationResult> {
        return invoke<TopicMutationResult>('skip_message_accumulate', { request });
    }

    static async sendTopicMessage(request: SendTopicMessageRequest): Promise<TopicSendMessageResult> {
        return invoke<TopicSendMessageResult>('send_topic_message', { request });
    }
}
