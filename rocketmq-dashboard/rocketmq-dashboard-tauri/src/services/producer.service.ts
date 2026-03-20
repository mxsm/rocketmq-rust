import { invoke } from '@tauri-apps/api/core';
import type {
    ProducerConnectionQueryRequest,
    ProducerConnectionView,
    ProducerTopicOptionsRequest,
    ProducerTopicOptionsView,
} from '../features/producer/types/producer.types';

export class ProducerService {
    static async getProducerTopicOptions(
        request: ProducerTopicOptionsRequest = {},
    ): Promise<ProducerTopicOptionsView> {
        return invoke<ProducerTopicOptionsView>('get_producer_topic_options', { request });
    }

    static async queryProducerConnections(
        request: ProducerConnectionQueryRequest,
    ): Promise<ProducerConnectionView> {
        return invoke<ProducerConnectionView>('query_producer_connections', { request });
    }
}
