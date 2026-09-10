import { invokeAuthenticatedCommand } from './invoke';
import type {
    ProducerConnectionQueryRequest,
    ProducerGroupItem,
    ProducerConnectionView,
    ProducerTopicOptionsRequest,
    ProducerTopicOptionsView,
} from '../features/producer/types/producer.types';

export class ProducerService {
    static async listProducerGroups(): Promise<ProducerGroupItem[]> {
        return invokeAuthenticatedCommand<ProducerGroupItem[]>('list_producer_groups');
    }

    static async getProducerTopicOptions(
        request: ProducerTopicOptionsRequest = {},
    ): Promise<ProducerTopicOptionsView> {
        return invokeAuthenticatedCommand<ProducerTopicOptionsView>('get_producer_topic_options', { request });
    }

    static async queryProducerConnections(
        request: ProducerConnectionQueryRequest,
    ): Promise<ProducerConnectionView> {
        return invokeAuthenticatedCommand<ProducerConnectionView>('query_producer_connections', { request });
    }
}
