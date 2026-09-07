import { invokeAuthenticatedCommand } from './invoke';
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
        return invokeAuthenticatedCommand<ProducerTopicOptionsView>('get_producer_topic_options', { request });
    }

    static async queryProducerConnections(
        request: ProducerConnectionQueryRequest,
    ): Promise<ProducerConnectionView> {
        return invokeAuthenticatedCommand<ProducerConnectionView>('query_producer_connections', { request });
    }
}
