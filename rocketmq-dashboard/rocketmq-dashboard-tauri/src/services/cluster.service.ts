import { invokeAuthenticatedCommand } from './invoke';
import type {
    ClusterBrokerConfigRequest,
    BrokerConfigUpdateRequest,
    BrokerConfigUpdateResult,
    ClusterBrokerConfigView,
    ClusterHomePageRequest,
    ClusterHomePageResponse,
    ClusterBrokerStatusRequest,
    ClusterBrokerStatusView,
} from '../features/cluster/types/cluster.types';

export class ClusterService {
    static async updateBrokerConfig(request: BrokerConfigUpdateRequest): Promise<BrokerConfigUpdateResult> {
        return invokeAuthenticatedCommand<BrokerConfigUpdateResult>('update_cluster_broker_config', { request });
    }

    static async getClusterHomePage(
        request: ClusterHomePageRequest = { forceRefresh: false }
    ): Promise<ClusterHomePageResponse> {
        return invokeAuthenticatedCommand<ClusterHomePageResponse>('get_cluster_home_page', { request });
    }

    static async getClusterBrokerConfig(
        request: ClusterBrokerConfigRequest
    ): Promise<ClusterBrokerConfigView> {
        return invokeAuthenticatedCommand<ClusterBrokerConfigView>('get_cluster_broker_config', { request });
    }

    static async getClusterBrokerStatus(
        request: ClusterBrokerStatusRequest
    ): Promise<ClusterBrokerStatusView> {
        return invokeAuthenticatedCommand<ClusterBrokerStatusView>('get_cluster_broker_status', { request });
    }
}
