import { invoke } from '@tauri-apps/api/core';
import type {
    ClusterBrokerConfigRequest,
    ClusterBrokerConfigView,
    ClusterHomePageRequest,
    ClusterHomePageResponse,
    ClusterBrokerStatusRequest,
    ClusterBrokerStatusView,
} from '../features/cluster/types/cluster.types';

export class ClusterService {
    static async getClusterHomePage(
        request: ClusterHomePageRequest = { forceRefresh: false }
    ): Promise<ClusterHomePageResponse> {
        return invoke<ClusterHomePageResponse>('get_cluster_home_page', { request });
    }

    static async getClusterBrokerConfig(
        request: ClusterBrokerConfigRequest
    ): Promise<ClusterBrokerConfigView> {
        return invoke<ClusterBrokerConfigView>('get_cluster_broker_config', { request });
    }

    static async getClusterBrokerStatus(
        request: ClusterBrokerStatusRequest
    ): Promise<ClusterBrokerStatusView> {
        return invoke<ClusterBrokerStatusView>('get_cluster_broker_status', { request });
    }
}
