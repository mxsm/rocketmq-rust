import { invoke } from '@tauri-apps/api/core';
import type {
    DashboardBrokerOverviewRequest,
    DashboardBrokerOverviewResponse,
    DashboardTopicCurrentResponse,
} from '../features/dashboard/types/dashboard.types';

export class DashboardService {
    static async getBrokerOverview(
        request: DashboardBrokerOverviewRequest = { forceRefresh: false }
    ): Promise<DashboardBrokerOverviewResponse> {
        return invoke<DashboardBrokerOverviewResponse>('get_dashboard_broker_overview', { request });
    }

    static async queryTopicCurrent(): Promise<DashboardTopicCurrentResponse> {
        return invoke<DashboardTopicCurrentResponse>('query_dashboard_topic_current');
    }
}
