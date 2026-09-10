import { invokeAuthenticatedCommand } from './invoke';
import type {
    DashboardBrokerOverviewRequest,
    DashboardOverview,
    DashboardBrokerOverviewResponse,
    DashboardTopicCurrentResponse,
} from '../features/dashboard/types/dashboard.types';

export class DashboardService {
    static getOverview(): Promise<DashboardOverview> {
        return invokeAuthenticatedCommand('get_dashboard_overview');
    }

    static async getBrokerOverview(
        request: DashboardBrokerOverviewRequest = { forceRefresh: false }
    ): Promise<DashboardBrokerOverviewResponse> {
        return invokeAuthenticatedCommand<DashboardBrokerOverviewResponse>('get_dashboard_broker_overview', { request });
    }

    static async queryTopicCurrent(): Promise<DashboardTopicCurrentResponse> {
        return invokeAuthenticatedCommand<DashboardTopicCurrentResponse>('query_dashboard_topic_current');
    }
}
