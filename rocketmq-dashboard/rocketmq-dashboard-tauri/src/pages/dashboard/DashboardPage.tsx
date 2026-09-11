import { HistoryChart } from '../../features/dashboard/components/HistoryChart';
import { GlobalOverview } from '../../features/dashboard/components/GlobalOverview';
import { BrokerOverview } from '../../features/dashboard/components/BrokerOverview';
import { DashboardCharts } from '../../features/dashboard/components/Charts';
import { useDashboardData } from '../../features/dashboard/hooks/useDashboardData';
import '../../features/dashboard/dashboard.css';

export const DashboardPage = () => {
    const data = useDashboardData();
    return <div className="ops-dashboard">
        <GlobalOverview overview={data.overview} brokers={data.brokers} />
        <BrokerOverview brokers={data.brokers} />
        <HistoryChart data={data} />
        <DashboardCharts brokers={data.brokers} topics={data.topicCharts} />
    </div>;
};
