import { GlobalOverview } from '../../features/dashboard/components/GlobalOverview';
import React from 'react';
import { BrokerOverview } from '../../features/dashboard/components/BrokerOverview';
import { DashboardCharts } from '../../features/dashboard/components/Charts';

export const DashboardPage = () => {
  return (
    <div className="dashboard-page">
      <GlobalOverview />
      <BrokerOverview />
      <DashboardCharts />
    </div>
  );
};
