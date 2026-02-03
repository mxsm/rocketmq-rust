import React from 'react';
import { BrokerOverview } from '../../features/dashboard/components/BrokerOverview';
import { DashboardCharts } from '../../features/dashboard/components/Charts';

export const DashboardPage = () => {
  return (
    <div className="max-w-[1600px] mx-auto space-y-6 animate-in fade-in slide-in-from-bottom-4 duration-500">
      <BrokerOverview />
      <DashboardCharts />
    </div>
  );
};
