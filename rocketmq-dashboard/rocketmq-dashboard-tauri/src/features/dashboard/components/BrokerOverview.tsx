import React, { useMemo } from 'react';
import { Card } from '../../../components/ui/LegacyCard';
import { Activity, Calendar, RefreshCw } from 'lucide-react';
import { Button } from '../../../components/ui/LegacyButton';
import { useClusterCatalog } from '../../cluster/hooks/useClusterCatalog';

const formatNumber = (value: number) => value.toLocaleString();

export const BrokerOverview = () => {
  const { data, isLoading, isRefreshing, loadError, refresh } = useClusterCatalog();

  const brokerRows = useMemo(() => {
    return [...(data?.items ?? [])]
      .sort((left, right) => right.todayReceivedTotal - left.todayReceivedTotal)
      .slice(0, 8);
  }, [data?.items]);

  const todayDate = useMemo(
    () =>
      new Intl.DateTimeFormat('en-CA', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
      }).format(new Date()),
    []
  );

  const totalThroughput = useMemo(
    () => (data?.items ?? []).reduce((sum, item) => sum + item.produceTps + item.consumeTps, 0),
    [data?.items]
  );

  const handleRefresh = async () => {
    try {
      await refresh();
    } catch (error) {
      console.error('Failed to refresh dashboard broker overview', error);
    }
  };

  return (
    <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
      <div className="lg:col-span-2">
        <Card
          title="Broker Overview"
          className="h-full"
          headerAction={
            <Button variant="ghost" onClick={() => void handleRefresh()} disabled={isRefreshing}>
              <RefreshCw className={`w-4 h-4 mr-2 ${isRefreshing ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
          }
        >
          {loadError ? (
            <div className="rounded-xl border border-red-200 bg-red-50/80 dark:border-red-900/40 dark:bg-red-950/20 px-4 py-3 text-sm text-red-700 dark:text-red-300">
              {loadError}
            </div>
          ) : null}
          <div className="overflow-x-auto">
            <table className="w-full text-sm text-left">
              <thead className="text-xs text-gray-500 uppercase bg-gray-50/50 border-b border-gray-100 dark:border-gray-800">
                <tr>
                  <th className="px-4 py-3 font-medium bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-200">Broker Name</th>
                  <th className="px-4 py-3 font-medium bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-200">Address</th>
                  <th className="px-4 py-3 font-medium text-right bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-200">Today Received</th>
                  <th className="px-4 py-3 font-medium text-right bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-200">Today Produced</th>
                  <th className="px-4 py-3 font-medium text-right bg-gray-100 dark:bg-gray-800 text-gray-900 dark:text-gray-200">Yesterday</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-gray-100 dark:divide-gray-800">
                {brokerRows.length ? (
                  brokerRows.map((broker) => (
                    <tr key={`${broker.clusterName}-${broker.brokerName}-${broker.brokerId}`} className="hover:bg-gray-50 dark:hover:bg-gray-800 transition-colors">
                      <td className="px-4 py-3 font-medium text-gray-900 dark:text-gray-100">
                        {broker.brokerName}
                      </td>
                      <td className="px-4 py-3 font-mono text-gray-600 dark:text-gray-400">{broker.address}</td>
                      <td className="px-4 py-3 text-right font-medium text-blue-600 dark:text-blue-400">
                        {formatNumber(broker.todayReceivedTotal)}
                      </td>
                      <td className="px-4 py-3 text-right dark:text-gray-300">
                        {formatNumber(broker.todayProduce)}
                      </td>
                      <td className="px-4 py-3 text-right text-gray-500 dark:text-gray-500">
                        {formatNumber(broker.yesterdayProduce)}
                      </td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5} className="px-4 py-10 text-center text-sm text-gray-500 dark:text-gray-400">
                      {isLoading ? 'Loading broker overview...' : 'No broker data available'}
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </Card>
      </div>

      <div className="space-y-6">
         <div className="bg-white dark:bg-gray-900 p-6 rounded-2xl border border-gray-100 dark:border-gray-800 shadow-sm flex items-center justify-between">
            <div>
              <h3 className="text-sm font-medium text-gray-500 dark:text-gray-400">Current Date</h3>
              <p className="text-xl font-bold text-gray-900 dark:text-white mt-1">{todayDate}</p>
            </div>
            <div className="w-10 h-10 rounded-xl bg-blue-50 dark:bg-blue-900/30 flex items-center justify-center text-blue-600 dark:text-blue-400">
              <Calendar className="w-5 h-5" />
            </div>
         </div>
         
         <div className="bg-white dark:bg-gray-900 rounded-2xl p-6 border border-gray-100 dark:border-gray-800 shadow-sm">
            <div className="flex items-center space-x-2 mb-4 text-gray-500 dark:text-gray-400">
                <Activity className="w-4 h-4 text-blue-500" />
                <span className="text-sm font-medium">Total Throughput</span>
            </div>
            <div className="text-3xl font-bold tracking-tight text-gray-900 dark:text-white">{totalThroughput.toFixed(2)}</div>
            <div className="text-sm text-gray-500 dark:text-gray-400 mt-1">Messages / sec</div>
         </div>
      </div>
    </div>
  );
};
