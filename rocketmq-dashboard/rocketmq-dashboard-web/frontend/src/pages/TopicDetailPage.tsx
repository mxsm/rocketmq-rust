import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { topicApi } from '../api/topic_api';
import DataTable from '../components/DataTable';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import MetricCard from '../components/MetricCard';
import type { TopicRouteInfo, TopicStatsInfo } from '../types/topic';

export default function TopicDetailPage() {
  const { topic = '' } = useParams();
  const [route, setRoute] = useState<TopicRouteInfo | null>(null);
  const [stats, setStats] = useState<TopicStatsInfo | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    setError(null);
    Promise.all([topicApi.route(topic), topicApi.stats(topic)])
      .then(([routeData, statsData]) => {
        setRoute(routeData);
        setStats(statsData);
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, [topic]);

  if (loading) return <LoadingState label="Loading topic" />;
  if (error) return <ErrorState message={error} onRetry={load} />;

  return (
    <>
      <PageHeader title={topic} description="Topic route and queue statistics." />
      <div className="metric-grid">
        <MetricCard label="Queues" value={stats?.queueCount ?? 0} />
        <MetricCard label="Min Offset" value={stats?.totalMinOffset ?? 0} />
        <MetricCard label="Max Offset" value={stats?.totalMaxOffset ?? 0} />
        <MetricCard label="Brokers" value={route?.brokers.length ?? 0} />
      </div>
      <DataTable
        rows={route?.queues ?? []}
        columns={[
          { header: 'Broker', render: (row) => row.brokerName },
          { header: 'Read', render: (row) => row.readQueueNums },
          { header: 'Write', render: (row) => row.writeQueueNums },
          { header: 'Perm', render: (row) => <code>{row.perm}</code> }
        ]}
        getRowId={(row) => `${row.brokerName}-${row.readQueueNums}-${row.writeQueueNums}`}
        emptyTitle="No route queues"
        onRefresh={load}
      />
    </>
  );
}
