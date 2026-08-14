import {
  Activity,
  CalendarDays,
  Database,
  Layers,
  RadioTower,
  Send,
  Server,
  Users
} from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState, type ReactNode } from 'react';
import { Link } from 'react-router-dom';
import { dashboardApi } from '../api/dashboard_api';
import EmptyState from '../components/EmptyState';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import RankingTable from '../components/RankingTable';
import RefreshButton from '../components/RefreshButton';
import StatusBadge from '../components/StatusBadge';
import TrendAreaChart from '../components/TrendAreaChart';
import { buttonVariants } from '../components/ui/Button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/Card';
import type { DashboardHistorySeries, DashboardOverview, DashboardTopicCurrent } from '../types/dashboard';
import { buildDashboardAdvisories, formatDashboardMetric, sortTopTopics } from './dashboard/dashboard-model';

function today() {
  const now = new Date();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${now.getFullYear()}-${month}-${day}`;
}

function historyPoints(series: DashboardHistorySeries | null) {
  return (
    series?.points.map((point) => ({
      time: new Date(point.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      value: point.value
    })) ?? []
  );
}

function rejectionMessage(result: PromiseRejectedResult) {
  return result.reason instanceof Error ? result.reason.message : String(result.reason);
}

export default function DashboardPage() {
  const [overview, setOverview] = useState<DashboardOverview | null>(null);
  const [topicCurrent, setTopicCurrent] = useState<DashboardTopicCurrent | null>(null);
  const [brokerHistory, setBrokerHistory] = useState<DashboardHistorySeries | null>(null);
  const [topicHistory, setTopicHistory] = useState<DashboardHistorySeries | null>(null);
  const [brokerHistoryError, setBrokerHistoryError] = useState<string | null>(null);
  const [topicHistoryError, setTopicHistoryError] = useState<string | null>(null);
  const [historyLoading, setHistoryLoading] = useState(true);
  const [historyDate, setHistoryDate] = useState(today);
  const [selectedTopic, setSelectedTopic] = useState('');
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const hasCoreData = useRef(false);
  const latestRequest = useRef(0);

  const load = useCallback(async () => {
    const requestId = ++latestRequest.current;
    if (hasCoreData.current) setRefreshing(true);
    else setLoading(true);
    setError(null);
    setHistoryLoading(true);
    setBrokerHistory(null);
    setTopicHistory(null);
    setBrokerHistoryError(null);
    setTopicHistoryError(null);

    const [overviewResult, topicCurrentResult, brokerHistoryResult, topicHistoryResult] = await Promise.allSettled([
      dashboardApi.overview(),
      dashboardApi.topicCurrent(),
      dashboardApi.brokerHistory({ date: historyDate }),
      dashboardApi.topicHistory({ date: historyDate, topicName: selectedTopic || undefined })
    ]);

    if (requestId !== latestRequest.current) return;

    if (overviewResult.status === 'rejected') {
      setError(rejectionMessage(overviewResult));
      setHistoryLoading(false);
      setLoading(false);
      setRefreshing(false);
      return;
    }

    if (topicCurrentResult.status === 'rejected') {
      setError(rejectionMessage(topicCurrentResult));
      setHistoryLoading(false);
      setLoading(false);
      setRefreshing(false);
      return;
    }

    setOverview(overviewResult.value);
    setTopicCurrent(topicCurrentResult.value);
    hasCoreData.current = true;

    if (brokerHistoryResult.status === 'fulfilled') {
      setBrokerHistory(brokerHistoryResult.value);
      setBrokerHistoryError(null);
    } else {
      setBrokerHistory(null);
      setBrokerHistoryError(rejectionMessage(brokerHistoryResult));
    }

    if (topicHistoryResult.status === 'fulfilled') {
      setTopicHistory(topicHistoryResult.value);
      setTopicHistoryError(null);
    } else {
      setTopicHistory(null);
      setTopicHistoryError(rejectionMessage(topicHistoryResult));
    }

    setHistoryLoading(false);
    setLoading(false);
    setRefreshing(false);
  }, [historyDate, selectedTopic]);

  useEffect(() => {
    void load();
  }, [load]);

  const topTopics = useMemo(() => sortTopTopics(topicCurrent?.topTopics ?? []), [topicCurrent]);
  const advisories = useMemo(() => (overview ? buildDashboardAdvisories(overview) : []), [overview]);

  if (loading) return <LoadingState label="Loading dashboard" />;
  if (error) return <ErrorState message={error} onRetry={() => void load()} />;
  if (!overview || !topicCurrent) return <EmptyState title="Dashboard unavailable" />;

  const brokerHistoryData = historyPoints(brokerHistory);
  const topicHistoryData = historyPoints(topicHistory);
  const topicRankingRows = topTopics.map((topic) => ({
    name: topic.topic,
    value: topic.totalMsg,
    detail: `in ${formatDashboardMetric(topic.inTps)} / out ${formatDashboardMetric(topic.outTps)} TPS`
  }));

  return (
    <div className="operations-dashboard">
      <PageHeader
        title="Operations overview"
        description="Live RocketMQ inventory, demand signals, and collection history in one operational workspace."
        actions={
          <>
            <label className="dashboard-date-field">
              <CalendarDays size={15} aria-hidden="true" />
              <span className="sr-only">History date</span>
              <input
                aria-label="History date"
                type="date"
                value={historyDate}
                onChange={(event) => {
                  setHistoryLoading(true);
                  setBrokerHistory(null);
                  setTopicHistory(null);
                  setHistoryDate(event.target.value);
                }}
              />
            </label>
            <RefreshButton refreshing={refreshing} onRefresh={() => void load()} />
          </>
        }
      />

      <Card className="dashboard-health-rail" aria-label="Cluster health summary">
        <HealthSignal
          icon={<Activity size={16} />}
          label="System"
          value={overview.systemStatus}
          tone={overview.systemStatus.toUpperCase() === 'UP' ? 'success' : 'warning'}
        />
        <HealthSignal icon={<RadioTower size={16} />} label="Brokers" value={String(overview.brokerCount)} tone={overview.brokerCount > 0 ? 'success' : 'danger'} />
        <HealthSignal icon={<Layers size={16} />} label="Backlog" value={formatDashboardMetric(overview.messageBacklog)} tone={overview.messageBacklog > 0 ? 'warning' : 'success'} />
        <HealthSignal icon={<Database size={16} />} label="Topics" value={String(overview.topicCount)} />
        <HealthSignal icon={<Server size={16} />} label="NameServer" value={overview.currentNamesrv ?? 'Not configured'} tone={overview.currentNamesrv ? 'success' : 'danger'} />
      </Card>

      <div className="metric-grid dashboard-metrics dashboard-metrics-flat">
        <MetricCard label="Brokers" value={overview.brokerCount} detail="Visible broker instances" icon={<RadioTower size={18} />} />
        <MetricCard label="Topics" value={overview.topicCount} detail="Current route inventory" icon={<Database size={18} />} />
        <MetricCard label="Consumers" value={overview.consumerGroupCount} detail="Known consumer groups" icon={<Users size={18} />} />
        <MetricCard label="Backlog" value={formatDashboardMetric(overview.messageBacklog)} detail="Messages awaiting consumption" icon={<Layers size={18} />} />
        <MetricCard label="Producers" value={overview.producerCount} detail="Known producer groups" icon={<Send size={18} />} />
      </div>

      <div className="dashboard-main-grid">
        <Card className="dashboard-advisories">
          <CardHeader>
            <div>
              <CardTitle>Action center</CardTitle>
              <CardDescription>Evidence-based checks from the current API response.</CardDescription>
            </div>
            <StatusBadge status={advisories.length === 0 ? 'No active advisories' : `${advisories.length} active`} tone={advisories.length === 0 ? 'success' : 'warning'} />
          </CardHeader>
          <CardContent>
            {advisories.length === 0 ? (
              <EmptyState title="No active advisories" detail="The available dashboard signals do not require action." />
            ) : (
              <div className="advisory-list">
                {advisories.map((advisory) => (
                  <article className={`advisory-item advisory-${advisory.tone}`} key={advisory.id}>
                    <div>
                      <strong>{advisory.title}</strong>
                      <p>{advisory.detail}</p>
                    </div>
                    <Link className={buttonVariants({ variant: 'outline', size: 'sm' })} to={advisory.target}>
                      {advisory.actionLabel}
                    </Link>
                  </article>
                ))}
              </div>
            )}
          </CardContent>
        </Card>

        <Card className="dashboard-ranking-card">
          <CardHeader>
            <div>
              <CardTitle>Top topics</CardTitle>
              <CardDescription>Ranked by currently reported total messages.</CardDescription>
            </div>
            <StatusBadge status={`${topicCurrent.totalTopics} observed`} />
          </CardHeader>
          <CardContent>
            <RankingTable
              rows={topicRankingRows}
              valueLabel="Messages"
              accent="var(--primary)"
              emptyTitle="No topic ranking"
              emptyDetail="Topic metrics are not available for the current cluster."
              formatValue={formatDashboardMetric}
            />
          </CardContent>
        </Card>
      </div>

      <div className="dashboard-history-grid">
        <HistoryCard
          title="Broker activity"
          description="Collected broker count samples for the selected date."
          series={brokerHistory}
          error={brokerHistoryError}
          loading={historyLoading}
          errorTitle="Broker history unavailable"
          data={brokerHistoryData}
          color="var(--info)"
          label="Broker count"
        />

        <Card className="dashboard-history-card">
          <CardHeader>
            <div>
              <CardTitle>Topic activity</CardTitle>
              <CardDescription>
                {selectedTopic ? 'Collected total message samples for the selected topic.' : 'Topic count collected across the cluster.'}
              </CardDescription>
            </div>
            <select
              className="dashboard-topic-select"
              aria-label="Topic history filter"
              value={selectedTopic}
              onChange={(event) => {
                setHistoryLoading(true);
                setTopicHistory(null);
                setSelectedTopic(event.target.value);
              }}
            >
              <option value="">All topics</option>
              {topTopics.map((topic) => (
                <option key={topic.topic} value={topic.topic}>{topic.topic}</option>
              ))}
            </select>
          </CardHeader>
          <CardContent>
            {historyLoading ? (
              <LoadingState label="Loading topic history" />
            ) : topicHistoryError ? (
              <HistoryUnavailable title="Topic history unavailable" detail={topicHistoryError} />
            ) : topicHistory?.collected ? (
              <TrendAreaChart
                data={topicHistoryData}
                color="var(--primary)"
                label={selectedTopic ? 'Total messages' : 'Topic count'}
                emptyTitle="No topic samples"
                emptyDetail="No collected topic points match these filters."
              />
            ) : (
              <EmptyState title="Topic history is warming up" detail="The collector has not stored a sample for these filters yet." />
            )}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

interface HealthSignalProps {
  icon: ReactNode;
  label: string;
  value: string;
  tone?: 'success' | 'warning' | 'danger';
}

function HealthSignal({ icon, label, value, tone }: HealthSignalProps) {
  return (
    <div className={`health-signal${tone ? ` health-${tone}` : ''}`}>
      <span className="health-signal-icon" aria-hidden="true">{icon}</span>
      <span>{label}</span>
      <strong title={value}>{value}</strong>
    </div>
  );
}

interface HistoryCardProps {
  title: string;
  description: string;
  series: DashboardHistorySeries | null;
  error: string | null;
  loading: boolean;
  errorTitle: string;
  data: Array<{ time: string; value: number }>;
  color: string;
  label: string;
}

function HistoryCard({ title, description, series, error, loading, errorTitle, data, color, label }: HistoryCardProps) {
  return (
    <Card className="dashboard-history-card">
      <CardHeader>
        <div>
          <CardTitle>{title}</CardTitle>
          <CardDescription>{description}</CardDescription>
        </div>
        <StatusBadge
          status={loading ? 'Loading' : error ? 'Unavailable' : series?.collected ? 'Collected' : 'Warming up'}
          tone={loading ? undefined : error ? 'danger' : series?.collected ? 'success' : 'warning'}
        />
      </CardHeader>
      <CardContent>
        {loading ? (
          <LoadingState label={`Loading ${title.toLowerCase()}`} />
        ) : error ? (
          <HistoryUnavailable title={errorTitle} detail={error} />
        ) : series?.collected ? (
          <TrendAreaChart
            data={data}
            color={color}
            label={label}
            emptyTitle="No samples"
            emptyDetail="No collected points match the selected date."
          />
        ) : (
          <EmptyState title={`${title} is warming up`} detail="The collector has not stored a sample for this date yet." />
        )}
      </CardContent>
    </Card>
  );
}

function HistoryUnavailable({ title, detail }: { title: string; detail: string }) {
  return (
    <div className="history-unavailable" role="status" aria-label={title} aria-live="polite">
      <Activity size={22} aria-hidden="true" />
      <strong>{title}</strong>
      <span>{detail}</span>
    </div>
  );
}
