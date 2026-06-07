import {
  Activity,
  CalendarDays,
  Database,
  Layers,
  RadioTower,
  RefreshCw,
  Send,
  Server,
  Users,
  Zap
} from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { brokerApi } from '../api/broker_api';
import { dashboardApi } from '../api/dashboard_api';
import EmptyState from '../components/EmptyState';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import RankingTable from '../components/RankingTable';
import StatusBadge from '../components/StatusBadge';
import TrendAreaChart from '../components/TrendAreaChart';
import type { BrokerListView, BrokerRuntimeStats } from '../types/broker';
import type { DashboardHistorySeries, DashboardOverview, DashboardTopicCurrent } from '../types/dashboard';

function today() {
  const now = new Date();
  const month = String(now.getMonth() + 1).padStart(2, '0');
  const day = String(now.getDate()).padStart(2, '0');
  return `${now.getFullYear()}-${month}-${day}`;
}

function formatNumber(value: number) {
  return new Intl.NumberFormat().format(Math.round(value));
}

interface HistoryChartPoint {
  time: string;
  value: number;
}

type BrokerRuntimeEntries = BrokerRuntimeStats['entries'];

interface BrokerOverviewRow {
  brokerName: string;
  address: string;
  totalMessagesReceivedToday: number;
  todayProduceCount: number;
  yesterdayProduceCount: number;
}

function historyPoints(series: DashboardHistorySeries | null): HistoryChartPoint[] {
  return (
    series?.points.map((point) => ({
      time: new Date(point.timestamp).toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' }),
      value: point.value
    })) ?? []
  );
}

function runtimeNumber(entries: BrokerRuntimeEntries | undefined, key: string) {
  const value = entries?.[key];
  if (value === undefined || value === null) {
    return 0;
  }
  const parsed = Number.parseFloat(String(value).split(/\s+/)[0]);
  return Number.isFinite(parsed) ? parsed : 0;
}

export default function DashboardPage() {
  const [overview, setOverview] = useState<DashboardOverview | null>(null);
  const [topicCurrent, setTopicCurrent] = useState<DashboardTopicCurrent | null>(null);
  const [brokerList, setBrokerList] = useState<BrokerListView | null>(null);
  const [brokerRuntimeEntriesByName, setBrokerRuntimeEntriesByName] = useState<Record<string, BrokerRuntimeEntries>>({});
  const [brokerHistory, setBrokerHistory] = useState<DashboardHistorySeries | null>(null);
  const [topicHistory, setTopicHistory] = useState<DashboardHistorySeries | null>(null);
  const [historyDate, setHistoryDate] = useState(today);
  const [selectedTopic, setSelectedTopic] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    setError(null);
    Promise.all([
      dashboardApi.overview(),
      dashboardApi.topicCurrent(),
      brokerApi.list(),
      dashboardApi.brokerHistory({ date: historyDate }),
      dashboardApi.topicHistory({ date: historyDate, topicName: selectedTopic || undefined })
    ])
      .then(async ([overviewData, topicData, brokerData, brokerHistoryData, topicHistoryData]) => {
        const runtimeResults = await Promise.allSettled(brokerData.items.map((broker) => brokerApi.runtime(broker.brokerName)));
        const nextRuntimeEntries = brokerData.items.reduce<Record<string, BrokerRuntimeEntries>>((entriesByName, broker, index) => {
          const result = runtimeResults[index];
          entriesByName[broker.brokerName] = result?.status === 'fulfilled' ? result.value.entries : {};
          return entriesByName;
        }, {});

        setOverview(overviewData);
        setTopicCurrent(topicData);
        setBrokerList(brokerData);
        setBrokerRuntimeEntriesByName(nextRuntimeEntries);
        setBrokerHistory(brokerHistoryData);
        setTopicHistory(topicHistoryData);
        if (!selectedTopic && topicData.topTopics[0]) {
          setSelectedTopic(topicData.topTopics[0].topic);
        }
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, [historyDate, selectedTopic]);

  const brokerTopData = useMemo(
    () =>
      (brokerList?.items ?? [])
        .map((broker) => ({
          name: broker.brokerName,
          totalTps: Number((broker.produceTps + broker.consumeTps).toFixed(2)),
          produceTps: Number(broker.produceTps.toFixed(2)),
          consumeTps: Number(broker.consumeTps.toFixed(2))
        }))
        .sort((left, right) => right.totalTps - left.totalTps)
        .slice(0, 10),
    [brokerList]
  );

  const topicTopData = useMemo(
    () =>
      (topicCurrent?.topTopics ?? [])
        .map((topic) => ({
          name: topic.topic,
          totalMsg: topic.totalMsg,
          inTps: Number(topic.inTps.toFixed(2)),
          outTps: Number(topic.outTps.toFixed(2))
        }))
        .sort((left, right) => right.totalMsg - left.totalMsg)
        .slice(0, 10),
    [topicCurrent]
  );

  if (loading) {
    return <LoadingState label="Loading dashboard" />;
  }

  if (error) {
    return <ErrorState message={error} onRetry={load} />;
  }

  if (!overview || !topicCurrent || !brokerList) {
    return <EmptyState title="Dashboard unavailable" />;
  }

  const brokerHistoryData = historyPoints(brokerHistory);
  const topicHistoryData = historyPoints(topicHistory);
  const brokerOverviewRows: BrokerOverviewRow[] = brokerList.items.map((broker) => {
    const entries = brokerRuntimeEntriesByName[broker.brokerName];
    const todayProduceMorning = runtimeNumber(entries, 'msgPutTotalTodayMorning');
    const yesterdayProduceMorning = runtimeNumber(entries, 'msgPutTotalYesterdayMorning');

    return {
      brokerName: broker.brokerName,
      address: broker.address,
      totalMessagesReceivedToday: runtimeNumber(entries, 'msgGetTotalTodayNow'),
      todayProduceCount: todayProduceMorning,
      yesterdayProduceCount: todayProduceMorning - yesterdayProduceMorning
    };
  });
  const brokerRankingRows = brokerTopData.map((broker) => ({
    name: broker.name,
    value: broker.totalTps,
    detail: `produce ${broker.produceTps} / consume ${broker.consumeTps}`
  }));
  const topicRankingRows = topicTopData.map((topic) => ({
    name: topic.name,
    value: topic.totalMsg,
    detail: `in ${topic.inTps} / out ${topic.outTps}`
  }));

  return (
    <>
      <PageHeader
        title="Dashboard"
        description="Broker overview, traffic top lists, and 5-minute trends from the Rust Web backend."
        actions={
          <>
            <label className="dashboard-filter" title="History date">
              <CalendarDays size={15} aria-hidden="true" />
              <input type="date" value={historyDate} onChange={(event) => setHistoryDate(event.target.value)} />
            </label>
            <StatusBadge status={overview.systemStatus} tone={overview.systemStatus === 'UP' ? 'success' : 'warning'} />
            <button type="button" className="icon-button" title="Refresh dashboard" onClick={load}>
              <RefreshCw size={15} aria-hidden="true" />
            </button>
          </>
        }
      />

      <div className="metric-grid dashboard-metrics">
        <MetricCard label="Brokers" value={overview.brokerCount} detail="GET /api/brokers" icon={<RadioTower size={18} />} />
        <MetricCard label="Topics" value={overview.topicCount} detail="topic-current" icon={<Database size={18} />} />
        <MetricCard label="Consumers" value={overview.consumerGroupCount} detail="group count" icon={<Users size={18} />} />
        <MetricCard label="Backlog" value={formatNumber(overview.messageBacklog)} detail="consumer lag" icon={<Layers size={18} />} />
        <MetricCard label="Producers" value={overview.producerCount} detail="connections" icon={<Send size={18} />} />
      </div>

      <div className="dashboard-overview-grid">
        <section className="panel dashboard-panel dashboard-panel-table">
          <div className="panel-heading">
            <div>
              <h2>Broker Overview</h2>
              <p>Broker address and daily message counters aligned with the Java dashboard overview.</p>
            </div>
            <StatusBadge status={`${brokerList.total} brokers`} tone={brokerList.total > 0 ? 'success' : 'neutral'} />
          </div>
          <BrokerOverviewTable rows={brokerOverviewRows} />
        </section>

        <section className="panel dashboard-panel">
          <div className="panel-heading">
            <div>
              <h2>Dashboard Window</h2>
              <p>History queries use broker/topic 5-minute collector samples.</p>
            </div>
            <StatusBadge status={overview.currentNamesrv ?? 'No NameServer'} tone={overview.currentNamesrv ? 'success' : 'warning'} />
          </div>
          <div className="dashboard-signal-grid">
            <div className="dashboard-signal">
              <Server size={18} aria-hidden="true" />
              <span>NameServer</span>
              <strong>{overview.currentNamesrv ?? 'unconfigured'}</strong>
            </div>
            <div className="dashboard-signal">
              <Activity size={18} aria-hidden="true" />
              <span>History</span>
              <strong>{brokerHistory?.collected || topicHistory?.collected ? 'collecting' : 'warming up'}</strong>
            </div>
            <div className="dashboard-signal">
              <Zap size={18} aria-hidden="true" />
              <span>Admin facade</span>
              <strong>ready</strong>
            </div>
          </div>
        </section>
      </div>

      <div className="dashboard-chart-grid">
        <section className="panel dashboard-panel">
          <div className="panel-heading">
            <div>
              <h2>Broker TOP 10</h2>
              <p>Ranked by produce TPS + consume TPS.</p>
            </div>
            <StatusBadge status="Total TPS" />
          </div>
          <RankingTable
            rows={brokerRankingRows}
            valueLabel="TPS"
            accent="var(--primary)"
            emptyTitle="No broker traffic"
            emptyDetail="Broker TPS is currently zero, so no TOP ranking is shown."
            formatValue={formatNumber}
          />
        </section>

        <section className="panel dashboard-panel">
          <div className="panel-heading">
            <div>
              <h2>Broker 5min trend</h2>
              <p>Broker count trend collected by the Rust history task.</p>
            </div>
            <StatusBadge status={brokerHistory?.collected ? 'collected' : 'warming up'} tone={brokerHistory?.collected ? 'success' : 'warning'} />
          </div>
          <TrendAreaChart
            data={brokerHistoryData}
            color="var(--info)"
            label="Broker count"
            emptyTitle="No broker trend yet"
            emptyDetail="The collector needs at least one sample for the selected date."
          />
        </section>

        <section className="panel dashboard-panel">
          <div className="panel-heading">
            <div>
              <h2>Topic TOP 10</h2>
              <p>Ranked by current total messages.</p>
            </div>
            <StatusBadge status={`${topicCurrent.totalTopics} observed`} />
          </div>
          <RankingTable
            rows={topicRankingRows}
            valueLabel="TotalMsg"
            accent="var(--accent)"
            emptyTitle="No topic ranking"
            emptyDetail="Topics with zero messages are hidden from TOP ranking."
            formatValue={formatNumber}
          />
        </section>

        <section className="panel dashboard-panel">
          <div className="panel-heading">
            <div>
              <h2>Topic 5min trend</h2>
              <p>Select topic, then query `/api/dashboard/topics/history`.</p>
            </div>
            <select className="dashboard-select" value={selectedTopic} onChange={(event) => setSelectedTopic(event.target.value)}>
              <option value="">All topics</option>
              {topicCurrent.topTopics.map((topic) => (
                <option key={topic.topic} value={topic.topic}>
                  {topic.topic}
                </option>
              ))}
            </select>
          </div>
          <TrendAreaChart
            data={topicHistoryData}
            color="var(--accent)"
            label="TotalMsg"
            emptyTitle="No topic trend yet"
            emptyDetail="The collector needs samples for the selected date and topic."
          />
        </section>
      </div>
    </>
  );
}

function BrokerOverviewTable({ rows }: { rows: BrokerOverviewRow[] }) {
  if (rows.length === 0) {
    return <EmptyState title="No broker data" />;
  }

  return (
    <div className="broker-overview-table-shell">
      <div className="table-scroll">
        <table className="broker-overview-table">
          <thead>
            <tr>
              <th>Broker name</th>
              <th>Broker address</th>
              <th>Total messages received today</th>
              <th>Today Produce Count</th>
              <th>Yesterday Produce Count</th>
            </tr>
          </thead>
          <tbody>
            {rows.map((row) => (
              <tr key={`${row.brokerName}-${row.address}`}>
                <td>
                  <code>{row.brokerName}</code>
                </td>
                <td>
                  <code>{row.address}</code>
                </td>
                <td>{formatNumber(row.totalMessagesReceivedToday)}</td>
                <td>{formatNumber(row.todayProduceCount)}</td>
                <td>{formatNumber(row.yesterdayProduceCount)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
