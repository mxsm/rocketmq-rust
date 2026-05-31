import React, { useMemo, type ReactNode } from 'react';
import { Activity, Calendar, Database, RefreshCw, Server, ShieldCheck } from 'lucide-react';
import { Button } from '../../../components/ui/LegacyButton';
import { useClusterCatalog } from '../../cluster/hooks/useClusterCatalog';

const formatNumber = (value: number) => value.toLocaleString();
const formatTps = (value: number) => value.toFixed(2);

const DashboardMetricCard = ({
  label,
  value,
  hint,
  tone,
  icon,
}: {
  label: string;
  value: string;
  hint: string;
  tone: 'success' | 'info' | 'violet' | 'warning';
  icon: ReactNode;
}) => (
  <article className={`dashboard-metric-card is-${tone}`}>
    <div className="dashboard-metric-copy">
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{hint}</small>
    </div>
    <div className="dashboard-metric-icon" aria-hidden="true">
      {icon}
    </div>
  </article>
);

export const BrokerOverview = () => {
  const { data, isLoading, isRefreshing, loadError, refresh } = useClusterCatalog();

  const brokers = data?.items ?? [];
  const summary = data?.summary;

  const brokerRows = useMemo(() => {
    return [...brokers]
      .sort((left, right) => right.todayReceivedTotal - left.todayReceivedTotal)
      .slice(0, 8);
  }, [brokers]);

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
    () => brokers.reduce((sum, item) => sum + item.produceTps + item.consumeTps, 0),
    [brokers]
  );

  const totals = useMemo(
    () =>
      brokers.reduce(
        (acc, item) => ({
          received: acc.received + item.todayReceivedTotal,
          produced: acc.produced + item.todayProduce,
        }),
        { received: 0, produced: 0 }
      ),
    [brokers]
  );

  const activeBrokerCount = summary?.activeBrokers ?? brokers.filter((item) => item.isActive).length;
  const totalBrokerCount = summary?.totalBrokers ?? brokers.length;
  const brokerHealthHint = `${summary?.inactiveBrokers ?? Math.max(totalBrokerCount - activeBrokerCount, 0)} inactive`;
  const topBroker = brokerRows[0];
  const transportState = `${data?.useVipChannel ? 'VIP on' : 'VIP off'} / ${data?.useTls ? 'TLS on' : 'TLS off'}`;

  const handleRefresh = async () => {
    try {
      await refresh();
    } catch (error) {
      console.error('Failed to refresh dashboard broker overview', error);
    }
  };

  return (
    <section className="dashboard-overview" aria-label="Broker overview">
      <div className="dashboard-kpi-grid">
        <DashboardMetricCard
          label="Broker health"
          value={`${activeBrokerCount} active`}
          hint={`${totalBrokerCount} total brokers, ${brokerHealthHint}`}
          tone="success"
          icon={<Server className="dashboard-icon" />}
        />
        <DashboardMetricCard
          label="Total throughput"
          value={formatTps(totalThroughput)}
          hint="Messages / sec"
          tone="info"
          icon={<Activity className="dashboard-icon" />}
        />
        <DashboardMetricCard
          label="Today's traffic"
          value={formatNumber(totals.received)}
          hint={`${formatNumber(totals.produced)} produced`}
          tone="violet"
          icon={<Database className="dashboard-icon" />}
        />
        <DashboardMetricCard
          label="Security posture"
          value={data?.useVipChannel ? 'VIP on' : 'VIP off'}
          hint={transportState}
          tone="warning"
          icon={<ShieldCheck className="dashboard-icon" />}
        />
      </div>

      <div className="dashboard-broker-layout">
        <div className="dashboard-panel dashboard-broker-panel">
          <div className="dashboard-panel-header">
            <div>
              <h2>Broker Overview</h2>
              <p>Sorted by received messages with address and production deltas.</p>
            </div>
            <Button variant="ghost" onClick={() => void handleRefresh()} disabled={isRefreshing}>
              <RefreshCw className={`ops-button-icon ${isRefreshing ? 'animate-spin' : ''}`} />
              Refresh
            </Button>
          </div>

          {loadError ? <div className="dashboard-alert is-error">{loadError}</div> : null}

          <div className="dashboard-table-scroll">
            <table className="dashboard-table">
              <thead>
                <tr>
                  <th>Broker Name</th>
                  <th>Address</th>
                  <th className="is-number">Today Received</th>
                  <th className="is-number">Produced</th>
                  <th className="is-number">Yesterday</th>
                </tr>
              </thead>
              <tbody>
                {brokerRows.length ? (
                  brokerRows.map((broker) => (
                    <tr
                      key={`${broker.clusterName}-${broker.brokerName}-${broker.brokerId}`}
                      className={broker.isActive ? 'is-active' : undefined}
                    >
                      <td>
                        <div className="dashboard-broker-name">
                          <span className={broker.isActive ? 'is-online' : 'is-offline'} />
                          <strong>{broker.brokerName}</strong>
                        </div>
                      </td>
                      <td className="dashboard-mono">{broker.address}</td>
                      <td className="is-number">{formatNumber(broker.todayReceivedTotal)}</td>
                      <td className="is-number">{formatNumber(broker.todayProduce)}</td>
                      <td className="is-number">{formatNumber(broker.yesterdayProduce)}</td>
                    </tr>
                  ))
                ) : (
                  <tr>
                    <td colSpan={5}>
                      <div className="dashboard-empty">
                        {isLoading ? 'Loading broker overview...' : 'No broker data available'}
                      </div>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        <aside className="dashboard-panel dashboard-runtime-panel" aria-label="Runtime snapshot">
          <div className="dashboard-panel-header">
            <div>
              <h2>Runtime Snapshot</h2>
              <p>Broker TPS and local clock stay visible beside the table.</p>
            </div>
          </div>

          <div className="dashboard-runtime-grid">
            <div className="dashboard-runtime-card">
              <Calendar className="dashboard-icon" />
              <span>Current Date</span>
              <strong>{todayDate}</strong>
            </div>
            <div className="dashboard-runtime-card">
              <Activity className="dashboard-icon" />
              <span>Total Throughput</span>
              <strong>{formatTps(totalThroughput)} msg/s</strong>
            </div>
          </div>

          <div className="dashboard-tps-highlight">
            <div>
              <span>Top broker</span>
              <strong>{topBroker ? `${topBroker.brokerName}-${topBroker.brokerId}` : 'No broker'}</strong>
              <small>{topBroker?.address ?? data?.currentNamesrv ?? 'No endpoint selected'}</small>
            </div>
            <div className="dashboard-tps-mini">
              <label>Producer TPS</label>
              <div className="dashboard-mini-track is-producer">
                <span style={{ width: topBroker ? `${Math.min(topBroker.produceTps * 10, 100)}%` : '0%' }} />
              </div>
              <label>Consumer TPS</label>
              <div className="dashboard-mini-track is-consumer">
                <span style={{ width: topBroker ? `${Math.min(topBroker.consumeTps * 10, 100)}%` : '0%' }} />
              </div>
            </div>
          </div>
        </aside>
      </div>
    </section>
  );
};
