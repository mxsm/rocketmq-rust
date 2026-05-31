import React, { useEffect, useMemo, useState, type ReactNode } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import {
  Activity,
  AlertTriangle,
  ArrowDownCircle,
  ArrowUpCircle,
  Check,
  ChevronDown,
  Clock,
  Crown,
  Database,
  Gauge,
  GitBranch,
  HardDrive,
  Layers3,
  Link2,
  Network,
  RefreshCw,
  Server,
  ShieldCheck,
} from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { Button } from '../components/ui/LegacyButton';
import { SideSheet } from './ui/SideSheet';
import { useClusterCatalog } from '../features/cluster/hooks/useClusterCatalog';
import type { ClusterBrokerCardItem } from '../features/cluster/types/cluster.types';

const formatNumber = (value: number) => value.toLocaleString();
const formatTps = (value: number) => value.toFixed(2);

const ClusterSummaryCard = ({
  label,
  value,
  hint,
  tone,
  icon,
}: {
  label: string;
  value: string;
  hint: string;
  tone: 'info' | 'violet' | 'success' | 'warning';
  icon: ReactNode;
}) => (
  <article className={`cluster-summary-card is-${tone}`}>
    <div>
      <span>{label}</span>
      <strong>{value}</strong>
      <small>{hint}</small>
    </div>
    <div className="cluster-summary-icon" aria-hidden="true">
      {icon}
    </div>
  </article>
);

const BrokerTpsTile = ({
  label,
  value,
  percent,
  tone,
  icon,
}: {
  label: string;
  value: number;
  percent: number;
  tone: 'produce' | 'consume';
  icon: ReactNode;
}) => (
  <div className={`cluster-tps-tile is-${tone}`}>
    <div className="cluster-tps-tile-header">
      <span>{label}</span>
      {icon}
    </div>
    <strong>{formatTps(value)}</strong>
    <div className="cluster-tps-track">
      <span style={{ width: `${Math.min(Math.max(percent, 0), 100)}%` }} />
    </div>
  </div>
);

export const ClusterView = () => {
  const {
    data,
    isLoading,
    isRefreshing,
    loadError,
    pendingConfigAddr,
    pendingStatusAddr,
    refresh,
    getBrokerConfig,
    getBrokerStatus,
  } = useClusterCatalog();
  const [selectedCluster, setSelectedCluster] = useState('');
  const [isSelectOpen, setIsSelectOpen] = useState(false);
  const [detailSheet, setDetailSheet] = useState<{
    isOpen: boolean;
    type: 'Status' | 'Config' | null;
    title: string;
    data: Record<string, string>;
  }>({
    isOpen: false,
    type: null,
    title: '',
    data: {},
  });

  useEffect(() => {
    if (!data?.clusters.length) {
      setSelectedCluster('');
      return;
    }

    setSelectedCluster((previous) =>
      previous && data.clusters.includes(previous) ? previous : data.clusters[0]
    );
  }, [data?.clusters]);

  const visibleCluster = selectedCluster || data?.clusters[0] || '';

  const clusterData = useMemo(
    () => (data?.items ?? []).filter((broker) => broker.clusterName === visibleCluster),
    [data?.items, visibleCluster]
  );

  const sortedClusterData = useMemo(
    () =>
      [...clusterData].sort((left, right) => {
        const leftMaster = left.role.toUpperCase() === 'MASTER' ? 1 : 0;
        const rightMaster = right.role.toUpperCase() === 'MASTER' ? 1 : 0;
        if (leftMaster !== rightMaster) {
          return rightMaster - leftMaster;
        }

        if (left.isActive !== right.isActive) {
          return Number(right.isActive) - Number(left.isActive);
        }

        return left.brokerName.localeCompare(right.brokerName);
      }),
    [clusterData]
  );

  const clusterMetrics = useMemo(
    () =>
      clusterData.reduce(
        (acc, broker) => ({
          produceTps: acc.produceTps + broker.produceTps,
          consumeTps: acc.consumeTps + broker.consumeTps,
          todayProduce: acc.todayProduce + broker.todayProduce,
          todayConsume: acc.todayConsume + broker.todayConsume,
          yesterdayProduce: acc.yesterdayProduce + broker.yesterdayProduce,
          yesterdayConsume: acc.yesterdayConsume + broker.yesterdayConsume,
          masters: acc.masters + (broker.role.toUpperCase() === 'MASTER' ? 1 : 0),
          slaves: acc.slaves + (broker.role.toUpperCase() === 'SLAVE' ? 1 : 0),
          active: acc.active + (broker.isActive ? 1 : 0),
          errors: acc.errors + (broker.statusLoadError ? 1 : 0),
        }),
        {
          produceTps: 0,
          consumeTps: 0,
          todayProduce: 0,
          todayConsume: 0,
          yesterdayProduce: 0,
          yesterdayConsume: 0,
          masters: 0,
          slaves: 0,
          active: 0,
          errors: 0,
        }
      ),
    [clusterData]
  );

  const allSystemsOperational =
    clusterData.length > 0 && clusterData.every((broker) => broker.isActive && !broker.statusLoadError);

  const strongestBroker = useMemo(
    () =>
      sortedClusterData.reduce<ClusterBrokerCardItem | null>((selected, broker) => {
        if (!selected) {
          return broker;
        }

        const selectedTps = selected.produceTps + selected.consumeTps;
        const currentTps = broker.produceTps + broker.consumeTps;
        return currentTps > selectedTps ? broker : selected;
      }, null),
    [sortedClusterData]
  );

  const maxBrokerTps = Math.max(
    ...clusterData.flatMap((broker) => [broker.produceTps, broker.consumeTps]),
    1
  );

  const clusterHealthTone = allSystemsOperational ? 'is-healthy' : clusterData.length ? 'is-warning' : 'is-muted';
  const clusterHealthLabel = allSystemsOperational
    ? 'All systems operational'
    : clusterData.length
      ? 'Cluster requires attention'
      : loadError
        ? 'Unable to load cluster data'
        : 'No brokers available';

  const openStatusSheet = async (brokerData: ClusterBrokerCardItem) => {
    setDetailSheet({
      isOpen: true,
      type: 'Status',
      title: `Status [${brokerData.brokerName}][${brokerData.brokerId}]`,
      data: {
        brokerAddr: brokerData.address,
        state: 'Loading broker status...',
      },
    });

    try {
      const status = await getBrokerStatus(brokerData.address);
      setDetailSheet({
        isOpen: true,
        type: 'Status',
        title: `Status [${brokerData.brokerName}][${brokerData.brokerId}]`,
        data: {
          brokerAddr: status.brokerAddr,
          ...status.entries,
        },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to load broker status';
      setDetailSheet({
        isOpen: true,
        type: 'Status',
        title: `Status [${brokerData.brokerName}][${brokerData.brokerId}]`,
        data: {
          brokerAddr: brokerData.address,
          error: message,
        },
      });
      toast.error(message);
    }
  };

  const openConfigSheet = async (brokerData: ClusterBrokerCardItem) => {
    setDetailSheet({
      isOpen: true,
      type: 'Config',
      title: `Config [${brokerData.brokerName}][${brokerData.brokerId}]`,
      data: {
        brokerAddr: brokerData.address,
        state: 'Loading broker config...',
      },
    });

    try {
      const config = await getBrokerConfig(brokerData.address);
      setDetailSheet({
        isOpen: true,
        type: 'Config',
        title: `Config [${brokerData.brokerName}][${brokerData.brokerId}]`,
        data: {
          brokerAddr: config.brokerAddr,
          ...config.entries,
        },
      });
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to load broker config';
      setDetailSheet({
        isOpen: true,
        type: 'Config',
        title: `Config [${brokerData.brokerName}][${brokerData.brokerId}]`,
        data: {
          brokerAddr: brokerData.address,
          error: message,
        },
      });
      toast.error(message);
    }
  };

  const handleRefresh = async () => {
    try {
      await refresh();
      toast.success('Cluster status refreshed');
    } catch (error) {
      const message = error instanceof Error ? error.message : 'Failed to refresh cluster status';
      toast.error(message);
    }
  };

  return (
    <div className="cluster-page">
      <SideSheet
        isOpen={detailSheet.isOpen}
        onClose={() => setDetailSheet({ ...detailSheet, isOpen: false })}
        title={detailSheet.title}
        data={detailSheet.data}
        type={detailSheet.type}
      />

      <section className="cluster-command-panel" aria-label="Cluster controls">
        <div className="cluster-select-shell">
          <button
            type="button"
            disabled={!data?.clusters.length}
            onClick={() => setIsSelectOpen(!isSelectOpen)}
            onBlur={() => setTimeout(() => setIsSelectOpen(false), 180)}
            className={`cluster-select-trigger ${isSelectOpen ? 'is-open' : ''}`}
          >
            <span className="cluster-select-icon">
              <Server className="cluster-icon" />
            </span>
            <span className="cluster-select-copy">
              <span>Current Cluster</span>
              <strong>
                {visibleCluster || (isLoading ? 'Loading...' : 'No Clusters')}
                <ChevronDown className={`cluster-icon ${isSelectOpen ? 'is-open' : ''}`} />
              </strong>
            </span>
          </button>

          <AnimatePresence>
            {isSelectOpen && (
              <motion.div
                initial={{ opacity: 0, y: 8, scale: 0.98 }}
                animate={{ opacity: 1, y: 0, scale: 1 }}
                exit={{ opacity: 0, y: 8, scale: 0.98 }}
                transition={{ duration: 0.16 }}
                className="cluster-select-menu"
              >
                {(data?.clusters ?? []).map((cluster) => (
                  <button
                    type="button"
                    key={cluster}
                    onClick={() => {
                      setSelectedCluster(cluster);
                      setIsSelectOpen(false);
                    }}
                    className={visibleCluster === cluster ? 'is-selected' : undefined}
                  >
                    <span>{cluster}</span>
                    {visibleCluster === cluster ? <Check className="cluster-icon" /> : null}
                  </button>
                ))}
              </motion.div>
            )}
          </AnimatePresence>
        </div>

        <div className={`cluster-health-chip ${clusterHealthTone}`}>
          <span />
          {clusterHealthLabel}
        </div>

        <Button variant="ghost" onClick={() => void handleRefresh()} disabled={isRefreshing}>
          <RefreshCw className={`ops-button-icon ${isRefreshing ? 'animate-spin' : ''}`} />
          Refresh
        </Button>
      </section>

      {loadError && !data ? (
        <section className="cluster-alert is-error">
          <AlertTriangle className="cluster-icon" />
          <div>
            <strong>Failed to load cluster data</strong>
            <span>{loadError}</span>
          </div>
          <Button variant="secondary" onClick={() => void handleRefresh()}>
            Retry
          </Button>
        </section>
      ) : null}

      {!loadError && !isLoading && clusterData.length === 0 ? (
        <section className="cluster-empty-state">
          <Network className="cluster-empty-icon" />
          <strong>No brokers found for this cluster</strong>
          <span>Select another cluster or verify the current NameServer is reachable.</span>
        </section>
      ) : null}

      {data ? (
        <section className="cluster-summary-grid" aria-label="Cluster summary">
          <ClusterSummaryCard
            label="Clusters"
            value={formatNumber(data.summary.totalClusters)}
            hint={visibleCluster || 'No cluster selected'}
            tone="info"
            icon={<Layers3 className="cluster-icon" />}
          />
          <ClusterSummaryCard
            label="Brokers"
            value={formatNumber(clusterData.length || data.summary.totalBrokers)}
            hint={`${clusterMetrics.masters || data.summary.totalMasters} master / ${clusterMetrics.slaves || data.summary.totalSlaves} slave`}
            tone="violet"
            icon={<Server className="cluster-icon" />}
          />
          <ClusterSummaryCard
            label="Active Brokers"
            value={formatNumber(clusterMetrics.active)}
            hint={`${Math.max(clusterData.length - clusterMetrics.active, 0)} inactive`}
            tone="success"
            icon={<ShieldCheck className="cluster-icon" />}
          />
          <ClusterSummaryCard
            label="NameServer"
            value={data.currentNamesrv || 'Not Selected'}
            hint={`VIP ${data.useVipChannel ? 'On' : 'Off'} / TLS ${data.useTls ? 'On' : 'Off'}`}
            tone="warning"
            icon={<Link2 className="cluster-icon" />}
          />
        </section>
      ) : null}

      {data ? (
        <section className="cluster-workspace">
          <div className="cluster-panel cluster-topology-panel">
            <div className="cluster-panel-header">
              <div>
                <h2>Broker Topology</h2>
                <p>Cluster roles, node health, and routing context.</p>
              </div>
              <Network className="cluster-panel-icon" />
            </div>

            <div className="cluster-topology-map">
              <div className="cluster-hub-card">
                <span>Cluster</span>
                <strong>{visibleCluster || 'No cluster selected'}</strong>
                <small>
                  {formatNumber(clusterData.length)} brokers, {formatNumber(clusterMetrics.masters)} masters
                </small>
                <div className={`cluster-hub-status ${clusterHealthTone}`}>
                  <span />
                  {clusterHealthLabel}
                </div>
              </div>

              <div className="cluster-topology-links" aria-hidden="true">
                <span />
              </div>

              <div className="cluster-node-list">
                {sortedClusterData.length ? (
                  sortedClusterData.map((broker) => {
                    const isMaster = broker.role.toUpperCase() === 'MASTER';
                    return (
                      <div
                        key={`${broker.clusterName}-${broker.brokerName}-${broker.brokerId}-topology`}
                        className={`cluster-node-card ${isMaster ? 'is-master' : 'is-slave'} ${broker.isActive ? 'is-active' : 'is-inactive'}`}
                      >
                        <div className="cluster-node-main">
                          <span className="cluster-node-status" />
                          <div>
                            <strong>{broker.brokerName}</strong>
                            <small>{broker.address}</small>
                          </div>
                        </div>
                        <div className="cluster-node-meta">
                          <span>{broker.role || 'Unknown'}</span>
                          <span>{broker.version || 'Unknown version'}</span>
                        </div>
                      </div>
                    );
                  })
                ) : (
                  <div className="cluster-node-empty">No broker nodes</div>
                )}
              </div>
            </div>
          </div>

          <aside className="cluster-panel cluster-inspector-panel">
            <div className="cluster-panel-header">
              <div>
                <h2>Broker Inspector</h2>
                <p>Runtime counters for the selected cluster.</p>
              </div>
              <Gauge className="cluster-panel-icon" />
            </div>

            <div className="cluster-inspector-focus">
              <span>Lead broker</span>
              <strong>{strongestBroker?.brokerName ?? 'No broker'}</strong>
              <small>{strongestBroker?.address ?? data.currentNamesrv ?? 'No endpoint selected'}</small>
            </div>

            <div className="cluster-inspector-grid">
              <div>
                <span>Produce TPS</span>
                <strong>{formatTps(clusterMetrics.produceTps)}</strong>
              </div>
              <div>
                <span>Consume TPS</span>
                <strong>{formatTps(clusterMetrics.consumeTps)}</strong>
              </div>
              <div>
                <span>Today produced</span>
                <strong>{formatNumber(clusterMetrics.todayProduce)}</strong>
              </div>
              <div>
                <span>Status errors</span>
                <strong>{formatNumber(clusterMetrics.errors)}</strong>
              </div>
            </div>
          </aside>
        </section>
      ) : null}

      {sortedClusterData.length ? (
        <section className="cluster-broker-grid" aria-label="Broker inventory">
          {sortedClusterData.map((broker, index) => {
            const isMaster = broker.role.toUpperCase() === 'MASTER';
            const brokerHasAttention = !broker.isActive || Boolean(broker.statusLoadError);

            return (
              <motion.article
                key={`${broker.clusterName}-${broker.brokerName}-${broker.brokerId}`}
                initial={{ opacity: 0, y: 16 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: index * 0.06, type: 'spring', stiffness: 180, damping: 22 }}
                className={`cluster-broker-card ${isMaster ? 'is-master' : 'is-slave'} ${brokerHasAttention ? 'is-attention' : ''}`}
              >
                <div className="cluster-broker-header">
                  <div className="cluster-broker-title">
                    <div>
                      <h3>{broker.brokerName}</h3>
                      <span>
                        <Server className="cluster-icon" />
                        {broker.address}
                        <i />
                        ID {broker.brokerId}
                      </span>
                    </div>
                    <div className={`cluster-role-badge ${isMaster ? 'is-master' : 'is-slave'}`}>
                      {isMaster ? <Crown className="cluster-icon" /> : <GitBranch className="cluster-icon" />}
                      {isMaster ? 'Master' : broker.role || 'Replica'}
                    </div>
                  </div>
                  <div className="cluster-broker-version">{broker.version || 'Unknown version'}</div>
                </div>

                <div className="cluster-broker-body">
                  <div className="cluster-broker-section-title">
                    <Activity className="cluster-icon" />
                    Real-time TPS
                  </div>
                  <div className="cluster-broker-tps-grid">
                    <BrokerTpsTile
                      label="Produce"
                      value={broker.produceTps}
                      percent={(broker.produceTps / maxBrokerTps) * 100}
                      tone="produce"
                      icon={<ArrowUpCircle className="cluster-icon" />}
                    />
                    <BrokerTpsTile
                      label="Consume"
                      value={broker.consumeTps}
                      percent={(broker.consumeTps / maxBrokerTps) * 100}
                      tone="consume"
                      icon={<ArrowDownCircle className="cluster-icon" />}
                    />
                  </div>

                  <div className="cluster-broker-section-title">
                    <Database className="cluster-icon" />
                    Message Statistics
                  </div>
                  <div className="cluster-message-panel">
                    <div className="cluster-message-row">
                      <div>
                        <Clock className="cluster-icon" />
                        <strong>Today</strong>
                      </div>
                      <span>
                        <small>Produce</small>
                        {formatNumber(broker.todayProduce)}
                      </span>
                      <span>
                        <small>Consume</small>
                        {formatNumber(broker.todayConsume)}
                      </span>
                    </div>
                    <div className="cluster-message-row is-muted">
                      <div>
                        <HardDrive className="cluster-icon" />
                        <strong>Yesterday</strong>
                      </div>
                      <span>{formatNumber(broker.yesterdayProduce)}</span>
                      <span>{formatNumber(broker.yesterdayConsume)}</span>
                    </div>
                  </div>

                  {broker.statusLoadError ? (
                    <div className="cluster-broker-warning">
                      <AlertTriangle className="cluster-icon" />
                      <span>Runtime stats could not be loaded: {broker.statusLoadError}</span>
                    </div>
                  ) : null}
                </div>

                <div className="cluster-broker-actions">
                  <Button
                    variant="secondary"
                    onClick={() => void openStatusSheet(broker)}
                    className="cluster-action-button"
                    disabled={pendingStatusAddr === broker.address}
                  >
                    {pendingStatusAddr === broker.address ? 'Loading...' : 'Status'}
                  </Button>
                  <Button
                    variant="primary"
                    onClick={() => void openConfigSheet(broker)}
                    className="cluster-action-button"
                    disabled={pendingConfigAddr === broker.address}
                  >
                    {pendingConfigAddr === broker.address ? 'Loading...' : 'Config'}
                  </Button>
                </div>
              </motion.article>
            );
          })}
        </section>
      ) : null}
    </div>
  );
};
