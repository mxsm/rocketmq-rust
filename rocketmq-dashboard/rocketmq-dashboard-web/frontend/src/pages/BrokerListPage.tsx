import { Activity, RefreshCw, Settings } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { Link } from 'react-router-dom';
import { brokerApi } from '../api/broker_api';
import BrokerInspectorDrawer from '../components/BrokerInspectorDrawer';
import DataTable from '../components/DataTable';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SelectMenu from '../components/SelectMenu';
import StatusBadge from '../components/StatusBadge';
import type { BrokerInfo, BrokerListView } from '../types/broker';

type BrokerDrawerTab = 'status' | 'config';

export default function BrokerListPage() {
  const [data, setData] = useState<BrokerListView | null>(null);
  const [selectedCluster, setSelectedCluster] = useState('all');
  const [drawerBroker, setDrawerBroker] = useState<string | null>(null);
  const [drawerTab, setDrawerTab] = useState<BrokerDrawerTab>('status');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const load = () => {
    setLoading(true);
    setError(null);
    brokerApi
      .list()
      .then(setData)
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, []);

  const clusters = useMemo(
    () => Array.from(new Set((data?.items ?? []).map((broker) => broker.clusterName))).filter(Boolean).sort(),
    [data]
  );
  const clusterOptions = useMemo(
    () => [{ value: 'all', label: 'All clusters' }, ...clusters.map((cluster) => ({ value: cluster, label: cluster }))],
    [clusters]
  );
  const visibleRows = useMemo(
    () =>
      selectedCluster === 'all'
        ? data?.items ?? []
        : (data?.items ?? []).filter((broker) => broker.clusterName === selectedCluster),
    [data, selectedCluster]
  );

  const openDrawer = (broker: BrokerInfo, tab: BrokerDrawerTab) => {
    setDrawerBroker(broker.brokerName);
    setDrawerTab(tab);
  };

  if (loading) return <LoadingState label="Loading brokers" />;
  if (error) return <ErrorState message={error} onRetry={load} />;

  return (
    <>
      <PageHeader
        title="Cluster"
        description="Broker topology, runtime status, and editable broker configuration."
        actions={
          <>
            <div className="cluster-filter">
              <span>Cluster</span>
              <SelectMenu value={selectedCluster} options={clusterOptions} onChange={setSelectedCluster} ariaLabel="Select cluster" />
            </div>
            <StatusBadge status={`${visibleRows.length} brokers`} tone={visibleRows.length > 0 ? 'success' : 'neutral'} />
            <button type="button" className="icon-button" title="Refresh brokers" onClick={load}>
              <RefreshCw size={15} aria-hidden="true" />
            </button>
          </>
        }
      />
      <DataTable
        rows={visibleRows}
        columns={[
          { header: 'Broker', render: (row) => <Link to={`/brokers/${encodeURIComponent(row.brokerName)}`}>{row.brokerName}</Link> },
          { header: 'NO.', render: (row) => row.brokerId },
          { header: 'Address', render: (row) => <code>{row.address}</code> },
          { header: 'Version', render: (row) => row.version || '-' },
          { header: 'Role', render: (row) => <StatusBadge status={row.role} /> },
          { header: 'Produce Message TPS', render: (row) => row.produceTps.toFixed(2) },
          { header: 'Consumer Message TPS', render: (row) => row.consumeTps.toFixed(2) },
          {
            header: 'Operation',
            render: (row) => (
              <div className="broker-action-buttons">
                <button type="button" className="button button-secondary" onClick={() => openDrawer(row, 'status')}>
                  <Activity size={15} aria-hidden="true" /> Status
                </button>
                <button type="button" className="button button-secondary" onClick={() => openDrawer(row, 'config')}>
                  <Settings size={15} aria-hidden="true" /> Config
                </button>
              </div>
            )
          }
        ]}
        getRowId={(row) => `${row.brokerName}-${row.brokerId}-${row.address}`}
        emptyTitle="No brokers"
        onRefresh={load}
      />
      <BrokerInspectorDrawer
        brokerName={drawerBroker}
        initialTab={drawerTab}
        open={drawerBroker !== null}
        onOpenChange={(open) => {
          if (!open) setDrawerBroker(null);
        }}
      />
    </>
  );
}
