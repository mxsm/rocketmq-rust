import { ExternalLink, Gauge, Layers3, RadioTower, ScanSearch, ShieldCheck } from 'lucide-react';
import { useEffect, useMemo, useRef, useState, type MouseEvent } from 'react';
import { Link } from 'react-router-dom';
import { brokerApi } from '../api/broker_api';
import AppDataTable, { type AppDataTableColumn } from '../components/AppDataTable';
import EntitySheet from '../components/EntitySheet';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import QueryToolbar from '../components/QueryToolbar';
import RefreshButton from '../components/RefreshButton';
import StatusBadge from '../components/StatusBadge';
import { Button } from '../components/ui/Button';
import type { BrokerInfo, BrokerListView } from '../types/broker';
import BrokerDetailContent from './brokers/BrokerDetailContent';

const PAGE_SIZE = 10;

export default function BrokerListPage() {
  const [data, setData] = useState<BrokerListView | null>(null);
  const [search, setSearch] = useState('');
  const [cluster, setCluster] = useState('all');
  const [role, setRole] = useState('all');
  const [page, setPage] = useState(1);
  const [selectedBroker, setSelectedBroker] = useState<BrokerInfo | null>(null);
  const detailTriggerRef = useRef<HTMLElement | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const load = async () => {
    if (data) setRefreshing(true);
    else setLoading(true);
    setError(null);
    try {
      setData(await brokerApi.list());
    } catch (requestError) {
      setError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    void load();
  }, []);

  const brokers = data?.items ?? [];
  const clusters = useMemo(() => Array.from(new Set(brokers.map((broker) => broker.clusterName))).filter(Boolean).sort(), [brokers]);
  const roles = useMemo(() => Array.from(new Set(brokers.map((broker) => broker.role))).filter(Boolean).sort(), [brokers]);
  const combinedTps = useMemo(
    () => Number(brokers.reduce((total, broker) => total + broker.produceTps + broker.consumeTps, 0).toFixed(2)),
    [brokers]
  );
  const filteredRows = useMemo(() => {
    const normalizedSearch = search.trim().toLowerCase();
    return brokers.filter((broker) => {
      if (cluster !== 'all' && broker.clusterName !== cluster) return false;
      if (role !== 'all' && broker.role !== role) return false;
      if (!normalizedSearch) return true;
      return `${broker.brokerName} ${broker.address} ${broker.clusterName} ${broker.version} ${broker.role}`
        .toLowerCase()
        .includes(normalizedSearch);
    });
  }, [brokers, cluster, role, search]);

  const pageCount = Math.max(1, Math.ceil(filteredRows.length / PAGE_SIZE));
  const currentPage = Math.min(page, pageCount);
  const visibleRows = filteredRows.slice((currentPage - 1) * PAGE_SIZE, currentPage * PAGE_SIZE);

  const updateFilter = (setter: (value: string) => void) => (value: string) => {
    setter(value);
    setPage(1);
  };

  const openDetails = (broker: BrokerInfo, origin?: HTMLElement) => {
    if (origin) detailTriggerRef.current = origin;
    setSelectedBroker(broker);
  };
  const stopRowActivation = (event: MouseEvent) => event.stopPropagation();

  const columns: AppDataTableColumn<BrokerInfo>[] = [
    {
      id: 'broker',
      header: 'Broker',
      width: '210px',
      cell: (broker) => (
        <div className="broker-name-cell">
          <strong>{broker.brokerName}</strong>
          <Link
            to={`/brokers/${encodeURIComponent(broker.brokerName)}`}
            aria-label={`View ${broker.brokerName}`}
            onClick={stopRowActivation}
          >
            Full page <ExternalLink size={12} aria-hidden="true" />
          </Link>
        </div>
      )
    },
    { id: 'cluster', header: 'Cluster', width: '150px', cell: (broker) => broker.clusterName },
    { id: 'role', header: 'Role', width: '120px', cell: (broker) => <StatusBadge status={broker.role} tone={broker.role.toUpperCase().includes('MASTER') ? 'success' : 'info'} /> },
    { id: 'address', header: 'Address', width: '190px', cell: (broker) => <code>{broker.address}</code> },
    { id: 'version', header: 'Version', width: '120px', cell: (broker) => broker.version || '—' },
    { id: 'produce', header: 'Produce TPS', width: '120px', align: 'right', cell: (broker) => broker.produceTps.toFixed(2) },
    { id: 'consume', header: 'Consume TPS', width: '120px', align: 'right', cell: (broker) => broker.consumeTps.toFixed(2) },
    {
      id: 'actions',
      header: 'Actions',
      width: '90px',
      align: 'right',
      cell: (broker) => (
        <Button
          type="button"
          variant="outline"
          size="icon"
          aria-label={`Inspect ${broker.brokerName}`}
          onClick={(event) => { event.stopPropagation(); openDetails(broker, event.currentTarget); }}
        >
          <ScanSearch size={15} aria-hidden="true" />
        </Button>
      )
    }
  ];

  if (loading) return <LoadingState label="Loading brokers" />;
  if (error) return <ErrorState message={error} onRetry={() => void load()} />;

  return (
    <div className="cluster-inventory-page">
      <PageHeader
        title="Cluster inventory"
        description="Filter broker topology, compare traffic evidence, and inspect runtime or configuration without leaving the list."
        actions={<RefreshButton refreshing={refreshing} onRefresh={() => void load()} />}
      />

      <div className="metric-grid cluster-metrics">
        <MetricCard label="Clusters" value={clusters.length} detail="Unique cluster names" icon={<Layers3 size={18} />} />
        <MetricCard label="Brokers" value={brokers.length} detail="Visible broker entries" icon={<RadioTower size={18} />} />
        <MetricCard label="Roles" value={roles.length} detail="Distinct reported roles" icon={<ShieldCheck size={18} />} />
        <MetricCard label="Combined TPS" value={combinedTps} detail="Produce plus consume" icon={<Gauge size={18} />} />
      </div>

      <section className="cluster-table-card">
        <QueryToolbar
          searchValue={search}
          searchPlaceholder="Filter brokers"
          onSearchChange={updateFilter(setSearch)}
          onReset={() => { setSearch(''); setCluster('all'); setRole('all'); setPage(1); }}
        >
          <label className="native-filter-field">
            <span>Cluster</span>
            <select aria-label="Cluster filter" value={cluster} onChange={(event) => updateFilter(setCluster)(event.target.value)}>
              <option value="all">All clusters</option>
              {clusters.map((clusterName) => <option key={clusterName} value={clusterName}>{clusterName}</option>)}
            </select>
          </label>
          <label className="native-filter-field">
            <span>Role</span>
            <select aria-label="Role filter" value={role} onChange={(event) => updateFilter(setRole)(event.target.value)}>
              <option value="all">All roles</option>
              {roles.map((roleName) => <option key={roleName} value={roleName}>{roleName}</option>)}
            </select>
          </label>
        </QueryToolbar>
        <AppDataTable
          ariaLabel="Broker inventory"
          rows={visibleRows}
          columns={columns}
          getRowId={(broker) => `${broker.brokerName}-${broker.brokerId}-${broker.address}`}
          page={currentPage}
          pageSize={PAGE_SIZE}
          total={filteredRows.length}
          onPageChange={setPage}
          onRowActivate={openDetails}
          emptyTitle="No brokers match"
          emptyDetail="Adjust the search, cluster, or role filters."
        />
      </section>

      <EntitySheet
        open={selectedBroker !== null}
        title={selectedBroker?.brokerName ?? 'Broker details'}
        description={selectedBroker ? `${selectedBroker.clusterName} · ${selectedBroker.address}` : undefined}
        restoreFocusRef={detailTriggerRef}
        onOpenChange={(open) => { if (!open) setSelectedBroker(null); }}
      >
        {selectedBroker ? <BrokerDetailContent brokerName={selectedBroker.brokerName} broker={selectedBroker} /> : null}
      </EntitySheet>
    </div>
  );
}
