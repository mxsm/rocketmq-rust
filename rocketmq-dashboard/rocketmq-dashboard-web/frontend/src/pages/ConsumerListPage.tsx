import { Activity, FileText, Plus, Power, RefreshCw, Search, Settings2, Trash2, Users } from 'lucide-react';
import { useEffect, useMemo, useState } from 'react';
import { consumerApi } from '../api/consumer_api';
import { configApi } from '../api/config_api';
import ConfirmDialog from '../components/ConfirmDialog';
import ConsumerInspectorDrawer, { type ConsumerInspectorTab } from '../components/ConsumerInspectorDrawer';
import ConsumerMutationDialog from '../components/ConsumerMutationDialog';
import EmptyState from '../components/EmptyState';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import SelectMenu from '../components/SelectMenu';
import StatusBadge from '../components/StatusBadge';
import type { ConsumerGroupInfo, ConsumerListView } from '../types/consumer';
import type { DashboardConfigView } from '../types/config';

const groupTypeFilters = [
  { key: 'normal', label: 'NORMAL', defaultChecked: true },
  { key: 'fifo', label: 'FIFO', defaultChecked: false },
  { key: 'system', label: 'SYSTEM', defaultChecked: false }
] as const;

type GroupTypeKey = (typeof groupTypeFilters)[number]['key'];
type GroupTypeFilterState = Record<GroupTypeKey, boolean>;

const pageSize = 10;

function defaultFilters(): GroupTypeFilterState {
  return Object.fromEntries(groupTypeFilters.map((item) => [item.key, item.defaultChecked])) as GroupTypeFilterState;
}

export default function ConsumerListPage() {
  const [data, setData] = useState<ConsumerListView | null>(null);
  const [config, setConfig] = useState<DashboardConfigView | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [groupQuery, setGroupQuery] = useState('');
  const [filters, setFilters] = useState<GroupTypeFilterState>(() => defaultFilters());
  const [selectedProxy, setSelectedProxy] = useState('');
  const [proxyEnabled, setProxyEnabled] = useState(false);
  const [mutationOpen, setMutationOpen] = useState(false);
  const [inspectorConsumer, setInspectorConsumer] = useState<ConsumerGroupInfo | null>(null);
  const [inspectorTab, setInspectorTab] = useState<ConsumerInspectorTab>('client');
  const [page, setPage] = useState(1);
  const [lastLoadedAt, setLastLoadedAt] = useState('');

  const load = () => {
    setLoading(true);
    setError(null);
    Promise.all([consumerApi.list(), configApi.getConfig().catch(() => null)])
      .then(([consumerData, configData]) => {
        setData(consumerData);
        setConfig(configData);
        const currentProxy = configData?.currentProxyAddr ?? configData?.proxyAddrList[0] ?? '';
        setSelectedProxy(currentProxy);
        setProxyEnabled(Boolean(configData?.currentProxyAddr));
        setLastLoadedAt(formatDateTime(new Date()));
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, []);

  const filteredConsumers = useMemo(() => {
    const query = groupQuery.trim().toLowerCase();
    const enabledTypes = new Set(
      Object.entries(filters)
        .filter(([, enabled]) => enabled)
        .map(([key]) => key)
    );
    return (data?.items ?? []).filter((consumer) => {
      const matchesQuery = !query || consumer.group.toLowerCase().includes(query);
      const matchesType = enabledTypes.size === 0 || enabledTypes.has(inferGroupType(consumer));
      return matchesQuery && matchesType;
    });
  }, [data?.items, filters, groupQuery]);

  const counts = useMemo(() => {
    const items = data?.items ?? [];
    const system = items.filter(isSystemGroup).length;
    const clients = items.reduce((sum, item) => sum + item.clientCount, 0);
    const delay = items.reduce((sum, item) => sum + item.diffTotal, 0);
    return {
      total: items.length,
      visible: filteredConsumers.length,
      system,
      application: Math.max(0, items.length - system),
      clients,
      delay
    };
  }, [data?.items, filteredConsumers.length]);

  const pageCount = Math.max(1, Math.ceil(filteredConsumers.length / pageSize));
  const currentPage = Math.min(page, pageCount);
  const visibleConsumers = useMemo(
    () => filteredConsumers.slice((currentPage - 1) * pageSize, currentPage * pageSize),
    [currentPage, filteredConsumers]
  );

  useEffect(() => {
    setPage(1);
  }, [filters, groupQuery]);

  const openInspector = (consumer: ConsumerGroupInfo, tab: ConsumerInspectorTab) => {
    setInspectorConsumer(consumer);
    setInspectorTab(tab);
  };

  const refreshGroup = (group: string) => {
    consumerApi
      .progress(group)
      .then((progress) => {
        setNotice(`Consumer ${group} refreshed. ${progress.queues.length} queue(s), lag ${progress.diffTotal}.`);
        load();
      })
      .catch((requestError: Error) => setError(requestError.message));
  };

  const switchProxy = (enabled: boolean) => {
    setProxyEnabled(enabled);
    if (!enabled) {
      setNotice('Proxy disabled for Consumer page context.');
      return;
    }
    if (!selectedProxy) {
      setNotice('No proxy endpoint configured. Add a proxy endpoint in Config first.');
      return;
    }
    configApi
      .switchProxy({ address: selectedProxy })
      .then((result) => {
        setConfig(result.config);
        setNotice(`Proxy switched to ${selectedProxy}.`);
      })
      .catch((requestError: Error) => {
        setProxyEnabled(false);
        setError(requestError.message);
      });
  };

  const deleteGroupPending = (group: string) => {
    setNotice(`Delete ${group} is pending Rust backend support for Java /consumer/deleteSubGroup.do.`);
  };

  if (loading) return <LoadingState label="Loading consumers" />;
  if (error) return <ErrorState message={error} onRetry={load} />;

  return (
    <>
      <PageHeader
        title="Consumer"
        description="Java Dashboard consumer parity with group filters, proxy context, connection detail, consume progress, config, refresh, and guarded deletion flows."
        actions={
          <div className="action-row">
            <button type="button" className="button" onClick={() => setMutationOpen(true)}>
              <Plus size={15} aria-hidden="true" /> Add / Update
            </button>
            <button type="button" className="button button-secondary" onClick={load}>
              <RefreshCw size={15} aria-hidden="true" /> Refresh
            </button>
          </div>
        }
      />

      {notice ? <div className="notice notice-warning">{notice}</div> : null}

      <section className="consumer-filter-panel">
        <div className="consumer-filter-left">
          <label className="field consumer-filter-search">
            SubscriptionGroup
            <input value={groupQuery} placeholder="Input subscription group" onChange={(event) => setGroupQuery(event.target.value)} />
          </label>
          <div className="topic-type-filters" aria-label="Consumer group type filters">
            {groupTypeFilters.map((filter) => (
              <label className="compact-check topic-type-check" key={filter.key}>
                <input
                  type="checkbox"
                  checked={filters[filter.key]}
                  onChange={(event) => setFilters((value) => ({ ...value, [filter.key]: event.target.checked }))}
                />
                {filter.label}
              </label>
            ))}
          </div>
        </div>
        <div className="consumer-filter-proxy">
          <label className="consumer-proxy-field">
            Select Proxy
            <SelectMenu
              value={selectedProxy}
              options={proxyOptions(config)}
              onChange={setSelectedProxy}
              ariaLabel="Select proxy endpoint"
              className="consumer-proxy-select"
            />
          </label>
          <label className="consumer-proxy-toggle">
            Enable Proxy
            <button type="button" className={`proxy-toggle ${proxyEnabled ? 'proxy-toggle-on' : ''}`} onClick={() => switchProxy(!proxyEnabled)}>
              <Power size={13} aria-hidden="true" />
              <span />
              {proxyEnabled ? 'Enabled' : 'Disabled'}
            </button>
          </label>
        </div>
      </section>

      <section className="consumer-metric-grid">
        <div>
          <span>Groups</span>
          <strong>{counts.total}</strong>
          <small>{counts.visible} visible</small>
        </div>
        <div>
          <span>Online clients</span>
          <strong>{counts.clients}</strong>
          <small>from consumer connection summary</small>
        </div>
        <div>
          <span>Total delay</span>
          <strong>{counts.delay}</strong>
          <small>diffTotal aggregate</small>
        </div>
        <div>
          <span>Proxy</span>
          <strong>{proxyEnabled ? 'Enabled' : 'Disabled'}</strong>
          <small>{selectedProxy || 'no endpoint selected'}</small>
        </div>
      </section>

      <section className="consumer-list-panel">
        <div className="topic-list-toolbar">
          <div>
            <h2>Consumer inventory</h2>
            <p>
              {counts.visible} visible / {counts.total} total
            </p>
          </div>
          <div className="topic-list-summary">
            <StatusBadge status={`${counts.application} normal`} tone="success" />
            <StatusBadge status={`${counts.system} system`} />
            <button type="button" className="icon-button" title="Refresh consumers" onClick={load}>
              <RefreshCw size={15} aria-hidden="true" />
            </button>
          </div>
        </div>
        <div className="topic-list-search">
          <Search size={16} aria-hidden="true" />
          <input value={groupQuery} placeholder="Search within filtered consumer groups" onChange={(event) => setGroupQuery(event.target.value)} />
        </div>
        <div className="consumer-inventory-scroll">
          <table className="consumer-inventory-table">
            <thead>
              <tr>
                <th>SubscriptionGroup</th>
                <th>Quantity</th>
                <th>Version</th>
                <th>Type</th>
                <th>Mode</th>
                <th>TPS</th>
                <th>Delay</th>
                <th>Update Time</th>
                <th>Operation</th>
              </tr>
            </thead>
            <tbody>
              {visibleConsumers.map((row) => (
                <tr className={isSystemGroup(row) ? 'consumer-row-system' : 'consumer-row-application'} key={row.group}>
                  <td>
                    <div className="consumer-name-cell">
                      <code>{row.group}</code>
                      <span>{isSystemGroup(row) ? 'system consumer group' : `${row.clientCount} active clients`}</span>
                    </div>
                  </td>
                  <td>{row.clientCount}</td>
                  <td>
                    <span className="muted-cell">-</span>
                  </td>
                  <td>
                    <StatusBadge status={normalizeConsumerType(row.consumeType)} />
                  </td>
                  <td>
                    <StatusBadge status={normalizeMessageModel(row.messageModel)} tone="success" />
                  </td>
                  <td>0</td>
                  <td>
                    <StatusBadge status={String(row.diffTotal)} tone={row.diffTotal > 0 ? 'warning' : 'success'} />
                  </td>
                  <td>{lastLoadedAt}</td>
                  <td>{renderConsumerActions(row, openInspector, refreshGroup, deleteGroupPending)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
        {visibleConsumers.length === 0 ? <EmptyState title="No consumer groups match the current filters" /> : null}
        <div className="topic-list-footer">
          <span>
            {filteredConsumers.length} rows / page {currentPage} of {pageCount}
          </span>
          <div className="pagination">
            <button type="button" className="button button-secondary" disabled={currentPage <= 1} onClick={() => setPage(currentPage - 1)}>
              Previous
            </button>
            <button type="button" className="button button-secondary" disabled={currentPage >= pageCount} onClick={() => setPage(currentPage + 1)}>
              Next
            </button>
          </div>
        </div>
      </section>

      <ConsumerMutationDialog open={mutationOpen} onOpenChange={setMutationOpen} />
      <ConsumerInspectorDrawer
        open={inspectorConsumer !== null}
        consumer={inspectorConsumer}
        initialTab={inspectorTab}
        onOpenChange={(open) => {
          if (!open) setInspectorConsumer(null);
        }}
        onRefreshGroup={refreshGroup}
      />
    </>
  );
}

function proxyOptions(config: DashboardConfigView | null) {
  const proxies = config?.proxyAddrList ?? [];
  if (proxies.length === 0) {
    return [{ value: '', label: 'No proxy configured', disabled: true }];
  }
  return proxies.map((address) => ({ value: address, label: address }));
}

function renderConsumerActions(
  row: ConsumerGroupInfo,
  openInspector: (consumer: ConsumerGroupInfo, tab: ConsumerInspectorTab) => void,
  refreshGroup: (group: string) => void,
  deleteGroup: (group: string) => void
) {
  return (
    <div className="consumer-action-grid">
      <button type="button" className="button button-secondary consumer-action-button" onClick={() => openInspector(row, 'client')}>
        <Users size={14} aria-hidden="true" /> CLIENT
      </button>
      <button type="button" className="button button-secondary consumer-action-button" onClick={() => openInspector(row, 'detail')}>
        <FileText size={14} aria-hidden="true" /> CONSUME DETAIL
      </button>
      <button type="button" className="button consumer-action-button" onClick={() => openInspector(row, 'config')}>
        <Settings2 size={14} aria-hidden="true" /> Config
      </button>
      <button type="button" className="button button-secondary consumer-action-button" onClick={() => refreshGroup(row.group)}>
        <Activity size={14} aria-hidden="true" /> REFRESH
      </button>
      <ConfirmDialog
        title="Delete consumer group"
        description={`Delete consumer group ${row.group}? This maps to Java /consumer/deleteSubGroup.do and changes broker subscription metadata.`}
        confirmLabel="Delete"
        onConfirm={() => deleteGroup(row.group)}
      >
        <button type="button" className="button button-danger consumer-action-button">
          <Trash2 size={14} aria-hidden="true" /> Delete
        </button>
      </ConfirmDialog>
    </div>
  );
}

function inferGroupType(consumer: ConsumerGroupInfo): GroupTypeKey {
  if (isSystemGroup(consumer)) return 'system';
  if (consumer.group.toUpperCase().includes('FIFO') || consumer.consumeType.toUpperCase().includes('FIFO')) return 'fifo';
  return 'normal';
}

function isSystemGroup(consumer: ConsumerGroupInfo) {
  const group = consumer.group;
  const upperGroup = group.toUpperCase();
  return (
    group.startsWith('%SYS%') ||
    upperGroup.startsWith('RMQ_SYS') ||
    upperGroup.startsWith('CID_SYS') ||
    upperGroup.startsWith('CID_RMQ_SYS') ||
    upperGroup.startsWith('CID_ONS') ||
    upperGroup.includes('ONSAPI') ||
    upperGroup.includes('ONS-HTTP-PROXY') ||
    upperGroup.includes('ONS_HTTP_PROXY') ||
    upperGroup.includes('FILTERSRV') ||
    upperGroup.startsWith('SELF_TEST') ||
    upperGroup === 'TOOLS_CONSUMER_GROUP' ||
    upperGroup === 'TOOLS_CONSUMER' ||
    upperGroup === 'FILTERSRV_CONSUMER_GROUP' ||
    upperGroup === 'FILTERSRV_CONSUMER' ||
    upperGroup === 'SELF_TEST_CONSUMER_GROUP' ||
    upperGroup === 'SELF_TEST_C_GROUP' ||
    upperGroup === 'ONS_HTTP_PROXY_GROUP' ||
    upperGroup === 'CID_ONSAPI_PULL_GROUP' ||
    upperGroup === 'CID_ONSAPI_PERMISSION_GROUP' ||
    upperGroup === 'CID_ONSAPI_OWNER_GROUP' ||
    upperGroup === 'CID_SYS_RMQ_TRANS' ||
    upperGroup === 'CID_DEFAULTHEARTBEATSYNCERTOPIC'
  );
}

function normalizeConsumerType(value: string) {
  return value.replace(/^CONSUME_/, '') || 'UNKNOWN';
}

function normalizeMessageModel(value: string) {
  return value.replace(/^MESSAGE_MODEL_/, '') || 'UNKNOWN';
}

function formatDateTime(date: Date) {
  const pad = (value: number) => String(value).padStart(2, '0');
  return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(date.getMinutes())}:${pad(date.getSeconds())}`;
}
