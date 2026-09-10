import { isReadOnlyConsumer } from '../features/consumer/mutation';
import { motion } from 'motion/react';
import { ConnectionStore } from '../services/connection.store';
import { getConnectionSettings } from '../services/connection.service';
import { resolveConsumerScope, consumerScopeKey } from '../features/consumer/scope';
import type { ConsumerQueryScope } from '../features/consumer/types/consumer.types';
import { useAppStore, useNavigationState } from '../stores/app.store';
import { findEntity } from '../stores/navigation';
import React, { useEffect, useMemo, useState, useRef, useSyncExternalStore } from 'react';
import { Users, Settings, Trash2, Search, Check, RefreshCw, Plus, Clock, FileText, Activity, Network, Cpu, AlertCircle, LoaderCircle } from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { Pagination } from './Pagination';
import { useConsumerCatalog } from '../features/consumer/hooks/useConsumerCatalog';
import { ConsumerClientModal } from '../features/consumer/components/ConsumerClientModal';
import { ConsumerConfigModal } from '../features/consumer/components/ConsumerConfigModal';
import { ConsumerDeleteModal } from '../features/consumer/components/ConsumerDeleteModal';
import { ConsumerDetailModal } from '../features/consumer/components/ConsumerDetailModal';
import { ConsumerEditorModal } from '../features/consumer/components/ConsumerEditorModal';
import type { ConsumerGroupListItem, ConsumerMutationResult } from '../features/consumer/types/consumer.types';

export const ConsumerView = () => {
  const settings = useSyncExternalStore(ConnectionStore.subscribe, ConnectionStore.getSnapshot, () => null);
  const { consumerQueryMode, setConsumerQueryMode, setActiveTab, navigation } = useAppStore();
  const [queryMode, setQueryMode] = useNavigationState<ConsumerQueryScope['mode']>('queryMode', navigation.target?.kind === 'consumer' ? navigation.target.scope.mode : consumerQueryMode);
  const changeMode = (mode: ConsumerQueryScope['mode']) => { setQueryMode(mode); setConsumerQueryMode(mode); };
  const [settingsError, setSettingsError] = useState('');
  useEffect(() => {
    let active = true;
    if (!settings) void getConnectionSettings().catch(() => { if (active) setSettingsError('Unable to load connection settings.'); });
    return () => { active = false; };
  }, [settings]);
  const scope = useMemo(() => resolveConsumerScope(settings, queryMode), [settings, queryMode]);
  if (!settings) return <p role="status" className="p-6">{settingsError || 'Loading connection settings...'}</p>;
  if (!scope) return <div className="p-6 space-y-3"><p role="alert">Select a Proxy in connection settings before using Proxy mode.</p><button type="button" className="underline mr-4" onClick={() => setActiveTab('Proxy')}>Configure Proxy</button><button type="button" className="underline" onClick={() => changeMode('name_server')}>Use NameServer</button></div>;
  return <ConsumerCatalog key={consumerScopeKey(scope)} scope={scope} proxyAddress={settings.proxy.currentProxyAddr} changeMode={changeMode} />;
};

const ConsumerCatalog = ({ scope, proxyAddress, changeMode }: { scope: ConsumerQueryScope; proxyAddress: string | null; changeMode: (mode: ConsumerQueryScope['mode']) => void }) => {
  const { navigation, openConsumer, goBack, setActiveTab } = useAppStore();
  const target = navigation.target?.kind === 'consumer' && consumerScopeKey(navigation.target.scope) === consumerScopeKey(scope) ? navigation.target : null;
  const openedTarget = useRef(false);

  const [searchTerm, setSearchTerm] = useNavigationState(`search:${consumerScopeKey(scope)}`, '');
  const [filters, setFilters] = useNavigationState<Record<string, boolean>>(`filters:${consumerScopeKey(scope)}`, {
    NORMAL: true,
    FIFO: !!target,
    SYSTEM: true,
  });
  const enableProxy = scope.mode === 'proxy';
  const [detailModal, setDetailModal] = useState<{ isOpen: boolean, consumer: ConsumerGroupListItem | null }>({isOpen: false, consumer: null});
  const [configModal, setConfigModal] = useState<{ isOpen: boolean, consumer: ConsumerGroupListItem | null }>({isOpen: false, consumer: null});
  const [clientModal, setClientModal] = useState<{ isOpen: boolean, consumer: ConsumerGroupListItem | null }>({isOpen: false, consumer: null});
  const [editorModal, setEditorModal] = useState<{ isOpen: boolean, consumer: ConsumerGroupListItem | null, preferredBrokerAddress?: string }>({isOpen: false, consumer: null});
  const [deleteModal, setDeleteModal] = useState<{ isOpen: boolean, consumer: ConsumerGroupListItem | null }>({isOpen: false, consumer: null});
  const [selectedGroup, setSelectedGroup] = useNavigationState(`selection:${consumerScopeKey(scope)}`, target?.name ?? '');
  const [currentPage, setCurrentPage] = useNavigationState(`page:${consumerScopeKey(scope)}`, 1);
  const itemsPerPage = 6;
  const {
    items,
    summary,
    response,
    isInitialLoading,
    isRefreshPending,
    isRefreshing,
    refreshingGroup,
    error,
    refresh,
    refreshGroup,
  } = useConsumerCatalog(scope);

  const activeCategories = useMemo(
    () => Object.entries(filters).filter(([, value]) => value).map(([key]) => key),
    [filters],
  );

  const filteredConsumers = useMemo(() => {
    const normalizedTerm = searchTerm.trim().toLowerCase();
    return items.filter((consumer) => {
      const matchesSearch =
        normalizedTerm.length === 0 ||
        consumer.displayGroupName.toLowerCase().includes(normalizedTerm) ||
        consumer.rawGroupName.toLowerCase().includes(normalizedTerm);
      const matchesCategory =
        activeCategories.length === 0 || activeCategories.includes(consumer.category);
      return matchesSearch && matchesCategory;
    });
  }, [activeCategories, items, searchTerm]);

  const totalPages = Math.max(1, Math.ceil(filteredConsumers.length / itemsPerPage));
  const currentConsumers = filteredConsumers.slice(
    (currentPage - 1) * itemsPerPage,
    currentPage * itemsPerPage,
  );
  const selectedConsumer = findEntity(target ? items : filteredConsumers, (consumer) => consumer.rawGroupName, selectedGroup || null);
  const targetMissing = target && response && !isInitialLoading && !error && !items.some((item) => item.rawGroupName === target.name);
  const activeClientCount = items.reduce((total, consumer) => total + consumer.connectionCount, 0);
  const totalLag = items.reduce((total, consumer) => total + consumer.diffTotal, 0);
  const lagHealthClass = selectedConsumer && selectedConsumer.diffTotal > 0 ? 'is-warning' : 'is-healthy';

  useEffect(() => {
    if (response && currentPage > totalPages) {
      setCurrentPage(totalPages);
    }
  }, [currentPage, totalPages]);

  useEffect(() => {
    if (target || !response) return;
    if (filteredConsumers.length === 0) {
      setSelectedGroup('');
      return;
    }
    if (!filteredConsumers.some((consumer) => consumer.rawGroupName === selectedGroup)) {
      setSelectedGroup(filteredConsumers[0].rawGroupName);
    }
  }, [filteredConsumers, selectedGroup]);

  useEffect(() => {
    if (!target || openedTarget.current || !response || isInitialLoading) return;
    const consumer = items.find((item) => item.rawGroupName === target.name);
    if (!consumer) return;
    openedTarget.current = true;
    const setters = { progress: setDetailModal, config: setConfigModal, clients: setClientModal };
    if (target.detail !== 'overview') setters[target.detail]({ isOpen: true, consumer });
  }, [target, response, isInitialLoading, items]);

  const toggleFilter = (key: string) => {
    setCurrentPage(1);
    setFilters((current) => ({...current, [key]: !current[key]}));
  };

  const goToPage = (page: number) => {
    if (page >= 1 && page <= totalPages) {
      setCurrentPage(page);
    }
  };

  const handleEditorSaved = async (result: ConsumerMutationResult) => {
    if (result.success) setEditorModal({isOpen: false, consumer: null});
    await refresh();
  };

  const handleDeleteSaved = async (result: ConsumerMutationResult) => {
    if (result.success) setDeleteModal({isOpen: false, consumer: null});
    await refresh();
  };

  const handleEditFromConfig = (consumer: ConsumerGroupListItem, preferredBrokerAddress?: string) => {
    if (isReadOnlyConsumer(consumer)) return;
    setConfigModal({isOpen: false, consumer: null});
    setEditorModal({isOpen: true, consumer, preferredBrokerAddress});
  };

  const formatUpdatedTime = (timestamp: number) => {
    if (!Number.isFinite(timestamp) || timestamp <= 0) {
      return '-';
    }
    return new Date(timestamp).toLocaleTimeString();
  };

  const getCategoryClass = (category: string) => {
    if (category === 'SYSTEM') {
      return 'is-system';
    }
    if (category === 'FIFO') {
      return 'is-fifo';
    }
    return 'is-normal';
  };

  return (
    <div className="consumer-page">
      {targetMissing && <p role="alert" className="p-4 text-red-600">Consumer group not found: {target.name}</p>}
      <ConsumerDetailModal
        isOpen={detailModal.isOpen}
        onClose={() => { setDetailModal({isOpen: false, consumer: null}); if (target) goBack(); }}
        consumer={detailModal.consumer}
        scope={scope}
      />
      <ConsumerConfigModal
        isOpen={configModal.isOpen}
        onClose={() => { setConfigModal({isOpen: false, consumer: null}); if (target) goBack(); }}
        consumer={configModal.consumer}
        onEdit={handleEditFromConfig}
      />
      <ConsumerClientModal
        isOpen={clientModal.isOpen}
        onClose={() => { setClientModal({isOpen: false, consumer: null}); if (target) goBack(); }}
        consumer={clientModal.consumer}
        scope={scope}
      />
      <ConsumerEditorModal
        isOpen={editorModal.isOpen}
        onClose={() => setEditorModal({isOpen: false, consumer: null})}
        consumer={editorModal.consumer}
        preferredBrokerAddress={editorModal.preferredBrokerAddress}
        onSaved={(result) => void handleEditorSaved(result)}
      />
      <ConsumerDeleteModal
        isOpen={deleteModal.isOpen}
        onClose={() => setDeleteModal({isOpen: false, consumer: null})}
        consumer={deleteModal.consumer}
        onDeleted={(result) => void handleDeleteSaved(result)}
      />

      <section className="consumer-summary-grid" aria-label="Consumer summary">
        <div className="topic-summary-card">
          <div>
            <span>Consumer Groups</span>
            <strong>{summary?.totalGroups ?? items.length}</strong>
            <small>{summary?.normalGroups ?? 0} normal / {summary?.systemGroups ?? 0} system</small>
          </div>
          <span className="topic-summary-icon">
            <Users className="topic-icon" aria-hidden="true"/>
          </span>
        </div>
        <div className="topic-summary-card is-success">
          <div>
            <span>Active Clients</span>
            <strong>{activeClientCount}</strong>
            <small>connected consumers</small>
          </div>
          <span className="topic-summary-icon">
            <Network className="topic-icon" aria-hidden="true"/>
          </span>
        </div>
        <div className={`topic-summary-card ${totalLag > 0 ? 'is-warning' : 'is-blue'}`}>
          <div>
            <span>Total Lag</span>
            <strong>{totalLag}</strong>
            <small>{totalLag > 0 ? 'queue pressure detected' : 'no queue pressure'}</small>
          </div>
          <span className="topic-summary-icon">
            <Activity className="topic-icon" aria-hidden="true"/>
          </span>
        </div>
        <div className="topic-summary-card is-blue">
          <div>
            <span>Transport</span>
            <strong>{response?.useVipChannel ? 'VIP' : 'Direct'}</strong>
            <small>{response?.useVipChannel ? 'VIP on' : 'VIP off'} / {response?.useTls ? 'TLS on' : 'TLS off'}</small>
          </div>
          <span className="topic-summary-icon">
            <Cpu className="topic-icon" aria-hidden="true"/>
          </span>
        </div>
      </section>

      <section className="consumer-command-panel" aria-label="Consumer filters and actions">
        <div className="consumer-command-copy is-compact">
          <span>Scope</span>
          <div className="consumer-scope-pills" aria-label="Consumer catalog filter scope">
            <b>Local filters</b>
            <b>Proxy source</b>
          </div>
        </div>

        <div className="consumer-query-controls">
          <label className="consumer-search-field">
            <Search className="topic-icon" aria-hidden="true"/>
            <input
              type="text"
              placeholder="SubscriptionGroup..."
              value={searchTerm}
              onChange={(event) => {
                setSearchTerm(event.target.value);
                setCurrentPage(1);
              }}
            />
          </label>

          <div className="consumer-filter-row" aria-label="Consumer group category filters">
            {Object.entries(filters).map(([key, value]) => (
              <button
                type="button"
                key={key}
                onClick={() => toggleFilter(key)}
                className={`consumer-filter-chip ${value ? 'is-active' : ''} ${getCategoryClass(key)}`}
              >
                {value && <Check className="topic-icon" aria-hidden="true"/>}
                <span>{key}</span>
              </button>
            ))}
          </div>

          <div className="consumer-proxy-control">
            <span>Proxy</span>
            <span>{proxyAddress ?? 'Not configured'}</span>
            <button type="button" className="underline" onClick={() => setActiveTab('Proxy')}>Configure</button>
          </div>

          <button
            type="button"
            className={`consumer-proxy-toggle ${enableProxy ? 'is-on' : ''}`}
            disabled={!proxyAddress && !enableProxy}
            onClick={() => changeMode(enableProxy ? 'name_server' : 'proxy')}
          >
            <span>Enable Proxy</span>
            <i aria-hidden="true"/>
          </button>

          <button
            type="button"
            className="topic-action-button is-status consumer-add-button"
            onClick={() => setEditorModal({isOpen: true, consumer: null})}
          >
            <Plus className="topic-icon" aria-hidden="true"/>
            <span>Add / Update</span>
          </button>

          <button
            type="button"
            className="topic-action-button consumer-refresh-button"
            onClick={() => void refresh()}
            disabled={isRefreshPending || isInitialLoading}
            title={isRefreshing ? 'Refreshing...' : 'Refresh consumer groups'}
          >
            {isRefreshing ? (
              <LoaderCircle className="topic-icon consumer-spin" aria-hidden="true"/>
            ) : (
              <RefreshCw className={`topic-icon ${isRefreshPending ? 'consumer-tilt' : ''}`} aria-hidden="true"/>
            )}
            <span className="sr-only">Refresh</span>
          </button>
        </div>
      </section>

      {error && (
        <div className="consumer-alert" role="alert">
          <AlertCircle className="topic-icon" aria-hidden="true"/>
          <span>{error}</span>
        </div>
      )}

      {response && (
        <section className="consumer-scope-strip" aria-label="Consumer source details">
          <div>
            <span>NameServer:</span>
            <strong>{response.currentNamesrv}</strong>
            <i aria-hidden="true">/</i>
            <span>Total</span>
            <strong>{summary?.totalGroups ?? 0}</strong>
            <i aria-hidden="true">/</i>
            <span>NORMAL</span>
            <strong>{summary?.normalGroups ?? 0}</strong>
            <i aria-hidden="true">/</i>
            <span>FIFO</span>
            <strong>{summary?.fifoGroups ?? 0}</strong>
            <i aria-hidden="true">/</i>
            <span>SYSTEM</span>
            <strong>{summary?.systemGroups ?? 0}</strong>
          </div>
          <b>{response.useVipChannel ? 'VIP ON' : 'VIP OFF'} / {response.useTls ? 'TLS ON' : 'TLS OFF'}</b>
        </section>
      )}

      {isInitialLoading ? (
        <div className="consumer-loading-state">
          <LoaderCircle className="topic-icon consumer-spin" aria-hidden="true"/>
          <span>Loading consumer groups...</span>
        </div>
      ) : filteredConsumers.length === 0 ? (
        <div className="consumer-empty-state">
          <Search className="topic-icon" aria-hidden="true"/>
          <strong>No consumer groups matched</strong>
          <span>Adjust the search term or category filters to inspect another subscription group.</span>
        </div>
      ) : (
        <section className="consumer-workspace">
          <div className="consumer-panel consumer-list-panel">
            <div className="topic-panel-header">
              <div>
                <h2>Consumer Catalog</h2>
                <p>Subscription groups ranked by lag, clients, and lifecycle category.</p>
              </div>
              <div className="topic-panel-meta">
                <span>{filteredConsumers.length} visible</span>
              </div>
            </div>

            <div className="consumer-list">
              {currentConsumers.map((consumer, index) => {
                const selected = selectedConsumer?.rawGroupName === consumer.rawGroupName;
                return (
                  <motion.button
                    type="button"
                    key={consumer.rawGroupName}
                    initial={{opacity: 0, y: 14}}
                    animate={{opacity: 1, y: 0}}
                    transition={{duration: 0.24, delay: index * 0.03}}
                    className={`consumer-row ${selected ? 'is-selected' : ''} ${getCategoryClass(consumer.category)}`}
                    onClick={() => setSelectedGroup(consumer.rawGroupName)}
                  >
                    <span className="consumer-row-main">
                      <span className="consumer-row-icon">
                        <Users className="topic-icon" aria-hidden="true"/>
                      </span>
                      <span className="consumer-row-copy">
                        <strong title={consumer.displayGroupName}>{consumer.displayGroupName}</strong>
                        <span>{consumer.rawGroupName}</span>
                      </span>
                    </span>
                    <span className="consumer-badge-stack">
                      <b className="is-model">{consumer.messageModel}</b>
                      <b className="is-type">{consumer.consumeType}</b>
                      <b className={getCategoryClass(consumer.category)}>{consumer.category}</b>
                    </span>
                    <span className="consumer-row-metric">
                      <span>TPS</span>
                      <strong>{consumer.consumeTps}</strong>
                    </span>
                    <span className={`consumer-row-metric ${consumer.diffTotal > 0 ? 'is-warning' : ''}`}>
                      <span>Lag</span>
                      <strong>{consumer.diffTotal}</strong>
                    </span>
                    <span className="consumer-row-metric">
                      <span>Clients</span>
                      <strong>{consumer.connectionCount}</strong>
                    </span>
                    <span className="consumer-row-updated">
                      <Clock className="topic-icon" aria-hidden="true"/>
                      {formatUpdatedTime(consumer.updateTimestamp)}
                    </span>
                  </motion.button>
                );
              })}
            </div>
          </div>

          <aside className="consumer-panel consumer-inspector-panel" aria-label="Selected consumer group">
            {selectedConsumer ? (
              <>
                <div className="consumer-inspector-header">
                  <span>Selected Consumer</span>
                  <strong>{selectedConsumer.displayGroupName}</strong>
                  <small>{selectedConsumer.rawGroupName}</small>
                  <div className="consumer-badge-stack">
                    <b className="is-model">{selectedConsumer.messageModel}</b>
                    <b className="is-type">{selectedConsumer.consumeType}</b>
                    <b className={getCategoryClass(selectedConsumer.category)}>{selectedConsumer.category}</b>
                  </div>
                </div>

                <div className={`consumer-health-card ${lagHealthClass}`}>
                  <span>Lag health</span>
                  <strong>{selectedConsumer.diffTotal > 0 ? 'Lagging' : 'Healthy'}</strong>
                  <small>
                    {selectedConsumer.diffTotal > 0
                      ? 'This group has queue backlog and needs inspection.'
                      : 'No queue pressure reported by this group.'}
                  </small>
                </div>

                <div className="consumer-detail-grid">
                  <div>
                    <span>TPS</span>
                    <strong>{selectedConsumer.consumeTps}</strong>
                  </div>
                  <div>
                    <span>Lag</span>
                    <strong>{selectedConsumer.diffTotal}</strong>
                  </div>
                  <div>
                    <span>Clients</span>
                    <strong>{selectedConsumer.connectionCount}</strong>
                  </div>
                  <div>
                    <span>Version</span>
                    <strong>{selectedConsumer.versionDesc}</strong>
                  </div>
                  <div>
                    <span>Brokers</span>
                    <strong>{selectedConsumer.brokerNames.length || '-'}</strong>
                  </div>
                  <div>
                    <span>Updated</span>
                    <strong>{formatUpdatedTime(selectedConsumer.updateTimestamp)}</strong>
                  </div>
                </div>

                <div className="consumer-action-grid">
                  <button type="button" className="topic-action-button" onClick={() => openConsumer(selectedConsumer.rawGroupName, 'clients', scope)}>
                    <Users className="topic-icon" aria-hidden="true"/>
                    <span>Client</span>
                  </button>
                  <button type="button" className="topic-action-button" onClick={() => openConsumer(selectedConsumer.rawGroupName, 'progress', scope)}>
                    <FileText className="topic-icon" aria-hidden="true"/>
                    <span>Detail</span>
                  </button>
                  <button type="button" className="topic-action-button" onClick={() => openConsumer(selectedConsumer.rawGroupName, 'config', scope)}>
                    <Settings className="topic-icon" aria-hidden="true"/>
                    <span>Config</span>
                  </button>
                  <button
                    type="button"
                    className="topic-action-button"
                    onClick={() => void refreshGroup(selectedConsumer.rawGroupName)}
                    disabled={refreshingGroup === selectedConsumer.rawGroupName}
                  >
                    {refreshingGroup === selectedConsumer.rawGroupName ? (
                      <LoaderCircle className="topic-icon consumer-spin" aria-hidden="true"/>
                    ) : (
                      <RefreshCw className="topic-icon" aria-hidden="true"/>
                    )}
                    <span>Sync</span>
                  </button>
                  <button type="button" className="topic-action-button is-danger" disabled={isReadOnlyConsumer(selectedConsumer)} title={isReadOnlyConsumer(selectedConsumer) ? "System Consumer groups are read-only." : undefined} onClick={() => setDeleteModal({isOpen: true, consumer: selectedConsumer})}>
                    <Trash2 className="topic-icon" aria-hidden="true"/>
                    <span>Delete</span>
                  </button>
                </div>
              </>
            ) : (
              <div className="consumer-empty-state is-compact">
                <Users className="topic-icon" aria-hidden="true"/>
                <strong>No consumer selected</strong>
                <span>Select a consumer group from the catalog to inspect actions.</span>
              </div>
            )}
          </aside>
        </section>
      )}

      <footer className="consumer-footer">
        <span>Consumer group data is read-only until Add/Update, Config, or Delete opens a scoped mutation flow.</span>
        <Pagination
          currentPage={currentPage}
          totalPages={totalPages}
          onPageChange={goToPage}
        />
      </footer>
    </div>
  );
};
