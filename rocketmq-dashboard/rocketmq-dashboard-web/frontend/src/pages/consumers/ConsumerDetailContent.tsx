import { ListChecks, Network, RadioTower, RotateCcw, Settings2, Users } from 'lucide-react';
import { useEffect, useMemo, useRef, useState } from 'react';
import { consumerApi } from '../../api/consumer_api';
import AppDataTable, { type AppDataTableColumn } from '../../components/AppDataTable';
import ErrorState from '../../components/ErrorState';
import LoadingState from '../../components/LoadingState';
import MetricCard from '../../components/MetricCard';
import StatusBadge from '../../components/StatusBadge';
import {
  AlertDialog,
  AlertDialogAction,
  AlertDialogCancel,
  AlertDialogContent,
  AlertDialogDescription,
  AlertDialogTitle
} from '../../components/ui/AlertDialog';
import { Button } from '../../components/ui/Button';
import { Input } from '../../components/ui/Input';
import { Label } from '../../components/ui/Label';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../components/ui/Tabs';
import type {
  ConsumerConfigView,
  ConsumerConnectionItem,
  ConsumerConnectionView,
  ConsumerProgressQueue,
  ConsumerProgressView,
  ConsumerSubscriptionItem,
  ConsumerSummaryView
} from '../../types/consumer';
import { useConsumerQueryScope } from './ConsumerQueryScopeProvider';
import { normalizeConsumerValue } from './consumer-model';

interface ConsumerDetailContentProps {
  group: string;
  initialTab?: 'overview' | 'clients' | 'progress' | 'config' | 'reset';
}

const connectionColumns: AppDataTableColumn<ConsumerConnectionItem>[] = [
  { id: 'clientId', header: 'Client ID', cell: (row) => row.clientId },
  { id: 'clientAddr', header: 'Address', cell: (row) => row.clientAddr },
  { id: 'language', header: 'Language', cell: (row) => row.language },
  { id: 'version', header: 'Version', cell: (row) => row.versionDesc || String(row.version) }
];

const subscriptionColumns: AppDataTableColumn<ConsumerSubscriptionItem>[] = [
  { id: 'topic', header: 'Topic', cell: (row) => row.topic },
  { id: 'expression', header: 'Expression', cell: (row) => row.subString },
  { id: 'type', header: 'Type', cell: (row) => row.expressionType },
  { id: 'tags', header: 'Tags', cell: (row) => row.tagsSet.join(', ') },
  { id: 'version', header: 'Version', cell: (row) => row.subVersion }
];

const queueColumns: AppDataTableColumn<ConsumerProgressQueue>[] = [
  { id: 'broker', header: 'Broker', width: '150px', cell: (row) => row.brokerName },
  { id: 'queue', header: 'Queue', width: '80px', cell: (row) => row.queueId },
  { id: 'brokerOffset', header: 'Broker offset', width: '130px', cell: (row) => row.brokerOffset },
  { id: 'consumerOffset', header: 'Consumer offset', width: '140px', cell: (row) => row.consumerOffset },
  { id: 'client', header: 'Client', width: '180px', cell: (row) => <span title={row.clientInfo}>{row.clientInfo || '-'}</span> },
  {
    id: 'lag',
    header: 'Lag',
    width: '100px',
    cell: (row) => <StatusBadge status={String(row.diffTotal)} tone={row.diffTotal > 0 ? 'warning' : 'success'} />
  },
  { id: 'lastConsume', header: 'Last consume time', width: '180px', cell: (row) => formatTimestamp(row.lastTimestamp) }
];

export default function ConsumerDetailContent({ group, initialTab = 'overview' }: ConsumerDetailContentProps) {
  const { scope, revision } = useConsumerQueryScope();
  const [activeTab, setActiveTab] = useState(initialTab);
  const [summary, setSummary] = useState<ConsumerSummaryView | null>(null);
  const [progress, setProgress] = useState<ConsumerProgressView | null>(null);
  const [connections, setConnections] = useState<ConsumerConnectionView | null>(null);
  const [config, setConfig] = useState<ConsumerConfigView | null>(null);
  const [clientsLoading, setClientsLoading] = useState(false);
  const [configLoading, setConfigLoading] = useState(false);
  const [clientsError, setClientsError] = useState<string | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [notice, setNotice] = useState<string | null>(null);
  const [resetTopic, setResetTopic] = useState('');
  const [resetTimestamp, setResetTimestamp] = useState(() => String(Date.now()));
  const [forceReset, setForceReset] = useState(false);
  const [validationError, setValidationError] = useState<string | null>(null);
  const [confirmOpen, setConfirmOpen] = useState(false);
  const [resetting, setResetting] = useState(false);
  const requestToken = useRef(0);
  const resetOperationToken = useRef(0);
  const currentGroupRef = useRef(group);

  const load = async () => {
    const token = ++requestToken.current;
    setLoading(true);
    setError(null);
    try {
      const [nextSummary, nextProgress] = await Promise.all([
        consumerApi.summary(group, scope),
        consumerApi.progress(group, scope)
      ]);
      if (token !== requestToken.current) return;
      setSummary(nextSummary);
      setProgress(nextProgress);
      setResetTopic((current) => current || nextProgress.topics[0]?.topic || '');
    } catch (requestError) {
      if (token === requestToken.current) {
        setError(requestError instanceof Error ? requestError.message : String(requestError));
      }
    } finally {
      if (token === requestToken.current) setLoading(false);
    }
  };

  const loadClients = async () => {
    setClientsLoading(true);
    setClientsError(null);
    try {
      setConnections(await consumerApi.connections(group, scope));
    } catch (requestError) {
      setClientsError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      setClientsLoading(false);
    }
  };

  const loadConfig = async () => {
    setConfigLoading(true);
    setConfigError(null);
    try {
      setConfig(await consumerApi.config(group, scope));
    } catch (requestError) {
      setConfigError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      setConfigLoading(false);
    }
  };

  useEffect(() => {
    currentGroupRef.current = group;
    resetOperationToken.current += 1;
    setActiveTab(initialTab);
    setSummary(null);
    setProgress(null);
    setConnections(null);
    setConfig(null);
    setClientsError(null);
    setConfigError(null);
    setResetTopic('');
    setValidationError(null);
    setNotice(null);
    setConfirmOpen(false);
    setResetting(false);
    void load();
    return () => {
      requestToken.current += 1;
      resetOperationToken.current += 1;
    };
  }, [group, initialTab, scope.mode, scope.proxyAddress, revision]);

  useEffect(() => {
    if (activeTab === 'clients' && !connections) void loadClients();
    if (activeTab === 'config' && !config) void loadConfig();
  }, [activeTab, connections, config, group, scope.mode, scope.proxyAddress]);

  const topics = progress?.topics ?? [];
  const topicOptions = useMemo(() => topics.map((topic) => topic.topic), [topics]);

  const reviewReset = () => {
    const timestamp = Number(resetTimestamp);
    if (!resetTopic) {
      setValidationError('Select a topic before resetting offsets.');
      return;
    }
    if (!resetTimestamp.trim() || !Number.isFinite(timestamp)) {
      setValidationError('Reset timestamp must be a millisecond timestamp.');
      return;
    }
    if (!Number.isSafeInteger(timestamp) || timestamp < 0) {
      setValidationError('Reset timestamp must be a non-negative safe integer.');
      return;
    }
    setValidationError(null);
    setConfirmOpen(true);
  };

  const resetOffsets = async () => {
    const operationToken = ++resetOperationToken.current;
    const operationGroup = group;
    const operationTopic = resetTopic;
    setResetting(true);
    try {
      await consumerApi.resetOffset(operationGroup, {
        topic: operationTopic,
        resetTimestamp: Number(resetTimestamp),
        force: forceReset
      });
      if (operationToken !== resetOperationToken.current || currentGroupRef.current !== operationGroup) return;
      setConfirmOpen(false);
      setNotice(`Offsets reset for ${operationGroup} on ${operationTopic}.`);
      await load();
    } catch (requestError) {
      if (operationToken !== resetOperationToken.current || currentGroupRef.current !== operationGroup) return;
      setConfirmOpen(false);
      setValidationError(requestError instanceof Error ? requestError.message : 'Unable to reset offsets.');
    } finally {
      if (operationToken === resetOperationToken.current && currentGroupRef.current === operationGroup) {
        setResetting(false);
      }
    }
  };

  if (loading && !progress) return <LoadingState label="Loading consumer workspace" />;
  if (error && !progress) return <ErrorState message={error} onRetry={() => void load()} />;

  return (
    <div className="entity-detail-content consumer-detail-content">
      {notice ? <div className="notice notice-success" role="status">{notice}</div> : null}
      {loading && progress ? <LoadingState label="Refreshing consumer workspace" /> : null}
      {!loading && error && progress ? (
        <ErrorState message={error} onRetry={() => void load()} retryLabel="Retry workspace refresh" />
      ) : null}

      <Tabs value={activeTab} onValueChange={(value) => setActiveTab(value as typeof activeTab)}>
        <TabsList aria-label="Consumer detail sections">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="clients">Clients</TabsTrigger>
          <TabsTrigger value="progress">Progress</TabsTrigger>
          <TabsTrigger value="config">Configuration</TabsTrigger>
          <TabsTrigger value="reset">Reset offset</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <div className="metric-grid entity-detail-metrics">
            <MetricCard label="Topics" value={progress?.topicCount ?? 0} detail="Tracked by queue progress" icon={<ListChecks size={18} />} />
            <MetricCard label="Total lag" value={progress?.totalDiff ?? summary?.diffTotal ?? 0} detail="Aggregate offset difference" icon={<RotateCcw size={18} />} />
            <MetricCard label="Connections" value={summary?.connectionCount ?? 0} detail="Consumer group summary" icon={<Users size={18} />} />
            <MetricCard label="TPS" value={summary?.consumeTps ?? 0} detail="Reported consume throughput" icon={<RadioTower size={18} />} />
          </div>
          <dl className="entity-description-grid">
            <div><dt>Consumer group</dt><dd className="mono">{summary?.group ?? group}</dd></div>
            <div><dt>Category</dt><dd>{summary?.category ?? '-'}</dd></div>
            <div><dt>Consume type</dt><dd>{normalizeConsumerValue(summary?.consumeType ?? '')}</dd></div>
            <div><dt>Message model</dt><dd>{normalizeConsumerValue(summary?.messageModel ?? '')}</dd></div>
            <div><dt>Version</dt><dd>{summary?.versionDesc ?? '-'}</dd></div>
            <div><dt>Brokers</dt><dd className="mono">{summary?.brokerNames.join(', ') ?? '-'}</dd></div>
          </dl>
        </TabsContent>

        <TabsContent value="clients">
          {clientsLoading ? <LoadingState label="Loading clients" /> : null}
          {clientsError ? <ErrorState message={clientsError} onRetry={() => void loadClients()} /> : null}
          {connections ? (
            <>
              <dl className="entity-description-grid">
                <div><dt>Consume type</dt><dd>{normalizeConsumerValue(connections.consumeType)}</dd></div>
                <div><dt>Message model</dt><dd>{normalizeConsumerValue(connections.messageModel)}</dd></div>
                <div><dt>Consume from</dt><dd>{connections.consumeFromWhere}</dd></div>
              </dl>
              <section className="consumer-resource-section">
                <h3>Client connections</h3>
                <AppDataTable
                  ariaLabel="Consumer connections"
                  rows={connections.connections}
                  columns={connectionColumns}
                  getRowId={(row) => row.clientId}
                  page={1}
                  pageSize={Math.max(connections.connections.length, 1)}
                  total={connections.connections.length}
                  onPageChange={() => undefined}
                  emptyTitle="No client connections"
                  emptyDetail="This group has no connected clients in the current response."
                />
              </section>
              <section className="consumer-resource-section">
                <h3>Subscriptions</h3>
                <AppDataTable
                  ariaLabel="Consumer subscriptions"
                  rows={connections.subscriptions}
                  columns={subscriptionColumns}
                  getRowId={(row) => row.topic}
                  page={1}
                  pageSize={Math.max(connections.subscriptions.length, 1)}
                  total={connections.subscriptions.length}
                  onPageChange={() => undefined}
                  emptyTitle="No subscriptions"
                  emptyDetail="This group has no subscription entries in the current response."
                />
              </section>
            </>
          ) : null}
        </TabsContent>

        <TabsContent value="progress">
          {topics.length === 0 ? (
            <div className="notice notice-neutral" role="status">No Topic progress is currently reported for this group.</div>
          ) : null}
          {topics.map((topic) => (
            <section key={topic.topic} className="consumer-progress-section">
              <header>
                <h3>{topic.topic}</h3>
                <span>Lag {topic.diffTotal}</span>
              </header>
              <AppDataTable
                ariaLabel={`Progress for ${topic.topic}`}
                rows={topic.queues}
                columns={queueColumns}
                getRowId={(row) => `${topic.topic}:${row.brokerName}:${row.queueId}`}
                page={1}
                pageSize={Math.max(topic.queues.length, 1)}
                total={topic.queues.length}
                onPageChange={() => undefined}
                emptyTitle="No queue progress"
                emptyDetail="This Topic has no queue offset entries in the current response."
              />
            </section>
          ))}
        </TabsContent>

        <TabsContent value="config">
          {configLoading ? <LoadingState label="Loading configuration" /> : null}
          {configError ? <ErrorState message={configError} onRetry={() => void loadConfig()} /> : null}
          {config ? (
            <>
              {config.inconsistentFields.length > 0 ? (
                <div className="notice notice-warning" role="alert">
                  {config.inconsistentFields.length} fields differ across brokers.
                </div>
              ) : null}
              <dl className="entity-description-grid">
                <div><dt>Effective retry queues</dt><dd>{config.effective?.retryQueueNums ?? '-'}</dd></div>
                <div><dt>Effective max retries</dt><dd>{config.effective?.retryMaxTimes ?? '-'}</dd></div>
                <div><dt>Consume timeout</dt><dd>{config.effective?.consumeTimeoutMinute ?? '-'}</dd></div>
                <div><dt>Consume enabled</dt><dd>{config.effective?.consumeEnable ?? false ? 'Yes' : 'No'}</dd></div>
              </dl>
              {config.targets.map((target) => (
                <section key={target.brokerName} className="consumer-resource-section">
                  <h3>{target.brokerName}</h3>
                  {target.error ? <div className="notice notice-danger" role="alert">{target.error}</div> : null}
                  {target.config ? <pre className="consumer-config-json">{JSON.stringify(target.config, null, 2)}</pre> : null}
                  {target.subscriptionTopics.length > 0 ? (
                    <p>Subscriptions: {target.subscriptionTopics.join(', ')}</p>
                  ) : null}
                </section>
              ))}
            </>
          ) : null}
        </TabsContent>

        <TabsContent value="reset">
          <section className="danger-zone consumer-reset-panel">
            <div>
              <h3>Reset consumer offsets</h3>
              <p>Move this group to a timestamp for one topic. Review the target carefully before confirming.</p>
            </div>
            <div className="form-grid consumer-reset-form">
              <div className="field">
                <Label htmlFor="consumer-reset-topic">Topic</Label>
                <select
                  id="consumer-reset-topic"
                  className="ui-select-native"
                  value={resetTopic}
                  onChange={(event) => setResetTopic(event.target.value)}
                >
                  {topicOptions.map((topic) => <option key={topic} value={topic}>{topic}</option>)}
                </select>
              </div>
              <div className="field">
                <Label htmlFor="consumer-reset-timestamp">Reset timestamp</Label>
                <Input
                  id="consumer-reset-timestamp"
                  type="number"
                  min="0"
                  step="1"
                  value={resetTimestamp}
                  onChange={(event) => setResetTimestamp(event.target.value)}
                />
              </div>
              <label className="compact-check" htmlFor="consumer-force-reset">
                <input
                  id="consumer-force-reset"
                  type="checkbox"
                  checked={forceReset}
                  onChange={(event) => setForceReset(event.target.checked)}
                />
                Force reset
              </label>
            </div>
            {validationError ? <div className="inline-validation" role="alert">{validationError}</div> : null}
            <Button type="button" variant="destructive" onClick={reviewReset}>
              <RotateCcw size={15} aria-hidden="true" /> Review reset
            </Button>
          </section>
        </TabsContent>
      </Tabs>

      <AlertDialog open={confirmOpen} onOpenChange={setConfirmOpen}>
        <AlertDialogContent>
          <AlertDialogTitle>Reset consumer offset?</AlertDialogTitle>
          <AlertDialogDescription>
            Reset {group} on {resetTopic} to timestamp {resetTimestamp}{forceReset ? ' with force enabled' : ''}?
          </AlertDialogDescription>
          <div className="ui-alert-dialog-actions">
            <AlertDialogCancel disabled={resetting}>Cancel</AlertDialogCancel>
            <AlertDialogAction
              disabled={resetting}
              onClick={(event) => {
                event.preventDefault();
                void resetOffsets();
              }}
            >
              {resetting ? 'Resetting' : 'Confirm reset'}
            </AlertDialogAction>
          </div>
        </AlertDialogContent>
      </AlertDialog>
    </div>
  );
}

function formatTimestamp(value: number): string {
  if (!value) return '-';
  return new Date(value).toLocaleString();
}
