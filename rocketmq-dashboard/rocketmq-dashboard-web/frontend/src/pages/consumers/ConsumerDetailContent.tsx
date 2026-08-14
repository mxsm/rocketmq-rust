import { ListChecks, RadioTower, RotateCcw, Users } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
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
import type { ConsumerGroupInfo, ConsumerProgress, ConsumerQueueProgress } from '../../types/consumer';
import { normalizeConsumerValue } from './consumer-model';

interface ConsumerDetailContentProps {
  group: string;
  consumer?: ConsumerGroupInfo | null;
  initialTab?: 'overview' | 'progress' | 'reset';
}

const progressColumns: AppDataTableColumn<ConsumerQueueProgress>[] = [
  { id: 'topic', header: 'Topic', width: '220px', cell: (row) => <code>{row.topic}</code> },
  { id: 'broker', header: 'Broker', width: '150px', cell: (row) => row.brokerName },
  { id: 'queue', header: 'Queue', width: '80px', cell: (row) => row.queueId },
  { id: 'brokerOffset', header: 'Broker offset', width: '130px', cell: (row) => row.brokerOffset },
  { id: 'consumerOffset', header: 'Consumer offset', width: '140px', cell: (row) => row.consumerOffset },
  {
    id: 'lag',
    header: 'Lag',
    width: '100px',
    cell: (row) => <StatusBadge status={String(row.diff)} tone={row.diff > 0 ? 'warning' : 'success'} />
  }
];

export default function ConsumerDetailContent({ group, consumer, initialTab = 'overview' }: ConsumerDetailContentProps) {
  const [activeTab, setActiveTab] = useState(initialTab);
  const [identity, setIdentity] = useState<ConsumerGroupInfo | null>(consumer ?? null);
  const [progress, setProgress] = useState<ConsumerProgress | null>(null);
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
      const [nextProgress, nextIdentity] = await Promise.all([
        consumerApi.progress(group),
        consumer
          ? Promise.resolve(consumer)
          : consumerApi.list().then((result) => result.items.find((item) => item.group === group) ?? null)
      ]);
      if (token !== requestToken.current) return;
      setProgress(nextProgress);
      setIdentity(nextIdentity);
      setResetTopic((current) => current || nextProgress.queues[0]?.topic || '');
    } catch (requestError) {
      if (token === requestToken.current) {
        setError(requestError instanceof Error ? requestError.message : String(requestError));
      }
    } finally {
      if (token === requestToken.current) setLoading(false);
    }
  };

  useEffect(() => {
    currentGroupRef.current = group;
    resetOperationToken.current += 1;
    setActiveTab(initialTab);
    setIdentity(consumer ?? null);
    setProgress(null);
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
  }, [group, consumer, initialTab]);

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

  if (loading && !progress) return <LoadingState label="Loading consumer progress" />;
  if (error && !progress) return <ErrorState message={error} onRetry={() => void load()} />;

  return (
    <div className="entity-detail-content consumer-detail-content">
      {notice ? <div className="notice notice-success" role="status">{notice}</div> : null}
      {loading && progress ? <LoadingState label="Refreshing consumer progress" /> : null}
      {!loading && error && progress ? (
        <ErrorState message={error} onRetry={() => void load()} retryLabel="Retry progress refresh" />
      ) : null}
      <Tabs value={activeTab} onValueChange={(value) => setActiveTab(value as typeof activeTab)}>
        <TabsList aria-label="Consumer detail sections">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="progress">Progress</TabsTrigger>
          <TabsTrigger value="reset">Reset offset</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <div className="metric-grid entity-detail-metrics">
            <MetricCard label="Topics" value={progress?.topicCount ?? 0} detail="Tracked by queue progress" icon={<ListChecks size={18} />} />
            <MetricCard label="Total lag" value={progress?.diffTotal ?? identity?.diffTotal ?? 0} detail="Aggregate offset difference" icon={<RotateCcw size={18} />} />
            <MetricCard label="Queues" value={progress?.queues.length ?? 0} detail="Reported queue entries" icon={<RadioTower size={18} />} />
            <MetricCard label="Connected clients" value={identity?.clientCount ?? 0} detail="Consumer group summary" icon={<Users size={18} />} />
          </div>
          <dl className="entity-description-grid">
            <div><dt>Consumer group</dt><dd className="mono">{group}</dd></div>
            <div><dt>Consume type</dt><dd>{normalizeConsumerValue(identity?.consumeType ?? '')}</dd></div>
            <div><dt>Message model</dt><dd>{normalizeConsumerValue(identity?.messageModel ?? '')}</dd></div>
            <div><dt>Progress source</dt><dd>Live admin API</dd></div>
          </dl>
        </TabsContent>

        <TabsContent value="progress">
          <AppDataTable
            ariaLabel="Consumer queue progress"
            rows={progress?.queues ?? []}
            columns={progressColumns}
            getRowId={(row) => `${row.topic}-${row.brokerName}-${row.queueId}`}
            page={1}
            pageSize={Math.max(progress?.queues.length ?? 0, 1)}
            total={progress?.queues.length ?? 0}
            onPageChange={() => undefined}
            emptyTitle="No queue progress"
            emptyDetail="This group has no queue offset entries in the current response."
          />
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
                  {Array.from(new Set(progress?.queues.map((queue) => queue.topic) ?? [])).map((topic) => (
                    <option key={topic} value={topic}>{topic}</option>
                  ))}
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
            {validationError ? <div className="inline-validation" role="status">{validationError}</div> : null}
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
