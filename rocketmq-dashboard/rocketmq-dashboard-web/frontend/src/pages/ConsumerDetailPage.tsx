import { RotateCcw } from 'lucide-react';
import { useEffect, useState } from 'react';
import { useParams } from 'react-router-dom';
import { consumerApi } from '../api/consumer_api';
import ConfirmDialog from '../components/ConfirmDialog';
import DataTable from '../components/DataTable';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import type { ConsumerProgress } from '../types/consumer';

export default function ConsumerDetailPage() {
  const { group = '' } = useParams();
  const [data, setData] = useState<ConsumerProgress | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [resetTopic, setResetTopic] = useState('');
  const [resetTimestamp, setResetTimestamp] = useState(() => String(Date.now()));
  const [forceReset, setForceReset] = useState(false);

  const load = () => {
    setLoading(true);
    setError(null);
    consumerApi
      .progress(group)
      .then((progress) => {
        setData(progress);
        if (!resetTopic && progress.queues.length > 0) {
          setResetTopic(progress.queues[0].topic);
        }
      })
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, [group]);

  if (loading) return <LoadingState label="Loading consumer progress" />;
  if (error) return <ErrorState message={error} onRetry={load} />;
  const resetOffset = () => {
    const timestamp = Number(resetTimestamp);
    if (!Number.isFinite(timestamp)) {
      setError('Reset timestamp must be a millisecond timestamp.');
      return;
    }
    consumerApi
      .resetOffset(group, {
        topic: resetTopic,
        resetTimestamp: timestamp,
        force: forceReset
      })
      .then(load)
      .catch((requestError: Error) => setError(requestError.message));
  };

  return (
    <>
      <PageHeader
        title={group}
        description="Consumer group progress and queue lag."
        actions={
          <div className="action-row">
            <input
              className="inline-input"
              value={resetTopic}
              placeholder="Topic"
              onChange={(event) => setResetTopic(event.target.value)}
            />
            <input
              className="inline-input"
              value={resetTimestamp}
              placeholder="Timestamp ms"
              onChange={(event) => setResetTimestamp(event.target.value)}
            />
            <label className="compact-check">
              <input type="checkbox" checked={forceReset} onChange={(event) => setForceReset(event.target.checked)} />
              Force
            </label>
            <ConfirmDialog
              title="Reset consumer offset"
              description={`Reset ${group} on ${resetTopic || 'selected topic'}?`}
              confirmLabel="Reset"
              onConfirm={resetOffset}
            >
              <button type="button" className="button button-danger">
                <RotateCcw size={15} aria-hidden="true" /> Reset
              </button>
            </ConfirmDialog>
          </div>
        }
      />
      <div className="metric-grid">
        <MetricCard label="Topics" value={data?.topicCount ?? 0} />
        <MetricCard label="Total Lag" value={data?.diffTotal ?? 0} />
        <MetricCard label="Queues" value={data?.queues.length ?? 0} />
        <MetricCard label="Status" value="Tracked" />
      </div>
      <DataTable
        rows={data?.queues ?? []}
        columns={[
          { header: 'Topic', render: (row) => row.topic },
          { header: 'Broker', render: (row) => row.brokerName },
          { header: 'Queue', render: (row) => row.queueId },
          { header: 'Broker Offset', render: (row) => row.brokerOffset },
          { header: 'Consumer Offset', render: (row) => row.consumerOffset },
          { header: 'Diff', render: (row) => <code>{row.diff}</code> }
        ]}
        getRowId={(row) => `${row.topic}-${row.brokerName}-${row.queueId}`}
        emptyTitle="No queue progress"
        onRefresh={load}
      />
    </>
  );
}
