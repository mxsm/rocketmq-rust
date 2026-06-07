import { Plus, Trash2 } from 'lucide-react';
import { useEffect, useState } from 'react';
import { monitorApi } from '../api/monitor_api';
import ConfirmDialog from '../components/ConfirmDialog';
import DataTable, { type DataTableColumn } from '../components/DataTable';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import PageHeader from '../components/PageHeader';
import type { ConsumerMonitorView } from '../types/monitor';

const columns = (onDelete: (consumerGroup: string) => void): DataTableColumn<ConsumerMonitorView>[] => [
  { header: 'Consumer Group', render: (row) => <code>{row.consumerGroup}</code> },
  { header: 'Minimum Count', render: (row) => row.minCount },
  { header: 'Max Diff Total', render: (row) => row.maxDiffTotal },
  {
    header: 'Actions',
    render: (row) => (
      <ConfirmDialog
        title="Delete monitor rule"
        description={`Delete monitor rule for ${row.consumerGroup}?`}
        confirmLabel="Delete"
        onConfirm={() => onDelete(row.consumerGroup)}
      >
        <button type="button" className="icon-button" title="Delete monitor rule">
          <Trash2 size={15} aria-hidden="true" />
        </button>
      </ConfirmDialog>
    )
  }
];

export default function MonitorPage() {
  const [rows, setRows] = useState<ConsumerMonitorView[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [consumerGroup, setConsumerGroup] = useState('');
  const [minCount, setMinCount] = useState('1');
  const [maxDiffTotal, setMaxDiffTotal] = useState('1000');

  const load = () => {
    setLoading(true);
    setError(null);
    monitorApi
      .listConsumerMonitors()
      .then(setRows)
      .catch((requestError: Error) => setError(requestError.message))
      .finally(() => setLoading(false));
  };

  useEffect(() => {
    load();
  }, []);

  const save = () => {
    setError(null);
    monitorApi
      .saveConsumerMonitor({
        consumerGroup,
        minCount: Number(minCount),
        maxDiffTotal: Number(maxDiffTotal)
      })
      .then(() => {
        setConsumerGroup('');
        load();
      })
      .catch((requestError: Error) => setError(requestError.message));
  };

  const remove = (group: string) => {
    setError(null);
    monitorApi
      .deleteConsumerMonitor(group)
      .then(load)
      .catch((requestError: Error) => setError(requestError.message));
  };

  if (loading) {
    return <LoadingState label="Loading monitor rules" />;
  }

  if (error) {
    return <ErrorState message={error} onRetry={load} />;
  }

  return (
    <>
      <PageHeader
        title="Monitors"
        description="Consumer accumulation thresholds and alert rules."
        actions={
          <div className="action-row">
            <input
              className="inline-input"
              value={consumerGroup}
              placeholder="Consumer group"
              onChange={(event) => setConsumerGroup(event.target.value)}
            />
            <input
              className="inline-input"
              type="number"
              min={0}
              value={minCount}
              placeholder="minCount"
              onChange={(event) => setMinCount(event.target.value)}
            />
            <input
              className="inline-input"
              type="number"
              min={0}
              value={maxDiffTotal}
              placeholder="maxDiffTotal"
              onChange={(event) => setMaxDiffTotal(event.target.value)}
            />
            <button type="button" className="button" onClick={save}>
              <Plus size={15} aria-hidden="true" /> Save
            </button>
          </div>
        }
      />
      <DataTable
        rows={rows}
        columns={columns(remove)}
        getRowId={(row) => row.consumerGroup}
        searchPlaceholder="Search monitor rules"
        emptyTitle="No monitor rules"
        onRefresh={load}
      />
    </>
  );
}
