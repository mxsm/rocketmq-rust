import { Pencil, Plus, Trash2 } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { configApi } from '../api/config_api';
import { ApiClientError } from '../api/client';
import { monitorApi } from '../api/monitor_api';
import DataTable, { type DataTableColumn } from '../components/DataTable';
import ErrorState from '../components/ErrorState';
import LoadingState from '../components/LoadingState';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import RefreshButton from '../components/RefreshButton';
import { AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription, AlertDialogTitle } from '../components/ui/AlertDialog';
import { Button } from '../components/ui/Button';
import type { ConsumerMonitorUpsertRequest, ConsumerMonitorView } from '../types/monitor';
import MonitorDialog from './monitor/MonitorDialog';
import { getConsumerMonitorMetrics } from './monitor/monitor-model';

interface MutationError {
  message: string;
  retry: () => void;
  retryLabel: string;
}

export default function MonitorPage() {
  const [rows, setRows] = useState<ConsumerMonitorView[]>([]);
  const [environmentId, setEnvironmentId] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [listError, setListError] = useState<string | null>(null);
  const [mutationError, setMutationError] = useState<MutationError | null>(null);
  const [dialogOpen, setDialogOpen] = useState(false);
  const [selectedRule, setSelectedRule] = useState<ConsumerMonitorView | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ConsumerMonitorView | null>(null);
  const [deleting, setDeleting] = useState(false);
  const listRequest = useRef(0);
  const mounted = useRef(true);

  useEffect(() => {
    mounted.current = true;
    return () => {
      mounted.current = false;
      listRequest.current += 1;
    };
  }, []);

  const load = useCallback(async () => {
    const request = ++listRequest.current;
    setLoading(true);
    setListError(null);
    try {
      const config = await configApi.getConfig();
      setEnvironmentId(config.environmentId);
      const nextRows = await monitorApi.listConsumerMonitors(config.environmentId);
      if (mounted.current && request === listRequest.current) setRows(nextRows);
    } catch (error) {
      if (mounted.current && request === listRequest.current) {
        setListError(error instanceof Error ? error.message : 'Unable to load monitor rules.');
      }
    } finally {
      if (mounted.current && request === listRequest.current) setLoading(false);
    }
  }, []);

  useEffect(() => { void load(); }, [load]);

  const refreshRule = useCallback(async (ruleEnvironmentId: string, consumerGroup: string) => {
    const config = await configApi.getConfig();
    if (config.environmentId !== ruleEnvironmentId) {
      throw new Error('The monitor environment changed. Refresh before retrying.');
    }
    const authoritativeRows = await monitorApi.listConsumerMonitors(ruleEnvironmentId);
    if (mounted.current) {
      setEnvironmentId(config.environmentId);
      setRows(authoritativeRows);
    }
    return authoritativeRows.find((item) => item.consumerGroup === consumerGroup) ?? null;
  }, []);

  const saveRule = async (request: ConsumerMonitorUpsertRequest) => {
    if (!environmentId) throw new Error('No environment is available for monitor rules.');
    if (request.environmentId !== environmentId) throw new Error('The monitor rule environment changed. Refresh before retrying the save.');
    await monitorApi.saveConsumerMonitor(request);
    void load();
  };

  const deleteRule = async (rule: ConsumerMonitorView) => {
    setDeleting(true);
    setMutationError(null);
    try {
      await monitorApi.deleteConsumerMonitor(rule.environmentId, rule.consumerGroup, rule.revision);
      if (!mounted.current) return;
      setDeleteTarget(null);
      void load();
    } catch (error) {
      if (mounted.current) {
        setDeleteTarget(null);
        let retryRule: ConsumerMonitorView | null = rule;
        let message = mutationErrorMessage(error, 'Unable to delete the monitor rule.');
        if (error instanceof ApiClientError && error.code === 'STORAGE_CONFLICT') {
          retryRule = null;
          try {
            retryRule = await refreshRule(rule.environmentId, rule.consumerGroup);
            message = retryRule
              ? `${error.message} The current rule revision is loaded; review it and retry delete.`
              : `${error.message} The rule was already removed. Refresh before taking another action.`;
          } catch {
            message = `${error.message} Refresh before retrying.`;
          }
        }
        setMutationError({
          message,
          retry: () => {
            setMutationError(null);
            if (retryRule) setDeleteTarget(retryRule);
            else void load();
          },
          retryLabel: retryRule ? 'Review delete' : 'Refresh rules'
        });
      }
    } finally {
      if (mounted.current) setDeleting(false);
    }
  };

  const metrics = useMemo(() => getConsumerMonitorMetrics(rows), [rows]);
  const columns = useMemo<DataTableColumn<ConsumerMonitorView>[]>(() => [
    { header: 'Group', render: (row) => <code>{row.consumerGroup}</code> },
    { header: 'Min Count', render: (row) => row.minCount },
    { header: 'Max Diff Total', render: (row) => row.maxDiffTotal },
    {
      header: 'Actions',
      render: (row) => (
        <div className="action-row">
          <Button type="button" variant="ghost" size="icon" aria-label={`Edit rule for ${row.consumerGroup}`} onClick={() => {
            setSelectedRule(row);
            setDialogOpen(true);
          }}>
            <Pencil size={15} aria-hidden="true" />
          </Button>
          <Button type="button" variant="ghost" size="icon" aria-label={`Delete rule for ${row.consumerGroup}`} onClick={() => setDeleteTarget(row)}>
            <Trash2 size={15} aria-hidden="true" />
          </Button>
        </div>
      )
    }
  ], []);

  return (
    <>
      <PageHeader
        title="Monitor"
        description="Persisted consumer-group threshold rules."
        actions={(
          <div className="action-row">
            <Button type="button" onClick={() => {
              setSelectedRule(null);
              setDialogOpen(true);
            }}>
              <Plus size={15} aria-hidden="true" /> Create rule
            </Button>
            <RefreshButton onRefresh={() => void load()} />
          </div>
        )}
      />

      <div className="metric-grid entity-metrics">
        <MetricCard label="Rules" value={metrics.ruleCount} detail="Persisted configurations" />
        <MetricCard label="Min Count range" value={metrics.minCountRange} detail="Across persisted rules" />
        <MetricCard label="Max Diff Total range" value={metrics.maxDiffTotalRange} detail="Across persisted rules" />
      </div>

      {mutationError ? <ErrorState message={mutationError.message} onRetry={mutationError.retry} retryLabel={mutationError.retryLabel} /> : null}
      {loading ? <LoadingState label="Loading monitor rules" /> : null}
      {!loading && listError ? <ErrorState message={listError} onRetry={() => void load()} retryLabel="Retry monitor rules" /> : null}
      {!loading && !listError ? (
        <DataTable rows={rows} columns={columns} getRowId={(row) => row.consumerGroup} searchPlaceholder="Filter monitor rules" emptyTitle="No monitor rules" />
      ) : null}

      <MonitorDialog
        open={dialogOpen}
        environmentId={environmentId ?? ''}
        rule={selectedRule}
        onOpenChange={setDialogOpen}
        onSubmit={saveRule}
        onConflict={async (consumerGroup) => {
          const authoritative = await refreshRule(environmentId ?? '', consumerGroup);
          if (authoritative) setSelectedRule(authoritative);
          return authoritative;
        }}
      />

      <AlertDialog open={Boolean(deleteTarget)} onOpenChange={(open) => {
        if (!open && !deleting) setDeleteTarget(null);
      }}>
        <AlertDialogContent>
          <AlertDialogTitle>Delete rule?</AlertDialogTitle>
          <AlertDialogDescription>
            Delete the persisted threshold rule for {deleteTarget?.consumerGroup ?? 'this group'}? This does not delete consumer data.
          </AlertDialogDescription>
          <div className="ui-alert-dialog-actions">
            <AlertDialogCancel disabled={deleting}>Cancel</AlertDialogCancel>
            <AlertDialogAction disabled={deleting} onClick={(event) => {
              event.preventDefault();
              if (deleteTarget) void deleteRule(deleteTarget);
            }}>
              {deleting ? 'Deleting' : 'Delete rule'}
            </AlertDialogAction>
          </div>
        </AlertDialogContent>
      </AlertDialog>
    </>
  );
}

function mutationErrorMessage(error: unknown, fallback: string) {
  if (error instanceof ApiClientError && error.code === 'STORAGE_CONFLICT') {
    return `${error.message} The saved state changed elsewhere; refresh before retrying.`;
  }
  return error instanceof Error ? error.message : fallback;
}
