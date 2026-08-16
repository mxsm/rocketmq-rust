import { Download, RotateCcw, Search, ShieldAlert } from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { consumerApi } from '../api/consumer_api';
import { dlqApi } from '../api/dlq_api';
import AppDataTable, { type AppDataTableColumn } from '../components/AppDataTable';
import EntitySheet from '../components/EntitySheet';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import RefreshButton from '../components/RefreshButton';
import StatusBadge from '../components/StatusBadge';
import {
  AlertDialog, AlertDialogAction, AlertDialogCancel, AlertDialogContent, AlertDialogDescription,
  AlertDialogTitle, AlertDialogTrigger
} from '../components/ui/AlertDialog';
import { Button } from '../components/ui/Button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/Card';
import { Input } from '../components/ui/Input';
import { Label } from '../components/ui/Label';
import type { DlqMessageQueryParams, DlqMessageResendResult, MessageView } from '../types/message';
import {
  dlqResendTarget, messageRowId, selectionForPage, toggleMessageSelection, uniqueDlqResendTargets
} from './messages/dlq-selection';
import MessageDetailContent from './messages/MessageDetailContent';
import {
  formatMessageTimestamp, messageKeys, messageTags, truncateIdentifier
} from './messages/message-model';

const pageSize = 20;

export default function DlqMessagePage() {
  const [groups, setGroups] = useState<string[]>([]);
  const [consumerGroup, setConsumerGroup] = useState('');
  const [begin, setBegin] = useState(() => formatDateTimeInput(new Date(Date.now() - 3 * 60 * 60 * 1000)));
  const [end, setEnd] = useState(() => formatDateTimeInput(new Date()));
  const [messageId, setMessageId] = useState('');
  const [rows, setRows] = useState<MessageView[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [exhaustedAfterPage, setExhaustedAfterPage] = useState<number | null>(null);
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set());
  const [selected, setSelected] = useState<MessageView | null>(null);
  const [loading, setLoading] = useState(false);
  const [queryError, setQueryError] = useState<string | null>(null);
  const [validation, setValidation] = useState<string | null>(null);
  const [resending, setResending] = useState(false);
  const [resendResults, setResendResults] = useState<DlqMessageResendResult[]>([]);
  const [resendError, setResendError] = useState<string | null>(null);
  const [exporting, setExporting] = useState(false);
  const [exportError, setExportError] = useState<string | null>(null);
  const [groupsLoading, setGroupsLoading] = useState(false);
  const [groupsError, setGroupsError] = useState<string | null>(null);
  const requestRef = useRef(0);
  const retryPageRef = useRef(1);
  const groupRequestRef = useRef(0);
  const resendRequestRef = useRef(0);
  const exportRequestRef = useRef(0);
  const resendPendingRef = useRef(false);
  const mountedRef = useRef(true);
  const restoreFocusRef = useRef<HTMLElement | null>(null);

  const loadGroups = useCallback(async () => {
    const requestId = ++groupRequestRef.current;
    setGroupsLoading(true);
    setGroupsError(null);
    try {
      const data = await consumerApi.list();
      if (groupRequestRef.current !== requestId) return;
      const nextGroups = data.items.map((item) => item.rawGroupName).filter((group) => !group.startsWith('CID_RMQ_SYS_')).sort();
      setGroups(nextGroups);
      setConsumerGroup((current) => current || nextGroups[0] || '');
    } catch (requestError) {
      if (groupRequestRef.current === requestId) {
        setGroups([]);
        setGroupsError(`Consumer-group discovery failed: ${requestError instanceof Error ? requestError.message : String(requestError)}`);
      }
    } finally {
      if (groupRequestRef.current === requestId) setGroupsLoading(false);
    }
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    void loadGroups();
    return () => {
      mountedRef.current = false;
      groupRequestRef.current += 1;
      requestRef.current += 1;
      resendRequestRef.current += 1;
      exportRequestRef.current += 1;
    };
  }, [loadGroups]);

  const buildQuery = (targetPage: number): DlqMessageQueryParams => ({
    consumerGroup: consumerGroup.trim(),
    begin: toTimestamp(begin),
    end: toTimestamp(end),
    messageId: messageId.trim() || undefined,
    pageNum: targetPage,
    pageSize
  });

  const query = async (targetPage = 1) => {
    const issue = validateQuery(consumerGroup, begin, end);
    if (issue) {
      setValidation(issue);
      return;
    }
    const requestPage = messageId.trim() ? 1 : targetPage;
    retryPageRef.current = requestPage;
    resendRequestRef.current += 1;
    exportRequestRef.current += 1;
    setExporting(false);
    setResendResults([]);
    setResendError(null);
    setExportError(null);
    const requestId = ++requestRef.current;
    if (requestPage === 1) setExhaustedAfterPage(null);
    setLoading(true);
    setQueryError(null);
    setValidation(null);
    try {
      const data = await dlqApi.list(buildQuery(requestPage));
      if (requestRef.current !== requestId) return;
      if (requestPage > 1 && data.items.length === 0) {
        const fallbackPage = requestPage - 1;
        const fallback = await dlqApi.list(buildQuery(fallbackPage));
        if (requestRef.current !== requestId) return;
        setRows(fallback.items);
        setTotal(fallback.total);
        setPage(fallbackPage);
        setExhaustedAfterPage(fallbackPage);
        setSelectedIds((current) => selectionForPage(current, fallback.items.map(messageRowId)));
        setSelected(null);
        setResendResults([]);
        return;
      }
      setRows(data.items);
      setTotal(data.total);
      setPage(requestPage);
      if (data.items.length === pageSize && requestPage === page) setExhaustedAfterPage(null);
      setSelectedIds((current) => selectionForPage(current, data.items.map(messageRowId)));
      setResendResults([]);
    } catch (requestError) {
      if (requestRef.current === requestId) {
        setRows([]);
        setTotal(0);
        setSelectedIds(new Set());
        setQueryError(requestError instanceof Error ? requestError.message : String(requestError));
      }
    } finally {
      if (requestRef.current === requestId) setLoading(false);
    }
  };

  const selectedTargets = uniqueDlqResendTargets(rows, selectedIds);
  const exactIdMode = messageId.trim().length > 0;
  const tablePageSize = exactIdMode ? Math.max(1, total, rows.length) : pageSize;
  const invalidateQueryResults = () => {
    requestRef.current += 1;
    resendRequestRef.current += 1;
    exportRequestRef.current += 1;
    setLoading(false);
    setExporting(false);
    setRows([]);
    setTotal(0);
    setPage(1);
    retryPageRef.current = 1;
    setExhaustedAfterPage(null);
    setSelectedIds(new Set());
    setSelected(null);
    setResendResults([]);
    setQueryError(null);
    setResendError(null);
    setExportError(null);
  };

  const changeConsumerGroup = (nextGroup: string) => {
    setConsumerGroup(nextGroup);
    invalidateQueryResults();
  };

  const resendSelected = async () => {
    if (selectedTargets.length === 0 || resendPendingRef.current) return;
    const requestId = ++resendRequestRef.current;
    resendPendingRef.current = true;
    setResending(true);
    setResendResults([]);
    setResendError(null);
    try {
      const results = await dlqApi.resend({
        messages: selectedTargets.map((target) => ({
          topicName: target.topicName,
          consumerGroup,
          msgId: target.msgId,
          clientId: undefined
        }))
      });
      if (resendRequestRef.current === requestId) setResendResults(results);
    } catch (requestError) {
      if (resendRequestRef.current === requestId) {
        setResendError(requestError instanceof Error ? requestError.message : String(requestError));
      }
    } finally {
      resendPendingRef.current = false;
      if (mountedRef.current) setResending(false);
    }
  };

  const exportCurrentQuery = async () => {
    const issue = validateQuery(consumerGroup, begin, end);
    if (issue) {
      setValidation(issue);
      return;
    }
    const requestId = ++exportRequestRef.current;
    setExporting(true);
    setExportError(null);
    try {
      const view = await dlqApi.export(buildQuery(page));
      if (exportRequestRef.current !== requestId) return;
      const url = URL.createObjectURL(new Blob([view.csv], { type: 'text/csv;charset=utf-8' }));
      const anchor = document.createElement('a');
      anchor.href = url;
      anchor.download = view.fileName;
      anchor.click();
      URL.revokeObjectURL(url);
    } catch (requestError) {
      if (exportRequestRef.current === requestId) {
        setExportError(requestError instanceof Error ? requestError.message : String(requestError));
      }
    } finally {
      if (exportRequestRef.current === requestId) setExporting(false);
    }
  };

  const columns: AppDataTableColumn<MessageView>[] = [
    {
      id: 'select', header: <span className="sr-only">Select</span>, width: '44px', align: 'center',
      cell: (row) => {
        const rowId = messageRowId(row);
        return (
          <input
          type="checkbox" aria-label={`Select ${row.messageId}`} checked={selectedIds.has(rowId)}
          disabled={dlqResendTarget(row) === null}
          title={dlqResendTarget(row) === null ? 'Missing RETRY_TOPIC or origin message ID; resend is disabled.' : undefined}
          onChange={(event) => {
            resendRequestRef.current += 1;
            setResendResults([]);
            setResendError(null);
            setSelectedIds((current) => toggleMessageSelection(current, rowId, event.target.checked));
          }}
        />
        );
      }
    },
    { id: 'message-id', header: 'Message ID', width: '31%', cell: (row) => <span className="mono message-id-value" title={row.messageId}>{truncateIdentifier(row.messageId, 34)}</span> },
    { id: 'topic', header: 'Original topic', cell: (row) => {
      const target = dlqResendTarget(row);
      return target ? <span className="mono">{target.topicName}</span> : <StatusBadge status="Unsafe metadata" tone="danger" />;
    } },
    { id: 'tags', header: 'Tags', cell: (row) => <StatusBadge status={messageTags(row)} tone={messageTags(row) === '-' ? 'neutral' : 'warning'} /> },
    { id: 'keys', header: 'Keys', cell: (row) => <span className="mono message-muted-value">{messageKeys(row)}</span> },
    { id: 'reconsume', header: 'Retries', align: 'right', cell: (row) => row.reconsumeTimes },
    { id: 'stored', header: 'Stored', cell: (row) => formatMessageTimestamp(row.storeTimestamp) }
  ];

  return (
    <div className="message-ops-workspace">
      <PageHeader
        title="Dead-letter messages"
        description="Inspect failed deliveries, preserve server pagination, and explicitly confirm every resend batch."
        actions={<RefreshButton refreshing={loading} onRefresh={() => void query(page)} />}
      />

      <Card className="message-query-card">
        <CardHeader><div><CardTitle>Query dead-letter queue</CardTitle><CardDescription>Results are scoped to one consumer group and time window.</CardDescription></div></CardHeader>
        <CardContent>
          <div className="dlq-query-grid">
            <div className="message-query-field"><Label htmlFor="dlq-group">Consumer group</Label><select id="dlq-group" value={consumerGroup} onChange={(event) => changeConsumerGroup(event.target.value)}>{groups.length === 0 ? <option value="">Select a group</option> : null}{groups.map((group) => <option key={group}>{group}</option>)}</select></div>
            <div className="message-query-field"><Label htmlFor="dlq-begin">Begin time</Label><Input id="dlq-begin" type="datetime-local" value={begin} onChange={(event) => { setBegin(event.target.value); invalidateQueryResults(); }} /></div>
            <div className="message-query-field"><Label htmlFor="dlq-end">End time</Label><Input id="dlq-end" type="datetime-local" value={end} onChange={(event) => { setEnd(event.target.value); invalidateQueryResults(); }} /></div>
            <div className="message-query-field"><Label htmlFor="dlq-message-id">Message ID optional</Label><Input id="dlq-message-id" value={messageId} onChange={(event) => { setMessageId(event.target.value); invalidateQueryResults(); }} /></div>
            <Button type="button" className="message-query-submit" loading={loading} aria-label="Search DLQ messages" onClick={() => void query(1)}><Search size={15} aria-hidden="true" /> Search DLQ messages</Button>
          </div>
          {validation ? <div className="notice notice-warning" role="alert">{validation}</div> : null}
          {groupsError ? (
            <div className="notice notice-danger message-discovery-notice" role="alert">
              <span>{groupsError}</span>
              <Button type="button" variant="outline" size="sm" loading={groupsLoading} aria-label="Retry consumer groups" onClick={() => void loadGroups()}>Retry consumer groups</Button>
            </div>
          ) : null}
        </CardContent>
      </Card>

      <div className="metric-grid message-metric-grid">
        <MetricCard label="Scanned through" value={total} detail="Rows visited by DLQ API" icon={<ShieldAlert size={18} />} />
        <MetricCard label="Page" value={page} detail={exactIdMode ? `${rows.length} exact matches` : `${pageSize} rows per request`} icon={<Search size={18} />} />
        <MetricCard label="Selected" value={selectedTargets.length} detail="Unique original messages" icon={<RotateCcw size={18} />} />
        <MetricCard label="Consumer group" value={consumerGroup || '-'} detail="Current DLQ scope" />
      </div>

      <Card className="message-results-card">
        <CardHeader>
          <div><CardTitle>Dead-letter queue</CardTitle><CardDescription>Message bodies are available only from the detail sheet.</CardDescription></div>
          <div className="dlq-action-bar">
            <span>{selectedTargets.length} selected</span>
            <Button type="button" variant="outline" loading={exporting} onClick={() => void exportCurrentQuery()}><Download size={15} aria-hidden="true" /> Export current query</Button>
            <AlertDialog>
              <AlertDialogTrigger asChild><Button type="button" variant="destructive" disabled={selectedTargets.length === 0 || resending}><RotateCcw size={15} aria-hidden="true" /> Review selected resend</Button></AlertDialogTrigger>
              <AlertDialogContent>
                <AlertDialogTitle>Resend selected DLQ messages?</AlertDialogTitle>
                <AlertDialogDescription>Resend {selectedTargets.length} selected message(s) to their original topics for {consumerGroup}. Results are reported per message.</AlertDialogDescription>
                <div className="ui-alert-dialog-actions"><AlertDialogCancel>Cancel</AlertDialogCancel><AlertDialogAction onClick={() => void resendSelected()}>Confirm resend</AlertDialogAction></div>
              </AlertDialogContent>
            </AlertDialog>
          </div>
        </CardHeader>
        <CardContent>
          {resendError ? <div className="notice notice-danger" role="alert">{resendError}. Review the selected batch again to retry.</div> : null}
          {exportError ? (
            <div className="notice notice-danger message-discovery-notice" role="alert">
              <span>{exportError}</span>
              <Button type="button" variant="outline" size="sm" onClick={() => void exportCurrentQuery()}>Retry export</Button>
            </div>
          ) : null}
          {resendResults.length > 0 ? (
            <ul className="dlq-resend-results" role="status">
              {resendResults.map((result) => {
                const failed = !result.success;
                return (
                  <li key={result.msgId} className={failed ? 'is-danger' : 'is-success'}>
                    <strong>{result.msgId}: {result.consumeResult}</strong>
                    {result.remark ? <span>{result.remark}</span> : null}
                  </li>
                );
              })}
            </ul>
          ) : null}
          <AppDataTable
            ariaLabel="Dead-letter messages" rows={rows} columns={columns} getRowId={messageRowId}
            page={page} pageSize={tablePageSize} total={total} onPageChange={(nextPage) => void query(nextPage)}
            hasNextPage={exactIdMode ? false : rows.length === pageSize && exhaustedAfterPage !== page}
            onRowActivate={(row, origin) => { restoreFocusRef.current = origin; setSelected(row); }}
            loading={loading} error={queryError} onRetry={() => void query(retryPageRef.current)} emptyTitle="No dead-letter messages"
            emptyDetail="Run a query to inspect failed deliveries."
          />
        </CardContent>
      </Card>

      <EntitySheet
        open={selected !== null} title="Dead-letter message detail"
        description={selected ? `${consumerGroup} · ${truncateIdentifier(selected.messageId, 40)}` : undefined}
        onOpenChange={(open) => { if (!open) setSelected(null); }} restoreFocusRef={restoreFocusRef}
      >
        {selected ? <MessageDetailContent message={selected} /> : null}
      </EntitySheet>
    </div>
  );
}

function validateQuery(consumerGroup: string, begin: string, end: string) {
  if (!consumerGroup.trim()) return 'Consumer group is required.';
  if (!begin || !end) return 'Begin time and end time are required.';
  if (toTimestamp(end) <= toTimestamp(begin)) return 'End time must be after begin time.';
  return null;
}

function toTimestamp(value: string) { return new Date(value).getTime(); }

function formatDateTimeInput(date: Date) {
  const offset = date.getTimezoneOffset();
  return new Date(date.getTime() - offset * 60_000).toISOString().slice(0, 16);
}
