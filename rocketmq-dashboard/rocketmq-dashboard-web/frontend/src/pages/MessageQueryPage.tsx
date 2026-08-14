import { Clock3, DatabaseZap, Hash, Search, Send } from 'lucide-react';
import { useCallback, useEffect, useMemo, useRef, useState } from 'react';
import { messageApi } from '../api/message_api';
import { topicApi } from '../api/topic_api';
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
import { Tabs, TabsList, TabsTrigger } from '../components/ui/Tabs';
import type { MessageListView, MessageView } from '../types/message';
import MessageDetailContent from './messages/MessageDetailContent';
import { messageResendTarget, messageRowId } from './messages/dlq-selection';
import {
  formatMessageSize, formatMessageTimestamp, messageKeys, messageTags, truncateIdentifier
} from './messages/message-model';

type QueryForm =
  | { mode: 'topic'; topic: string; begin: string; end: string }
  | { mode: 'key'; topic: string; key: string }
  | { mode: 'id'; topic: string; messageId: string };

const pageSize = 10;

export default function MessageQueryPage() {
  const [topics, setTopics] = useState<string[]>([]);
  const [form, setForm] = useState<QueryForm>(() => topicForm(''));
  const [rows, setRows] = useState<MessageView[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [validation, setValidation] = useState<string | null>(null);
  const [selected, setSelected] = useState<MessageView | null>(null);
  const [consumerGroup, setConsumerGroup] = useState('');
  const [clientId, setClientId] = useState('');
  const [resending, setResending] = useState(false);
  const [resendNotice, setResendNotice] = useState<{ tone: 'success' | 'danger'; message: string } | null>(null);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState<string | null>(null);
  const requestRef = useRef(0);
  const topicRequestRef = useRef(0);
  const resendRequestRef = useRef(0);
  const resendPendingRef = useRef(false);
  const mountedRef = useRef(true);
  const restoreFocusRef = useRef<HTMLElement | null>(null);
  const searchButtonRef = useRef<HTMLButtonElement>(null);

  const loadTopics = useCallback(async () => {
    const requestId = ++topicRequestRef.current;
    setTopicsLoading(true);
    setTopicsError(null);
    try {
      const data = await topicApi.list();
      if (topicRequestRef.current !== requestId) return;
      const nextTopics = data.items.map((item) => item.topic).sort((left, right) => left.localeCompare(right));
      setTopics(nextTopics);
      setForm((current) => current.topic ? current : { ...current, topic: preferredTopic(nextTopics) });
    } catch (requestError) {
      if (topicRequestRef.current === requestId) {
        setTopics([]);
        setTopicsError(`Topic discovery failed: ${requestError instanceof Error ? requestError.message : String(requestError)}`);
      }
    } finally {
      if (topicRequestRef.current === requestId) setTopicsLoading(false);
    }
  }, []);

  const invalidateResend = useCallback(() => {
    resendRequestRef.current += 1;
    setResendNotice(null);
  }, []);

  useEffect(() => {
    mountedRef.current = true;
    void loadTopics();
    return () => {
      mountedRef.current = false;
      requestRef.current += 1;
      topicRequestRef.current += 1;
      resendRequestRef.current += 1;
    };
  }, [loadTopics]);

  useEffect(() => {
    invalidateResend();
    setConsumerGroup('');
    setClientId('');
  }, [invalidateResend, selected?.messageId]);

  const visibleRows = useMemo(() => rows.slice((page - 1) * pageSize, page * pageSize), [page, rows]);
  const columns: AppDataTableColumn<MessageView>[] = [
    {
      id: 'message-id', header: 'Message ID', width: '31%',
      cell: (row) => <span className="mono message-id-value" title={row.messageId}>{truncateIdentifier(row.messageId, 34)}</span>
    },
    { id: 'tags', header: 'Tags', cell: (row) => <StatusBadge status={messageTags(row)} tone={messageTags(row) === '-' ? 'neutral' : 'success'} /> },
    { id: 'keys', header: 'Keys', cell: (row) => <span className="mono message-muted-value">{messageKeys(row)}</span> },
    { id: 'queue', header: 'Queue / offset', cell: (row) => <span>{row.queueId} / {row.queueOffset}</span> },
    { id: 'size', header: 'Size', align: 'right', cell: (row) => formatMessageSize(row.storeSize) },
    { id: 'stored', header: 'Stored', cell: (row) => formatMessageTimestamp(row.storeTimestamp) }
  ];

  const invalidateResults = () => {
    requestRef.current += 1;
    setLoading(false);
    setRows([]);
    setTotal(0);
    setPage(1);
    setValidation(null);
    setError(null);
    setSelected(null);
  };

  const updateForm = (nextForm: QueryForm) => {
    setForm(nextForm);
    invalidateResults();
  };

  const switchMode = (mode: string) => {
    const topic = form.topic;
    setForm(mode === 'key' ? { mode, topic, key: '' } : mode === 'id' ? { mode, topic, messageId: '' } : topicForm(topic));
    invalidateResults();
  };

  const searchMessages = async () => {
    const issue = validateForm(form);
    if (issue) {
      setValidation(issue);
      return;
    }
    const requestId = ++requestRef.current;
    setLoading(true);
    setValidation(null);
    setError(null);
    try {
      const data = form.mode === 'key'
        ? await messageApi.byKey(form.topic.trim(), form.key.trim())
        : form.mode === 'id'
          ? await messageApi.byId(form.topic.trim(), form.messageId.trim())
          : await messageApi.list({ topic: form.topic.trim(), begin: toTimestamp(form.begin), end: toTimestamp(form.end) });
      if (requestRef.current !== requestId) return;
      applyResult(data);
      if (form.mode === 'id' && data.items[0]) {
        restoreFocusRef.current = searchButtonRef.current;
        setSelected(data.items[0]);
      }
    } catch (requestError) {
      if (requestRef.current === requestId) {
        setRows([]);
        setTotal(0);
        setError(requestError instanceof Error ? requestError.message : String(requestError));
      }
    } finally {
      if (requestRef.current === requestId) setLoading(false);
    }
  };

  const applyResult = (data: MessageListView) => {
    setRows(data.items);
    setTotal(data.total || data.items.length);
    setPage(1);
  };

  const openDetail = (message: MessageView, origin: HTMLElement) => {
    restoreFocusRef.current = origin;
    setSelected(message);
  };

  const resend = async () => {
    if (!selected || !consumerGroup.trim() || resendPendingRef.current) return;
    const target = messageResendTarget(selected);
    if (!target) {
      setResendNotice({ tone: 'danger', message: resendUnavailableMessage(selected) });
      return;
    }
    const requestId = ++resendRequestRef.current;
    resendPendingRef.current = true;
    setResending(true);
    setResendNotice(null);
    try {
      const result = await messageApi.resend(target.messageId, {
        topic: target.topic, consumerGroup: consumerGroup.trim(), clientId: clientId.trim() || undefined
      });
      if (resendRequestRef.current !== requestId) return;
      setResendNotice({
        tone: result.success ? 'success' : 'danger',
        message: result.remark ? `${result.consumeResult}: ${result.remark}` : result.message
      });
    } catch (requestError) {
      if (resendRequestRef.current === requestId) {
        setResendNotice({ tone: 'danger', message: requestError instanceof Error ? requestError.message : String(requestError) });
      }
    } finally {
      resendPendingRef.current = false;
      if (mountedRef.current) setResending(false);
    }
  };

  const selectedResendTarget = selected ? messageResendTarget(selected) : null;
  const selectedResendError = selected && !selectedResendTarget ? resendUnavailableMessage(selected) : null;

  return (
    <div className="message-ops-workspace">
      <PageHeader
        title="Message search"
        description="Locate a message by one explicit query path, then inspect its broker-backed details in a focused sheet."
        actions={<RefreshButton refreshing={loading} onRefresh={() => void searchMessages()} />}
      />

      <Card className="message-query-card">
        <CardHeader>
          <div><CardTitle>Query messages</CardTitle><CardDescription>Only fields in the active mode are sent to the API.</CardDescription></div>
          <Tabs value={form.mode} onValueChange={switchMode}>
            <TabsList aria-label="Message query mode">
              <TabsTrigger value="topic">By topic</TabsTrigger>
              <TabsTrigger value="key">By message key</TabsTrigger>
              <TabsTrigger value="id">By message ID</TabsTrigger>
            </TabsList>
          </Tabs>
        </CardHeader>
        <CardContent>
          <div className="message-query-grid">
            <div className="message-query-field">
              <Label htmlFor="message-topic">Message topic</Label>
              <select id="message-topic" value={form.topic} onChange={(event) => updateForm({ ...form, topic: event.target.value })}>
                {topics.length === 0 ? <option value="">Select a topic</option> : null}
                {topics.map((topic) => <option key={topic} value={topic}>{topic}</option>)}
              </select>
            </div>
            {form.mode === 'topic' ? (
              <>
                <div className="message-query-field"><Label htmlFor="message-begin">Begin time</Label><Input id="message-begin" type="datetime-local" value={form.begin} onChange={(event) => updateForm({ ...form, begin: event.target.value })} /></div>
                <div className="message-query-field"><Label htmlFor="message-end">End time</Label><Input id="message-end" type="datetime-local" value={form.end} onChange={(event) => updateForm({ ...form, end: event.target.value })} /></div>
              </>
            ) : form.mode === 'key' ? (
              <div className="message-query-field message-query-field-wide"><Label htmlFor="message-key">Message key</Label><Input id="message-key" value={form.key} onChange={(event) => updateForm({ ...form, key: event.target.value })} /></div>
            ) : (
              <div className="message-query-field message-query-field-wide"><Label htmlFor="message-id">Message ID</Label><Input id="message-id" value={form.messageId} onChange={(event) => updateForm({ ...form, messageId: event.target.value })} /></div>
            )}
            <Button ref={searchButtonRef} type="button" className="message-query-submit" loading={loading} aria-label="Search messages" onClick={() => void searchMessages()}>
              <Search size={15} aria-hidden="true" /> Search messages
            </Button>
          </div>
          {validation ? <div className="notice notice-warning" role="alert">{validation}</div> : null}
          {topicsError ? (
            <div className="notice notice-danger message-discovery-notice" role="alert">
              <span>{topicsError}</span>
              <Button type="button" variant="outline" size="sm" loading={topicsLoading} aria-label="Retry topics" onClick={() => void loadTopics()}>Retry topics</Button>
            </div>
          ) : null}
        </CardContent>
      </Card>

      <div className="metric-grid message-metric-grid">
        <MetricCard label="Returned" value={rows.length} detail={`${total} reported by API`} icon={<DatabaseZap size={18} />} />
        <MetricCard label="Query path" value={form.mode === 'topic' ? 'Topic range' : form.mode === 'key' ? 'Message key' : 'Message ID'} detail="One active API contract" icon={<Search size={18} />} />
        <MetricCard label="Topic" value={form.topic || '-'} detail="Current scope" icon={<Hash size={18} />} />
        <MetricCard label="Window" value={form.mode === 'topic' ? 'Time bounded' : 'Identifier'} detail="Current condition" icon={<Clock3 size={18} />} />
      </div>

      <Card className="message-results-card">
        <CardHeader><div><CardTitle>Query results</CardTitle><CardDescription>Activate a row to inspect the message body and properties.</CardDescription></div></CardHeader>
        <CardContent>
          <AppDataTable
            ariaLabel="Message search results" rows={visibleRows} columns={columns} getRowId={messageRowId}
            page={page} pageSize={pageSize} total={rows.length} onPageChange={setPage} onRowActivate={openDetail}
            loading={loading} error={error} onRetry={() => void searchMessages()} emptyTitle="No messages"
            emptyDetail="Run a query to inspect matching messages."
          />
        </CardContent>
      </Card>

      <EntitySheet
        open={selected !== null} title="Message detail"
        description={selected ? `${selected.topic} · ${truncateIdentifier(selected.messageId, 42)}` : undefined}
        onOpenChange={(open) => { if (!open) setSelected(null); }} restoreFocusRef={restoreFocusRef}
      >
        {selected ? (
          <>
            <MessageDetailContent message={selected} />
            <section className="message-resend-panel">
              <div><h3>Resend message</h3><p>Submit a broker-backed resend request to one consumer group.</p></div>
              {selectedResendError ? <div className="notice notice-danger" role="alert">{selectedResendError}</div> : null}
              {resendNotice ? <div className={`notice notice-${resendNotice.tone}`} role="status">{resendNotice.message}</div> : null}
              <div className="message-resend-fields">
                <div><Label htmlFor="resend-consumer-group">Consumer group</Label><Input id="resend-consumer-group" value={consumerGroup} disabled={resending} onChange={(event) => setConsumerGroup(event.target.value)} /></div>
                <div><Label htmlFor="resend-client-id">Client ID optional</Label><Input id="resend-client-id" value={clientId} disabled={resending} onChange={(event) => setClientId(event.target.value)} /></div>
              </div>
              <AlertDialog>
                <AlertDialogTrigger asChild><Button type="button" variant="destructive" disabled={!selectedResendTarget || !consumerGroup.trim() || resending}><Send size={15} aria-hidden="true" /> Review resend</Button></AlertDialogTrigger>
                <AlertDialogContent>
                  <AlertDialogTitle>Resend message?</AlertDialogTitle>
                  <AlertDialogDescription>
                    Resend original message {selectedResendTarget?.messageId} to topic {selectedResendTarget?.topic} for {consumerGroup || 'the selected consumer group'}. This operation is not reversible.
                  </AlertDialogDescription>
                  <div className="ui-alert-dialog-actions"><AlertDialogCancel>Cancel</AlertDialogCancel><AlertDialogAction onClick={() => void resend()}>Confirm resend</AlertDialogAction></div>
                </AlertDialogContent>
              </AlertDialog>
            </section>
          </>
        ) : null}
      </EntitySheet>
    </div>
  );
}

function topicForm(topic: string): QueryForm {
  return { mode: 'topic', topic, begin: formatDateTimeInput(new Date(Date.now() - 60 * 60 * 1000)), end: formatDateTimeInput(new Date()) };
}

function preferredTopic(topics: string[]) {
  return topics.find((topic) => !topic.startsWith('%') && !topic.startsWith('RMQ_SYS')) ?? topics[0] ?? '';
}

function validateForm(form: QueryForm) {
  if (!form.topic.trim()) return 'Message topic is required.';
  if (form.mode === 'key' && !form.key.trim()) return 'Message key is required.';
  if (form.mode === 'id' && !form.messageId.trim()) return 'Message ID is required.';
  if (form.mode === 'topic') {
    if (!form.begin || !form.end) return 'Begin time and end time are required.';
    if (toTimestamp(form.end) <= toTimestamp(form.begin)) return 'End time must be after begin time.';
  }
  return null;
}

function toTimestamp(value: string) { return new Date(value).getTime(); }

function resendUnavailableMessage(message: MessageView) {
  return message.topic.startsWith('%DLQ%')
    ? 'Missing RETRY_TOPIC or origin message ID. Resend is disabled for this DLQ message.'
    : 'Missing STORE_MESSAGE_ID. Resend is disabled for this message.';
}

function formatDateTimeInput(date: Date) {
  const offset = date.getTimezoneOffset();
  return new Date(date.getTime() - offset * 60_000).toISOString().slice(0, 16);
}
