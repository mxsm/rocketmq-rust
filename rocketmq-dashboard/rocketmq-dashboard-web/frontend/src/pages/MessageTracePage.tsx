import { GitBranch, Hash, Network, Search } from 'lucide-react';
import { useCallback, useEffect, useRef, useState } from 'react';
import { messageApi } from '../api/message_api';
import { topicApi } from '../api/topic_api';
import AppDataTable, { type AppDataTableColumn } from '../components/AppDataTable';
import MetricCard from '../components/MetricCard';
import PageHeader from '../components/PageHeader';
import RefreshButton from '../components/RefreshButton';
import StatusBadge from '../components/StatusBadge';
import { Button } from '../components/ui/Button';
import { Card, CardContent, CardDescription, CardHeader, CardTitle } from '../components/ui/Card';
import { Input } from '../components/ui/Input';
import { Label } from '../components/ui/Label';
import { Tabs, TabsList, TabsTrigger } from '../components/ui/Tabs';
import type { MessageTraceView, MessageView } from '../types/message';
import { messageRowId, messageTraceId } from './messages/dlq-selection';
import { formatMessageTimestamp, messageKeys, messageTags, truncateIdentifier } from './messages/message-model';
import TraceTimeline from './messages/TraceTimeline';

type TraceQuery =
  | { mode: 'id'; topic: string; messageId: string }
  | { mode: 'key'; topic: string; messageKey: string };

const pageSize = 10;
const defaultTraceTopic = 'RMQ_SYS_TRACE_TOPIC';

export default function MessageTracePage() {
  const [topics, setTopics] = useState<string[]>([]);
  const [query, setQuery] = useState<TraceQuery>({ mode: 'id', topic: '', messageId: '' });
  const [traceTopic, setTraceTopic] = useState(defaultTraceTopic);
  const [rows, setRows] = useState<MessageView[]>([]);
  const [page, setPage] = useState(1);
  const [selected, setSelected] = useState<MessageView | null>(null);
  const [trace, setTrace] = useState<MessageTraceView | null>(null);
  const [candidateLoading, setCandidateLoading] = useState(false);
  const [candidateError, setCandidateError] = useState<string | null>(null);
  const [traceLoading, setTraceLoading] = useState(false);
  const [traceError, setTraceError] = useState<string | null>(null);
  const [validation, setValidation] = useState<string | null>(null);
  const [topicsLoading, setTopicsLoading] = useState(false);
  const [topicsError, setTopicsError] = useState<string | null>(null);
  const topicRequestRef = useRef(0);
  const candidateRequestRef = useRef(0);
  const traceRequestRef = useRef(0);

  const loadTopics = useCallback(async () => {
    const requestId = ++topicRequestRef.current;
    setTopicsLoading(true);
    setTopicsError(null);
    try {
      const data = await topicApi.list();
      if (topicRequestRef.current !== requestId) return;
      const nextTopics = data.items.map((item) => item.topic).sort((left, right) => left.localeCompare(right));
      setTopics(nextTopics);
      setQuery((current) => current.topic ? current : { ...current, topic: preferredTopic(nextTopics) });
    } catch (requestError) {
      if (topicRequestRef.current === requestId) {
        setTopics([]);
        setTopicsError(`Topic discovery failed: ${requestError instanceof Error ? requestError.message : String(requestError)}`);
      }
    } finally {
      if (topicRequestRef.current === requestId) setTopicsLoading(false);
    }
  }, []);

  useEffect(() => {
    void loadTopics();
    return () => { topicRequestRef.current += 1; };
  }, [loadTopics]);

  useEffect(() => () => {
    candidateRequestRef.current += 1;
    traceRequestRef.current += 1;
  }, []);

  const invalidateCandidates = () => {
    candidateRequestRef.current += 1;
    traceRequestRef.current += 1;
    setCandidateLoading(false);
    setRows([]);
    setPage(1);
    setSelected(null);
    setTrace(null);
    setCandidateError(null);
    setTraceError(null);
    setTraceLoading(false);
    setValidation(null);
  };

  const updateQuery = (nextQuery: TraceQuery) => {
    setQuery(nextQuery);
    invalidateCandidates();
  };

  const switchMode = (mode: string) => {
    setQuery(mode === 'key' ? { mode, topic: query.topic, messageKey: '' } : { mode: 'id', topic: query.topic, messageId: '' });
    invalidateCandidates();
  };

  const findCandidates = async () => {
    const issue = validateQuery(query, traceTopic);
    if (issue) {
      setValidation(issue);
      return;
    }
    const requestId = ++candidateRequestRef.current;
    traceRequestRef.current += 1;
    setCandidateLoading(true);
    setCandidateError(null);
    setValidation(null);
    setSelected(null);
    setTrace(null);
    setTraceError(null);
    setTraceLoading(false);
    try {
      const data = query.mode === 'id'
        ? await messageApi.byId(query.topic.trim(), query.messageId.trim())
        : await messageApi.byKey(query.topic.trim(), query.messageKey.trim());
      if (candidateRequestRef.current !== requestId) return;
      setRows(query.mode === 'id' ? data.items.slice(0, 1) : data.items.slice(0, 64));
      setPage(1);
    } catch (requestError) {
      if (candidateRequestRef.current === requestId) {
        setRows([]);
        setCandidateError(requestError instanceof Error ? requestError.message : String(requestError));
      }
    } finally {
      if (candidateRequestRef.current === requestId) setCandidateLoading(false);
    }
  };

  const loadTrace = async (message = selected) => {
    if (!message) return;
    const requestId = ++traceRequestRef.current;
    setSelected(message);
    setTrace(null);
    setTraceLoading(true);
    setTraceError(null);
    try {
      const data = await messageApi.trace(messageTraceId(message), message.topic, traceTopic.trim());
      if (traceRequestRef.current === requestId) setTrace(data);
    } catch (requestError) {
      if (traceRequestRef.current === requestId) setTraceError(requestError instanceof Error ? requestError.message : String(requestError));
    } finally {
      if (traceRequestRef.current === requestId) setTraceLoading(false);
    }
  };

  const columns: AppDataTableColumn<MessageView>[] = [
    { id: 'message-id', header: 'Message ID', width: '34%', cell: (row) => <span className="mono message-id-value" title={row.messageId}>{truncateIdentifier(row.messageId, 38)}</span> },
    { id: 'topic', header: 'Topic', cell: (row) => <span className="mono">{row.topic}</span> },
    { id: 'tags', header: 'Tags', cell: (row) => <StatusBadge status={messageTags(row)} tone={messageTags(row) === '-' ? 'neutral' : 'success'} /> },
    { id: 'keys', header: 'Keys', cell: (row) => <span className="mono message-muted-value">{messageKeys(row)}</span> },
    { id: 'stored', header: 'Stored', cell: (row) => formatMessageTimestamp(row.storeTimestamp) }
  ];
  const visibleRows = rows.slice((page - 1) * pageSize, page * pageSize);

  return (
    <div className="message-ops-workspace">
      <PageHeader
        title="Message trace"
        description="Find real message candidates, then render only the trace nodes returned by the backend."
        actions={<RefreshButton refreshing={candidateLoading} onRefresh={() => void findCandidates()} />}
      />

      <Card className="message-query-card">
        <CardHeader>
          <div><CardTitle>Find trace candidates</CardTitle><CardDescription>Select one message before loading its returned trace nodes.</CardDescription></div>
          <Tabs value={query.mode} onValueChange={switchMode}>
            <TabsList aria-label="Trace query mode"><TabsTrigger value="id">By message ID</TabsTrigger><TabsTrigger value="key">By message key</TabsTrigger></TabsList>
          </Tabs>
        </CardHeader>
        <CardContent>
          <div className="trace-query-grid">
            <div className="message-query-field"><Label htmlFor="trace-topic">Trace topic</Label><Input id="trace-topic" value={traceTopic} onChange={(event) => { traceRequestRef.current += 1; setTraceTopic(event.target.value); setSelected(null); setTrace(null); setTraceError(null); setTraceLoading(false); }} /></div>
            <div className="message-query-field"><Label htmlFor="trace-message-topic">Message topic</Label><select id="trace-message-topic" value={query.topic} onChange={(event) => updateQuery({ ...query, topic: event.target.value })}>{topics.length === 0 ? <option value="">Select a topic</option> : null}{topics.map((topic) => <option key={topic}>{topic}</option>)}</select></div>
            {query.mode === 'id' ? (
              <div className="message-query-field trace-identifier-field"><Label htmlFor="trace-message-id">Message ID</Label><Input id="trace-message-id" value={query.messageId} onChange={(event) => updateQuery({ ...query, messageId: event.target.value })} /></div>
            ) : (
              <div className="message-query-field trace-identifier-field"><Label htmlFor="trace-message-key">Message key</Label><Input id="trace-message-key" value={query.messageKey} onChange={(event) => updateQuery({ ...query, messageKey: event.target.value })} /></div>
            )}
            <Button type="button" className="message-query-submit" loading={candidateLoading} aria-label="Find trace candidates" onClick={() => void findCandidates()}><Search size={15} aria-hidden="true" /> Find trace candidates</Button>
          </div>
          {topicsError ? (
            <div className="notice notice-danger message-discovery-notice" role="alert">
              <span>{topicsError}</span>
              <Button type="button" variant="outline" size="sm" loading={topicsLoading} aria-label="Retry topics" onClick={() => void loadTopics()}>Retry topics</Button>
            </div>
          ) : null}
          {validation ? <div className="notice notice-warning" role="alert">{validation}</div> : null}
        </CardContent>
      </Card>

      <div className="metric-grid message-metric-grid">
        <MetricCard label="Candidates" value={rows.length} detail="Returned by message query" icon={<Search size={18} />} />
        <MetricCard label="Trace nodes" value={trace?.nodes.length ?? 0} detail="Returned without inference" icon={<GitBranch size={18} />} />
        <MetricCard label="Trace topic" value={traceTopic || '-'} detail="Forwarded to trace API" icon={<Network size={18} />} />
        <MetricCard label="Query" value={query.mode === 'id' ? 'Message ID' : 'Message key'} detail="One active query path" icon={<Hash size={18} />} />
      </div>

      <div className="trace-workspace-grid">
        <Card className="message-results-card">
          <CardHeader><div><CardTitle>Candidate messages</CardTitle><CardDescription>Activate a row to request its trace.</CardDescription></div></CardHeader>
          <CardContent>
            <AppDataTable
              ariaLabel="Trace candidate messages" rows={visibleRows} columns={columns} getRowId={messageRowId}
              page={page} pageSize={pageSize} total={rows.length} onPageChange={setPage}
              onRowActivate={(row) => void loadTrace(row)} loading={candidateLoading} error={candidateError}
              onRetry={() => void findCandidates()} emptyTitle="No candidate messages"
              emptyDetail="Run a message ID or message key query to locate candidates."
            />
          </CardContent>
        </Card>

        <Card className="trace-detail-card">
          <CardHeader>
            <div><CardTitle>Returned trace nodes</CardTitle><CardDescription>{selected ? truncateIdentifier(selected.messageId, 44) : 'Select a candidate message.'}</CardDescription></div>
            {selected ? <Button type="button" variant="outline" size="sm" loading={traceLoading} onClick={() => void loadTrace()}>Reload trace</Button> : null}
          </CardHeader>
          <CardContent>
            {selected ? <TraceTimeline nodes={trace?.nodes ?? []} loading={traceLoading} error={traceError} onRetry={() => void loadTrace()} /> : <div className="state-block"><strong>No message selected</strong><span>Choose one candidate to request its trace nodes.</span></div>}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function validateQuery(query: TraceQuery, traceTopic: string) {
  if (!traceTopic.trim()) return 'Trace topic is required.';
  if (!query.topic.trim()) return 'Message topic is required.';
  if (query.mode === 'id' && !query.messageId.trim()) return 'Message ID is required.';
  if (query.mode === 'key' && !query.messageKey.trim()) return 'Message key is required.';
  return null;
}

function preferredTopic(topics: string[]) {
  return topics.find((topic) => !topic.startsWith('%') && !topic.startsWith('RMQ_SYS')) ?? topics[0] ?? '';
}
