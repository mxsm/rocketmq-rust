import * as Dialog from '@radix-ui/react-dialog';
import {
  Clock3,
  Copy,
  DatabaseZap,
  Eye,
  GitBranch,
  Hash,
  Network,
  RefreshCw,
  Search,
  X
} from 'lucide-react';
import type { ReactNode } from 'react';
import { useEffect, useMemo, useState } from 'react';
import { messageApi } from '../api/message_api';
import { topicApi } from '../api/topic_api';
import EmptyState from '../components/EmptyState';
import PageHeader from '../components/PageHeader';
import SelectMenu, { type SelectMenuOption } from '../components/SelectMenu';
import StatusBadge from '../components/StatusBadge';
import type { MessageListView, MessageTraceNode, MessageTraceView, MessageView } from '../types/message';

type QueryMode = 'key' | 'id';
type NoticeTone = 'success' | 'warning' | 'danger';

const traceDefaultTopic = 'RMQ_SYS_TRACE_TOPIC';

const queryModes: Array<{ key: QueryMode; label: string; description: string }> = [
  {
    key: 'key',
    label: 'Message Key',
    description: 'Query by topic and message key. Only return 64 messages.'
  },
  {
    key: 'id',
    label: 'Message ID',
    description: 'Resolve one message by topic and message ID before loading trace detail.'
  }
];

export default function MessageTracePage() {
  const [topics, setTopics] = useState<string[]>([]);
  const [topic, setTopic] = useState('');
  const [traceTopic, setTraceTopic] = useState(traceDefaultTopic);
  const [mode, setMode] = useState<QueryMode>('key');
  const [messageKey, setMessageKey] = useState('');
  const [messageId, setMessageId] = useState('');
  const [rows, setRows] = useState<MessageView[]>([]);
  const [total, setTotal] = useState(0);
  const [tableQuery, setTableQuery] = useState('');
  const [selected, setSelected] = useState<MessageView | null>(null);
  const [trace, setTrace] = useState<MessageTraceView | null>(null);
  const [loading, setLoading] = useState(false);
  const [traceLoading, setTraceLoading] = useState(false);
  const [notice, setNotice] = useState<{ tone: NoticeTone; message: string } | null>(null);

  const activeMode = queryModes.find((item) => item.key === mode) ?? queryModes[0];

  const topicOptions = useMemo<SelectMenuOption[]>(() => {
    const values = Array.from(new Set([...topics, topic].filter(Boolean))).sort((left, right) => left.localeCompare(right));
    return values.map((value) => ({ value, label: value }));
  }, [topic, topics]);

  const traceTopicOptions = useMemo<SelectMenuOption[]>(() => {
    const values = Array.from(
      new Set([
        traceDefaultTopic,
        traceTopic,
        ...topics.filter((item) => !item.startsWith('%RETRY%') && !item.startsWith('%DLQ%'))
      ].filter(Boolean))
    ).sort((left, right) => (left === traceDefaultTopic ? -1 : right === traceDefaultTopic ? 1 : left.localeCompare(right)));
    return values.map((value) => ({ value, label: value }));
  }, [topics, traceTopic]);

  const filteredRows = useMemo(() => {
    const normalized = tableQuery.trim().toLowerCase();
    if (!normalized) {
      return rows;
    }
    return rows.filter((row) => JSON.stringify(row).toLowerCase().includes(normalized));
  }, [rows, tableQuery]);

  const querySummary = mode === 'key' ? messageKey.trim() || 'message key pending' : messageId.trim() || 'message id pending';

  useEffect(() => {
    topicApi
      .list()
      .then((data) => {
        const nextTopics = data.items.map((item) => item.topic).sort((left, right) => left.localeCompare(right));
        setTopics(nextTopics);
        setTopic((current) => current || preferredMessageTopic(nextTopics));
        setTraceTopic((current) => current || preferredTraceTopic(nextTopics));
      })
      .catch((requestError: Error) => {
        setNotice({ tone: 'warning', message: `Topic list unavailable: ${requestError.message}` });
      });
  }, []);

  const submitQuery = () => {
    const validation = validateQuery(mode, topic, messageKey, messageId);
    if (validation) {
      setNotice({ tone: 'warning', message: validation });
      return;
    }

    setLoading(true);
    setTrace(null);
    setSelected(null);
    setNotice(null);

    const request = mode === 'key' ? messageApi.byKey(topic.trim(), messageKey.trim()) : messageApi.byId(topic.trim(), messageId.trim());

    request
      .then((data: MessageListView) => {
        const nextItems = normalizeTraceRows(mode, topic.trim(), data.items);
        setRows(nextItems);
        setTotal(data.total || data.items.length);
        setNotice({
          tone: nextItems.length > 0 ? 'success' : 'warning',
          message:
            nextItems.length > 0
              ? `Trace query completed. ${nextItems.length} message(s) loaded.`
              : 'No messages matched the current trace query.'
        });
      })
      .catch((requestError: Error) => {
        setRows([]);
        setTotal(0);
        setNotice({ tone: 'danger', message: requestError.message });
      })
      .finally(() => setLoading(false));
  };

  const resetQuery = () => {
    setMessageKey('');
    setMessageId('');
    setRows([]);
    setTotal(0);
    setTableQuery('');
    setSelected(null);
    setTrace(null);
    setNotice(null);
  };

  const openTrace = (message: MessageView) => {
    setSelected(message);
    setTrace(null);
    setTraceLoading(true);
    messageApi
      .trace(message.messageId, message.topic, traceTopic || traceDefaultTopic)
      .then((data) => {
        setTrace(data);
        setNotice({
          tone: data.nodes.length > 0 ? 'success' : 'warning',
          message: data.nodes.length > 0 ? `Trace detail loaded with ${data.nodes.length} node(s).` : 'Trace detail returned no nodes.'
        });
      })
      .catch((requestError: Error) => {
        setTrace(null);
        setNotice({ tone: 'danger', message: requestError.message });
      })
      .finally(() => setTraceLoading(false));
  };

  const copyMessageId = (value: string) => {
    copyText(value);
    setNotice({ tone: 'success', message: `Copied message ID ${truncateMiddle(value, 28)}.` });
  };

  return (
    <>
      <PageHeader
        title="MessageTrace"
        description="Query message trace by key or message ID, then inspect delivery and consume status in a focused detail panel."
        actions={
          <button type="button" className="button button-secondary" onClick={submitQuery} disabled={loading}>
            <RefreshCw className={loading ? 'spin' : undefined} size={15} aria-hidden="true" />
            Refresh
          </button>
        }
      />

      {notice ? <div className={`notice notice-${notice.tone}`}>{notice.message}</div> : null}

      <section className="trace-topic-panel">
        <label className="message-query-field">
          <span>
            <strong>*</strong> TraceTopic
          </span>
          <SelectMenu
            value={traceTopic}
            options={traceTopicOptions}
            onChange={setTraceTopic}
            ariaLabel="Select trace topic"
            className="trace-topic-select"
          />
        </label>
        <p>Trace topic selects where the dashboard reads message trace records.</p>
      </section>

      <section className="trace-query-panel">
        <div className="trace-query-head">
          <div className="message-mode-tabs" role="tablist" aria-label="Message trace query mode">
            {queryModes.map((item) => (
              <button
                type="button"
                role="tab"
                aria-selected={mode === item.key}
                className={mode === item.key ? 'active' : ''}
                key={item.key}
                onClick={() => {
                  setMode(item.key);
                  setRows([]);
                  setTotal(0);
                  setSelected(null);
                  setTrace(null);
                  setNotice(null);
                }}
              >
                {item.label}
              </button>
            ))}
          </div>
          <p>{activeMode.description}</p>
        </div>

        <div className="trace-query-fields">
          <label className="message-query-field">
            <span>
              <strong>*</strong> Topic
            </span>
            {topicOptions.length > 0 ? (
              <SelectMenu value={topic} options={topicOptions} onChange={setTopic} ariaLabel="Select message topic" className="trace-topic-name-select" />
            ) : (
              <input value={topic} placeholder="Topic name" onChange={(event) => setTopic(event.target.value)} />
            )}
          </label>

          {mode === 'key' ? (
            <label className="message-query-field trace-wide-field">
              <span>
                <strong>*</strong> Key
              </span>
              <input value={messageKey} placeholder="Message key" onChange={(event) => setMessageKey(event.target.value)} />
            </label>
          ) : (
            <label className="message-query-field trace-wide-field">
              <span>
                <strong>*</strong> Message ID
              </span>
              <input value={messageId} placeholder="Message ID" onChange={(event) => setMessageId(event.target.value)} />
            </label>
          )}

          <button type="button" className="button trace-action-button" onClick={submitQuery} disabled={loading}>
            {loading ? <RefreshCw className="spin" size={15} aria-hidden="true" /> : <Search size={15} aria-hidden="true" />}
            Search
          </button>
          <button type="button" className="button button-secondary trace-action-button" onClick={resetQuery}>
            Reset
          </button>
        </div>
      </section>

      <section className="message-metric-grid trace-metric-grid">
        <TraceMetric label="Rows" value={rows.length.toLocaleString()} detail={`${total.toLocaleString()} total from latest query`} icon={<DatabaseZap size={18} />} />
        <TraceMetric label="Trace nodes" value={(trace?.nodes.length ?? 0).toLocaleString()} detail="loaded after detail opens" icon={<GitBranch size={18} />} />
        <TraceMetric label="Trace topic" value={traceTopic || '-'} detail="selected trace topic" icon={<Network size={18} />} />
        <TraceMetric label="Condition" value={querySummary} detail="current query condition" icon={mode === 'key' ? <Hash size={18} /> : <Clock3 size={18} />} />
      </section>

      <section className="trace-results-wide">
        <div className="table-shell">
          <div className="table-toolbar trace-table-toolbar">
            <div>
              <h2>Trace messages</h2>
              <p>Open Message Trace Detail for the selected message.</p>
            </div>
            <div className="trace-table-tools">
              <StatusBadge status={`${filteredRows.length} visible`} tone={filteredRows.length > 0 ? 'success' : 'neutral'} />
              <button type="button" className="icon-button" onClick={submitQuery} title="Refresh table" disabled={loading}>
                <RefreshCw className={loading ? 'spin' : undefined} size={16} aria-hidden="true" />
              </button>
            </div>
          </div>
          <div className="trace-search-band">
            <label className="search-box">
              <Search size={16} aria-hidden="true" />
              <input
                value={tableQuery}
                placeholder="Search message id, tag, key, or store time"
                onChange={(event) => setTableQuery(event.target.value)}
              />
            </label>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th>Message ID</th>
                  <th>Tag</th>
                  <th>Message Key</th>
                  <th>StoreTime</th>
                  <th>Trace State</th>
                  <th>Operation</th>
                </tr>
              </thead>
              <tbody>
                {filteredRows.map((row) => (
                  <tr key={row.messageId}>
                    <td>
                      <div className="message-id-cell trace-message-id-cell">
                        <button type="button" className="message-id-link" title={row.messageId} onClick={() => openTrace(row)}>
                          <span>{row.messageId}</span>
                        </button>
                        <button type="button" className="message-copy-button" title="Copy message ID" onClick={() => copyMessageId(row.messageId)}>
                          <Copy size={13} aria-hidden="true" /> Copy
                        </button>
                      </div>
                    </td>
                    <td>
                      <StatusBadge status={messageTags(row)} tone={messageTags(row) !== '-' ? 'success' : 'neutral'} />
                    </td>
                    <td>
                      <span className="message-muted-value">{messageKeys(row)}</span>
                    </td>
                    <td>{formatTimestamp(row.storeTimestamp)}</td>
                    <td>
                      <StatusBadge status={traceStateFor(row, selected, trace, traceLoading)} tone={traceStateTone(row, selected, trace, traceLoading)} />
                    </td>
                    <td>
                      <button type="button" className="message-action-button" onClick={() => openTrace(row)} disabled={traceLoading && selected?.messageId === row.messageId}>
                        {traceLoading && selected?.messageId === row.messageId ? <RefreshCw className="spin" size={14} aria-hidden="true" /> : <Eye size={14} aria-hidden="true" />}
                        Trace Detail
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {filteredRows.length === 0 ? <EmptyState title={loading ? 'Loading trace messages' : 'No trace messages'} /> : null}
          <div className="table-footer">
            <span>{filteredRows.length.toLocaleString()} rows / latest query</span>
          </div>
        </div>
      </section>

      <MessageTraceDetailDialog
        message={selected}
        trace={trace}
        traceTopic={traceTopic}
        traceLoading={traceLoading}
        onClose={() => {
          setSelected(null);
          setTrace(null);
        }}
        onCopyMessageId={copyMessageId}
      />
    </>
  );
}

interface TraceMetricProps {
  label: string;
  value: string;
  detail: string;
  icon: ReactNode;
}

function TraceMetric({ label, value, detail, icon }: TraceMetricProps) {
  return (
    <article className="message-metric-card">
      <div>
        <span>{label}</span>
        <strong title={value}>{value}</strong>
        <small>{detail}</small>
      </div>
      <div className="message-metric-icon">{icon}</div>
    </article>
  );
}

interface MessageTraceDetailDialogProps {
  message: MessageView | null;
  trace: MessageTraceView | null;
  traceTopic: string;
  traceLoading: boolean;
  onClose: () => void;
  onCopyMessageId: (value: string) => void;
}

function MessageTraceDetailDialog({
  message,
  trace,
  traceTopic,
  traceLoading,
  onClose,
  onCopyMessageId
}: MessageTraceDetailDialogProps) {
  const nodes = trace?.nodes ?? [];
  const groupedNodes = groupTraceNodes(nodes);

  return (
    <Dialog.Root open={message !== null} onOpenChange={(open) => !open && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="drawer-content message-detail-drawer trace-detail-drawer">
          <div className="drawer-header">
            <div>
              <Dialog.Title>Message Trace Detail</Dialog.Title>
              <div className="drawer-meta">
                <StatusBadge status={traceTopic || traceDefaultTopic} tone="neutral" />
                <StatusBadge status={traceLoading ? 'loading trace' : `${nodes.length} nodes`} tone={nodes.length > 0 ? 'success' : 'neutral'} />
              </div>
            </div>
            <div className="message-detail-header-actions">
              {message ? (
                <button type="button" className="button button-secondary" onClick={() => onCopyMessageId(message.messageId)}>
                  <Copy size={15} aria-hidden="true" />
                  Copy ID
                </button>
              ) : null}
              <Dialog.Close className="icon-button" title="Close">
                <X size={16} aria-hidden="true" />
              </Dialog.Close>
            </div>
          </div>

          {message ? (
            <>
              <section className="drawer-section">
                <div className="message-detail-section-heading">
                  <h3>Message trace graph</h3>
                  <StatusBadge status={trace?.traceTopic ?? traceTopic} tone="neutral" />
                </div>
                <TraceGraph nodes={nodes} loading={traceLoading} />
              </section>

              <section className="drawer-section">
                <div className="message-detail-section-heading">
                  <h3>Send message trace</h3>
                  <StatusBadge status={message.topic} tone="success" />
                </div>
                <div className="message-info-grid">
                  {sendTraceRows(message).map((row) => (
                    <div className={row.wide ? 'wide' : undefined} key={row.label}>
                      <span>{row.label}</span>
                      <strong title={row.value}>{row.value}</strong>
                    </div>
                  ))}
                </div>
              </section>

              <section className="drawer-section">
                <div className="message-detail-section-heading">
                  <h3>Consume message trace</h3>
                  <StatusBadge status={`${groupedNodes.length} consumer groups`} tone={groupedNodes.length > 0 ? 'success' : 'neutral'} />
                </div>
                {groupedNodes.length > 0 ? (
                  <div className="trace-node-table">
                    <div className="trace-node-row trace-node-head">
                      <span>Consumer group</span>
                      <span>Status</span>
                      <span>Timestamp</span>
                      <span>Node type</span>
                    </div>
                    {groupedNodes.map((node) => (
                      <div className="trace-node-row" key={`${node.name}-${node.status}-${node.timestamp}-${node.nodeType}`}>
                        <code>{node.name || '-'}</code>
                        <StatusBadge status={node.status || 'UNKNOWN'} tone={traceNodeTone(node)} />
                        <span>{formatTimestamp(node.timestamp)}</span>
                        <span>{node.nodeType || '-'}</span>
                      </div>
                    ))}
                  </div>
                ) : (
                  <EmptyState title={traceLoading ? 'Loading trace nodes' : 'No consumer trace nodes'} />
                )}
              </section>
            </>
          ) : null}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function TraceGraph({ nodes, loading }: { nodes: MessageTraceNode[]; loading: boolean }) {
  if (loading) {
    return <EmptyState title="Loading trace graph" />;
  }
  if (nodes.length === 0) {
    return <EmptyState title="No trace graph data" />;
  }

  return (
    <div className="trace-graph">
      <div className="trace-graph-axis" />
      {nodes.map((node, index) => (
        <div
          className={`trace-graph-node trace-graph-node-${traceNodeTone(node)}`}
          style={{ left: `${Math.min(84, 8 + index * 18)}%`, top: `${24 + (index % 3) * 34}px` }}
          key={`${node.name}-${node.status}-${index}`}
        >
          <span>{node.name || node.nodeType}</span>
          <small>{node.status || 'UNKNOWN'}</small>
        </div>
      ))}
    </div>
  );
}

function validateQuery(mode: QueryMode, topic: string, messageKey: string, messageId: string) {
  if (!topic.trim()) {
    return 'Topic is required.';
  }
  if (mode === 'key' && !messageKey.trim()) {
    return 'Topic and message key are required.';
  }
  if (mode === 'id' && !messageId.trim()) {
    return 'Topic and message ID are required.';
  }
  return null;
}

function preferredMessageTopic(topics: string[]) {
  return topics.find((item) => item === 'TopicTest1111') ?? topics.find((item) => isApplicationTopic(item)) ?? topics[0] ?? '';
}

function normalizeTraceRows(mode: QueryMode, topic: string, items: MessageView[]) {
  if (mode === 'id') {
    const topicRows = items.filter((item) => item.topic === topic);
    return topicRows.length > 0 ? topicRows.slice(0, 1) : items.slice(0, 1);
  }
  return items.slice(0, 64);
}

function preferredTraceTopic(topics: string[]) {
  return topics.find((item) => item === traceDefaultTopic) ?? traceDefaultTopic;
}

function isApplicationTopic(topic: string) {
  return (
    !topic.startsWith('%RETRY%') &&
    !topic.startsWith('%DLQ%') &&
    !topic.startsWith('RMQ_SYS') &&
    !topic.startsWith('rmq_sys') &&
    !topic.startsWith('SCHEDULE_TOPIC') &&
    topic !== 'OFFSET_MOVED_EVENT' &&
    !topic.endsWith('_REPLY_TOPIC') &&
    !topic.includes('SYNC_BROKER_MEMBER')
  );
}

function sendTraceRows(message: MessageView) {
  return [
    { label: 'Topic', value: message.topic },
    { label: 'Message ID', value: message.messageId, wide: true },
    { label: 'Message Key', value: messageKeys(message) },
    { label: 'Tag', value: messageTags(message) },
    { label: 'StoreTime', value: formatTimestamp(message.storeTimestamp) },
    { label: 'BornTime', value: formatTimestamp(message.bornTimestamp) },
    { label: 'Queue ID', value: String(message.queueId) },
    { label: 'Queue Offset', value: String(message.queueOffset) },
    { label: 'Store Message ID', value: message.properties.STORE_MESSAGE_ID || '-', wide: true }
  ];
}

function groupTraceNodes(nodes: MessageTraceNode[]) {
  return [...nodes].sort((left, right) => left.name.localeCompare(right.name));
}

function traceStateFor(row: MessageView, selected: MessageView | null, trace: MessageTraceView | null, traceLoading: boolean) {
  if (selected?.messageId !== row.messageId) {
    return 'not loaded';
  }
  if (traceLoading) {
    return 'loading';
  }
  return trace && trace.nodes.length > 0 ? `${trace.nodes.length} nodes` : 'no nodes';
}

function traceStateTone(row: MessageView, selected: MessageView | null, trace: MessageTraceView | null, traceLoading: boolean) {
  if (selected?.messageId !== row.messageId || traceLoading) {
    return 'neutral';
  }
  return trace && trace.nodes.length > 0 ? 'success' : 'warning';
}

function traceNodeTone(node: MessageTraceNode) {
  const normalized = node.status.toLowerCase();
  if (normalized.includes('consume') || normalized.includes('success') || normalized.includes('ok')) {
    return 'success';
  }
  if (normalized.includes('fail') || normalized.includes('error')) {
    return 'danger';
  }
  return 'warning';
}

function messageTags(message: MessageView | null | undefined) {
  return message?.tags || message?.properties?.TAGS || '-';
}

function messageKeys(message: MessageView | null | undefined) {
  return message?.keys || message?.properties?.KEYS || '-';
}

function formatTimestamp(value: number | undefined) {
  if (!value) return '-';
  return new Date(value).toLocaleString();
}

function truncateMiddle(value: string, maxLength: number) {
  if (value.length <= maxLength) return value;
  const edge = Math.floor((maxLength - 3) / 2);
  return `${value.slice(0, edge)}...${value.slice(-edge)}`;
}

function copyText(value: string) {
  if (!value) return;
  if (navigator.clipboard?.writeText) {
    void navigator.clipboard.writeText(value).catch(() => legacyCopyText(value));
    return;
  }
  legacyCopyText(value);
}

function legacyCopyText(value: string) {
  const textarea = document.createElement('textarea');
  textarea.value = value;
  textarea.setAttribute('readonly', 'true');
  textarea.style.position = 'fixed';
  textarea.style.left = '-9999px';
  textarea.style.top = '0';
  document.body.appendChild(textarea);
  textarea.select();
  document.execCommand('copy');
  textarea.remove();
}
