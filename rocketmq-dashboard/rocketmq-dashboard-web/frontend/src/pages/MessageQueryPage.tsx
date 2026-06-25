import * as Dialog from '@radix-ui/react-dialog';
import { Clock3, Copy, DatabaseZap, Eye, GitBranch, Hash, KeyRound, RefreshCw, RotateCcw, Search, Send, X } from 'lucide-react';
import { useEffect, useMemo, useState, type FocusEvent, type MouseEvent } from 'react';
import { messageApi } from '../api/message_api';
import { topicApi } from '../api/topic_api';
import ConfirmDialog from '../components/ConfirmDialog';
import DataTable, { type DataTableColumn } from '../components/DataTable';
import EmptyState from '../components/EmptyState';
import PageHeader from '../components/PageHeader';
import SelectMenu, { type SelectMenuOption } from '../components/SelectMenu';
import StatusBadge from '../components/StatusBadge';
import type { MessageListView, MessageTraceView, MessageView } from '../types/message';
import type { MutationResult } from '../types/topic';

type QueryMode = 'topic' | 'key' | 'id';
type NoticeTone = 'success' | 'warning' | 'danger';
type BodyToast = { body: string; x: number; y: number };
type BodyToastPoint = { clientX: number; clientY: number };

const queryModes: Array<{ key: QueryMode; label: string; description: string }> = [
  {
    key: 'topic',
    label: 'Topic',
    description: 'Scan messages by topic and store time range.'
  },
  {
    key: 'key',
    label: 'Message Key',
    description: 'Query by topic and message key. Java Dashboard returns up to 64 messages.'
  },
  {
    key: 'id',
    label: 'Message ID',
    description: 'Query by message ID and open the detail panel directly.'
  }
];

export default function MessageQueryPage() {
  const [rows, setRows] = useState<MessageView[]>([]);
  const [total, setTotal] = useState(0);
  const [topics, setTopics] = useState<string[]>([]);
  const [mode, setMode] = useState<QueryMode>('topic');
  const [topic, setTopic] = useState('');
  const [begin, setBegin] = useState(() => formatDateTimeInput(new Date(Date.now() - 60 * 60 * 1000)));
  const [end, setEnd] = useState(() => formatDateTimeInput(new Date()));
  const [key, setKey] = useState('');
  const [messageId, setMessageId] = useState('');
  const [consumerGroup, setConsumerGroup] = useState('');
  const [clientId, setClientId] = useState('');
  const [loading, setLoading] = useState(false);
  const [topicLoading, setTopicLoading] = useState(true);
  const [notice, setNotice] = useState<{ tone: NoticeTone; message: string } | null>(null);
  const [selected, setSelected] = useState<MessageView | null>(null);
  const [trace, setTrace] = useState<MessageTraceView | null>(null);
  const [traceLoading, setTraceLoading] = useState(false);
  const [resending, setResending] = useState(false);
  const [bodyToast, setBodyToast] = useState<BodyToast | null>(null);

  const activeMode = queryModes.find((item) => item.key === mode) ?? queryModes[0];

  const topicOptions = useMemo<SelectMenuOption[]>(() => {
    const values = Array.from(new Set([...topics, topic].filter(Boolean))).sort((left, right) => left.localeCompare(right));
    return values.map((value) => ({ value, label: value }));
  }, [topic, topics]);

  const querySummary = useMemo(() => {
    if (mode === 'topic') {
      return begin && end ? `${formatInputDateForDisplay(begin)} - ${formatInputDateForDisplay(end)}` : 'time range pending';
    }
    if (mode === 'key') {
      return key.trim() || 'message key pending';
    }
    return messageId.trim() || 'message id pending';
  }, [begin, end, key, messageId, mode]);

  useEffect(() => {
    setTopicLoading(true);
    topicApi
      .list()
      .then((data) => {
        const nextTopics = data.items.map((item) => item.topic).sort((left, right) => left.localeCompare(right));
        setTopics(nextTopics);
        setTopic((value) => value || preferredMessageTopic(nextTopics));
      })
      .catch((requestError: Error) => {
        setNotice({ tone: 'warning', message: `Topic list unavailable: ${requestError.message}` });
      })
      .finally(() => setTopicLoading(false));
  }, []);

  const submitQuery = () => {
    const validation = validateQuery(mode, topic, key, messageId, begin, end);
    if (validation) {
      setNotice({ tone: 'warning', message: validation });
      return;
    }

    setLoading(true);
    setNotice(null);
    const request =
      mode === 'key'
        ? messageApi.byKey(topic.trim(), key.trim())
        : mode === 'id'
          ? messageApi.byId(topic.trim(), messageId.trim())
          : messageApi.list({
              topic: topic.trim(),
              begin: toTimestamp(begin),
              end: toTimestamp(end)
            });

    request
      .then((data) => {
        applyQueryResult(data);
        if (mode === 'id') {
          const detail = data.items[0];
          if (detail) {
            setSelected(detail);
            setTrace(null);
          }
        }
      })
      .catch((requestError: Error) => {
        setRows([]);
        setTotal(0);
        setNotice({
          tone: 'danger',
          message: requestError.message
        });
      })
      .finally(() => setLoading(false));
  };

  const applyQueryResult = (data: MessageListView) => {
    setRows(data.items);
    setTotal(data.total || data.items.length);
    setNotice({
      tone: data.items.length > 0 ? 'success' : 'warning',
      message: data.items.length > 0 ? `Query completed. ${data.items.length} message(s) loaded.` : 'No matched messages.'
    });
  };

  const resetQuery = () => {
    setKey('');
    setMessageId('');
    setRows([]);
    setTotal(0);
    setSelected(null);
    setTrace(null);
    setNotice(null);
    setBegin(formatDateTimeInput(new Date(Date.now() - 60 * 60 * 1000)));
    setEnd(formatDateTimeInput(new Date()));
  };

  const loadTrace = (message = selected) => {
    if (!message) return;
    setTraceLoading(true);
    setTrace(null);
    messageApi
      .trace(message.messageId, message.topic)
      .then(setTrace)
      .catch((requestError: Error) => setNotice({ tone: 'danger', message: requestError.message }))
      .finally(() => setTraceLoading(false));
  };

  const resendMessage = (message: MessageView) => {
    if (!consumerGroup.trim()) {
      setNotice({ tone: 'warning', message: 'Consumer group is required before resending a message.' });
      return;
    }

    const targetTopic = message.topic.startsWith('%DLQ%') ? message.properties.RETRY_TOPIC || message.topic : message.topic;
    const targetMessageId = message.topic.startsWith('%DLQ%') ? message.properties.ORIGIN_MESSAGE_ID || message.messageId : message.messageId;

    setResending(true);
    messageApi
      .resend(targetMessageId, {
        topic: targetTopic,
        consumerGroup: consumerGroup.trim(),
        clientId: clientId.trim() || undefined
      })
      .then((result: MutationResult) => setNotice({ tone: 'success', message: result.message || 'Message resend requested.' }))
      .catch((requestError: Error) => setNotice({ tone: 'danger', message: requestError.message }))
      .finally(() => setResending(false));
  };

  const copyMessageId = (messageId: string) => {
    copyText(messageId);
    setNotice({ tone: 'success', message: `Copied message ID ${truncateMiddle(messageId, 26)}.` });
  };

  const showBodyToast = (body: string, point: BodyToastPoint) => {
    if (!body) {
      setBodyToast(null);
      return;
    }
    setBodyToast({ body, ...bodyToastPosition(point) });
  };

  const columns: DataTableColumn<MessageView>[] = [
    {
      header: 'Message ID',
      width: '820px',
      render: (row) => (
        <div className="message-id-cell">
          <button type="button" className="message-id-link" title={row.messageId} onClick={() => openDetail(row)}>
            <span>{row.messageId}</span>
          </button>
          <button type="button" className="message-copy-button" title="Copy message ID" onClick={() => copyMessageId(row.messageId)}>
            <Copy size={13} aria-hidden="true" /> Copy
          </button>
        </div>
      )
    },
    { header: 'Tag', width: '110px', render: (row) => <StatusBadge status={messageTags(row)} tone={messageTags(row) !== '-' ? 'success' : 'neutral'} /> },
    { header: 'Key', width: '150px', render: (row) => <span className="message-muted-value">{messageKeys(row)}</span> },
    { header: 'StoreTime', width: '210px', render: (row) => formatTimestamp(row.storeTimestamp) },
    {
      header: 'Queue',
      width: '120px',
      render: (row) => (
        <code className="message-queue-chip">
          {row.queueId} / {row.queueOffset}
        </code>
      )
    },
    {
      header: 'Body',
      width: '260px',
      render: (row) => <MessageBodyPreview body={row.body} onShow={showBodyToast} onHide={() => setBodyToast(null)} />
    },
    {
      header: 'Operation',
      width: '220px',
      render: (row) => (
        <div className="message-table-actions">
          <button type="button" className="message-action-button" title="Message detail" onClick={() => openDetail(row)}>
            <Eye size={14} aria-hidden="true" /> Detail
          </button>
          <button type="button" className="message-action-button" title="Trace message" onClick={() => openTrace(row)}>
            <GitBranch size={14} aria-hidden="true" /> Trace
          </button>
        </div>
      )
    }
  ];

  const openDetail = (message: MessageView) => {
    setSelected(message);
    setTrace(null);
  };

  const openTrace = (message: MessageView) => {
    setSelected(message);
    loadTrace(message);
  };

  return (
    <>
      <PageHeader
        title="Message"
        description="Query by topic, message key, or message ID, then inspect message body, properties, trace, and resend state."
        actions={
          <button type="button" className="button button-secondary" onClick={submitQuery} disabled={loading}>
            <RefreshCw className={loading ? 'spin' : undefined} size={15} aria-hidden="true" /> Refresh
          </button>
        }
      />

      {notice ? <div className={`notice notice-${notice.tone}`}>{notice.message}</div> : null}
      {bodyToast ? (
        <div className="message-body-hover-toast" role="tooltip" style={{ left: bodyToast.x, top: bodyToast.y }}>
          {bodyToast.body}
        </div>
      ) : null}

      <section className="message-query-panel">
        <div className="message-mode-row">
          <div className="message-mode-tabs" role="tablist" aria-label="Message query mode">
            {queryModes.map((item) => (
              <button
                type="button"
                role="tab"
                aria-selected={mode === item.key}
                className={mode === item.key ? 'active' : ''}
                key={item.key}
                onClick={() => {
                  setMode(item.key);
                  setNotice(null);
                  setTrace(null);
                }}
              >
                {item.label}
              </button>
            ))}
          </div>
          <p>{activeMode.description}</p>
        </div>

        <div className="message-query-fields">
          <label className="message-query-field">
            <span>
              <strong>*</strong> Topic
            </span>
            {topicOptions.length > 0 ? (
              <SelectMenu
                value={topic}
                options={topicOptions}
                onChange={setTopic}
                ariaLabel="Select message topic"
                className="message-topic-select"
                searchable
                searchPlaceholder="Search topics"
              />
            ) : (
              <input value={topic} placeholder={topicLoading ? 'Loading topics' : 'Topic name'} onChange={(event) => setTopic(event.target.value)} />
            )}
          </label>

          {mode === 'topic' ? (
            <>
              <label className="message-query-field message-date-field">
                <span>Begin</span>
                <input type="datetime-local" value={begin} onChange={(event) => setBegin(event.target.value)} />
              </label>
              <label className="message-query-field message-date-field">
                <span>End</span>
                <input type="datetime-local" value={end} onChange={(event) => setEnd(event.target.value)} />
              </label>
            </>
          ) : null}

          {mode === 'key' ? (
            <label className="message-query-field message-wide-field">
              <span>
                <strong>*</strong> Key
              </span>
              <input value={key} placeholder="Message key" onChange={(event) => setKey(event.target.value)} />
            </label>
          ) : null}

          {mode === 'id' ? (
            <label className="message-query-field message-wide-field">
              <span>
                <strong>*</strong> Message ID
              </span>
              <input value={messageId} placeholder="Message ID" onChange={(event) => setMessageId(event.target.value)} />
            </label>
          ) : null}

          <button type="button" className="button message-search-button" onClick={submitQuery} disabled={loading}>
            {loading ? <RefreshCw className="spin" size={15} aria-hidden="true" /> : <Search size={15} aria-hidden="true" />}
            Search
          </button>
          <button type="button" className="button button-secondary message-search-button" onClick={resetQuery}>
            Reset
          </button>
        </div>
      </section>

      <section className="message-metric-grid">
        <MessageMetric label="Rows" value={rows.length.toLocaleString()} detail={`${total.toLocaleString()} total from latest query`} icon={<DatabaseZap size={18} />} />
        <MessageMetric label="Mode" value={activeMode.label} detail="active query path" icon={<Search size={18} />} />
        <MessageMetric label="Topic" value={topic || '-'} detail="selected topic" icon={<Hash size={18} />} />
        <MessageMetric label="Condition" value={querySummary} detail="current query condition" icon={<Clock3 size={18} />} />
      </section>

      <section className="message-results-wide">
        <DataTable
          rows={rows}
          columns={columns}
          getRowId={(row) => row.messageId}
          searchPlaceholder="Search message id, tag, key, queue, body, or store time"
          emptyTitle={loading ? 'Loading messages' : 'No messages'}
          onRefresh={submitQuery}
        />
      </section>

      <MessageDetailDrawer
        message={selected}
        trace={trace}
        traceLoading={traceLoading}
        consumerGroup={consumerGroup}
        clientId={clientId}
        resending={resending}
        onConsumerGroupChange={setConsumerGroup}
        onClientIdChange={setClientId}
        onClose={() => {
          setSelected(null);
          setTrace(null);
        }}
        onTrace={() => loadTrace()}
        onResend={resendMessage}
      />
    </>
  );
}

interface MessageMetricProps {
  label: string;
  value: string;
  detail: string;
  icon: React.ReactNode;
}

function MessageMetric({ label, value, detail, icon }: MessageMetricProps) {
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

interface MessageBodyPreviewProps {
  body: string;
  onShow: (body: string, point: BodyToastPoint) => void;
  onHide: () => void;
}

function MessageBodyPreview({ body, onShow, onHide }: MessageBodyPreviewProps) {
  if (!body) {
    return <span className="message-body-preview message-body-preview-empty">-</span>;
  }

  const showFromMouse = (event: MouseEvent<HTMLElement>) => {
    onShow(body, { clientX: event.clientX, clientY: event.clientY });
  };
  const showFromFocus = (event: FocusEvent<HTMLElement>) => {
    const rect = event.currentTarget.getBoundingClientRect();
    onShow(body, { clientX: rect.left, clientY: rect.bottom });
  };

  return (
    <span
      className="message-body-preview"
      tabIndex={0}
      onMouseEnter={showFromMouse}
      onMouseMove={showFromMouse}
      onMouseLeave={onHide}
      onFocus={showFromFocus}
      onBlur={onHide}
    >
      {body}
    </span>
  );
}

interface MessageDetailDrawerProps {
  message: MessageView | null;
  trace: MessageTraceView | null;
  traceLoading: boolean;
  consumerGroup: string;
  clientId: string;
  resending: boolean;
  onConsumerGroupChange: (value: string) => void;
  onClientIdChange: (value: string) => void;
  onClose: () => void;
  onTrace: () => void;
  onResend: (message: MessageView) => void;
}

function MessageDetailDrawer({
  message,
  trace,
  traceLoading,
  consumerGroup,
  clientId,
  resending,
  onConsumerGroupChange,
  onClientIdChange,
  onClose,
  onTrace,
  onResend
}: MessageDetailDrawerProps) {
  const properties = Object.entries(message?.properties ?? {});
  const infoRows = message ? messageInfoRows(message) : [];

  return (
    <Dialog.Root open={message !== null} onOpenChange={(open) => !open && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="drawer-content message-detail-drawer">
          <div className="drawer-header">
            <div>
              <Dialog.Title>Message Detail</Dialog.Title>
              <div className="drawer-meta">
                <StatusBadge status={message?.topic ?? 'No topic'} tone="neutral" />
                <StatusBadge status={messageTags(message) || 'NO_TAG'} tone={messageTags(message) ? 'success' : 'neutral'} />
              </div>
            </div>
            <div className="message-detail-header-actions">
              <button type="button" className="button button-secondary" onClick={onTrace} disabled={!message || traceLoading}>
                <GitBranch className={traceLoading ? 'spin' : undefined} size={15} aria-hidden="true" /> Trace
              </button>
              {message ? (
                <ConfirmDialog
                  title="Resend message"
                  description={`Resend message ${message.messageId} to consumer group ${consumerGroup || '(required)'}?`}
                  confirmLabel="Resend"
                  onConfirm={() => onResend(message)}
                >
                  <button type="button" className="button button-danger" disabled={resending}>
                    {resending ? <RefreshCw className="spin" size={15} aria-hidden="true" /> : <Send size={15} aria-hidden="true" />}
                    Resend
                  </button>
                </ConfirmDialog>
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
                  <h3>Message info</h3>
                  <button type="button" className="icon-button" title="Copy message id" onClick={() => copyText(message.messageId)}>
                    <Copy size={14} aria-hidden="true" />
                  </button>
                </div>
                <div className="message-info-grid">
                  {infoRows.map((row) => (
                    <div className={row.wide ? 'wide' : undefined} key={row.label}>
                      <span>{row.label}</span>
                      <strong title={row.value}>{row.value}</strong>
                    </div>
                  ))}
                </div>
              </section>

              <section className="drawer-section">
                <div className="message-detail-section-heading">
                  <h3>Message properties</h3>
                  <StatusBadge status={`${properties.length} properties`} tone={properties.length > 0 ? 'success' : 'neutral'} />
                </div>
                {properties.length > 0 ? (
                  <div className="message-property-table">
                    {properties.map(([name, value]) => (
                      <div key={name}>
                        <span>{name}</span>
                        <strong title={value}>{value}</strong>
                      </div>
                    ))}
                  </div>
                ) : (
                  <EmptyState title="No message properties" />
                )}
              </section>

              <section className="drawer-section">
                <div className="message-detail-section-heading">
                  <h3>Message body</h3>
                  <button type="button" className="icon-button" title="Copy message body" onClick={() => copyText(message.body)}>
                    <Copy size={14} aria-hidden="true" />
                  </button>
                </div>
                <pre className="message-body-block">{message.body || '-'}</pre>
              </section>

              <section className="drawer-section">
                <div className="message-detail-section-heading">
                  <h3>Message tracking</h3>
                  <StatusBadge status={trace ? `${trace.nodes.length} trace nodes` : 'not loaded'} tone={trace ? 'success' : 'neutral'} />
                </div>
                <div className="message-resend-panel">
                  <label>
                    Consumer group
                    <input value={consumerGroup} placeholder="Consumer group for resend" onChange={(event) => onConsumerGroupChange(event.target.value)} />
                  </label>
                  <label>
                    Client ID
                    <input value={clientId} placeholder="Optional client id" onChange={(event) => onClientIdChange(event.target.value)} />
                  </label>
                </div>
                {trace ? (
                  <div className="message-trace-list">
                    {trace.nodes.map((node, index) => (
                      <div className="message-trace-node" key={`${node.nodeType}-${node.name}-${node.timestamp}-${index}`}>
                        <KeyRound size={15} aria-hidden="true" />
                        <div>
                          <strong>{node.name || node.nodeType}</strong>
                          <span>
                            {node.status || 'UNKNOWN'} / {formatTimestamp(node.timestamp)}
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                ) : (
                  <EmptyState title={traceLoading ? 'Loading trace' : 'No tracking info loaded'} />
                )}
              </section>
            </>
          ) : null}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function validateQuery(mode: QueryMode, topic: string, key: string, messageId: string, begin: string, end: string) {
  if (!topic.trim()) {
    return 'Topic is required.';
  }
  if (mode === 'key' && !key.trim()) {
    return 'Topic and message key are required.';
  }
  if (mode === 'id' && !messageId.trim()) {
    return 'Topic and message ID are required.';
  }
  if (mode === 'topic') {
    const beginTime = toTimestamp(begin);
    const endTime = toTimestamp(end);
    if (!beginTime || !endTime) {
      return 'Begin and end time are required.';
    }
    if (endTime < beginTime) {
      return 'End time must be later than begin time.';
    }
  }
  return null;
}

function preferredMessageTopic(topics: string[]) {
  return topics.find((item) => item === 'TopicTest1111') ?? topics.find((item) => isApplicationTopic(item)) ?? topics[0] ?? '';
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

function messageInfoRows(message: MessageView) {
  return [
    { label: 'Topic', value: message.topic, wide: true },
    { label: 'Message ID', value: message.messageId, wide: true },
    { label: 'Store Message ID', value: message.properties.STORE_MESSAGE_ID || '-', wide: true },
    { label: 'StoreHost', value: optionalMessageValue(message, 'storeHost') },
    { label: 'BornHost', value: optionalMessageValue(message, 'bornHost') },
    { label: 'StoreTime', value: formatTimestamp(message.storeTimestamp) },
    { label: 'BornTime', value: formatTimestamp(message.bornTimestamp) },
    { label: 'Queue ID', value: String(message.queueId) },
    { label: 'Queue Offset', value: String(message.queueOffset) },
    { label: 'StoreSize', value: optionalMessageValue(message, 'storeSize', message.body ? `${message.body.length} chars` : '-') },
    { label: 'ReconsumeTimes', value: optionalMessageValue(message, 'reconsumeTimes') },
    { label: 'BodyCRC', value: optionalMessageValue(message, 'bodyCRC') },
    { label: 'SysFlag', value: optionalMessageValue(message, 'sysFlag') },
    { label: 'Flag', value: optionalMessageValue(message, 'flag') },
    { label: 'PreparedTransactionOffset', value: optionalMessageValue(message, 'preparedTransactionOffset') }
  ];
}

function optionalMessageValue(message: MessageView, key: string, fallback = '-') {
  const value = (message as unknown as Record<string, unknown>)[key];
  return value === undefined || value === null || value === '' ? fallback : String(value);
}

function messageTags(message: MessageView | null | undefined) {
  return message?.tags || message?.properties?.TAGS || '-';
}

function messageKeys(message: MessageView | null | undefined) {
  return message?.keys || message?.properties?.KEYS || '-';
}

function formatDateTimeInput(date: Date) {
  const year = date.getFullYear();
  const month = pad2(date.getMonth() + 1);
  const day = pad2(date.getDate());
  const hour = pad2(date.getHours());
  const minute = pad2(date.getMinutes());
  return `${year}-${month}-${day}T${hour}:${minute}`;
}

function formatInputDateForDisplay(value: string) {
  return value.replace('T', ' ');
}

function toTimestamp(value: string) {
  const timestamp = new Date(value).getTime();
  return Number.isNaN(timestamp) ? undefined : timestamp;
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

function bodyToastPosition(point: BodyToastPoint) {
  const offset = 14;
  const toastMaxWidth = 760;
  const toastMaxHeight = 320;
  const viewportPadding = 16;
  const maxLeft = Math.max(viewportPadding, window.innerWidth - toastMaxWidth - viewportPadding);
  const maxTop = Math.max(viewportPadding, window.innerHeight - toastMaxHeight - viewportPadding);
  return {
    x: Math.min(Math.max(point.clientX + offset, viewportPadding), maxLeft),
    y: Math.min(Math.max(point.clientY + offset, viewportPadding), maxTop)
  };
}

function pad2(value: number) {
  return String(value).padStart(2, '0');
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
