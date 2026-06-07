import * as Dialog from '@radix-ui/react-dialog';
import {
  CheckSquare,
  Clock3,
  Copy,
  DatabaseZap,
  Download,
  Eye,
  FileDown,
  Hash,
  Inbox,
  RefreshCw,
  RotateCcw,
  Search,
  Send,
  ShieldAlert,
  X
} from 'lucide-react';
import type { ReactNode } from 'react';
import { useEffect, useMemo, useState } from 'react';
import { consumerApi } from '../api/consumer_api';
import { dlqApi } from '../api/dlq_api';
import ConfirmDialog from '../components/ConfirmDialog';
import EmptyState from '../components/EmptyState';
import PageHeader from '../components/PageHeader';
import SelectMenu, { type SelectMenuOption } from '../components/SelectMenu';
import StatusBadge from '../components/StatusBadge';
import type { DlqMessageQueryParams, MessageListView, MessageView } from '../types/message';

type QueryMode = 'consumer' | 'messageId';
type NoticeTone = 'success' | 'warning' | 'danger';
type ExportSource = 'single' | 'selected';

interface ExportDialogState {
  rows: MessageView[];
  source: ExportSource;
}

const pageSize = 20;
const sysConsumerPrefix = 'CID_RMQ_SYS_';

const queryModes: Array<{ key: QueryMode; label: string; description: string }> = [
  {
    key: 'consumer',
    label: 'Consumer',
    description: 'Scan DLQ messages for a consumer group across a store-time range.'
  },
  {
    key: 'messageId',
    label: 'Message ID',
    description: 'Look up one DLQ message by consumer group and message ID.'
  }
];

export default function DlqMessagePage() {
  const [mode, setMode] = useState<QueryMode>('consumer');
  const [consumerGroup, setConsumerGroup] = useState('');
  const [consumerGroups, setConsumerGroups] = useState<string[]>([]);
  const [consumerLoading, setConsumerLoading] = useState(true);
  const [begin, setBegin] = useState(() => formatDateTimeInput(new Date(Date.now() - 3 * 60 * 60 * 1000)));
  const [end, setEnd] = useState(() => formatDateTimeInput(new Date()));
  const [messageId, setMessageId] = useState('');
  const [rows, setRows] = useState<MessageView[]>([]);
  const [total, setTotal] = useState(0);
  const [page, setPage] = useState(1);
  const [tableQuery, setTableQuery] = useState('');
  const [selectedIds, setSelectedIds] = useState<Set<string>>(() => new Set());
  const [selectedMessage, setSelectedMessage] = useState<MessageView | null>(null);
  const [exportDialog, setExportDialog] = useState<ExportDialogState | null>(null);
  const [loading, setLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState(false);
  const [notice, setNotice] = useState<{ tone: NoticeTone; message: string } | null>(null);

  const activeMode = queryModes.find((item) => item.key === mode) ?? queryModes[0];

  const consumerOptions = useMemo<SelectMenuOption[]>(() => {
    const values = Array.from(new Set([...consumerGroups, consumerGroup].filter(Boolean))).sort((left, right) => left.localeCompare(right));
    return values.map((value) => ({ value, label: value }));
  }, [consumerGroup, consumerGroups]);

  const selectedRows = useMemo(() => rows.filter((row) => selectedIds.has(row.messageId)), [rows, selectedIds]);

  const filteredRows = useMemo(() => {
    const normalized = tableQuery.trim().toLowerCase();
    if (!normalized) {
      return rows;
    }
    return rows.filter((row) => JSON.stringify(row).toLowerCase().includes(normalized));
  }, [rows, tableQuery]);

  const pageCount = Math.max(1, Math.ceil(Math.max(total, rows.length) / pageSize));
  const allVisibleSelected = filteredRows.length > 0 && filteredRows.every((row) => selectedIds.has(row.messageId));
  const querySummary =
    mode === 'consumer'
      ? `${formatInputDateForDisplay(begin)} - ${formatInputDateForDisplay(end)}`
      : messageId.trim() || 'message id pending';

  useEffect(() => {
    setConsumerLoading(true);
    consumerApi
      .list()
      .then((data) => {
        const groups = data.items
          .map((item) => item.group)
          .filter((group) => group && !group.startsWith(sysConsumerPrefix))
          .sort((left, right) => left.localeCompare(right));
        setConsumerGroups(groups);
        setConsumerGroup((current) => current || preferredConsumerGroup(groups));
      })
      .catch((requestError: Error) => {
        setNotice({ tone: 'warning', message: `Consumer group list unavailable: ${requestError.message}` });
      })
      .finally(() => setConsumerLoading(false));
  }, []);

  const submitQuery = (targetPage = 1) => {
    const validation = validateQuery(mode, consumerGroup, messageId, begin, end);
    if (validation) {
      setNotice({ tone: 'warning', message: validation });
      return;
    }

    setLoading(true);
    setNotice(null);
    setSelectedIds(new Set());

    dlqApi
      .list(buildQuery(targetPage))
      .then((data: MessageListView) => {
        setRows(data.items);
        setTotal(data.total || data.items.length);
        setPage(targetPage);
        setNotice({
          tone: data.items.length > 0 ? 'success' : 'warning',
          message:
            data.items.length > 0
              ? `DLQ query completed. ${data.items.length} message(s) loaded.`
              : 'No DLQ messages matched the current query.'
        });
      })
      .catch((requestError: Error) => {
        setRows([]);
        setTotal(0);
        setNotice({ tone: 'danger', message: requestError.message });
      })
      .finally(() => setLoading(false));
  };

  const buildQuery = (targetPage = page): DlqMessageQueryParams => {
    const base = { consumerGroup: consumerGroup.trim() };
    if (mode === 'messageId') {
      return {
        ...base,
        messageId: messageId.trim()
      };
    }
    return {
      ...base,
      begin: toTimestamp(begin),
      end: toTimestamp(end),
      pageNum: targetPage,
      pageSize
    };
  };

  const toggleRow = (row: MessageView, checked: boolean) => {
    setSelectedIds((current) => {
      const next = new Set(current);
      if (checked) {
        next.add(row.messageId);
      } else {
        next.delete(row.messageId);
      }
      return next;
    });
  };

  const toggleVisibleRows = (checked: boolean) => {
    setSelectedIds((current) => {
      const next = new Set(current);
      filteredRows.forEach((row) => {
        if (checked) {
          next.add(row.messageId);
        } else {
          next.delete(row.messageId);
        }
      });
      return next;
    });
  };

  const resetQuery = () => {
    setMessageId('');
    setRows([]);
    setTotal(0);
    setPage(1);
    setTableQuery('');
    setSelectedIds(new Set());
    setSelectedMessage(null);
    setExportDialog(null);
    setNotice(null);
    setBegin(formatDateTimeInput(new Date(Date.now() - 3 * 60 * 60 * 1000)));
    setEnd(formatDateTimeInput(new Date()));
  };

  const resendRows = (targetRows: MessageView[]) => {
    if (targetRows.length === 0) {
      setNotice({ tone: 'warning', message: 'Select at least one DLQ message before resending.' });
      return;
    }
    if (!consumerGroup.trim()) {
      setNotice({ tone: 'warning', message: 'Consumer group is required before resending DLQ messages.' });
      return;
    }

    setActionLoading(true);
    setNotice(null);
    dlqApi
      .resend({
        messages: targetRows.map((message) => ({
          topicName: retryTopic(message),
          consumerGroup: consumerGroup.trim(),
          msgId: originMessageId(message),
          clientId: undefined
        }))
      })
      .then((results) => {
        const failed = results.filter((item) => !isResendOk(item.consumeResult)).length;
        setSelectedIds(new Set());
        setNotice({
          tone: failed === 0 ? 'success' : 'warning',
          message:
            failed === 0
              ? `Resend requested for ${results.length} DLQ message(s).`
              : `Resend completed with ${failed} failed item(s).`
        });
      })
      .catch((requestError: Error) => {
        setNotice({ tone: 'danger', message: requestError.message });
      })
      .finally(() => setActionLoading(false));
  };

  const openExportDialog = (targetRows: MessageView[], source: ExportSource) => {
    if (targetRows.length === 0) {
      setNotice({ tone: 'warning', message: 'Select at least one DLQ message before exporting.' });
      return;
    }
    setExportDialog({ rows: targetRows, source });
  };

  const downloadExport = () => {
    if (!exportDialog) return;
    const fileName =
      exportDialog.source === 'single'
        ? `dlq-${sanitizeFileName(exportDialog.rows[0]?.messageId ?? 'message')}.csv`
        : `dlq-${sanitizeFileName(consumerGroup || 'selected')}-${exportDialog.rows.length}.csv`;
    downloadTextFile(fileName, buildDlqCsv(exportDialog.rows));
    setNotice({ tone: 'success', message: `Exported ${exportDialog.rows.length} DLQ message(s) to ${fileName}.` });
    setExportDialog(null);
  };

  const exportCurrentQuery = () => {
    const validation = validateQuery(mode, consumerGroup, messageId, begin, end);
    if (validation) {
      setNotice({ tone: 'warning', message: validation });
      return;
    }

    setActionLoading(true);
    dlqApi
      .export(buildQuery(page))
      .then((view) => {
        downloadTextFile(view.fileName, view.csv);
        setNotice({ tone: 'success', message: `Exported ${view.rows.length} DLQ message(s) from the current query.` });
      })
      .catch((requestError: Error) => setNotice({ tone: 'danger', message: requestError.message }))
      .finally(() => setActionLoading(false));
  };

  const copyMessageId = (value: string) => {
    copyText(value);
    setNotice({ tone: 'success', message: `Copied message ID ${truncateMiddle(value, 28)}.` });
  };

  return (
    <>
      <PageHeader
        title="DLQMessage"
        description="Recover dead-letter messages by consumer group or message ID, then inspect, resend, or export the matched records."
        actions={
          <button type="button" className="button button-secondary" onClick={() => submitQuery(page)} disabled={loading}>
            <RefreshCw className={loading ? 'spin' : undefined} size={15} aria-hidden="true" />
            Refresh
          </button>
        }
      />

      {notice ? <div className={`notice notice-${notice.tone}`}>{notice.message}</div> : null}

      <section className="dlq-query-panel">
        <div className="dlq-query-head">
          <div className="message-mode-tabs" role="tablist" aria-label="DLQ query mode">
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
                  setPage(1);
                  setSelectedIds(new Set());
                  setNotice(null);
                }}
              >
                {item.label}
              </button>
            ))}
          </div>
          <p>{activeMode.description}</p>
        </div>

        <div className="dlq-query-fields">
          <label className="message-query-field">
            <span>
              <strong>*</strong> Consumer
            </span>
            {consumerOptions.length > 0 ? (
              <SelectMenu
                value={consumerGroup}
                options={consumerOptions}
                onChange={setConsumerGroup}
                ariaLabel="Select consumer group"
                className="dlq-consumer-select"
              />
            ) : (
              <input
                value={consumerGroup}
                placeholder={consumerLoading ? 'Loading consumer groups' : 'Consumer group'}
                onChange={(event) => setConsumerGroup(event.target.value)}
              />
            )}
          </label>

          {mode === 'consumer' ? (
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
          ) : (
            <label className="message-query-field dlq-message-id-field">
              <span>
                <strong>*</strong> Message ID
              </span>
              <input value={messageId} placeholder="DLQ message ID" onChange={(event) => setMessageId(event.target.value)} />
            </label>
          )}

          <button type="button" className="button dlq-action-button" onClick={() => submitQuery(1)} disabled={loading}>
            {loading ? <RefreshCw className="spin" size={15} aria-hidden="true" /> : <Search size={15} aria-hidden="true" />}
            Search
          </button>
          <button type="button" className="button button-secondary dlq-action-button" onClick={resetQuery}>
            Reset
          </button>
          <ConfirmDialog
            title="Resend selected DLQ messages"
            description={`Resend ${selectedRows.length} selected DLQ message(s) to consumer group ${consumerGroup || '(required)'}?`}
            confirmLabel="Resend selected"
            onConfirm={() => resendRows(selectedRows)}
          >
            <button type="button" className="button button-danger dlq-action-button" disabled={selectedRows.length === 0 || actionLoading}>
              <Send size={15} aria-hidden="true" />
              Batch resend
            </button>
          </ConfirmDialog>
          <button
            type="button"
            className="button button-secondary dlq-action-button"
            disabled={selectedRows.length === 0 || actionLoading}
            onClick={() => openExportDialog(selectedRows, 'selected')}
          >
            <FileDown size={15} aria-hidden="true" />
            Batch export
          </button>
        </div>
      </section>

      <section className="message-metric-grid dlq-metric-grid">
        <MessageMetric label="Rows" value={rows.length.toLocaleString()} detail={`${total.toLocaleString()} total from latest query`} icon={<DatabaseZap size={18} />} />
        <MessageMetric label="Selected" value={selectedRows.length.toLocaleString()} detail="batch resend/export target" icon={<CheckSquare size={18} />} />
        <MessageMetric label="Consumer" value={consumerGroup || '-'} detail="selected consumer group" icon={<Inbox size={18} />} />
        <MessageMetric label="Condition" value={querySummary} detail="current query condition" icon={mode === 'consumer' ? <Clock3 size={18} /> : <Hash size={18} />} />
      </section>

      <section className="dlq-results-wide">
        <div className="table-shell">
          <div className="table-toolbar dlq-table-toolbar">
            <div>
              <h2>DLQ messages</h2>
              <p>Java-compatible row operations with guarded resend and CSV export flows.</p>
            </div>
            <div className="dlq-table-tools">
              <StatusBadge status={`${filteredRows.length} visible`} tone={filteredRows.length > 0 ? 'success' : 'neutral'} />
              <button type="button" className="button button-secondary" onClick={exportCurrentQuery} disabled={actionLoading || rows.length === 0}>
                <Download size={15} aria-hidden="true" />
                Export query
              </button>
              <button type="button" className="icon-button" onClick={() => submitQuery(page)} title="Refresh table" disabled={loading}>
                <RefreshCw className={loading ? 'spin' : undefined} size={16} aria-hidden="true" />
              </button>
            </div>
          </div>
          <div className="dlq-search-band">
            <label className="search-box">
              <Search size={16} aria-hidden="true" />
              <input
                value={tableQuery}
                placeholder="Search message id, tag, key, retry topic, or store time"
                onChange={(event) => setTableQuery(event.target.value)}
              />
            </label>
          </div>
          <div className="table-scroll">
            <table>
              <thead>
                <tr>
                  <th className="dlq-checkbox-column">
                    <input
                      aria-label="Select visible DLQ messages"
                      className="dlq-checkbox"
                      type="checkbox"
                      checked={allVisibleSelected}
                      disabled={filteredRows.length === 0}
                      onChange={(event) => toggleVisibleRows(event.target.checked)}
                    />
                  </th>
                  <th>Message ID</th>
                  <th>Tag</th>
                  <th>Key</th>
                  <th>StoreTime</th>
                  <th>Retry Topic</th>
                  <th>Queue</th>
                  <th>Operation</th>
                </tr>
              </thead>
              <tbody>
                {filteredRows.map((row) => (
                  <tr key={row.messageId}>
                    <td className="dlq-checkbox-column">
                      <input
                        aria-label={`Select DLQ message ${row.messageId}`}
                        className="dlq-checkbox"
                        type="checkbox"
                        checked={selectedIds.has(row.messageId)}
                        onChange={(event) => toggleRow(row, event.target.checked)}
                      />
                    </td>
                    <td>
                      <div className="message-id-cell dlq-message-id-cell">
                        <button type="button" className="message-id-link" title={row.messageId} onClick={() => setSelectedMessage(row)}>
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
                      <code className="dlq-topic-code" title={retryTopic(row)}>
                        {retryTopic(row)}
                      </code>
                    </td>
                    <td>
                      <code className="message-queue-chip">
                        {row.queueId} / {row.queueOffset}
                      </code>
                    </td>
                    <td>
                      <div className="message-table-actions dlq-table-actions">
                        <button type="button" className="message-action-button" title="Message detail" onClick={() => setSelectedMessage(row)}>
                          <Eye size={14} aria-hidden="true" /> Detail
                        </button>
                        <ConfirmDialog
                          title="Resend DLQ message"
                          description={`Resend message ${row.messageId} to consumer group ${consumerGroup || '(required)'}?`}
                          confirmLabel="Resend"
                          onConfirm={() => resendRows([row])}
                        >
                          <button type="button" className="message-action-button dlq-danger-action" title="Resend message" disabled={actionLoading}>
                            <RotateCcw size={14} aria-hidden="true" /> Resend
                          </button>
                        </ConfirmDialog>
                        <button type="button" className="message-action-button" title="Export message" onClick={() => openExportDialog([row], 'single')}>
                          <Download size={14} aria-hidden="true" /> Export
                        </button>
                      </div>
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
          {filteredRows.length === 0 ? <EmptyState title={loading ? 'Loading DLQ messages' : 'No DLQ messages'} /> : null}
          <div className="table-footer">
            <span>
              {total.toLocaleString()} rows / page {page} of {pageCount}
            </span>
            <div className="pagination">
              <button type="button" className="button button-secondary" disabled={page <= 1 || loading} onClick={() => submitQuery(page - 1)}>
                Previous
              </button>
              <button type="button" className="button button-secondary" disabled={page >= pageCount || loading} onClick={() => submitQuery(page + 1)}>
                Next
              </button>
            </div>
          </div>
        </div>
      </section>

      <DlqMessageDetailDrawer
        message={selectedMessage}
        consumerGroup={consumerGroup}
        actionLoading={actionLoading}
        onClose={() => setSelectedMessage(null)}
        onCopyMessageId={copyMessageId}
        onResend={(message) => resendRows([message])}
        onExport={(message) => openExportDialog([message], 'single')}
      />
      <ExportDialog state={exportDialog} onClose={() => setExportDialog(null)} onDownload={downloadExport} />
    </>
  );
}

interface MessageMetricProps {
  label: string;
  value: string;
  detail: string;
  icon: ReactNode;
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

interface DlqMessageDetailDrawerProps {
  message: MessageView | null;
  consumerGroup: string;
  actionLoading: boolean;
  onClose: () => void;
  onCopyMessageId: (value: string) => void;
  onResend: (message: MessageView) => void;
  onExport: (message: MessageView) => void;
}

function DlqMessageDetailDrawer({
  message,
  consumerGroup,
  actionLoading,
  onClose,
  onCopyMessageId,
  onResend,
  onExport
}: DlqMessageDetailDrawerProps) {
  const properties = Object.entries(message?.properties ?? {});
  const infoRows = message ? messageInfoRows(message) : [];

  return (
    <Dialog.Root open={message !== null} onOpenChange={(open) => !open && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="drawer-content message-detail-drawer dlq-detail-drawer">
          <div className="drawer-header">
            <div>
              <Dialog.Title>DLQ Message Detail</Dialog.Title>
              <div className="drawer-meta">
                <StatusBadge status={message?.topic ?? 'No topic'} tone="neutral" />
                <StatusBadge status={messageTags(message) || 'NO_TAG'} tone={messageTags(message) ? 'success' : 'neutral'} />
              </div>
            </div>
            <div className="message-detail-header-actions">
              {message ? (
                <>
                  <button type="button" className="button button-secondary" onClick={() => onCopyMessageId(message.messageId)}>
                    <Copy size={15} aria-hidden="true" />
                    Copy ID
                  </button>
                  <button type="button" className="button button-secondary" onClick={() => onExport(message)}>
                    <Download size={15} aria-hidden="true" />
                    Export
                  </button>
                  <ConfirmDialog
                    title="Resend DLQ message"
                    description={`Resend ${message.messageId} to consumer group ${consumerGroup || '(required)'}?`}
                    confirmLabel="Resend"
                    onConfirm={() => onResend(message)}
                  >
                    <button type="button" className="button button-danger" disabled={actionLoading}>
                      <Send size={15} aria-hidden="true" />
                      Resend
                    </button>
                  </ConfirmDialog>
                </>
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
                  <button type="button" className="icon-button" title="Copy message id" onClick={() => onCopyMessageId(message.messageId)}>
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
                  <h3>Recovery</h3>
                  <StatusBadge status={retryTopic(message)} tone="warning" />
                </div>
                <div className="dlq-recovery-panel">
                  <ShieldAlert size={18} aria-hidden="true" />
                  <div>
                    <strong>Resend target</strong>
                    <span>
                      Consumer group <code>{consumerGroup || '-'}</code>, retry topic <code>{retryTopic(message)}</code>, origin message{' '}
                      <code>{originMessageId(message)}</code>.
                    </span>
                  </div>
                </div>
              </section>
            </>
          ) : null}
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

interface ExportDialogProps {
  state: ExportDialogState | null;
  onClose: () => void;
  onDownload: () => void;
}

function ExportDialog({ state, onClose, onDownload }: ExportDialogProps) {
  return (
    <Dialog.Root open={state !== null} onOpenChange={(open) => !open && onClose()}>
      <Dialog.Portal>
        <Dialog.Overlay className="dialog-overlay" />
        <Dialog.Content className="dialog-content dlq-export-dialog">
          <Dialog.Title className="dialog-title">
            <FileDown size={18} aria-hidden="true" />
            Export DLQ messages
          </Dialog.Title>
          <Dialog.Description className="dialog-description">
            {state?.source === 'single'
              ? 'Export this DLQ message as CSV with Java-compatible fields.'
              : `Export ${state?.rows.length ?? 0} selected DLQ message(s) as CSV.`}
          </Dialog.Description>
          <div className="dlq-export-preview">
            <span>Rows</span>
            <strong>{state?.rows.length ?? 0}</strong>
          </div>
          <div className="dialog-actions">
            <Dialog.Close className="button button-secondary">Cancel</Dialog.Close>
            <button type="button" className="button" onClick={onDownload}>
              <Download size={15} aria-hidden="true" />
              Download CSV
            </button>
          </div>
        </Dialog.Content>
      </Dialog.Portal>
    </Dialog.Root>
  );
}

function validateQuery(mode: QueryMode, consumerGroup: string, messageId: string, begin: string, end: string) {
  if (!consumerGroup.trim()) {
    return 'Consumer group is required.';
  }
  if (mode === 'messageId' && !messageId.trim()) {
    return 'Message ID is required.';
  }
  if (mode === 'consumer') {
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

function preferredConsumerGroup(groups: string[]) {
  return groups.find((item) => item === 'please_rename_unique_group_name_4') ?? groups[0] ?? '';
}

function messageInfoRows(message: MessageView) {
  return [
    { label: 'Topic', value: message.topic, wide: true },
    { label: 'Message ID', value: message.messageId, wide: true },
    { label: 'Origin Message ID', value: originMessageId(message), wide: true },
    { label: 'Retry Topic', value: retryTopic(message), wide: true },
    { label: 'Store Message ID', value: message.properties.STORE_MESSAGE_ID || '-', wide: true },
    { label: 'StoreHost', value: optionalMessageValue(message, 'storeHost') },
    { label: 'BornHost', value: optionalMessageValue(message, 'bornHost') },
    { label: 'StoreTime', value: formatTimestamp(message.storeTimestamp) },
    { label: 'BornTime', value: formatTimestamp(message.bornTimestamp) },
    { label: 'Queue ID', value: String(message.queueId) },
    { label: 'Queue Offset', value: String(message.queueOffset) },
    { label: 'ReconsumeTimes', value: optionalMessageValue(message, 'reconsumeTimes') },
    { label: 'Tag', value: messageTags(message) },
    { label: 'Key', value: messageKeys(message) }
  ];
}

function optionalMessageValue(message: MessageView, key: string, fallback = '-') {
  const value = (message as unknown as Record<string, unknown>)[key];
  return value === undefined || value === null || value === '' ? fallback : String(value);
}

function retryTopic(message: MessageView) {
  return message.properties.RETRY_TOPIC || message.properties.ORIGIN_TOPIC || message.topic;
}

function originMessageId(message: MessageView) {
  return message.properties.ORIGIN_MESSAGE_ID || message.properties.UNIQ_KEY || message.messageId;
}

function messageTags(message: MessageView | null | undefined) {
  return message?.tags || message?.properties?.TAGS || '-';
}

function messageKeys(message: MessageView | null | undefined) {
  return message?.keys || message?.properties?.KEYS || '-';
}

function isResendOk(value: string) {
  const normalized = value.toLowerCase();
  return normalized.includes('success') || normalized.includes('ok') || normalized.includes('consume_success');
}

function buildDlqCsv(rows: MessageView[]) {
  const headers = ['messageId', 'topic', 'retryTopic', 'originMessageId', 'tag', 'key', 'storeTime', 'queueId', 'queueOffset', 'body'];
  const lines = rows.map((row) =>
    [
      row.messageId,
      row.topic,
      retryTopic(row),
      originMessageId(row),
      messageTags(row),
      messageKeys(row),
      formatTimestamp(row.storeTimestamp),
      String(row.queueId),
      String(row.queueOffset),
      row.body
    ]
      .map(csvEscape)
      .join(',')
  );
  return [headers.join(','), ...lines].join('\n');
}

function csvEscape(value: string) {
  return `"${value.replace(/"/g, '""')}"`;
}

function downloadTextFile(fileName: string, content: string) {
  const blob = new Blob([content], { type: 'text/csv;charset=utf-8' });
  const url = URL.createObjectURL(blob);
  const anchor = document.createElement('a');
  anchor.href = url;
  anchor.download = fileName;
  document.body.appendChild(anchor);
  anchor.click();
  anchor.remove();
  URL.revokeObjectURL(url);
}

function sanitizeFileName(value: string) {
  return value.replace(/[^a-zA-Z0-9._-]+/g, '_').slice(0, 96) || 'dlq';
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
