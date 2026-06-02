import { useEffect, useMemo, useState, type ReactNode } from 'react';
import { AnimatePresence, motion } from 'motion/react';
import {
  Activity,
  AlertCircle,
  Clock3,
  Copy,
  Database,
  FileText,
  Flag,
  Globe2,
  HardDrive,
  Hash,
  Info,
  KeyRound,
  Layers,
  RefreshCw,
  Server,
  Tag,
  Users,
  X,
} from 'lucide-react';
import type { LucideIcon } from 'lucide-react';
import { toast } from 'sonner@2.0.3';
import { MessageService } from '../services/message.service';
import type { MessageDetail, MessageSummary, MessageTrack } from '../features/message/types/message.types';

interface MessageDetailModalProps {
  isOpen: boolean;
  onClose: () => void;
  message: MessageSummary | null;
}

const MONO_FAMILY = 'ui-monospace, SFMono-Regular, Consolas, "Liberation Mono", monospace';

export const MessageDetailModal = ({ isOpen, onClose, message }: MessageDetailModalProps) => {
  const [detail, setDetail] = useState<MessageDetail | null>(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState('');
  const [consumingGroup, setConsumingGroup] = useState<string | null>(null);

  useEffect(() => {
    if (!isOpen || !message) {
      setDetail(null);
      setError('');
      setIsLoading(false);
      setConsumingGroup(null);
      return;
    }

    let cancelled = false;
    setIsLoading(true);
    setError('');
    setDetail(null);

    void MessageService.viewMessageDetail({
      topic: message.topic,
      messageId: message.queryMsgId,
    })
      .then((result) => {
        if (!cancelled) {
          setDetail(result);
        }
      })
      .catch((loadError) => {
        if (!cancelled) {
          setError(loadError instanceof Error ? loadError.message : 'Failed to load message detail.');
        }
      })
      .finally(() => {
        if (!cancelled) {
          setIsLoading(false);
        }
      });

    return () => {
      cancelled = true;
    };
  }, [isOpen, message]);

  const copyToClipboard = (text: string, label = 'Value') => {
    void navigator.clipboard.writeText(text);
    toast.success(`${label} copied`);
  };

  const handleDirectConsume = async (consumerGroup: string) => {
    if (!detail) {
      return;
    }

    const confirmed = window.confirm(
      `Request direct consume for message ${detail.msgId} in consumer group ${consumerGroup}?`,
    );
    if (!confirmed) {
      return;
    }

    setConsumingGroup(consumerGroup);

    try {
      const result = await MessageService.consumeMessageDirectly({
        topic: detail.topic,
        consumerGroup,
        messageId: detail.msgId,
      });

      if (result.success) {
        toast.success(result.message);
      } else {
        toast.error(result.message);
      }
    } catch (consumeError) {
      const messageText =
        consumeError instanceof Error ? consumeError.message : 'Failed to request direct consume.';
      toast.error(messageText);
    } finally {
      setConsumingGroup(null);
    }
  };

  const systemPropertyEntries = useMemo(() => {
    if (!detail) {
      return [];
    }
    return Object.entries(detail.properties ?? {}).filter(([key]) => key === key.toUpperCase());
  }, [detail]);

  const userPropertyEntries = useMemo(() => {
    if (!detail) {
      return [];
    }
    return Object.entries(detail.properties ?? {}).filter(([key]) => key !== key.toUpperCase());
  }, [detail]);

  const messageTitle = detail?.topic ?? message?.topic ?? 'Message';
  const messageId = detail?.msgId ?? message?.queryMsgId ?? message?.msgId ?? '-';
  const trackCount = detail?.messageTrackList?.length ?? 0;

  return (
    <AnimatePresence>
      {isOpen ? (
        <div className="topic-status-modal-root message-detail-root">
          <motion.div
            className="topic-status-backdrop"
            initial={{ opacity: 0 }}
            animate={{ opacity: 1 }}
            exit={{ opacity: 0 }}
            onClick={onClose}
          />

          <motion.div
            className="topic-status-dialog message-detail-dialog"
            initial={{ opacity: 0, y: 20, scale: 0.98 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 20, scale: 0.98 }}
            transition={{ duration: 0.2 }}
            role="dialog"
            aria-modal="true"
            aria-labelledby="message-detail-title"
          >
            <header className="topic-status-header message-detail-header">
              <div className="topic-status-title-wrap">
                <span className="topic-status-icon">
                  <FileText className="topic-icon" aria-hidden="true" />
                </span>
                <div>
                  <span>Message inspection</span>
                  <h3 id="message-detail-title">Message Detail</h3>
                  <p>
                    <strong>{messageTitle}</strong>
                    <i aria-hidden="true"> / </i>
                    <code>{messageId}</code>
                  </p>
                </div>
              </div>

              <div className="topic-status-header-actions message-detail-header-actions">
                <span className="message-detail-chip is-blue">
                  <Layers className="topic-icon" aria-hidden="true" />
                  Queue {detail?.queueId ?? '-'}
                </span>
                <span className="message-detail-chip is-green">
                  <Activity className="topic-icon" aria-hidden="true" />
                  {trackCount} tracks
                </span>
                <button type="button" onClick={onClose} className="topic-status-close" aria-label="Close">
                  <X className="topic-icon" aria-hidden="true" />
                </button>
              </div>
            </header>

            <div className="topic-status-body message-detail-body">
              {isLoading ? (
                <MessageDetailState
                  icon={RefreshCw}
                  title="Loading message detail"
                  copy="Fetching broker metadata, payload, properties, and tracking records."
                  active
                />
              ) : error ? (
                <MessageDetailState icon={AlertCircle} title="Message detail failed" copy={error} tone="danger" />
              ) : detail ? (
                <>
                  <section className="topic-status-kpi-grid message-detail-summary-grid" aria-label="Message detail summary">
                    <DetailMetric
                      label="Topic"
                      value={detail.topic}
                      note="source topic"
                      icon={Database}
                      tone="blue"
                    />
                    <DetailMetric
                      label="Queue"
                      value={String(detail.queueId ?? '-')}
                      note={`offset ${detail.queueOffset ?? '-'}`}
                      icon={Layers}
                      tone="cyan"
                    />
                    <DetailMetric
                      label="Store Size"
                      value={formatBytes(detail.storeSize)}
                      note="broker payload"
                      icon={HardDrive}
                      tone="green"
                    />
                    <DetailMetric
                      label="Reconsume"
                      value={String(detail.reconsumeTimes ?? 0)}
                      note={`${trackCount} tracking records`}
                      icon={RefreshCw}
                      tone="violet"
                    />
                  </section>

                  <section className="message-detail-workspace">
                    <div className="message-detail-main">
                      <DetailPanel
                        title="Message Info"
                        eyebrow="broker runtime snapshot"
                        icon={Info}
                        badge="readonly"
                      >
                        <div className="message-detail-info-grid">
                          <DetailInfoCell icon={Hash} label="Message ID" value={detail.msgId} copyable />
                          <DetailInfoCell icon={Database} label="Topic" value={detail.topic} />
                          <DetailInfoCell icon={Globe2} label="Born Host" value={detail.bornHost || '-'} />
                          <DetailInfoCell icon={Server} label="Store Host" value={detail.storeHost || '-'} />
                          <DetailInfoCell icon={Clock3} label="Born Time" value={formatTimestamp(detail.bornTimestamp)} />
                          <DetailInfoCell icon={Clock3} label="Store Time" value={formatTimestamp(detail.storeTimestamp)} />
                          <DetailInfoCell icon={Layers} label="Queue ID" value={String(detail.queueId ?? '-')} />
                          <DetailInfoCell icon={Layers} label="Queue Offset" value={String(detail.queueOffset ?? '-')} />
                          <DetailInfoCell icon={KeyRound} label="Body CRC" value={String(detail.bodyCrc ?? '-')} />
                          <DetailInfoCell icon={Flag} label="Sys Flag" value={String(detail.sysFlag ?? '-')} />
                          <DetailInfoCell icon={Tag} label="Flag" value={String(detail.flag ?? '-')} />
                          <DetailInfoCell
                            icon={RefreshCw}
                            label="Trans Offset"
                            value={String(detail.preparedTransactionOffset ?? '-')}
                          />
                        </div>
                      </DetailPanel>

                      <div className="message-detail-property-grid">
                        <PropertyPanel
                          title="System Properties"
                          icon={Activity}
                          entries={systemPropertyEntries}
                          empty="No system properties"
                          tone="blue"
                        />
                        <PropertyPanel
                          title="User Properties"
                          icon={Users}
                          entries={userPropertyEntries}
                          empty="No user properties"
                          tone="violet"
                        />
                      </div>

                      <DetailPanel title="Message Body" eyebrow="decoded payload" icon={FileText}>
                        <MessageBody detail={detail} />
                      </DetailPanel>
                    </div>

                    <aside className="message-detail-rail" aria-label="Message inspector">
                      <DetailPanel title="Identity" eyebrow="copyable message coordinates" icon={Hash}>
                        <div className="message-detail-identity-list">
                          <IdentityRow label="Message ID" value={detail.msgId} onCopy={copyToClipboard} />
                          <IdentityRow label="Query ID" value={message?.queryMsgId ?? detail.msgId} onCopy={copyToClipboard} />
                          <IdentityRow label="Topic" value={detail.topic} onCopy={copyToClipboard} />
                        </div>
                      </DetailPanel>

                      <DetailPanel title="Message Tracking" eyebrow="consumer delivery state" icon={Activity} badge={String(trackCount)}>
                        <TrackingList
                          tracks={detail.messageTrackList ?? []}
                          consumingGroup={consumingGroup}
                          onDirectConsume={(consumerGroup) => void handleDirectConsume(consumerGroup)}
                        />
                      </DetailPanel>
                    </aside>
                  </section>
                </>
              ) : (
                <MessageDetailState icon={Info} title="No message selected" copy="Choose a result card to inspect the detail payload." />
              )}
            </div>

            <footer className="topic-status-footer message-detail-footer">
              <span>Message data is read-only and reflects the current broker response.</span>
              <button type="button" onClick={onClose} className="topic-status-secondary-button">
                Close
              </button>
            </footer>
          </motion.div>
        </div>
      ) : null}
    </AnimatePresence>
  );
};

const formatTimestamp = (value?: number | null) => {
  if (!value) {
    return '-';
  }
  return new Date(value).toLocaleString();
};

const formatBytes = (value?: number | null) => {
  if (value === null || value === undefined) {
    return '-';
  }
  return `${value.toLocaleString()} bytes`;
};

const DetailMetric = ({
  label,
  value,
  note,
  icon: Icon,
  tone,
}: {
  label: string;
  value: string;
  note: string;
  icon: LucideIcon;
  tone: 'blue' | 'cyan' | 'green' | 'violet';
}) => (
  <article className={`topic-status-kpi message-detail-kpi is-${tone}`}>
    <div>
      <span>{label}</span>
      <strong title={value}>{value}</strong>
      <small>{note}</small>
    </div>
    <Icon className="topic-icon" aria-hidden="true" />
  </article>
);

const DetailPanel = ({
  title,
  eyebrow,
  icon: Icon,
  badge,
  children,
}: {
  title: string;
  eyebrow: string;
  icon: LucideIcon;
  badge?: string;
  children: ReactNode;
}) => (
  <section className="message-detail-panel">
    <div className="message-detail-panel-head">
      <span className="message-detail-panel-icon">
        <Icon className="topic-icon" aria-hidden="true" />
      </span>
      <div>
        <h4>{title}</h4>
        <p>{eyebrow}</p>
      </div>
      {badge ? <strong>{badge}</strong> : null}
    </div>
    <div className="message-detail-panel-body">{children}</div>
  </section>
);

const DetailInfoCell = ({
  icon: Icon,
  label,
  value,
  copyable = false,
}: {
  icon: LucideIcon;
  label: string;
  value: string;
  copyable?: boolean;
}) => (
  <div className="message-detail-info-cell">
    <Icon className="topic-icon" aria-hidden="true" />
    <div>
      <span>{label}</span>
      <strong title={value}>{value}</strong>
    </div>
    {copyable ? (
      <button
        type="button"
        onClick={() => {
          void navigator.clipboard.writeText(value);
          toast.success(`${label} copied`);
        }}
        aria-label={`Copy ${label}`}
      >
        <Copy className="topic-icon" aria-hidden="true" />
      </button>
    ) : null}
  </div>
);

const PropertyPanel = ({
  title,
  icon: Icon,
  entries,
  empty,
  tone,
}: {
  title: string;
  icon: LucideIcon;
  entries: Array<[string, string]>;
  empty: string;
  tone: 'blue' | 'violet';
}) => (
  <section className={`message-detail-property-panel is-${tone}`}>
    <div className="message-detail-property-head">
      <span>
        <Icon className="topic-icon" aria-hidden="true" />
        {title}
      </span>
      <strong>{entries.length}</strong>
    </div>
    {entries.length > 0 ? (
      <div className="message-detail-property-list">
        {entries.map(([key, value]) => (
          <div key={key} className="message-detail-property-row">
            <span title={key}>{key}</span>
            <strong title={value}>{value}</strong>
          </div>
        ))}
      </div>
    ) : (
      <div className="message-detail-empty">{empty}</div>
    )}
  </section>
);

const MessageBody = ({ detail }: { detail: MessageDetail }) => {
  const hasTextBody = detail.bodyText !== null && detail.bodyText !== undefined;
  const body = hasTextBody ? detail.bodyText : detail.bodyBase64 || '-';

  return (
    <div className="message-detail-body-block">
      <div className="message-detail-body-toolbar">
        <span>{hasTextBody ? 'UTF-8 body' : 'Base64 body'}</span>
        <button
          type="button"
          onClick={() => {
            void navigator.clipboard.writeText(body || '');
            toast.success('Message body copied');
          }}
        >
          <Copy className="topic-icon" aria-hidden="true" />
          Copy Body
        </button>
      </div>
      {!hasTextBody ? (
        <div className="message-detail-notice">
          <Info className="topic-icon" aria-hidden="true" />
          <span>Message body is not valid UTF-8. Showing the base64 payload returned by the broker.</span>
        </div>
      ) : null}
      <pre style={{ fontFamily: MONO_FAMILY }}>{body}</pre>
    </div>
  );
};

const IdentityRow = ({
  label,
  value,
  onCopy,
}: {
  label: string;
  value: string;
  onCopy: (value: string, label?: string) => void;
}) => (
  <div className="message-detail-identity-row">
    <span>{label}</span>
    <strong title={value}>{value}</strong>
    <button type="button" onClick={() => onCopy(value, label)} aria-label={`Copy ${label}`}>
      <Copy className="topic-icon" aria-hidden="true" />
    </button>
  </div>
);

const TrackingList = ({
  tracks,
  consumingGroup,
  onDirectConsume,
}: {
  tracks: MessageTrack[];
  consumingGroup: string | null;
  onDirectConsume: (consumerGroup: string) => void;
}) => {
  if (tracks.length === 0) {
    return (
      <div className="message-detail-empty is-tall">
        <Info className="topic-icon" aria-hidden="true" />
        <span>No consumer track records matched this message.</span>
      </div>
    );
  }

  return (
    <div className="message-detail-track-list">
      {tracks.map((track) => (
        <article key={`${track.consumerGroup}-${track.trackType}`} className="message-detail-track-card">
          <div>
            <strong title={track.consumerGroup}>{track.consumerGroup}</strong>
            <span>{track.trackType}</span>
          </div>
          <button
            type="button"
            onClick={() => onDirectConsume(track.consumerGroup)}
            disabled={consumingGroup === track.consumerGroup}
          >
            <RefreshCw className={`topic-icon ${consumingGroup === track.consumerGroup ? 'is-spinning' : ''}`} aria-hidden="true" />
            {consumingGroup === track.consumerGroup ? 'Requesting' : 'Resend'}
          </button>
          {track.exceptionDesc ? <p>{track.exceptionDesc}</p> : null}
        </article>
      ))}
    </div>
  );
};

const MessageDetailState = ({
  icon: Icon,
  title,
  copy,
  active = false,
  tone = 'neutral',
}: {
  icon: LucideIcon;
  title: string;
  copy: string;
  active?: boolean;
  tone?: 'neutral' | 'danger';
}) => (
  <div className={`topic-status-state message-detail-state ${active ? 'is-active' : ''} ${tone === 'danger' ? 'is-error' : ''}`}>
    <Icon className={`topic-icon ${active ? 'is-spinning' : ''}`} aria-hidden="true" />
    <strong>{title}</strong>
    <span>{copy}</span>
  </div>
);
