import { Braces, Clock3, Copy, Database, RotateCcw } from 'lucide-react';
import { useEffect, useRef, useState } from 'react';
import { messageApi } from '../../api/message_api';
import MetricCard from '../../components/MetricCard';
import { Button } from '../../components/ui/Button';
import { Tabs, TabsContent, TabsList, TabsTrigger } from '../../components/ui/Tabs';
import type { MessageTraceView, MessageView } from '../../types/message';
import MessageBody from './MessageBody';
import { messageRowId, messageTraceId } from './dlq-selection';
import TraceTimeline from './TraceTimeline';
import {
  formatMessageSize,
  formatMessageTimestamp,
  messageKeys,
  messageTags,
  sortedMessageProperties,
  truncateIdentifier
} from './message-model';

interface MessageDetailContentProps {
  message: MessageView;
  traceTopic?: string;
  initialTab?: 'overview' | 'properties' | 'body' | 'trace';
}

export default function MessageDetailContent({
  message,
  traceTopic = 'RMQ_SYS_TRACE_TOPIC',
  initialTab = 'overview'
}: MessageDetailContentProps) {
  const [activeTab, setActiveTab] = useState(initialTab);
  const [copyNotice, setCopyNotice] = useState<{ message: string; tone: 'success' | 'danger' } | null>(null);
  const [trace, setTrace] = useState<MessageTraceView | null>(null);
  const [traceLoading, setTraceLoading] = useState(false);
  const [traceError, setTraceError] = useState<string | null>(null);
  const [traceLoaded, setTraceLoaded] = useState(false);
  const traceRequestRef = useRef(0);

  useEffect(() => {
    setActiveTab(initialTab);
    setCopyNotice(null);
    setTrace(null);
    setTraceError(null);
    setTraceLoaded(false);
    setTraceLoading(false);
    traceRequestRef.current += 1;
  }, [initialTab, messageRowId(message), traceTopic]);

  const loadTrace = async () => {
    if (traceLoaded || traceLoading) return;
    const requestId = ++traceRequestRef.current;
    setTraceLoaded(true);
    setTraceLoading(true);
    setTraceError(null);
    try {
      const nextTrace = await messageApi.trace(messageTraceId(message), message.topic, traceTopic);
      if (traceRequestRef.current === requestId) setTrace(nextTrace);
    } catch (requestError) {
      if (traceRequestRef.current === requestId) {
        setTraceLoaded(false);
        setTraceError(requestError instanceof Error ? requestError.message : String(requestError));
      }
    } finally {
      if (traceRequestRef.current === requestId) setTraceLoading(false);
    }
  };

  const changeTab = (value: string) => {
    const nextTab = value as typeof activeTab;
    setActiveTab(nextTab);
    if (nextTab === 'trace') void loadTrace();
  };

  const copyMessageId = async () => {
    try {
      await navigator.clipboard.writeText(message.messageId);
      setCopyNotice({ message: 'Message ID copied.', tone: 'success' });
    } catch {
      setCopyNotice({ message: 'Unable to copy the message ID.', tone: 'danger' });
    }
  };

  return (
    <div className="entity-detail-content message-detail-content">
      {copyNotice ? (
        <div className={`notice notice-${copyNotice.tone}`} role={copyNotice.tone === 'danger' ? 'alert' : 'status'}>
          {copyNotice.message}
        </div>
      ) : null}
      <div className="message-detail-identity">
        <div>
          <span>Message ID</span>
          <strong className="mono" title={message.messageId}>{truncateIdentifier(message.messageId, 38)}</strong>
        </div>
        <Button type="button" variant="outline" size="sm" onClick={() => void copyMessageId()}>
          <Copy size={14} aria-hidden="true" /> Copy message ID
        </Button>
      </div>

      <Tabs value={activeTab} onValueChange={changeTab}>
        <TabsList aria-label="Message detail sections">
          <TabsTrigger value="overview">Overview</TabsTrigger>
          <TabsTrigger value="properties">Properties</TabsTrigger>
          <TabsTrigger value="body">Body</TabsTrigger>
          <TabsTrigger value="trace">Trace</TabsTrigger>
        </TabsList>

        <TabsContent value="overview">
          <div className="metric-grid entity-detail-metrics">
            <MetricCard label="Body size" value={formatMessageSize(message.storeSize)} detail="Stored message bytes" icon={<Database size={18} />} />
            <MetricCard label="Reconsume" value={message.reconsumeTimes} detail="API retry count" icon={<RotateCcw size={18} />} />
            <MetricCard label="Queue" value={message.queueId} detail={`Offset ${message.queueOffset}`} icon={<Braces size={18} />} />
            <MetricCard label="Stored" value={formatMessageTimestamp(message.storeTimestamp)} detail="Broker store timestamp" icon={<Clock3 size={18} />} />
          </div>
          <dl className="entity-description-grid">
            <div><dt>Topic</dt><dd className="mono">{message.topic}</dd></div>
            <div><dt>Tags</dt><dd>{messageTags(message)}</dd></div>
            <div><dt>Keys</dt><dd className="mono">{messageKeys(message)}</dd></div>
            <div><dt>Queue / offset</dt><dd>{message.queueId} / {message.queueOffset}</dd></div>
            <div><dt>Born host</dt><dd className="mono">{message.bornHost || '-'}</dd></div>
            <div><dt>Store host</dt><dd className="mono">{message.storeHost || '-'}</dd></div>
            <div><dt>Born time</dt><dd>{formatMessageTimestamp(message.bornTimestamp)}</dd></div>
            <div><dt>Store time</dt><dd>{formatMessageTimestamp(message.storeTimestamp)}</dd></div>
          </dl>
        </TabsContent>

        <TabsContent value="properties">
          {sortedMessageProperties(message).length > 0 ? (
            <div className="message-properties-table" role="region" aria-label="Message properties">
              <table>
                <thead><tr><th>Key</th><th>Value</th></tr></thead>
                <tbody>
                  {sortedMessageProperties(message).map(([key, value]) => (
                    <tr key={key}><td><code>{key}</code></td><td className="mono">{value}</td></tr>
                  ))}
                </tbody>
              </table>
            </div>
          ) : <div className="state-block"><strong>No message properties</strong></div>}
        </TabsContent>

        <TabsContent value="body">
          <MessageBody body={message.body} />
        </TabsContent>

        <TabsContent value="trace">
          <TraceTimeline
            nodes={trace?.nodes ?? []}
            loading={traceLoading}
            error={traceError}
            onRetry={() => void loadTrace()}
          />
        </TabsContent>
      </Tabs>
    </div>
  );
}
