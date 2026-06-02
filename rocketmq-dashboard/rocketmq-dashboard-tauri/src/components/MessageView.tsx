import { useEffect, useMemo, useState } from 'react';
import {
  AlertCircle,
  Calendar,
  ChevronDown,
  Clock3,
  Copy,
  FileText,
  Hash,
  Info,
  KeyRound,
  Layers,
  Search,
  Tag,
} from 'lucide-react';
import { motion } from 'motion/react';
import { toast } from 'sonner@2.0.3';
import { MessageDetailModal } from './MessageDetailModal';
import { Pagination } from './Pagination';
import { useTopicCatalog } from '../features/topic/hooks/useTopicCatalog';
import type { MessageSummary } from '../features/message/types/message.types';
import { MessageService } from '../services/message.service';

type MessageTab = 'Topic' | 'Message Key' | 'Message ID';

const DEFAULT_PAGE_SIZE = 12;

const defaultPagination = {
  currentPage: 1,
  pageSize: DEFAULT_PAGE_SIZE,
  totalPages: 0,
  totalElements: 0,
};

const pad = (value: number) => value.toString().padStart(2, '0');

const formatDateTimeInput = (date: Date) =>
  `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())} ${pad(date.getHours())}:${pad(
    date.getMinutes(),
  )}:${pad(date.getSeconds())}`;

const formatMessageTimestamp = (value: number) => {
  if (!value) {
    return '-';
  }
  return new Date(value).toLocaleString();
};

export const MessageView = () => {
  const [activeTab, setActiveTab] = useState<MessageTab>('Topic');
  const [topic, setTopic] = useState('');
  const [msgKey, setMsgKey] = useState('');
  const [msgId, setMsgId] = useState('');
  const [startDate, setStartDate] = useState(() => formatDateTimeInput(new Date(Date.now() - 60 * 60 * 1000)));
  const [endDate, setEndDate] = useState(() => formatDateTimeInput(new Date()));
  const [messages, setMessages] = useState<MessageSummary[]>([]);
  const [selectedMessage, setSelectedMessage] = useState<MessageSummary | null>(null);
  const [isSearching, setIsSearching] = useState(false);
  const [searchError, setSearchError] = useState('');
  const [hasSearched, setHasSearched] = useState(false);
  const [topicTaskId, setTopicTaskId] = useState('');
  const [topicPagination, setTopicPagination] = useState(defaultPagination);
  const { data: topicCatalog, isLoading: isTopicCatalogLoading, error: topicCatalogError } = useTopicCatalog();

  const availableTopics = useMemo(
    () => topicCatalog?.items.map((item) => item.topic) ?? [],
    [topicCatalog],
  );

  useEffect(() => {
    if (!topic && availableTopics.length > 0) {
      setTopic(availableTopics[0]);
    }
  }, [availableTopics, topic]);

  useEffect(() => {
    setMessages([]);
    setSelectedMessage(null);
    setSearchError('');
    setHasSearched(false);
    setTopicTaskId('');
    setTopicPagination(defaultPagination);
  }, [activeTab]);

  const resetTopicPagingState = () => {
    setMessages([]);
    setSelectedMessage(null);
    setSearchError('');
    setHasSearched(false);
    setTopicTaskId('');
    setTopicPagination(defaultPagination);
  };

  const parseDateTimeInput = (value: string): number | null => {
    const normalized = value.trim().replace(' ', 'T');
    if (!normalized) {
      return null;
    }

    const parsed = new Date(normalized);
    const timestamp = parsed.getTime();
    return Number.isNaN(timestamp) ? null : timestamp;
  };

  const queryTopicPage = async (pageNum = 1) => {
    if (!topic.trim()) {
      setSearchError('Topic is required.');
      return;
    }

    const begin = parseDateTimeInput(startDate);
    const end = parseDateTimeInput(endDate);
    if (begin === null || end === null) {
      setSearchError('Begin and end must be valid date-time strings.');
      return;
    }
    if (end < begin) {
      setSearchError('End time must be greater than or equal to begin time.');
      return;
    }

    setIsSearching(true);
    setSearchError('');
    setHasSearched(true);

    try {
      const response = await MessageService.queryMessagePageByTopic({
        topic: topic.trim(),
        begin,
        end,
        pageNum,
        pageSize: topicPagination.pageSize,
        taskId: topicTaskId || undefined,
      });

      setMessages(response.page.content);
      setTopicTaskId(response.taskId);
      setTopicPagination({
        currentPage: response.page.number + 1,
        pageSize: response.page.size,
        totalPages: response.page.totalPages,
        totalElements: response.page.totalElements,
      });
    } catch (error) {
      setMessages([]);
      setSearchError(error instanceof Error ? error.message : 'Failed to query messages by topic.');
    } finally {
      setIsSearching(false);
    }
  };

  const handleSearch = async () => {
    if (activeTab === 'Topic') {
      await queryTopicPage(1);
      return;
    }

    if (!topic.trim()) {
      setSearchError('Topic is required.');
      return;
    }

    if (activeTab === 'Message Key' && !msgKey.trim()) {
      setSearchError('Message Key is required.');
      return;
    }

    if (activeTab === 'Message ID' && !msgId.trim()) {
      setSearchError('Message ID is required.');
      return;
    }

    if (activeTab === 'Message ID') {
      setSearchError('');
      setHasSearched(false);
      setMessages([]);
      setSelectedMessage({
        topic: topic.trim(),
        msgId: msgId.trim(),
        queryMsgId: msgId.trim(),
        tags: null,
        keys: null,
        storeTimestamp: 0,
      });
      return;
    }

    setIsSearching(true);
    setSearchError('');
    setHasSearched(true);

    try {
      const response = activeTab === 'Message Key'
        ? await MessageService.queryMessageByTopicKey({
            topic: topic.trim(),
            key: msgKey.trim(),
          })
        : await MessageService.queryMessageById({
            topic: topic.trim(),
            messageId: msgId.trim(),
          });

      setMessages(response.items);
    } catch (error) {
      setMessages([]);
      setSearchError(error instanceof Error ? error.message : 'Failed to query messages.');
    } finally {
      setIsSearching(false);
    }
  };

  const renderTopicSelector = () => (
    <label className="message-query-field is-topic">
      <span>Topic</span>
      <div className="message-select-wrap">
        <select
          value={topic}
          onChange={(event) => {
            setTopic(event.target.value);
            if (activeTab === 'Topic') {
              resetTopicPagingState();
            }
          }}
        >
          {availableTopics.length === 0 ? (
            <option value="">
              {isTopicCatalogLoading ? 'Loading topics...' : 'No topics'}
            </option>
          ) : (
            availableTopics.map((item) => (
              <option key={item} value={item}>
                {item}
              </option>
            ))
          )}
        </select>
        <ChevronDown className="message-query-icon" aria-hidden="true" />
      </div>
    </label>
  );

  const renderSearchArea = () => {
    switch (activeTab) {
      case 'Topic':
        return (
          <>
            {renderTopicSelector()}
            <div className="message-query-divider" />
            <DateTimeField
              label="Begin"
              value={startDate}
              onChange={(value) => {
                setStartDate(value);
                resetTopicPagingState();
              }}
            />
            <DateTimeField
              label="End"
              value={endDate}
              onChange={(value) => {
                setEndDate(value);
                resetTopicPagingState();
              }}
            />
          </>
        );
      case 'Message Key':
        return (
          <>
            {renderTopicSelector()}
            <div className="message-query-divider" />
            <TextQueryField
              label="Key"
              value={msgKey}
              placeholder="Enter Message Key..."
              onChange={setMsgKey}
            />
            <span className="message-query-limit">Only returns up to 64 messages</span>
          </>
        );
      case 'Message ID':
        return (
          <>
            {renderTopicSelector()}
            <div className="message-query-divider" />
            <TextQueryField
              label="Message ID"
              mono
              value={msgId}
              placeholder="Enter Message ID..."
              onChange={setMsgId}
            />
          </>
        );
      default:
        return null;
    }
  };

  const renderEmptyCopy = () => {
    if (activeTab === 'Topic') {
      return 'Select a topic and time window to load paged results.';
    }
    if (activeTab === 'Message Key') {
      return 'Enter a topic and message key to start searching.';
    }
    return 'Enter a topic and message id to open the real detail dialog directly.';
  };

  const statusText = activeTab === 'Topic'
    ? hasSearched
      ? `${messages.length} item(s) on page ${topicPagination.currentPage} / ${Math.max(topicPagination.totalPages, 1)}`
      : 'ready'
    : activeTab === 'Message ID'
      ? selectedMessage
        ? 'detail open'
        : 'ready'
      : hasSearched
        ? `${messages.length} result(s)`
        : 'ready';

  const helperText = activeTab === 'Topic'
    ? 'Topic query follows dynamic time defaults plus real topic/time pagination with backend taskId continuity.'
    : activeTab === 'Message Key'
      ? 'Message Key uses the real query path and returns up to 64 messages, matching the Java dashboard behavior.'
      : 'Message ID opens the real detail dialog directly, while list results stay in the compact card layout.';

  const pageWindowLabel = hasSearched && activeTab === 'Topic'
    ? `${topicPagination.currentPage}/${Math.max(topicPagination.totalPages, 1)}`
    : 'Ready';

  return (
    <div className="message-page">
      <MessageDetailModal
        isOpen={!!selectedMessage}
        onClose={() => setSelectedMessage(null)}
        message={selectedMessage}
      />

      <section className="message-mode-strip" aria-label="Message query mode">
        {(['Topic', 'Message Key', 'Message ID'] as MessageTab[]).map((tab) => (
          <button
            key={tab}
            type="button"
            onClick={() => setActiveTab(tab)}
            className={`message-mode-button ${activeTab === tab ? 'is-active' : ''}`}
          >
            {tab}
          </button>
        ))}
      </section>

      <section className="message-query-surface" aria-label="Message search controls">
        <div className="message-query-fields">
          {renderSearchArea()}
        </div>

        <button
          type="button"
          onClick={() => void handleSearch()}
          disabled={isSearching || isTopicCatalogLoading}
          className="message-search-button"
        >
          <Search className="topic-icon" aria-hidden="true" />
          {isSearching ? 'Searching' : 'Search'}
        </button>
      </section>

      {topicCatalogError ? (
        <MessageAlert tone="warning" message={`Failed to load topic catalog: ${topicCatalogError}`} />
      ) : null}

      {searchError ? <MessageAlert tone="danger" message={searchError} /> : null}

      <section className="message-info-strip" aria-label="Message query status">
        <div>
          <Info className="topic-icon" aria-hidden="true" />
          <span>{helperText}</span>
        </div>
        <strong>{statusText}</strong>
      </section>

      <section className="message-summary-grid" aria-label="Message query summary">
        <MessageKpiCard label="Mode" value={activeTab} note="active query path" tone="blue" icon={Layers} />
        <MessageKpiCard label="Results" value={String(messages.length)} note="current result set" tone="cyan" icon={FileText} />
        <MessageKpiCard label="Topic" value={topic || '-'} note="selected source" tone="green" icon={Tag} />
        <MessageKpiCard label="Page" value={pageWindowLabel} note="task continuity" tone="violet" icon={Hash} />
      </section>

      {isSearching ? (
        <MessageState icon={Search} title="Querying messages" copy="The broker query is running for the selected mode and scope." active />
      ) : messages.length > 0 ? (
        <>
          <section className="message-result-grid" aria-label="Message search results">
            {messages.map((message, index) => (
              <MessageResultCard
                key={`${message.topic}-${message.msgId}`}
                message={message}
                index={index}
                formatTimestamp={formatMessageTimestamp}
                onOpen={() => setSelectedMessage(message)}
              />
            ))}
          </section>

          {activeTab === 'Topic' && topicPagination.totalPages > 1 ? (
            <div className="message-pagination-wrap">
              <Pagination
                currentPage={topicPagination.currentPage}
                totalPages={topicPagination.totalPages}
                onPageChange={(page) => void queryTopicPage(page)}
              />
            </div>
          ) : null}
        </>
      ) : hasSearched ? (
        <MessageState icon={Search} title="No matching messages" copy="Check the current topic, time range, key, or message id." />
      ) : (
        <MessageState icon={Search} title="Search is ready" copy={renderEmptyCopy()} />
      )}
    </div>
  );
};

const DateTimeField = ({
  label,
  value,
  onChange,
}: {
  label: string;
  value: string;
  onChange: (value: string) => void;
}) => (
  <label className="message-query-field is-time">
    <span>{label}</span>
    <div className="message-input-wrap">
      <input value={value} onChange={(event) => onChange(event.target.value)} />
      <Calendar className="message-query-icon" aria-hidden="true" />
    </div>
  </label>
);

const TextQueryField = ({
  label,
  value,
  placeholder,
  mono = false,
  onChange,
}: {
  label: string;
  value: string;
  placeholder: string;
  mono?: boolean;
  onChange: (value: string) => void;
}) => (
  <label className="message-query-field is-text">
    <span>{label}</span>
    <input
      value={value}
      onChange={(event) => onChange(event.target.value)}
      placeholder={placeholder}
      className={mono ? 'is-mono' : ''}
    />
  </label>
);

const MessageAlert = ({ tone, message }: { tone: 'warning' | 'danger'; message: string }) => (
  <div className={`message-alert is-${tone}`}>
    <AlertCircle className="topic-icon" aria-hidden="true" />
    <span>{message}</span>
  </div>
);

const MessageKpiCard = ({
  label,
  value,
  note,
  tone,
  icon: Icon,
}: {
  label: string;
  value: string;
  note: string;
  tone: 'blue' | 'cyan' | 'green' | 'violet';
  icon: typeof Layers;
}) => (
  <article className={`message-kpi-card is-${tone}`}>
    <div>
      <span>{label}</span>
      <strong title={value}>{value}</strong>
      <small>{note}</small>
    </div>
    <Icon className="topic-icon" aria-hidden="true" />
  </article>
);

const MessageResultCard = ({
  message,
  index,
  formatTimestamp,
  onOpen,
}: {
  message: MessageSummary;
  index: number;
  formatTimestamp: (timestamp: number) => string;
  onOpen: () => void;
}) => (
  <motion.article
    initial={{ opacity: 0, y: 14 }}
    animate={{ opacity: 1, y: 0 }}
    transition={{ duration: 0.22, delay: Math.min(index * 0.025, 0.22) }}
    className="message-result-card"
  >
    <div className="message-result-head">
      <span className="message-result-icon">
        <FileText className="topic-icon" aria-hidden="true" />
      </span>
      <div>
        <span>Message ID</span>
        <strong title={message.msgId}>{message.msgId}</strong>
      </div>
      <button
        type="button"
        onClick={(event) => {
          event.stopPropagation();
          navigator.clipboard.writeText(message.msgId);
          toast.success('Copied message ID');
        }}
        className="message-copy-button"
        aria-label="Copy message id"
      >
        <Copy className="topic-icon" aria-hidden="true" />
      </button>
    </div>

    <div className="message-result-body">
      <MessageMeta icon={Tag} label="Tag" value={message.tags || '-'} tone="blue" />
      <MessageMeta icon={KeyRound} label="Key" value={message.keys || '-'} tone="amber" mono />
      <MessageMeta icon={Clock3} label="Store Time" value={formatTimestamp(message.storeTimestamp)} tone="green" mono />
    </div>

    <div className="message-result-footer">
      <button type="button" onClick={onOpen} className="message-detail-button">
        <FileText className="topic-icon" aria-hidden="true" />
        View Details
      </button>
    </div>
  </motion.article>
);

const MessageMeta = ({
  icon: Icon,
  label,
  value,
  tone,
  mono = false,
}: {
  icon: typeof Tag;
  label: string;
  value: string;
  tone: 'blue' | 'amber' | 'green';
  mono?: boolean;
}) => (
  <div className={`message-meta is-${tone}`}>
    <span className="message-meta-icon">
      <Icon className="topic-icon" aria-hidden="true" />
    </span>
    <div>
      <span>{label}</span>
      <strong className={mono ? 'is-mono' : ''} title={value}>{value}</strong>
    </div>
  </div>
);

const MessageState = ({
  icon: Icon,
  title,
  copy,
  active = false,
}: {
  icon: typeof Search;
  title: string;
  copy: string;
  active?: boolean;
}) => (
  <div className={`message-state ${active ? 'is-active' : ''}`}>
    <Icon className="topic-icon" aria-hidden="true" />
    <strong>{title}</strong>
    <span>{copy}</span>
  </div>
);
